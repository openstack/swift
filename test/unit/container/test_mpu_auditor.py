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
import itertools
import os
import shutil
import time
import unittest
import random
from tempfile import mkdtemp
from unittest import mock

from swift.common.internal_client import InternalClient
from swift.common.middleware.mpu import MPU_DELETED_MARKER_SUFFIX, \
    MPU_MARKER_CONTENT_TYPE, MPU_SESSION_CREATED_CONTENT_TYPE, \
    MPU_SESSION_COMPLETED_CONTENT_TYPE, MPU_SESSION_ABORTED_CONTENT_TYPE, \
    MPU_SESSION_COMPLETING_CONTENT_TYPE, normalize_part_number, MPUItem
from swift.common.middleware.s3api.utils import unique_id
from swift.common.object_ref import HistoryId, ObjectRef, UploadId
from swift.common.request_helpers import get_reserved_name
from swift.common.swob import Request, HTTPOk, HTTPNoContent, \
    HTTPNotFound, HTTPAccepted, HTTPServerError, wsgi_quote, \
    HTTPInternalServerError
from swift.common.utils import md5, Timestamp, decode_timestamps, \
    encode_timestamps, MD5_OF_EMPTY_STRING, param_str_from_dict, quote
from swift.container.backend import ContainerBroker
from swift.container.mpu_auditor import MpuAuditor, yield_item_batches, \
    BaseMpuAuditor, MpuAuditorConfig
from swift.container.server import ContainerController
from test.debug_logger import debug_logger
from test.unit import make_timestamp_iter
from test.unit.common.middleware.helpers import FakeSwift


class BaseTestMpuAuditor(unittest.TestCase):
    user_container = 'c'
    audit_container = get_reserved_name('test', user_container)

    def setUp(self):
        self.tempdir = mkdtemp()
        self.ts_iter = make_timestamp_iter()
        self.name_iter = map(lambda x: 'obj%06d' % x, itertools.count())
        self.logger = debug_logger('mpu-auditor-test')
        self.fake_statsd_client = self.logger.logger.statsd_client
        self.account = 'a'
        self.obj_name = 'obj'
        self.upload_id = UploadId(next(self.ts_iter))
        self.vers_name = self._create_history_ref(self.obj_name, null=True)
        self.obj_path = '/v1/%s/%s/%s' % (self.account,
                                          self.user_container,
                                          self.obj_name)
        self.audit_container_path = '/v1/%s/%s' % (self.account,
                                                   self.audit_container)
        self.broker = self._make_broker(self.audit_container)
        self.server = ContainerController({'mount_check': False,
                                           'devices': self.tempdir})

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _make_broker(self, container_name):
        hash_ = md5(container_name.encode('utf-8'),
                    usedforsecurity=False).hexdigest()
        datadir = os.path.join(
            self.tempdir, 'sda1', 'containers', 'p', hash_[-3:], hash_)
        filename = hash_ + '.db'
        db_file = os.path.join(datadir, filename)
        broker = ContainerBroker(
            db_file,
            account=self.account,
            container=container_name,
            logger=self.logger)
        broker.initialize(put_timestamp=float(next(self.ts_iter)))
        return broker

    def _create_history_ref(self, obj, ts_data=None, null=True):
        ts_data = ts_data or next(self.ts_iter)
        vers_id = HistoryId(ts_data, null=null)
        return ObjectRef(obj, vers_id)

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
            'x-size': obj['size'],
            'x-systags': obj['systags'],
            'x-backend-object-state': obj['deleted']
        }
        path = '/sda1/p/a/%s/%s' % (self.audit_container, obj['name'])
        # Request.blank unquotes path so quote it before passing in ?
        req = Request.blank(
            path=wsgi_quote(path), method='PUT', headers=headers)
        resp = self.make_request(req)
        self.assertEqual(201, resp.status_int, resp.body)

    def put_objects(self, items, shuffle_order=True):
        # PUT object updates to container in random order
        if shuffle_order:
            # don't mutate the passed-in list!
            items = random.sample(items, k=len(items))
        for obj in items:
            self.put_object(obj)
        # commit the puts
        self.broker.get_info()

    def delete_object(self, item, ts=None):
        ts = ts or next(self.ts_iter)
        headers = {'name': item['name'],
                   'x-timestamp': ts.internal}
        req = Request.blank(
            '/sda1/p/a/%s/%s' % (self.audit_container, item['name']),
            method='DELETE', headers=headers)
        resp = self.make_request(req)
        self.assertEqual(204, resp.status_int)

    def delete_objects(self, items):
        for item in items:
            self.delete_object(item)
        # commit the deletes
        self.broker.get_info()

    def _create_item(self, name, ts_data, ts_ctype=None, ts_meta=None,
                     ctype='text/plain', size=0, etag='', systags=None,
                     state=0):
        created_at = encode_timestamps(ts_data, ts_ctype, ts_meta)
        item = {
            'name': str(name),
            'created_at': created_at,
            'ctype_timestamp': created_at,
            'meta_timestamp': created_at,
            'content_type': ctype,
            'etag': etag,
            'size': size,
            'deleted': state,
            'storage_policy_index': 0,
            'systags': param_str_from_dict(systags),
        }
        return item

    def _create_marker_spec(self, vers_name, ts_data=None):
        ts_data = ts_data or next(self.ts_iter)
        marker_name = '%s/%s' % (vers_name, MPU_DELETED_MARKER_SUFFIX)
        return self._create_item(marker_name,
                                 ts_data,
                                 ctype=MPU_MARKER_CONTENT_TYPE,
                                 etag=MD5_OF_EMPTY_STRING,
                                 state=2)

    def _check_broker_rows(self, expected_items, include_states=None,
                           table='object'):
        include_states = include_states or {0}
        rows = self.broker.get_objects(
            include_states=include_states, table=table)
        self.assertEqual(sorted([o['name'] for o in expected_items]),
                         sorted([row['name'] for row in rows]))
        return rows

    def _check_object_rows(self, expected_items, include_states=None):
        return self._check_broker_rows(
            expected_items, include_states=include_states, table='object')

    def _check_action_rows(self, expected_items, include_states=None):
        return self._check_broker_rows(
            expected_items, include_states=include_states, table='action')

    def _check_deleted_broker_rows(self, expected_items):
        rows = self.broker.get_objects(include_deleted=True)
        self.assertEqual(sorted([o['name'] for o in expected_items]),
                         sorted([row['name'] for row in rows]))
        return rows


class TestModuleFunctions(BaseTestMpuAuditor):
    def _setup_for_yield_item_batches(self, table, state):
        timestamps = [next(self.ts_iter) for _ in range(12)]
        vers_names = [
            self._create_history_ref(next(self.name_iter), ts)
            for ts in timestamps]
        items = [self._create_item(str(vers_name), ts, state=state)
                 for vers_name, ts in zip(vers_names, timestamps)]
        with self.broker.get() as conn:
            self.broker._really_merge_items(table, conn, items)

        # sanity check...
        self._check_broker_rows(items, include_states=[state], table=table)
        return items

    def test_yield_item_batches_from_object_table(self):
        items = self._setup_for_yield_item_batches(table='object', state=0)
        actual = [batch for batch in yield_item_batches(
            self.broker, 10, 5, include_states=[0], table='object')]
        self.assertEqual(3, len(actual))
        sorted_names = [it['name']
                        for it in sorted(items, key=lambda i: i['name'])]
        self.assertEqual(sorted_names[:5], [a['name'] for a in actual[0]])
        self.assertEqual(sorted_names[5:10], [a['name'] for a in actual[1]])
        self.assertEqual(sorted_names[10:], [a['name'] for a in actual[2]])
        actual = [batch for batch in yield_item_batches(
            self.broker, 10, 5, include_states=[0], table='action')]
        self.assertFalse(actual)

    def test_yield_item_batches_from_action_table(self):
        items = self._setup_for_yield_item_batches(table='action', state=0)
        actual = [batch for batch in yield_item_batches(
            self.broker, 10, 5, include_states=[0], table='action')]
        self.assertEqual(3, len(actual))
        sorted_names = [it['name']
                        for it in sorted(items, key=lambda i: i['name'])]
        self.assertEqual(sorted_names[:5], [a['name'] for a in actual[0]])
        self.assertEqual(sorted_names[5:10], [a['name'] for a in actual[1]])
        self.assertEqual(sorted_names[10:], [a['name'] for a in actual[2]])
        actual = [batch for batch in yield_item_batches(
            self.broker, 10, 5, include_states=[0], table='object')]
        self.assertFalse(actual)

    def test_yield_item_batches_stops_at_max_batches(self):
        items = self._setup_for_yield_item_batches(table='object', state=0)
        actual = [batch for batch in yield_item_batches(
            self.broker, 2, 5, include_states=[0], table='object')]
        self.assertEqual(2, len(actual))
        sorted_names = [it['name']
                        for it in sorted(items, key=lambda i: i['name'])]
        self.assertEqual(sorted_names[:5], [a['name'] for a in actual[0]])
        self.assertEqual(sorted_names[5:10], [a['name'] for a in actual[1]])

    def test_yield_item_batches_stops_at_end_of_items(self):
        items = self._setup_for_yield_item_batches(table='action', state=0)
        actual = [batch for batch in yield_item_batches(
            self.broker, 1000, 10, include_states=[0], table='action')]
        self.assertEqual(2, len(actual))
        sorted_names = [it['name']
                        for it in sorted(items, key=lambda i: i['name'])]
        self.assertEqual(sorted_names[:10], [a['name'] for a in actual[0]])
        self.assertEqual(sorted_names[10:], [a['name'] for a in actual[1]])

    def test_yield_item_batches_items_added(self):
        items = self._setup_for_yield_item_batches(table='object', state=0)
        it = yield_item_batches(
            self.broker, 1000, 10, include_states=[0], table='object')
        actual = [next(it)]
        new_items = [dict(item, created_at=next(self.ts_iter).internal)
                     for item in items[:3]]
        self.put_objects(new_items)
        self._check_broker_rows(items[3:] + new_items, include_states=[0])
        actual.extend([batch for batch in it])
        sorted_names = [it['name']
                        for it in sorted(items, key=lambda i: i['name'])]
        self.assertEqual(sorted_names[:10], [a['name'] for a in actual[0]])
        self.assertEqual(sorted_names[10:], [a['name'] for a in actual[1]])

    def test_yield_item_batches_selects_state(self):
        items = self._setup_for_yield_item_batches(state=3, table='action')
        actual = [batch for batch in yield_item_batches(
            self.broker, 2, 5, include_states=[2], table='action')]
        self.assertEqual([], actual)

        actual = [batch for batch in yield_item_batches(
            self.broker, 2, 5, include_states=[3], table='action')]
        self.assertEqual(2, len(actual))
        sorted_names = [it['name']
                        for it in sorted(items, key=lambda i: i['name'])]
        self.assertEqual(sorted_names[:5], [a['name'] for a in actual[0]])
        self.assertEqual(sorted_names[5:10], [a['name'] for a in actual[1]])


class TestMpuAuditor(BaseTestMpuAuditor):
    # TODO: add tests covering MpuAuditorConfig
    def setUp(self):
        super().setUp()
        with self._mock_internal_client():
            self.auditor = MpuAuditor({}, self.logger)

    def test_init(self):
        self.assertIsInstance(self.auditor.config, MpuAuditorConfig)
        self.assertIs(self.logger, self.auditor.logger)

    def test_audit_slo_segments(self):
        broker = self._make_broker('test+segments')
        with mock.patch('swift.container.mpu_auditor.MpuPartMarkerAuditor'
                        ) as mocked:
            self.auditor.audit(broker)
        self.assertEqual([mock.call(self.auditor.config, self.auditor.client,
                                    self.auditor.logger, broker, 'test')],
                         mocked.call_args_list)
        self.assertEqual([mock.call()],
                         mocked.return_value.audit.call_args_list)

    def test_audit_mpu_parts(self):
        broker = self._make_broker(get_reserved_name('mpu_parts', 'test'))
        with mock.patch('swift.container.mpu_auditor.MpuPartMarkerAuditor'
                        ) as mocked:
            self.auditor.audit(broker)
        self.assertEqual([mock.call(self.auditor.config, self.auditor.client,
                                    self.auditor.logger, broker, 'test')],
                         mocked.call_args_list)
        self.assertEqual([mock.call()],
                         mocked.return_value.audit.call_args_list)

    def test_audit_mpu_sessions(self):
        broker = self._make_broker(get_reserved_name('mpu_sessions', 'test'))
        with mock.patch('swift.container.mpu_auditor.MpuSessionAuditor'
                        ) as mocked:
            self.auditor.audit(broker)
        self.assertEqual([mock.call(self.auditor.config, self.auditor.client,
                                    self.auditor.logger, broker, 'test')],
                         mocked.call_args_list)
        self.assertEqual([mock.call()],
                         mocked.return_value.audit.call_args_list)

    def test_audit_objects(self):
        def do_test(container):
            broker = self._make_broker(container)
            with mock.patch('swift.container.mpu_auditor.MpuHistoryAuditor'
                            ) as mocked:
                self.auditor.audit(broker)
            self.assertEqual(
                [mock.call(self.auditor.config, self.auditor.client,
                           self.auditor.logger, broker, 'test')],
                mocked.call_args_list)
            self.assertEqual([mock.call()],
                             mocked.return_value.audit.call_args_list)

        do_test('test')
        do_test(get_reserved_name('versions', 'test'))


class TestBaseMpuBrokerAuditor(BaseTestMpuAuditor):
    # test abstract auditor behavior not specific to the type of resource
    # being audited
    def test_audit_stats(self):
        timestamps = [next(self.ts_iter) for _ in range(3)]
        items = [self._create_item(next(self.name_iter), ts, state=0)
                 for ts in timestamps]
        self.broker.merge_actions(items)
        fake_ic = InternalClient(None, 'test-ic', 1, app=FakeSwift())
        auditor = BaseMpuAuditor(
            MpuAuditorConfig({}), fake_ic, self.logger, self.broker,
            self.user_container)

        with mock.patch(
                'swift.container.mpu_auditor.BaseMpuAuditor._audit_item',
                side_effect=[False, ValueError('kaboom'), False]) \
                as mock_audit_item:
            auditor.audit()

        self.assertEqual([mock.call(MPUItem(**item)) for item in items],
                         mock_audit_item.call_args_list)
        info_lines = auditor.logger.get_lines_for_level('info')
        self.assertEqual(1, len(info_lines), info_lines)
        self.assertIn('processed=3, audited=2, errors=1', info_lines[0])
        error_lines = auditor.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines))
        self.assertIn('Error while auditing ', error_lines[0])
        self.assertIn('boom', error_lines[0])
        exp_stats = {'processed': 3, 'audited': 2, 'errors': 1}
        exp_metrics = dict(('base.%s' % k, v) for k, v in exp_stats.items())
        self.assertEqual(exp_metrics, self.fake_statsd_client.counters)


class TestMpuHistoryAuditor(BaseTestMpuAuditor):
    audit_container = get_reserved_name(
        'history', BaseTestMpuAuditor.user_container)

    def setUp(self):
        super().setUp()
        self.parts_container = get_reserved_name(
            'mpu_parts', self.user_container)

    def _create_history_item(self, name, timestamp, systags=None, state=0):
        return self._create_item(
            name,
            timestamp,
            ctype='application/x-phony',
            size=0,
            etag=MD5_OF_EMPTY_STRING,
            state=state,
            systags=systags
        )

    def test_audit_null_versions(self):
        timestamps = [next(self.ts_iter) for _ in range(5)]
        random.shuffle(timestamps)
        non_mpu_history_items = [
            self._create_history_item(self.obj_name, ts)
            for ts in timestamps[:2]
        ]
        older_upload_ids = [UploadId(ts) for ts in timestamps[2:]]
        mpu_history_items = [
            self._create_history_item(
                self.obj_name,
                upload_id.timestamp,
                systags={
                    'relic_id': quote(upload_id.serialize()),
                    'child': quote('%s/%s/' % (self.obj_name, upload_id)),
                    'child_container': quote(self.parts_container),
                }
            )
            for upload_id in older_upload_ids
        ]
        # the most recent variant is always an mpu
        latest_upload_id = UploadId(next(self.ts_iter))
        mpu_history_items += [
            self._create_history_item(
                self.obj_name,
                latest_upload_id.timestamp,
                systags={
                    'relic_id': quote(latest_upload_id.serialize()),
                    'child': quote('%s/%s/%s/' % (
                        self.parts_container, self.obj_name, latest_upload_id))
                }
            )
            for ts in [next(self.ts_iter)]
        ]

        # as these are merged they'll generate relics for all but latest mpu
        self.put_objects(non_mpu_history_items + mpu_history_items,
                         shuffle_order=True)
        self._check_broker_rows(mpu_history_items[-1:], include_states={0})
        exp_relic_items = [
            self._create_history_item(
                '%s\x00%s' % (self.obj_name, upload_id),
                upload_id.timestamp,
                systags={
                    'relic_id': quote(upload_id.serialize()),
                    'child': quote('%s/%s/%s/' % (
                        self.parts_container, self.obj_name, upload_id))})
            for upload_id in older_upload_ids
        ]
        exp_action_items = [
            dict(exp_relic_item) for exp_relic_item in exp_relic_items
        ]
        self._check_broker_rows(exp_relic_items, include_states={2})
        self._check_action_rows(exp_action_items, include_states={0})

        # when the auditor runs each relic generates a parts delete marker...
        registered_calls = [
            ('DELETE', '/v1/a/%s/%s/%s/'
             % (self.parts_container, self.obj_name, upload_id),
             HTTPNoContent, {})
            for upload_id in older_upload_ids
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = list(sorted([call[:2] for call in registered_calls]))
        self.assertEqual(expected_calls, list(sorted(fake_swift.calls)))
        self._check_broker_rows(mpu_history_items[-1:])
        self._check_broker_rows([], include_states={1})
        self._check_broker_rows(exp_relic_items, include_states={2})
        self._check_action_rows(exp_action_items, include_states={1})

        # repeat audit does nothing...
        with self._mock_internal_client([]) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        self.assertFalse(fake_swift.calls)
        self._check_broker_rows(mpu_history_items[-1:])
        self._check_broker_rows([], include_states={1})
        self._check_broker_rows(exp_relic_items, include_states={2})
        self._check_action_rows(exp_action_items, include_states={1})

    def test_audit_retained_versions(self):
        # verify cleanup with versioned object names
        ts_vers = next(self.ts_iter)
        upload_id = UploadId(ts_vers)
        vers_name = '\x00%s\x00%s' % (self.obj_name, (~(ts_vers)).normal)
        mpu_history_items = [
            self._create_history_item(
                vers_name,
                ts_vers,
                systags={
                    'relic_id': quote(upload_id.serialize()),
                    'child': quote('%s/%s/' % (vers_name, upload_id)),
                    'child_container': quote(self.parts_container),
                }
            ),
            self._create_history_item(
                vers_name,
                next(self.ts_iter),
                state=1
            ),
        ]
        # as these are merged they'll generate a relic and action
        self.put_objects(mpu_history_items, shuffle_order=True)
        exp_relic_items = [
            self._create_history_item(
                '%s\x00%s' % (vers_name, upload_id),
                ts_vers,
                systags={
                    'relic_id': quote(upload_id.serialize()),
                    'child': quote('%s/%s/' % (vers_name, upload_id)),
                    'child_container': quote(self.parts_container),
                }
            )
        ]
        exp_action_items = [
            dict(exp_relic_item) for exp_relic_item in exp_relic_items
        ]
        self._check_broker_rows([], include_states={0})
        self._check_broker_rows(mpu_history_items[1:], include_states={1})
        self._check_broker_rows(exp_relic_items, include_states={2})
        self._check_action_rows(exp_action_items, include_states={0})

        registered_calls = [
            ('DELETE', '/v1/a/%s/%s/%s/'
             % (self.parts_container, vers_name, upload_id),
             HTTPNoContent, {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = list(sorted([call[:2] for call in registered_calls]))
        self.assertEqual(expected_calls, list(sorted(fake_swift.calls)))
        self._check_broker_rows([], include_states={0})
        self._check_broker_rows(mpu_history_items[1:], include_states={1})
        self._check_broker_rows(exp_relic_items, include_states={2})
        self._check_action_rows(exp_action_items, include_states={1})

        # repeat audit does nothing...
        with self._mock_internal_client([]) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        self.assertFalse(fake_swift.calls)
        self._check_broker_rows([], include_states={0})
        self._check_broker_rows(mpu_history_items[1:], include_states={1})
        self._check_broker_rows(exp_relic_items, include_states={2})
        self._check_action_rows(exp_action_items, include_states={1})


class TestMpuAuditorParts(BaseTestMpuAuditor):
    audit_container = get_reserved_name(
        'mpu_parts', BaseTestMpuAuditor.user_container)

    def _create_part_spec(self, obj_name, upload_id, part_number, ts_data=None,
                          systags=None, state=0):
        systags = systags or {}
        ts_data = ts_data or next(self.ts_iter)
        part_prefix = '%s/%s/' % (obj_name, upload_id)
        part_name = '%s%s' % (part_prefix, normalize_part_number(part_number))
        relic_name = '%s\x00%s' % (part_prefix, upload_id)
        systags['parent'] = quote(relic_name)
        return self._create_item(part_name,
                                 ts_data,
                                 ctype='application/octet-stream',
                                 size=123456,
                                 systags=systags,
                                 state=state)

    def _create_lifeline_spec(self, obj_name, upload_id, ts_data=None,
                              systags=None, state=0):
        systags = systags or {}
        ts_data = ts_data or next(self.ts_iter)
        part_prefix = '%s/%s/' % (obj_name, upload_id)
        systags.update({
            'relic_id': quote(str(upload_id)),
            'child_prefix': quote(part_prefix),
        })
        return self._create_item(part_prefix,
                                 ts_data,
                                 ctype='application/octet-stream',
                                 size=123456,
                                 systags=systags,
                                 state=state)

    def test_audit_lifeline_deleted_after_parts_merged(self):
        # verify that deletion of the lifeline will result in existing
        # *undeleted* parts being cleaned up
        lifeline = self._create_lifeline_spec(
            self.obj_name, self.upload_id)
        parts = [
            self._create_part_spec(self.obj_name, self.upload_id, i, state=0)
            for i in range(1, 6)]
        # merge parts and lifeline...
        self.put_objects(parts + [lifeline], shuffle_order=True)
        self.delete_objects(parts[:3])
        self._check_broker_rows(parts[3:] + [lifeline], include_states={0})
        # run cleanup... nothing to do
        with self._mock_internal_client([]) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)
        self.assertFalse(fake_swift.calls)
        self._check_broker_rows(parts[3:] + [lifeline], include_states={0})
        self._check_broker_rows(parts[:3], include_states={1})

        # delete lifeline...
        self.delete_objects([lifeline])

        part_prefix = '%s/%s/' % (self.obj_name, self.upload_id)
        exp_relic_name = '%s\x00%s' % (part_prefix, self.upload_id)
        exp_relic = dict(lifeline, name=exp_relic_name, state=2)
        exp_action = dict(exp_relic, state=0)
        self._check_broker_rows(parts[3:], include_states={0})
        self._check_broker_rows(parts[:3] + [lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([exp_action], include_states={0})
        self._check_action_rows([], include_states={1})

        # run cleanup...
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in parts[3:]
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        # expect DELETEs for parts...
        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        # part rows aren't deleted yet (object DELETE updates will do that)
        self._check_broker_rows(parts[3:], include_states={0})
        self._check_broker_rows(parts[:3] + [lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([], include_states={0})
        self._check_action_rows([exp_action], include_states={1})

    def test_audit_lifeline_deleted_before_parts_merged(self):
        # verify that deletion of the lifeline will result in subsequent parts
        # being cleaned up
        lifeline = self._create_lifeline_spec(
            self.obj_name, self.upload_id)
        parts = [
            self._create_part_spec(self.obj_name, self.upload_id, i, state=0)
            for i in range(1, 6)]
        # merge lifeline then delete it to create a relic...
        self.put_objects([lifeline])
        self.delete_objects([lifeline])

        part_prefix = '%s/%s/' % (self.obj_name, self.upload_id)
        exp_relic_name = '%s\x00%s' % (part_prefix, self.upload_id)
        exp_relic = dict(lifeline, name=exp_relic_name, state=2)
        exp_action = dict(exp_relic, state=0)
        self._check_broker_rows([], include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([exp_action], include_states={0})
        self._check_action_rows([], include_states={1})

        # run cleanup - nothing to do
        with self._mock_internal_client([]) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)
        self.assertFalse(fake_swift.calls)
        self._check_broker_rows([], include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([], include_states={0})
        self._check_action_rows([exp_action], include_states={1})

        # merge parts...which creates an action
        with mock.patch('swift.container.mpu_auditor.time.time',
                        return_value=float(next(self.ts_iter))):
            self.put_objects(parts)
        self._check_broker_rows(parts, include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([exp_action], include_states={0})
        self._check_action_rows([], include_states={1})

        # run cleanup...
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in parts
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        # rows aren't deleted yet (object DELETE updates will do that)
        self._check_broker_rows(parts, include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([], include_states={0})
        self._check_action_rows([exp_action], include_states={1})

    def test_audit_delete_marker_parts_not_found(self):
        # verify that part DELETE 404s don't prevent progress
        lifeline = self._create_lifeline_spec(self.obj_name, self.upload_id)
        parts = [
            self._create_part_spec(self.obj_name, self.upload_id, i, state=0)
            for i in range(1, 6)]
        # merge parts and lifeline then delete lifeline to create a relic...
        self.put_objects(parts + [lifeline])
        self.delete_objects([lifeline])
        part_prefix = '%s/%s/' % (self.obj_name, self.upload_id)
        exp_relic_name = '%s\x00%s' % (part_prefix, self.upload_id)
        exp_relic = dict(lifeline, name=exp_relic_name, state=2)
        exp_action = dict(exp_relic, state=0)
        self._check_broker_rows(parts, include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([exp_action], include_states={0})
        self._check_action_rows([], include_states={1})

        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNotFound,
             {})
            for part in parts[:3]
        ] + [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in parts[3:]
        ]

        # run cleanup, expect action to be deleted
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(parts, include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([], include_states={0})
        self._check_action_rows([exp_action], include_states={1})

    def test_audit_delete_marker_parts_fail(self):
        # verify that part DELETE 503s don't prevent progress, but action
        # remains intact
        lifeline = self._create_lifeline_spec(self.obj_name, self.upload_id)
        parts = [
            self._create_part_spec(self.obj_name, self.upload_id, i, state=0)
            for i in range(1, 6)]
        # merge parts and lifeline then delete lifeline to create a relic...
        self.put_objects(parts + [lifeline])
        self.delete_objects([lifeline])
        part_prefix = '%s/%s/' % (self.obj_name, self.upload_id)
        exp_relic_name = '%s\x00%s' % (part_prefix, self.upload_id)
        exp_relic = dict(lifeline, name=exp_relic_name, state=2)
        exp_action = dict(exp_relic, state=0)
        self._check_broker_rows(parts, include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([exp_action], include_states={0})
        self._check_action_rows([], include_states={1})

        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPInternalServerError,
             {})
            for part in parts[:3]
        ] + [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in parts[3:]
        ]

        # run cleanup, expect action to be deleted
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(parts, include_states={0})
        self._check_broker_rows([lifeline], include_states={1})
        self._check_broker_rows([exp_relic], include_states={2})
        self._check_action_rows([exp_action], include_states={0})
        self._check_action_rows([], include_states={1})


class TestMpuAuditorSessions(BaseTestMpuAuditor):
    audit_container = get_reserved_name(
        'mpu_sessions', BaseTestMpuAuditor.user_container)

    def setUp(self):
        super().setUp()
        self.ts_data = Timestamp(time.time())
        self.part_container_path = '/'.join([
            '', 'v1', self.account,
            get_reserved_name('mpu_parts', self.user_container)])

    def _create_session_spec(
            self, vers_name, ctype, ts_data=None, ts_ctype=None, ts_meta=None):
        ts_data = ts_data or self.ts_data
        return self._create_item(
            vers_name, ts_data=ts_data, ts_ctype=ts_ctype,
            ts_meta=ts_meta, ctype=ctype)

    def test_audit_in_progress_session(self):
        # verify that an in progress session is passed over
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_CREATED_CONTENT_TYPE)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        with self._mock_internal_client():
            auditor = MpuAuditor({}, self.logger)
            auditor.audit(self.broker)

        self.assertEqual(1, self.broker.get_max_row())  # no new row
        rows = self._check_broker_rows([session])
        self.assertEqual(self.ts_data, rows[0]['created_at'])
        self._check_deleted_broker_rows([])

    def test_audit_completing_session_is_recent(self):
        # verify that a recent completing session is deferred for revisit; the
        # ctype_timestamp is the significant time
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        ts_ctype = Timestamp(float(ts_now) - 123)  # recent
        ts_meta = Timestamp(float(ts_now) - 4)
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_COMPLETING_CONTENT_TYPE,
            ts_data=ts_data, ts_ctype=ts_ctype, ts_meta=ts_meta)
        self.put_objects([session])
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
        rows = self._check_broker_rows([session])
        # update meta_timestamp
        self.assertEqual((ts_data, ts_ctype, ts_now),
                         decode_timestamps(rows[0]['created_at']))
        self._check_deleted_broker_rows([])

    def test_audit_completing_session_is_old(self):
        # verify that an old completing session is passed over; the
        # ctype_timestamp is the significant time
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        ts_ctype = Timestamp(float(ts_now) - 3601)  # old
        ts_meta = ts_now
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_COMPLETING_CONTENT_TYPE,
            ts_data=ts_data, ts_ctype=ts_ctype, ts_meta=ts_meta)
        self.put_objects([session])
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
        rows = self._check_broker_rows([session])
        # no change to session timestamps
        self.assertEqual((ts_data, ts_ctype, ts_meta),
                         decode_timestamps(rows[0]['created_at']))
        self._check_deleted_broker_rows([])

    def test_audit_completing_session_is_old_error_checking_user_obj(self):
        # verify that an old completing session is not passed over if the
        # user namespace check failed
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_COMPLETING_CONTENT_TYPE,
            ts_data=ts_data)
        self.put_objects([session])
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
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_completing_session_and_user_obj_is_mpu(self):
        # verify that a completing session that is found to be completed is
        # removed
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_COMPLETING_CONTENT_TYPE)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        user_obj_resp_hdrs = {'x-object-sysmeta-mpu-upload-id':
                              str(self.vers_name.obj_id)}
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, user_obj_resp_hdrs),
            ('DELETE',
             '/'.join([self.audit_container_path, session['name']]),
             HTTPAccepted,
             {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, self.logger)
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_completing_session_is_completed_but_delete_fails(self):
        # verify that a completing session that is found to be completed
        # is bumped if session delete fails
        ts_data = next(self.ts_iter)
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_COMPLETING_CONTENT_TYPE,
            ts_data=ts_data)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        user_obj_resp_hdrs = {'x-object-sysmeta-mpu-upload-id':
                              str(self.vers_name.obj_id)}
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, user_obj_resp_hdrs),
            ('DELETE',
             '/'.join([self.audit_container_path, session['name']]),
             HTTPServerError,
             {})
        ]
        ts_now = next(self.ts_iter)
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(ts_now)):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_completed_session(self):
        # verify that a completed session is removed
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_COMPLETED_CONTENT_TYPE)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, session['name']]),
             HTTPNoContent,
             {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, self.logger)
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_aborted_session_and_user_obj_is_mpu(self):
        # verify that an aborted session is cleaned up if mpu exists
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_ABORTED_CONTENT_TYPE)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        user_obj_resp_hdrs = {'x-object-sysmeta-mpu-upload-id':
                              str(self.vers_name.obj_id)}
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, user_obj_resp_hdrs),
            ('DELETE',
             '/'.join([self.audit_container_path, session['name']]),
             HTTPNoContent,
             {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, self.logger)
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_aborted_session_is_not_yet_purged(self):
        # verify that an aborted session younger than purge_delay is deferred
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_ABORTED_CONTENT_TYPE,
            ts_data=ts_data)
        self.put_objects([session])
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
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_aborted_session_error_checking_user_obj(self):
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_ABORTED_CONTENT_TYPE)
        self.put_objects([session])
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
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_aborted_session_is_purged(self):
        # verify that an aborted session older than purge_delay is cleaned up
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_ABORTED_CONTENT_TYPE)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('DELETE',
             '/'.join([self.part_container_path, session['name'] + '/']),
             HTTPNoContent,
             {}),
            ('DELETE',
             '/'.join([self.audit_container_path, session['name']]),
             HTTPNoContent,
             {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({'mpu_aborted_purge_delay': 0},
                                 self.logger)
            auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_aborted_session_error_putting_part_delete_marker(self):
        # verify that an aborted session older than purge_delay is cleaned up
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_ABORTED_CONTENT_TYPE)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('DELETE',
             '/'.join([self.part_container_path, session['name'] + '/']),
             HTTPInternalServerError,
             {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({'mpu_aborted_purge_delay': 0},
                                 self.logger)
            auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_aborted_session_error_deleting_session(self):
        # verify that an aborted session older than purge_delay is cleaned up
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_ABORTED_CONTENT_TYPE)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('DELETE',
             '/'.join([self.part_container_path, session['name'] + '/']),
             HTTPNoContent,
             {}),
            ('DELETE',
             '/'.join([self.audit_container_path, session['name']]),
             HTTPServerError,
             {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({'mpu_aborted_purge_delay': 0},
                                 self.logger)
            auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows([session])
        self._check_deleted_broker_rows([])

    def test_audit_aborted_session_is_eventually_purged(self):
        # verify that an aborted session younger than purge_delay is deferred,
        # but is *eventually* considered old enough to purge
        now = float(time.time())
        ts_data = Timestamp(now - 3600)
        ts_ctype = Timestamp(now - 2001)
        session = self._create_session_spec(
            self.vers_name, MPU_SESSION_ABORTED_CONTENT_TYPE,
            ts_data=ts_data, ts_ctype=ts_ctype)
        self.put_objects([session])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
        ]
        for audit_time_offset in [0, 1000, 8600]:
            audit_time = now + audit_time_offset
            with self._mock_internal_client(registered_calls) as fake_swift:
                with mock.patch('swift.container.mpu_auditor.time.time',
                                return_value=audit_time):
                    auditor = MpuAuditor({}, self.logger)
                    auditor.audit(self.broker)
            self._check_broker_rows([session])

        # this time the session will be purged...
        audit_time = now + 86400  # purge_delay in the future
        self.assertGreater(audit_time, float(ts_ctype) + 86400)  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('DELETE',
             '/'.join([self.part_container_path, session['name'] + '/']),
             HTTPNoContent,
             {}),
            ('DELETE',
             '/'.join([self.audit_container_path, session['name']]),
             HTTPNoContent,
             {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=audit_time):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)
        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        # row is not updated until the object server forwards DELETE update
        self._check_broker_rows([session])


@unittest.skip
class TestMpuAuditorSLO(BaseTestMpuAuditor):
    user_container = 'c'
    audit_container = user_container + '+segments'

    def _create_upload_parts(self, num_parts):
        ts = next(self.ts_iter)
        upload_id = unique_id()
        upload = '%s/%s' % (self.obj_name, upload_id)
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
