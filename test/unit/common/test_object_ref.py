# Copyright (c) 2025 NVIDIA
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
import os
import random
import unittest
from tempfile import mkdtemp

from swift.common.object_ref import ObjectRef, HistoryId, UploadId

from swift.common.swob import Request
from swift.common.utils import Namespace, Timestamp, quote
from swift.container.backend import ContainerBroker
from test.unit import make_timestamp_iter


class TestUploadId(unittest.TestCase):
    def test_init(self):
        upload_id = UploadId(Timestamp(123.45678))
        self.assertEqual(Timestamp(123.45678), upload_id.timestamp)
        self.assertEqual('0000000123.45678&$', str(upload_id))
        self.assertEqual('0000000123.45678&$', upload_id.serialize())

        upload_id = UploadId(123.45678)
        self.assertEqual(Timestamp(123.45678), upload_id.timestamp)
        self.assertEqual('0000000123.45678&$', str(upload_id))
        self.assertEqual('0000000123.45678&$', upload_id.serialize())

    def test_parse(self):
        upload_id = UploadId.parse('0000000123.45678&$')
        self.assertEqual(Timestamp(123.45678), upload_id.timestamp)

    def test_parse_bad(self):
        def do_test(value):
            with self.assertRaises(ValueError) as cm:
                UploadId.parse(value)
            self.assertEqual('Invalid UploadId: %s' % value,
                             str(cm.exception))

        do_test(None)
        do_test('')
        do_test('\x00\x00')
        # extra final delimiter
        do_test('0000001234.5678&$&')
        # missing shard alignment
        do_test('0000001234.5678&')
        do_test('not9999999876.54321&$')
        do_test('-bad-time')

    def test_newest(self):
        v_max = UploadId.newest()
        self.assertEqual('9999999999.99999&$', v_max.serialize())
        self.assertGreater(v_max.serialize(), UploadId(123.45678).serialize())


class TestHistoryId(unittest.TestCase):
    def test_init_real(self):
        obj_id = HistoryId(Timestamp(123.45678))
        self.assertEqual(Timestamp(123.45678), obj_id.timestamp)
        self.assertFalse(obj_id.null)
        self.assertEqual('9999999876.54321&$&', str(obj_id))
        self.assertEqual('9999999876.54321&$&', obj_id.serialize())
        self.assertEqual('9999999876.54321',
                         obj_id.serialize(prefix_only=True))

    def test_init_real_timestamp_offset(self):
        # offset is ignored when inverting timestamp
        obj_id = HistoryId(Timestamp(123.45678, offset=789))
        self.assertEqual(Timestamp(123.45678, offset=789), obj_id.timestamp)
        self.assertFalse(obj_id.null)
        self.assertEqual('9999999876.54321&$&', str(obj_id))
        self.assertEqual('9999999876.54321&$&', obj_id.serialize())

    def test_init_null(self):
        obj_id = HistoryId(Timestamp(123.45678), null=True)
        self.assertEqual(Timestamp(123.45678), obj_id.timestamp)
        self.assertTrue(obj_id.null)
        self.assertEqual('-null-&$&9999999876.54321', str(obj_id))
        self.assertEqual('-null-&$&9999999876.54321', obj_id.serialize())
        self.assertEqual('-null-', obj_id.serialize(prefix_only=True))

    def test_init_bad(self):
        with self.assertRaises(TypeError):
            HistoryId(None)
        with self.assertRaises(ValueError):
            HistoryId('the timestamp')

    def test_parse_real(self):
        obj_id = HistoryId.parse('9999999876.54321&$&')
        self.assertEqual(Timestamp(123.45678), obj_id.timestamp)
        self.assertFalse(obj_id.null)
        self.assertEqual('9999999876.54321&$&', str(obj_id))

    def test_parse_null(self):
        obj_id = HistoryId.parse('-null-&$&9999999876.54321')
        self.assertEqual(Timestamp(123.45678), obj_id.timestamp)
        self.assertTrue(obj_id.null)
        self.assertEqual('-null-&$&9999999876.54321', str(obj_id))

    def test_parse_bad(self):
        def do_test(value):
            with self.assertRaises(ValueError) as cm:
                HistoryId.parse(value)
            self.assertEqual('Invalid HistoryId: %s' % value,
                             str(cm.exception))

        # TODO: update with more relevant bad cases
        do_test(None)
        do_test('')
        do_test('\x00\x00')
        # missing shard alignment
        do_test('not9999999876.54321&$&')
        do_test('-bad-time')

    def test_eq(self):
        self.assertEqual(HistoryId(Timestamp(123.45678)),
                         HistoryId(Timestamp(123.45678)))
        self.assertNotEqual(HistoryId(Timestamp(123.45678)),
                            HistoryId(Timestamp(124)))

        self.assertEqual(HistoryId(Timestamp(123.45678), null=False),
                         HistoryId(Timestamp(123.45678), null=False))
        self.assertNotEqual(
            HistoryId(Timestamp(123.45678), null=False),
            HistoryId(Timestamp(123.45678), null=True))


class TestObjectRef(unittest.TestCase):
    def test_serialize(self):
        obj_ref = ObjectRef('foo', HistoryId(123.45678), '000001')
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&/000001',
            str(obj_ref))
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&/000001',
            obj_ref.serialize())
        self.assertEqual('\x00foo\x009999999876.54321&$&',
                         obj_ref.serialize(drop_tail=True))

    def _do_test_init(self, obj, obj_id):
        obj_ref = ObjectRef(obj, obj_id)
        self.assertEqual(obj, obj_ref.user_name)
        self.assertEqual('\x00%s' % obj, obj_ref.basename)
        self.assertEqual(str(obj_id), obj_ref.obj_id)
        self.assertIsNone(obj_ref.tail)
        return obj_ref

    def test_init(self):
        obj_id = HistoryId(123.45678, null=False)
        obj_ref = self._do_test_init('foo', obj_id.serialize())
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&', str(obj_ref))
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&', obj_ref.serialize())

    def test_init_utf8_name(self):
        obj_id = HistoryId(123.45678, null=False)
        obj_ref = self._do_test_init('foo\N{SNOWMAN}', obj_id)
        self.assertEqual(
            '\x00foo\N{SNOWMAN}\x009999999876.54321&$&', str(obj_ref))
        self.assertEqual(
            '\x00foo\N{SNOWMAN}\x009999999876.54321&$&', obj_ref.serialize())
        obj_ref = self._do_test_init('fooünicode', obj_id)
        self.assertEqual(
            '\x00fooünicode\x009999999876.54321&$&', str(obj_ref))
        self.assertEqual(
            '\x00fooünicode\x009999999876.54321&$&', obj_ref.serialize())

    def test_init_with_history_id_instance(self):
        # it's ok to pass in a HistoryId instance...
        obj_id = HistoryId(123.45678, null=False)
        obj_ref = self._do_test_init('foo', obj_id)
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&', str(obj_ref))
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&', obj_ref.serialize())

    def test_init_with_upload_id_instance(self):
        # it's ok to pass in a UploadId instance...
        obj_id = UploadId(123.45678)
        obj_ref = self._do_test_init('foo', obj_id)
        self.assertEqual(
            '\x00foo\x000000000123.45678&$', str(obj_ref))
        self.assertEqual(
            '\x00foo\x000000000123.45678&$', obj_ref.serialize())

    def test_init_no_object_id(self):
        obj_ref = ObjectRef('foo')
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('\x00foo', obj_ref.basename)
        self.assertIsNone(obj_ref.obj_id)
        self.assertIsNone(obj_ref.tail)
        self.assertEqual('\x00foo', str(obj_ref))

    def test_init_with_tail(self):
        obj_ref = ObjectRef('foo', HistoryId(123.45678), '000001')
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('\x00foo', obj_ref.basename)
        self.assertEqual(HistoryId(123.45678), obj_ref.obj_id)
        self.assertEqual('000001', obj_ref.tail)
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&/000001', str(obj_ref))

        obj_ref = ObjectRef('foo', HistoryId(123.45678), '')
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('\x00foo', obj_ref.basename)
        self.assertEqual(HistoryId(123.45678), obj_ref.obj_id)
        self.assertEqual('', obj_ref.tail)
        self.assertEqual(
            '\x00foo\x009999999876.54321&$&/', str(obj_ref))

    def _do_test_user_name(self, obj):
        # check that ObjectRef.user_name results in identical Request.path
        obj_ref = ObjectRef(obj)
        req1 = Request.blank('/v1/a/c/%s' % quote(obj))
        req2 = Request.blank(quote('/v1/a/c/%s' % obj_ref.user_name))
        self.assertEqual(req1.path, req2.path)
        self.assertEqual(req1.environ['PATH_INFO'], req2.environ['PATH_INFO'])

    def test_user_name(self):
        self._do_test_user_name('foo')
        self._do_test_user_name('foo\N{SNOWMAN}')
        self._do_test_user_name('fooünicode')

    def test_clone(self):
        obj_ref = ObjectRef('foo', HistoryId(123.45678))
        clone = obj_ref.clone()
        self.assertEqual('\x00foo\x009999999876.54321&$&',
                         str(clone))

    def test_eq(self):
        obj_ref1 = ObjectRef('foo', HistoryId(123.45678))
        obj_ref2 = ObjectRef('foo', HistoryId(123.45678))
        obj_ref3 = ObjectRef('foo', HistoryId(123.99999))
        obj_ref4 = ObjectRef('foo', HistoryId(123.45678, null=True))
        self.assertEqual(obj_ref1, obj_ref2)
        self.assertEqual(obj_ref1, obj_ref1.clone())
        self.assertNotEqual(obj_ref1, obj_ref3)
        self.assertNotEqual(obj_ref1, obj_ref4)

    def test_parse(self):
        obj_ref_str = '\x00foo\x009999999876.54321&$&'
        obj_ref = ObjectRef.parse(obj_ref_str)
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('\x00foo', obj_ref.basename)
        self.assertEqual(HistoryId(123.45678), obj_ref.obj_id)
        self.assertIsNone(obj_ref.tail)
        self.assertEqual(obj_ref_str, str(obj_ref))

        obj_ref_str = '\x00foo\x00-null-&$&9999999876.54321'
        obj_ref = ObjectRef.parse(obj_ref_str)
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('\x00foo', obj_ref.basename)
        self.assertEqual(HistoryId(123.45678, null=True), obj_ref.obj_id)
        self.assertIsNone(obj_ref.tail)
        self.assertEqual(obj_ref_str, str(obj_ref))

    def test_parse_with_tail(self):
        obj_ref_str = '\x00foo\x009999999876.54321&$&/000001'
        obj_ref = ObjectRef.parse(obj_ref_str)
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('\x00foo', obj_ref.basename)
        self.assertEqual(HistoryId(123.45678), obj_ref.obj_id)
        self.assertEqual('000001', obj_ref.tail)
        self.assertEqual(obj_ref_str, str(obj_ref))

    def test_parse_with_empty_tail(self):
        obj_ref_str = '\x00foo\x009999999876.54321&$&/'
        obj_ref = ObjectRef.parse(obj_ref_str)
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('\x00foo', obj_ref.basename)
        self.assertEqual(HistoryId(123.45678), obj_ref.obj_id)
        self.assertEqual('', obj_ref.tail)
        self.assertEqual(obj_ref_str, str(obj_ref))

    def test_parse_bad(self):
        with self.assertRaises(AttributeError):
            ObjectRef.parse(None)

        def do_test(value):
            with self.assertRaises(ValueError):
                ObjectRef.parse(value)

        do_test('')
        do_test('foo')
        do_test('\x00foo')

    def test_sort_order(self):
        obj_refs = [
            ObjectRef('foo', HistoryId(Timestamp(123.45678))),
            ObjectRef('foo', HistoryId(Timestamp(124), null=True)),
            ObjectRef('foo', HistoryId(Timestamp(126), null=True)),
            ObjectRef('foo', HistoryId(Timestamp(125))),
        ]
        # null history ids sort ahead of non-null history ids
        self.assertEqual([
            '\x00foo\x00-null-&$&9999999873.99999',
            '\x00foo\x00-null-&$&9999999875.99999',
            '\x00foo\x009999999874.99999&$&',
            '\x00foo\x009999999876.54321&$&',
        ], sorted([str(obj_ref) for obj_ref in obj_refs]))

    def test_aligned_sharding(self):
        ts_iter = make_timestamp_iter()
        container_name = 'test-container'
        db_path = os.path.join(mkdtemp(), '%s.db' % container_name)
        broker = ContainerBroker(
            db_path, account='a', container=container_name)
        broker.initialize(next(ts_iter).internal, 0)
        self.assertEqual(([], False), broker.find_shard_ranges(10))  # sanity

        obj_refs_null = [
            ObjectRef('foo', HistoryId(Timestamp(128), null=True), 'PUT'),
            ObjectRef('foo', HistoryId(Timestamp(127), null=True), 'PUT'),
            ObjectRef('foo', HistoryId(Timestamp(126), null=True), 'PUT'),
            ObjectRef('foo', HistoryId(Timestamp(124), null=True), 'PUT'),
        ]
        obj_refs_125 = [
            ObjectRef('foo', HistoryId(Timestamp(125)), 'PUT'),
        ]
        obj_refs_123 = [
            ObjectRef('foo', HistoryId(Timestamp(123)), 'DELETE'),
            ObjectRef('foo', HistoryId(Timestamp(123)), 'PUT'),
        ]
        history_names = [
            str(obj_ref)
            for obj_ref in obj_refs_null + obj_refs_123 + obj_refs_125]
        self.assertEqual([
            '\x00foo\x00-null-&$&9999999871.99999/PUT',
            '\x00foo\x00-null-&$&9999999872.99999/PUT',
            '\x00foo\x00-null-&$&9999999873.99999/PUT',
            '\x00foo\x00-null-&$&9999999875.99999/PUT',
            '\x00foo\x009999999874.99999&$&/PUT',
            '\x00foo\x009999999876.99999&$&/DELETE',
            '\x00foo\x009999999876.99999&$&/PUT',
        ], sorted(history_names))

        random.shuffle(history_names)
        for obj_name in history_names:
            broker.put_object(
                obj_name, next(ts_iter).internal, 0, 'text/plain', 'etag')

        # sanity check: non-aligned sharding
        ranges, _ = broker.find_shard_ranges(2)
        self.assertEqual([2, 2, 2, 1], [r['object_count'] for r in ranges])
        namespaces = [Namespace(i, r['lower'], r['upper'])
                      for i, r in enumerate(ranges)]
        # null versions history split across shards
        self.assertIn(str(obj_refs_null[0]), namespaces[0])
        self.assertIn(str(obj_refs_null[1]), namespaces[0])
        self.assertIn(str(obj_refs_null[2]), namespaces[1])
        self.assertIn(str(obj_refs_null[3]), namespaces[1])
        self.assertIn(str(obj_refs_125[0]), namespaces[2])
        # real version history split across shards
        self.assertIn(str(obj_refs_123[0]), namespaces[2])
        self.assertIn(str(obj_refs_123[1]), namespaces[3])

        # now with aligned sharding...
        broker.set_sharding_sysmeta('Delimiter', '$')
        ranges, _ = broker.find_shard_ranges(2)
        self.assertEqual([4, 3], [r['object_count'] for r in ranges])
        namespaces = [Namespace(i, r['lower'], r['upper'])
                      for i, r in enumerate(ranges)]
        # null versions history in same shard
        self.assertIn(str(obj_refs_null[0]), namespaces[0])
        self.assertIn(str(obj_refs_null[1]), namespaces[0])
        self.assertIn(str(obj_refs_null[2]), namespaces[0])
        self.assertIn(str(obj_refs_null[3]), namespaces[0])
        self.assertIn(str(obj_refs_125[0]), namespaces[1])
        # real version history in same shard
        self.assertIn(str(obj_refs_123[0]), namespaces[1])
        self.assertIn(str(obj_refs_123[1]), namespaces[1])
