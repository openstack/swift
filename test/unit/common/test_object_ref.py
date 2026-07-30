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

from swift.common.object_ref import ObjectRef, UploadId

from swift.common.swob import Request
from swift.common.utils import Namespace, Timestamp, quote
from swift.container.backend import ContainerBroker
from test.unit import make_timestamp_iter, BaseUnitTestCase


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


class TestObjectRef(BaseUnitTestCase):
    def test_serialize(self):
        obj_id = UploadId(123.45678)
        obj_ref = ObjectRef('foo', obj_id, '000001')
        self.assertEqual('foo/%s/000001' % obj_id, str(obj_ref))
        self.assertEqual('foo/%s/000001' % obj_id, obj_ref.serialize())
        self.assertEqual('foo/%s' % obj_id, obj_ref.serialize(drop_tail=True))

    def _do_test_init(self, obj, obj_id):
        obj_ref = ObjectRef(obj, obj_id)
        self.assertEqual(obj, obj_ref.user_name)
        self.assertEqual('%s' % obj, obj_ref.basename)
        self.assertEqual(str(obj_id), obj_ref.obj_id)
        self.assertIsNone(obj_ref.tail)
        return obj_ref

    def test_init(self):
        obj_id = UploadId(123.45678)
        obj_ref = self._do_test_init('foo', obj_id.serialize())
        self.assertEqual(
            'foo/%s' % obj_id, str(obj_ref))
        self.assertEqual(
            'foo/%s' % obj_id, obj_ref.serialize())

    def test_init_reserved(self):
        obj_ref = ObjectRef('foo', 'bar', reserved=True)
        self.assertEqual('\x00foo/bar', str(obj_ref))
        self.assertEqual('\x00foo/bar', obj_ref.serialize())

    def test_init_no_obj_id(self):
        obj_ref = ObjectRef('foo')
        self.assertEqual('foo', str(obj_ref))
        self.assertEqual('foo', obj_ref.serialize())

        obj_ref = ObjectRef('foo', reserved=False)
        self.assertEqual('foo', str(obj_ref))
        self.assertEqual('foo', obj_ref.serialize())

    def test_init_utf8_name(self):
        obj_id = UploadId(123.45678)
        obj_ref = self._do_test_init('foo\N{SNOWMAN}', obj_id)
        self.assertEqual(
            'foo\N{SNOWMAN}/%s' % obj_id, str(obj_ref))
        self.assertEqual(
            'foo\N{SNOWMAN}/%s' % obj_id, obj_ref.serialize())
        obj_ref = self._do_test_init('fünicode', obj_id)
        self.assertEqual(
            'fünicode/%s' % obj_id, str(obj_ref))
        self.assertEqual(
            'fünicode/%s' % obj_id, obj_ref.serialize())

    def test_init_with_history_id_instance(self):
        # it's ok to pass in a UploadId instance...
        obj_id = UploadId(123.45678)
        obj_ref = self._do_test_init('foo', obj_id)
        self.assertEqual(
            'foo/%s' % obj_id, str(obj_ref))
        self.assertEqual(
            'foo/%s' % obj_id, obj_ref.serialize())

    def test_init_with_upload_id_instance(self):
        # it's ok to pass in a UploadId instance...
        obj_id = UploadId(123.45678)
        obj_ref = self._do_test_init('foo', obj_id)
        self.assertEqual(
            'foo/0000000123.45678&$', str(obj_ref))
        self.assertEqual(
            'foo/0000000123.45678&$', obj_ref.serialize())

    def test_init_no_object_id(self):
        obj_ref = ObjectRef('foo')
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('foo', obj_ref.basename)
        self.assertIsNone(obj_ref.obj_id)
        self.assertIsNone(obj_ref.tail)
        self.assertEqual('foo', str(obj_ref))

    def test_init_with_tail(self):
        obj_id = UploadId(123.45678)
        obj_ref = ObjectRef('foo', obj_id, '000001')
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('foo', obj_ref.basename)
        self.assertEqual(obj_id, obj_ref.obj_id)
        self.assertEqual('000001', obj_ref.tail)
        self.assertEqual('foo/%s/000001' % obj_id, str(obj_ref))

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
        obj_ref = ObjectRef('foo', UploadId(123.45678))
        clone = obj_ref.clone()
        self.assertEqual(str(obj_ref), str(clone))

    def test_eq(self):
        obj_ref1 = ObjectRef('foo', UploadId(123.45678))
        obj_ref2 = ObjectRef('foo', UploadId(123.45678))
        obj_ref3 = ObjectRef('foo', UploadId(123.99999))
        self.assertEqual(obj_ref1, obj_ref2)
        self.assertEqual(obj_ref1, obj_ref1.clone())
        self.assertNotEqual(obj_ref1, obj_ref3)

    def test_parse(self):
        obj_ref_str = 'foo/0000000123.45678&$'
        obj_ref = ObjectRef.parse(obj_ref_str)
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('foo', obj_ref.basename)
        self.assertEqual(UploadId(123.45678), obj_ref.obj_id)
        self.assertIsNone(obj_ref.tail)
        self.assertEqual(obj_ref_str, str(obj_ref))

    def test_parse_with_tail(self):
        obj_ref_str = 'foo/0000000123.45678&$/000001'
        obj_ref = ObjectRef.parse(obj_ref_str)
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('foo', obj_ref.basename)
        self.assertEqual(UploadId(123.45678), obj_ref.obj_id)
        self.assertEqual('000001', obj_ref.tail)
        self.assertEqual(obj_ref_str, str(obj_ref))

    def test_parse_with_empty_tail(self):
        obj_ref_str = 'foo/0000000123.45678&$/'
        obj_ref = ObjectRef.parse(obj_ref_str)
        self.assertEqual('foo', obj_ref.user_name)
        self.assertEqual('foo', obj_ref.basename)
        self.assertEqual(UploadId(123.45678), obj_ref.obj_id)
        self.assertEqual('', obj_ref.tail)
        self.assertEqual(obj_ref_str, str(obj_ref))

    def test_sort_order(self):
        timestamps = [self.ts() for _ in range(4)]
        obj_ids = [UploadId(ts) for ts in timestamps]
        obj_refs = [ObjectRef('foo', obj_id) for obj_id in obj_ids]
        self.assertEqual(['foo/%s' % obj_id for obj_id in obj_ids],
                         sorted([str(obj_ref) for obj_ref in obj_refs]))

    def test_aligned_sharding(self):
        ts_iter = make_timestamp_iter()
        container_name = 'test-container'
        db_path = os.path.join(mkdtemp(), '%s.db' % container_name)
        broker = ContainerBroker(
            db_path, account='a', container=container_name)
        broker.initialize(next(ts_iter).internal, 0)
        self.assertEqual(([], False), broker.find_shard_ranges(10))  # sanity

        obj_refs_1 = [
            ObjectRef('obj1', UploadId(Timestamp(1)), ''),
            ObjectRef('obj1', UploadId(Timestamp(1)), '000001'),
            ObjectRef('obj1', UploadId(Timestamp(1)), '000002'),
        ]
        obj_refs_2 = [
            ObjectRef('obj2', UploadId(Timestamp(2)), ''),
            ObjectRef('obj2', UploadId(Timestamp(2)), '000001'),
        ]
        obj_refs_3 = [
            ObjectRef('obj3', UploadId(Timestamp(3)), ''),
            ObjectRef('obj3', UploadId(Timestamp(3)), '000001'),
        ]
        obj_names = [
            str(obj_ref)
            for obj_ref in obj_refs_1 + obj_refs_2 + obj_refs_3]
        random.shuffle(obj_names)
        for obj_name in obj_names:
            broker.put_object(
                obj_name, next(ts_iter).internal, 0, 'text/plain', 'etag')

        # sanity check: non-aligned sharding
        ranges, _ = broker.find_shard_ranges(2)
        self.assertEqual([2, 2, 2, 1], [r['object_count'] for r in ranges])
        namespaces = [Namespace(i, r['lower'], r['upper'])
                      for i, r in enumerate(ranges)]
        # obj1 parts split across shards
        self.assertEqual(
            {'index': 0,
             'object_count': 2,
             'lower': '',
             'upper': str(obj_refs_1[1])},
            ranges[0])
        self.assertEqual(
            {'index': 1,
             'object_count': 2,
             'lower': str(obj_refs_1[1]),
             'upper': str(obj_refs_2[0])},
            ranges[1])
        self.assertEqual(
            {'index': 2,
             'object_count': 2,
             'lower': str(obj_refs_2[0]),
             'upper': str(obj_refs_3[0])},
            ranges[2])
        self.assertEqual(
            {'index': 3,
             'object_count': 1,
             'lower': str(obj_refs_3[0]),
             'upper': ''},
            ranges[3])
        self.assertIn(str(obj_refs_1[0]), namespaces[0])
        self.assertIn(str(obj_refs_1[1]), namespaces[0])
        self.assertIn(str(obj_refs_1[2]), namespaces[1])
        # obj2 parts split across shards
        self.assertIn(str(obj_refs_2[0]), namespaces[1])
        self.assertIn(str(obj_refs_2[1]), namespaces[2])
        # obj3 parts split across shards
        self.assertIn(str(obj_refs_3[0]), namespaces[2])
        self.assertIn(str(obj_refs_3[1]), namespaces[3])

        # now with aligned sharding...
        broker.set_sharding_sysmeta('Delimiter', '$')
        ranges, _ = broker.find_shard_ranges(2)
        self.assertEqual([3, 2, 2], [r['object_count'] for r in ranges])
        namespaces = [Namespace(i, r['lower'], r['upper'])
                      for i, r in enumerate(ranges)]
        # obj1 parts in same shard
        self.assertIn(str(obj_refs_1[0]), namespaces[0])
        self.assertIn(str(obj_refs_1[1]), namespaces[0])
        self.assertIn(str(obj_refs_1[2]), namespaces[0])
        # obj2 parts in same shard
        self.assertIn(str(obj_refs_2[0]), namespaces[1])
        self.assertIn(str(obj_refs_2[1]), namespaces[1])
        # obj3 parts in same shard
        self.assertIn(str(obj_refs_3[0]), namespaces[2])
        self.assertIn(str(obj_refs_3[1]), namespaces[2])
