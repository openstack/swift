# Copyright (c) 2010-2013 OpenStack, LLC.
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

"""Tests for swift.common.ondisk"""

from __future__ import with_statement
from test.unit import temptree

import os

import unittest

from swift.common import ondisk


class TestOndisk(unittest.TestCase):
    """Tests for swift.common.ondisk"""

    def setUp(self):
        ondisk.HASH_PATH_SUFFIX = 'endcap'
        ondisk.HASH_PATH_PREFIX = 'startcap'

    def test_normalize_timestamp(self):
        # Test swift.common.ondisk.normalize_timestamp
        self.assertEquals(ondisk.normalize_timestamp('1253327593.48174'),
                          "1253327593.48174")
        self.assertEquals(ondisk.normalize_timestamp(1253327593.48174),
                          "1253327593.48174")
        self.assertEquals(ondisk.normalize_timestamp('1253327593.48'),
                          "1253327593.48000")
        self.assertEquals(ondisk.normalize_timestamp(1253327593.48),
                          "1253327593.48000")
        self.assertEquals(ondisk.normalize_timestamp('253327593.48'),
                          "0253327593.48000")
        self.assertEquals(ondisk.normalize_timestamp(253327593.48),
                          "0253327593.48000")
        self.assertEquals(ondisk.normalize_timestamp('1253327593'),
                          "1253327593.00000")
        self.assertEquals(ondisk.normalize_timestamp(1253327593),
                          "1253327593.00000")
        self.assertRaises(ValueError, ondisk.normalize_timestamp, '')
        self.assertRaises(ValueError, ondisk.normalize_timestamp, 'abc')

    def test_validate_device_partition(self):
        # Test swift.common.ondisk.validate_device_partition
        ondisk.validate_device_partition('foo', 'bar')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, '', '')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, '', 'foo')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, 'foo', '')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, 'foo/bar', 'foo')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, 'foo', 'foo/bar')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, '.', 'foo')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, '..', 'foo')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, 'foo', '.')
        self.assertRaises(ValueError,
                          ondisk.validate_device_partition, 'foo', '..')
        try:
            ondisk.validate_device_partition('o\nn e', 'foo')
        except ValueError as err:
            self.assertEquals(str(err), 'Invalid device: o%0An%20e')
        try:
            ondisk.validate_device_partition('foo', 'o\nn e')
        except ValueError as err:
            self.assertEquals(str(err), 'Invalid partition: o%0An%20e')

    def test_storage_directory(self):
        self.assertEquals(ondisk.storage_directory('objects', '1', 'ABCDEF'),
                          'objects/1/DEF/ABCDEF')

    def test_hash_path(self):
        _prefix = ondisk.HASH_PATH_PREFIX
        ondisk.HASH_PATH_PREFIX = ''
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results hash_path produces, they know it
        try:
            self.assertEquals(ondisk.hash_path('a'),
                              '1c84525acb02107ea475dcd3d09c2c58')
            self.assertEquals(ondisk.hash_path('a', 'c'),
                              '33379ecb053aa5c9e356c68997cbb59e')
            self.assertEquals(ondisk.hash_path('a', 'c', 'o'),
                              '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEquals(
                ondisk.hash_path('a', 'c', 'o', raw_digest=False),
                '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEquals(ondisk.hash_path('a', 'c', 'o', raw_digest=True),
                              '\x06\xfb\xf0\xb5\x14\xe5\x19\x9d\xfcN'
                              '\x00\xf4.\xb5\xea\x83')
            self.assertRaises(ValueError, ondisk.hash_path, 'a', object='o')
            ondisk.HASH_PATH_PREFIX = 'abcdef'
            self.assertEquals(
                ondisk.hash_path('a', 'c', 'o', raw_digest=False),
                '363f9b535bfb7d17a43a46a358afca0e')
        finally:
            ondisk.HASH_PATH_PREFIX = _prefix


class TestAuditLocationGenerator(unittest.TestCase):
    def test_non_dir_contents(self):
        with temptree([]) as tmpdir:
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            with open(os.path.join(data, "partition1"), "w"):
                pass
            partition = os.path.join(data, "partition2")
            os.makedirs(partition)
            with open(os.path.join(partition, "suffix1"), "w"):
                pass
            suffix = os.path.join(partition, "suffix2")
            os.makedirs(suffix)
            with open(os.path.join(suffix, "hash1"), "w"):
                pass
            locations = ondisk.audit_location_generator(
                tmpdir, "data", mount_check=False
            )
            self.assertEqual(list(locations), [])


if __name__ == '__main__':
    unittest.main()
