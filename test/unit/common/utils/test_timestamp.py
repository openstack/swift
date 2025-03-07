# Copyright (c) 2010-2023 OpenStack Foundation
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

"""Tests for swift.common.utils.timestamp"""
import random
import time
import unittest

from unittest import mock

from swift.common.utils import timestamp


class TestTimestamp(unittest.TestCase):
    """Tests for swift.common.utils.timestamp.Timestamp"""

    def test_invalid_input(self):
        with self.assertRaises(ValueError):
            timestamp.Timestamp(time.time(), offset=-1)
        with self.assertRaises(ValueError):
            timestamp.Timestamp('123.456_78_90')

    def test_invalid_string_conversion(self):
        t = timestamp.Timestamp.now()
        self.assertRaises(TypeError, str, t)

    def test_offset_limit(self):
        t = 1417462430.78693
        # can't have a offset above MAX_OFFSET
        with self.assertRaises(ValueError):
            timestamp.Timestamp(t, offset=timestamp.MAX_OFFSET + 1)
        # exactly max offset is fine
        ts = timestamp.Timestamp(t, offset=timestamp.MAX_OFFSET)
        self.assertEqual(ts.internal, '1417462430.78693_ffffffffffffffff')
        # but you can't offset it further
        with self.assertRaises(ValueError):
            timestamp.Timestamp(ts.internal, offset=1)
        # unless you start below it
        ts = timestamp.Timestamp(t, offset=timestamp.MAX_OFFSET - 1)
        self.assertEqual(timestamp.Timestamp(ts.internal, offset=1),
                         '1417462430.78693_ffffffffffffffff')

    def test_normal_format_no_offset(self):
        expected = '1402436408.91203'
        test_values = (
            '1402436408.91203',
            '1402436408.91203_00000000',
            '1402436408.912030000',
            '1402436408.912030000_0000000000000',
            '000001402436408.912030000',
            '000001402436408.912030000_0000000000',
            1402436408.91203,
            1402436408.912029,
            1402436408.9120300000000000,
            1402436408.91202999999999999,
            timestamp.Timestamp(1402436408.91203),
            timestamp.Timestamp(1402436408.91203, offset=0),
            timestamp.Timestamp(1402436408.912029),
            timestamp.Timestamp(1402436408.912029, offset=0),
            timestamp.Timestamp('1402436408.91203'),
            timestamp.Timestamp('1402436408.91203', offset=0),
            timestamp.Timestamp('1402436408.91203_00000000'),
            timestamp.Timestamp('1402436408.91203_00000000', offset=0),
        )
        for value in test_values:
            ts = timestamp.Timestamp(value)
            self.assertEqual(ts.normal, expected)
            # timestamp instance can also compare to string or float
            self.assertEqual(ts, expected)
            self.assertEqual(ts, float(expected))
            self.assertEqual(ts, timestamp.normalize_timestamp(expected))

    def test_isoformat(self):
        expected = '2014-06-10T22:47:32.054580'
        test_values = (
            '1402440452.05458',
            '1402440452.054579',
            '1402440452.05458_00000000',
            '1402440452.054579_00000000',
            '1402440452.054580000',
            '1402440452.054579999',
            '1402440452.054580000_0000000000000',
            '1402440452.054579999_0000ff00',
            '000001402440452.054580000',
            '000001402440452.0545799',
            '000001402440452.054580000_0000000000',
            '000001402440452.054579999999_00000fffff',
            1402440452.05458,
            1402440452.054579,
            1402440452.0545800000000000,
            1402440452.054579999,
            timestamp.Timestamp(1402440452.05458),
            timestamp.Timestamp(1402440452.0545799),
            timestamp.Timestamp(1402440452.05458, offset=0),
            timestamp.Timestamp(1402440452.05457999999, offset=0),
            timestamp.Timestamp(1402440452.05458, offset=100),
            timestamp.Timestamp(1402440452.054579, offset=100),
            timestamp.Timestamp('1402440452.05458'),
            timestamp.Timestamp('1402440452.054579999'),
            timestamp.Timestamp('1402440452.05458', offset=0),
            timestamp.Timestamp('1402440452.054579', offset=0),
            timestamp.Timestamp('1402440452.05458', offset=300),
            timestamp.Timestamp('1402440452.05457999', offset=300),
            timestamp.Timestamp('1402440452.05458_00000000'),
            timestamp.Timestamp('1402440452.05457999_00000000'),
            timestamp.Timestamp('1402440452.05458_00000000', offset=0),
            timestamp.Timestamp('1402440452.05457999_00000aaa', offset=0),
            timestamp.Timestamp('1402440452.05458_00000000', offset=400),
            timestamp.Timestamp('1402440452.054579_0a', offset=400),
        )
        for value in test_values:
            self.assertEqual(timestamp.Timestamp(value).isoformat, expected)
        expected = '1970-01-01T00:00:00.000000'
        test_values = (
            '0',
            '0000000000.00000',
            '0000000000.00000_ffffffffffff',
            0,
            0.0,
        )
        for value in test_values:
            self.assertEqual(timestamp.Timestamp(value).isoformat, expected)

    def test_from_isoformat(self):
        ts = timestamp.Timestamp.from_isoformat('2014-06-10T22:47:32.054580')
        self.assertIsInstance(ts, timestamp.Timestamp)
        self.assertEqual(1402440452.05458, float(ts))
        self.assertEqual('2014-06-10T22:47:32.054580', ts.isoformat)

        ts = timestamp.Timestamp.from_isoformat('1970-01-01T00:00:00.000000')
        self.assertIsInstance(ts, timestamp.Timestamp)
        self.assertEqual(0.0, float(ts))
        self.assertEqual('1970-01-01T00:00:00.000000', ts.isoformat)

        ts = timestamp.Timestamp(1402440452.05458)
        self.assertIsInstance(ts, timestamp.Timestamp)
        self.assertEqual(ts, timestamp.Timestamp.from_isoformat(ts.isoformat))

    def test_ceil(self):
        self.assertEqual(0.0, timestamp.Timestamp(0).ceil())
        self.assertEqual(1.0, timestamp.Timestamp(0.00001).ceil())
        self.assertEqual(1.0, timestamp.Timestamp(0.000001).ceil())
        self.assertEqual(12345678.0, timestamp.Timestamp(12345678.0).ceil())
        self.assertEqual(12345679.0,
                         timestamp.Timestamp(12345678.000001).ceil())

    def test_not_equal(self):
        ts = '1402436408.91203_0000000000000001'
        test_values = (
            timestamp.Timestamp('1402436408.91203_0000000000000002'),
            timestamp.Timestamp('1402436408.91203'),
            timestamp.Timestamp(1402436408.91203),
            timestamp.Timestamp(1402436408.91204),
            timestamp.Timestamp(1402436408.91203, offset=0),
            timestamp.Timestamp(1402436408.91203, offset=2),
        )
        for value in test_values:
            self.assertTrue(value != ts)

        self.assertIs(True, timestamp.Timestamp(ts) == ts)  # sanity
        self.assertIs(False,
                      timestamp.Timestamp(ts) != timestamp.Timestamp(ts))
        self.assertIs(False, timestamp.Timestamp(ts) != ts)
        self.assertIs(False, timestamp.Timestamp(ts) is None)
        self.assertIs(True, timestamp.Timestamp(ts) is not None)

    def test_no_force_internal_no_offset(self):
        """Test that internal is the same as normal with no offset"""
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=False):
            self.assertEqual(timestamp.Timestamp(0).internal,
                             '0000000000.00000')
            self.assertEqual(timestamp.Timestamp(1402437380.58186).internal,
                             '1402437380.58186')
            self.assertEqual(timestamp.Timestamp(1402437380.581859).internal,
                             '1402437380.58186')
            self.assertEqual(timestamp.Timestamp(0).internal,
                             timestamp.normalize_timestamp(0))

    def test_no_force_internal_with_offset(self):
        """Test that internal always includes the offset if significant"""
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=False):
            self.assertEqual(timestamp.Timestamp(0, offset=1).internal,
                             '0000000000.00000_0000000000000001')
            self.assertEqual(
                timestamp.Timestamp(1402437380.58186, offset=16).internal,
                '1402437380.58186_0000000000000010')
            self.assertEqual(
                timestamp.Timestamp(1402437380.581859, offset=240).internal,
                '1402437380.58186_00000000000000f0')
            self.assertEqual(
                timestamp.Timestamp('1402437380.581859_00000001',
                                    offset=240).internal,
                '1402437380.58186_00000000000000f1')

    def test_force_internal(self):
        """Test that internal always includes the offset if forced"""
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=True):
            self.assertEqual(timestamp.Timestamp(0).internal,
                             '0000000000.00000_0000000000000000')
            self.assertEqual(timestamp.Timestamp(1402437380.58186).internal,
                             '1402437380.58186_0000000000000000')
            self.assertEqual(timestamp.Timestamp(1402437380.581859).internal,
                             '1402437380.58186_0000000000000000')
            self.assertEqual(timestamp.Timestamp(0, offset=1).internal,
                             '0000000000.00000_0000000000000001')
            self.assertEqual(
                timestamp.Timestamp(1402437380.58186, offset=16).internal,
                '1402437380.58186_0000000000000010')
            self.assertEqual(
                timestamp.Timestamp(1402437380.581859, offset=16).internal,
                '1402437380.58186_0000000000000010')

    def test_internal_format_no_offset(self):
        expected = '1402436408.91203_0000000000000000'
        test_values = (
            '1402436408.91203',
            '1402436408.91203_00000000',
            '1402436408.912030000',
            '1402436408.912030000_0000000000000',
            '000001402436408.912030000',
            '000001402436408.912030000_0000000000',
            1402436408.91203,
            1402436408.9120300000000000,
            1402436408.912029,
            1402436408.912029999999999999,
            timestamp.Timestamp(1402436408.91203),
            timestamp.Timestamp(1402436408.91203, offset=0),
            timestamp.Timestamp(1402436408.912029),
            timestamp.Timestamp(1402436408.91202999999999999, offset=0),
            timestamp.Timestamp('1402436408.91203'),
            timestamp.Timestamp('1402436408.91203', offset=0),
            timestamp.Timestamp('1402436408.912029'),
            timestamp.Timestamp('1402436408.912029', offset=0),
            timestamp.Timestamp('1402436408.912029999999999'),
            timestamp.Timestamp('1402436408.912029999999999', offset=0),
        )
        for value in test_values:
            # timestamp instance is always equivalent
            self.assertEqual(timestamp.Timestamp(value), expected)
            if timestamp.FORCE_INTERNAL:
                # the FORCE_INTERNAL flag makes the internal format always
                # include the offset portion of the timestamp even when it's
                # not significant and would be bad during upgrades
                self.assertEqual(timestamp.Timestamp(value).internal, expected)
            else:
                # unless we FORCE_INTERNAL, when there's no offset the
                # internal format is equivalent to the normalized format
                self.assertEqual(timestamp.Timestamp(value).internal,
                                 '1402436408.91203')

    def test_internal_format_with_offset(self):
        expected = '1402436408.91203_00000000000000f0'
        test_values = (
            '1402436408.91203_000000f0',
            u'1402436408.91203_000000f0',
            b'1402436408.91203_000000f0',
            '1402436408.912030000_0000000000f0',
            '1402436408.912029_000000f0',
            '1402436408.91202999999_0000000000f0',
            '000001402436408.912030000_000000000f0',
            '000001402436408.9120299999_000000000f0',
            timestamp.Timestamp(1402436408.91203, offset=240),
            timestamp.Timestamp(1402436408.912029, offset=240),
            timestamp.Timestamp('1402436408.91203', offset=240),
            timestamp.Timestamp('1402436408.91203_00000000', offset=240),
            timestamp.Timestamp('1402436408.91203_0000000f', offset=225),
            timestamp.Timestamp('1402436408.9120299999', offset=240),
            timestamp.Timestamp('1402436408.9120299999_00000000', offset=240),
            timestamp.Timestamp('1402436408.9120299999_00000010', offset=224),
        )
        for value in test_values:
            ts = timestamp.Timestamp(value)
            self.assertEqual(ts.internal, expected)
            # can compare with offset if the string is internalized
            self.assertEqual(ts, expected)
            # if comparison value only includes the normalized portion and the
            # timestamp includes an offset, it is considered greater
            normal = timestamp.Timestamp(expected).normal
            self.assertTrue(ts > normal,
                            '%r is not bigger than %r given %r' % (
                                ts, normal, value))
            self.assertTrue(ts > float(normal),
                            '%r is not bigger than %f given %r' % (
                                ts, float(normal), value))

    def test_short_format_with_offset(self):
        expected = '1402436408.91203_f0'
        ts = timestamp.Timestamp(1402436408.91203, 0xf0)
        self.assertEqual(expected, ts.short)

        expected = '1402436408.91203'
        ts = timestamp.Timestamp(1402436408.91203)
        self.assertEqual(expected, ts.short)

    def test_raw(self):
        expected = 140243640891203
        ts = timestamp.Timestamp(1402436408.91203)
        self.assertEqual(expected, ts.raw)

        # 'raw' does not include offset
        ts = timestamp.Timestamp(1402436408.91203, 0xf0)
        self.assertEqual(expected, ts.raw)

    def test_delta(self):
        def _assertWithinBounds(expected, timestamp):
            tolerance = 0.00001
            minimum = expected - tolerance
            maximum = expected + tolerance
            self.assertTrue(float(timestamp) > minimum)
            self.assertTrue(float(timestamp) < maximum)

        ts = timestamp.Timestamp(1402436408.91203, delta=100)
        _assertWithinBounds(1402436408.91303, ts)
        self.assertEqual(140243640891303, ts.raw)

        ts = timestamp.Timestamp(1402436408.91203, delta=-100)
        _assertWithinBounds(1402436408.91103, ts)
        self.assertEqual(140243640891103, ts.raw)

        ts = timestamp.Timestamp(1402436408.91203, delta=0)
        _assertWithinBounds(1402436408.91203, ts)
        self.assertEqual(140243640891203, ts.raw)

        # delta is independent of offset
        ts = timestamp.Timestamp(1402436408.91203, offset=42, delta=100)
        self.assertEqual(140243640891303, ts.raw)
        self.assertEqual(42, ts.offset)

        # cannot go negative
        self.assertRaises(ValueError, timestamp.Timestamp, 1402436408.91203,
                          delta=-140243640891203)

    def test_int(self):
        expected = 1402437965
        test_values = (
            '1402437965.91203',
            '1402437965.91203_00000000',
            '1402437965.912030000',
            '1402437965.912030000_0000000000000',
            '000001402437965.912030000',
            '000001402437965.912030000_0000000000',
            1402437965.91203,
            1402437965.9120300000000000,
            1402437965.912029,
            1402437965.912029999999999999,
            timestamp.Timestamp(1402437965.91203),
            timestamp.Timestamp(1402437965.91203, offset=0),
            timestamp.Timestamp(1402437965.91203, offset=500),
            timestamp.Timestamp(1402437965.912029),
            timestamp.Timestamp(1402437965.91202999999999999, offset=0),
            timestamp.Timestamp(1402437965.91202999999999999, offset=300),
            timestamp.Timestamp('1402437965.91203'),
            timestamp.Timestamp('1402437965.91203', offset=0),
            timestamp.Timestamp('1402437965.91203', offset=400),
            timestamp.Timestamp('1402437965.912029'),
            timestamp.Timestamp('1402437965.912029', offset=0),
            timestamp.Timestamp('1402437965.912029', offset=200),
            timestamp.Timestamp('1402437965.912029999999999'),
            timestamp.Timestamp('1402437965.912029999999999', offset=0),
            timestamp.Timestamp('1402437965.912029999999999', offset=100),
        )
        for value in test_values:
            ts = timestamp.Timestamp(value)
            self.assertEqual(int(ts), expected)
            self.assertTrue(ts > expected)

    def test_float(self):
        expected = 1402438115.91203
        test_values = (
            '1402438115.91203',
            '1402438115.91203_00000000',
            '1402438115.912030000',
            '1402438115.912030000_0000000000000',
            '000001402438115.912030000',
            '000001402438115.912030000_0000000000',
            1402438115.91203,
            1402438115.9120300000000000,
            1402438115.912029,
            1402438115.912029999999999999,
            timestamp.Timestamp(1402438115.91203),
            timestamp.Timestamp(1402438115.91203, offset=0),
            timestamp.Timestamp(1402438115.91203, offset=500),
            timestamp.Timestamp(1402438115.912029),
            timestamp.Timestamp(1402438115.91202999999999999, offset=0),
            timestamp.Timestamp(1402438115.91202999999999999, offset=300),
            timestamp.Timestamp('1402438115.91203'),
            timestamp.Timestamp('1402438115.91203', offset=0),
            timestamp.Timestamp('1402438115.91203', offset=400),
            timestamp.Timestamp('1402438115.912029'),
            timestamp.Timestamp('1402438115.912029', offset=0),
            timestamp.Timestamp('1402438115.912029', offset=200),
            timestamp.Timestamp('1402438115.912029999999999'),
            timestamp.Timestamp('1402438115.912029999999999', offset=0),
            timestamp.Timestamp('1402438115.912029999999999', offset=100),
        )
        tolerance = 0.00001
        minimum = expected - tolerance
        maximum = expected + tolerance
        for value in test_values:
            ts = timestamp.Timestamp(value)
            self.assertTrue(float(ts) > minimum,
                            '%f is not bigger than %f given %r' % (
                                ts, minimum, value))
            self.assertTrue(float(ts) < maximum,
                            '%f is not smaller than %f given %r' % (
                                ts, maximum, value))
            # direct comparison of timestamp works too
            self.assertTrue(ts > minimum,
                            '%s is not bigger than %f given %r' % (
                                ts.normal, minimum, value))
            self.assertTrue(ts < maximum,
                            '%s is not smaller than %f given %r' % (
                                ts.normal, maximum, value))
            # ... even against strings
            self.assertTrue(ts > '%f' % minimum,
                            '%s is not bigger than %s given %r' % (
                                ts.normal, minimum, value))
            self.assertTrue(ts < '%f' % maximum,
                            '%s is not smaller than %s given %r' % (
                                ts.normal, maximum, value))

    def test_false(self):
        self.assertFalse(timestamp.Timestamp(0))
        self.assertFalse(timestamp.Timestamp(0, offset=0))
        self.assertFalse(timestamp.Timestamp('0'))
        self.assertFalse(timestamp.Timestamp('0', offset=0))
        self.assertFalse(timestamp.Timestamp(0.0))
        self.assertFalse(timestamp.Timestamp(0.0, offset=0))
        self.assertFalse(timestamp.Timestamp('0.0'))
        self.assertFalse(timestamp.Timestamp('0.0', offset=0))
        self.assertFalse(timestamp.Timestamp(00000000.00000000))
        self.assertFalse(timestamp.Timestamp(00000000.00000000, offset=0))
        self.assertFalse(timestamp.Timestamp('00000000.00000000'))
        self.assertFalse(timestamp.Timestamp('00000000.00000000', offset=0))

    def test_true(self):
        self.assertTrue(timestamp.Timestamp(1))
        self.assertTrue(timestamp.Timestamp(1, offset=1))
        self.assertTrue(timestamp.Timestamp(0, offset=1))
        self.assertTrue(timestamp.Timestamp('1'))
        self.assertTrue(timestamp.Timestamp('1', offset=1))
        self.assertTrue(timestamp.Timestamp('0', offset=1))
        self.assertTrue(timestamp.Timestamp(1.1))
        self.assertTrue(timestamp.Timestamp(1.1, offset=1))
        self.assertTrue(timestamp.Timestamp(0.0, offset=1))
        self.assertTrue(timestamp.Timestamp('1.1'))
        self.assertTrue(timestamp.Timestamp('1.1', offset=1))
        self.assertTrue(timestamp.Timestamp('0.0', offset=1))
        self.assertTrue(timestamp.Timestamp(11111111.11111111))
        self.assertTrue(timestamp.Timestamp(11111111.11111111, offset=1))
        self.assertTrue(timestamp.Timestamp(00000000.00000000, offset=1))
        self.assertTrue(timestamp.Timestamp('11111111.11111111'))
        self.assertTrue(timestamp.Timestamp('11111111.11111111', offset=1))
        self.assertTrue(timestamp.Timestamp('00000000.00000000', offset=1))

    def test_greater_no_offset(self):
        now = time.time()
        older = now - 1
        ts = timestamp.Timestamp(now)
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443112.213252, '1402443112.213252', '1402443112.213252_ffff',
            older, '%f' % older, '%f_0000ffff' % older,
        )
        for value in test_values:
            other = timestamp.Timestamp(value)
            self.assertNotEqual(ts, other)  # sanity
            self.assertTrue(ts > value,
                            '%r is not greater than %r given %r' % (
                                ts, value, value))
            self.assertTrue(ts > other,
                            '%r is not greater than %r given %r' % (
                                ts, other, value))
            self.assertTrue(ts > other.normal,
                            '%r is not greater than %r given %r' % (
                                ts, other.normal, value))
            self.assertTrue(ts > other.internal,
                            '%r is not greater than %r given %r' % (
                                ts, other.internal, value))
            self.assertTrue(ts > float(other),
                            '%r is not greater than %r given %r' % (
                                ts, float(other), value))
            self.assertTrue(ts > int(other),
                            '%r is not greater than %r given %r' % (
                                ts, int(other), value))

    def _test_greater_with_offset(self, now, test_values):
        for offset in range(1, 1000, 100):
            ts = timestamp.Timestamp(now, offset=offset)
            for value in test_values:
                other = timestamp.Timestamp(value)
                self.assertNotEqual(ts, other)  # sanity
                self.assertTrue(ts > value,
                                '%r is not greater than %r given %r' % (
                                    ts, value, value))
                self.assertTrue(ts > other,
                                '%r is not greater than %r given %r' % (
                                    ts, other, value))
                self.assertTrue(ts > other.normal,
                                '%r is not greater than %r given %r' % (
                                    ts, other.normal, value))
                self.assertTrue(ts > other.internal,
                                '%r is not greater than %r given %r' % (
                                    ts, other.internal, value))
                self.assertTrue(ts > float(other),
                                '%r is not greater than %r given %r' % (
                                    ts, float(other), value))
                self.assertTrue(ts > int(other),
                                '%r is not greater than %r given %r' % (
                                    ts, int(other), value))

    def test_greater_with_offset(self):
        # Part 1: use the natural time of the Python. This is deliciously
        # unpredictable, but completely legitimate and realistic. Finds bugs!
        now = time.time()
        older = now - 1
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443346.935174, '1402443346.93517', '1402443346.935169_ffff',
            older, now,
        )
        self._test_greater_with_offset(now, test_values)
        # Part 2: Same as above, but with fixed time values that reproduce
        # specific corner cases.
        now = 1519830570.6949348
        older = now - 1
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443346.935174, '1402443346.93517', '1402443346.935169_ffff',
            older, now,
        )
        self._test_greater_with_offset(now, test_values)
        # Part 3: The '%f' problem. Timestamps cannot be converted to %f
        # strings, then back to timestamps, then compared with originals.
        # You can only "import" a floating point representation once.
        now = 1519830570.6949348
        now = float('%f' % now)
        older = now - 1
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            older, '%f' % older, '%f_0000ffff' % older,
            now, '%f' % now, '%s_00000000' % now,
        )
        self._test_greater_with_offset(now, test_values)

    def test_smaller_no_offset(self):
        now = time.time()
        newer = now + 1
        ts = timestamp.Timestamp(now)
        test_values = (
            9999999999.99999, '9999999999.99999', '9999999999.99999_ffff',
            newer, '%f' % newer, '%f_0000ffff' % newer,
        )
        for value in test_values:
            other = timestamp.Timestamp(value)
            self.assertNotEqual(ts, other)  # sanity
            self.assertTrue(ts < value,
                            '%r is not smaller than %r given %r' % (
                                ts, value, value))
            self.assertTrue(ts < other,
                            '%r is not smaller than %r given %r' % (
                                ts, other, value))
            self.assertTrue(ts < other.normal,
                            '%r is not smaller than %r given %r' % (
                                ts, other.normal, value))
            self.assertTrue(ts < other.internal,
                            '%r is not smaller than %r given %r' % (
                                ts, other.internal, value))
            self.assertTrue(ts < float(other),
                            '%r is not smaller than %r given %r' % (
                                ts, float(other), value))
            self.assertTrue(ts < int(other),
                            '%r is not smaller than %r given %r' % (
                                ts, int(other), value))

    def test_smaller_with_offset(self):
        now = time.time()
        newer = now + 1
        test_values = (
            9999999999.99999, '9999999999.99999', '9999999999.99999_ffff',
            newer, '%f' % newer, '%f_0000ffff' % newer,
        )
        for offset in range(1, 1000, 100):
            ts = timestamp.Timestamp(now, offset=offset)
            for value in test_values:
                other = timestamp.Timestamp(value)
                self.assertNotEqual(ts, other)  # sanity
                self.assertTrue(ts < value,
                                '%r is not smaller than %r given %r' % (
                                    ts, value, value))
                self.assertTrue(ts < other,
                                '%r is not smaller than %r given %r' % (
                                    ts, other, value))
                self.assertTrue(ts < other.normal,
                                '%r is not smaller than %r given %r' % (
                                    ts, other.normal, value))
                self.assertTrue(ts < other.internal,
                                '%r is not smaller than %r given %r' % (
                                    ts, other.internal, value))
                self.assertTrue(ts < float(other),
                                '%r is not smaller than %r given %r' % (
                                    ts, float(other), value))
                self.assertTrue(ts < int(other),
                                '%r is not smaller than %r given %r' % (
                                    ts, int(other), value))

    def test_cmp_with_none(self):
        self.assertGreater(timestamp.Timestamp(0), None)
        self.assertGreater(timestamp.Timestamp(1.0), None)
        self.assertGreater(timestamp.Timestamp(1.0, 42), None)

    def test_ordering(self):
        given = [
            '1402444820.62590_000000000000000a',
            '1402444820.62589_0000000000000001',
            '1402444821.52589_0000000000000004',
            '1402444920.62589_0000000000000004',
            '1402444821.62589_000000000000000a',
            '1402444821.72589_000000000000000a',
            '1402444920.62589_0000000000000002',
            '1402444820.62589_0000000000000002',
            '1402444820.62589_000000000000000a',
            '1402444820.62590_0000000000000004',
            '1402444920.62589_000000000000000a',
            '1402444820.62590_0000000000000002',
            '1402444821.52589_0000000000000002',
            '1402444821.52589_0000000000000000',
            '1402444920.62589',
            '1402444821.62589_0000000000000004',
            '1402444821.72589_0000000000000001',
            '1402444820.62590',
            '1402444820.62590_0000000000000001',
            '1402444820.62589_0000000000000004',
            '1402444821.72589_0000000000000000',
            '1402444821.52589_000000000000000a',
            '1402444821.72589_0000000000000004',
            '1402444821.62589',
            '1402444821.52589_0000000000000001',
            '1402444821.62589_0000000000000001',
            '1402444821.62589_0000000000000002',
            '1402444821.72589_0000000000000002',
            '1402444820.62589',
            '1402444920.62589_0000000000000001']
        expected = [
            '1402444820.62589',
            '1402444820.62589_0000000000000001',
            '1402444820.62589_0000000000000002',
            '1402444820.62589_0000000000000004',
            '1402444820.62589_000000000000000a',
            '1402444820.62590',
            '1402444820.62590_0000000000000001',
            '1402444820.62590_0000000000000002',
            '1402444820.62590_0000000000000004',
            '1402444820.62590_000000000000000a',
            '1402444821.52589',
            '1402444821.52589_0000000000000001',
            '1402444821.52589_0000000000000002',
            '1402444821.52589_0000000000000004',
            '1402444821.52589_000000000000000a',
            '1402444821.62589',
            '1402444821.62589_0000000000000001',
            '1402444821.62589_0000000000000002',
            '1402444821.62589_0000000000000004',
            '1402444821.62589_000000000000000a',
            '1402444821.72589',
            '1402444821.72589_0000000000000001',
            '1402444821.72589_0000000000000002',
            '1402444821.72589_0000000000000004',
            '1402444821.72589_000000000000000a',
            '1402444920.62589',
            '1402444920.62589_0000000000000001',
            '1402444920.62589_0000000000000002',
            '1402444920.62589_0000000000000004',
            '1402444920.62589_000000000000000a',
        ]
        # less visual version
        """
        now = time.time()
        given = [
            timestamp.Timestamp(now + i, offset=offset).internal
            for i in (0, 0.00001, 0.9, 1.0, 1.1, 100.0)
            for offset in (0, 1, 2, 4, 10)
        ]
        expected = [t for t in given]
        random.shuffle(given)
        """
        self.assertEqual(len(given), len(expected))  # sanity
        timestamps = [timestamp.Timestamp(t) for t in given]
        # our expected values don't include insignificant offsets
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=False):
            self.assertEqual(
                [t.internal for t in sorted(timestamps)], expected)
            # string sorting works as well
            self.assertEqual(
                sorted([t.internal for t in timestamps]), expected)

    def test_hashable(self):
        ts_0 = timestamp.Timestamp('1402444821.72589')
        ts_0_also = timestamp.Timestamp('1402444821.72589')
        self.assertEqual(ts_0, ts_0_also)  # sanity
        self.assertEqual(hash(ts_0), hash(ts_0_also))
        d = {ts_0: 'whatever'}
        self.assertIn(ts_0, d)  # sanity
        self.assertIn(ts_0_also, d)

    def test_out_of_range_comparisons(self):
        now = timestamp.Timestamp.now()

        def check_is_later(val):
            self.assertTrue(now != val)
            self.assertFalse(now == val)
            self.assertTrue(now <= val)
            self.assertTrue(now < val)
            self.assertTrue(val > now)
            self.assertTrue(val >= now)

        check_is_later(1e30)
        check_is_later(1579753284000)  # someone gave us ms instead of s!
        check_is_later('1579753284000')
        check_is_later(b'1e15')
        check_is_later(u'1.e+10_f')

        def check_is_earlier(val):
            self.assertTrue(now != val)
            self.assertFalse(now == val)
            self.assertTrue(now >= val)
            self.assertTrue(now > val)
            self.assertTrue(val < now)
            self.assertTrue(val <= now)

        check_is_earlier(-1)
        check_is_earlier(-0.1)
        check_is_earlier('-9999999')
        check_is_earlier(b'-9999.999')
        check_is_earlier(u'-1234_5678')

    def test_inversion(self):
        ts = timestamp.Timestamp(0)
        self.assertIsInstance(~ts, timestamp.Timestamp)
        self.assertEqual((~ts).internal, '9999999999.99999')

        ts = timestamp.Timestamp(123456.789)
        self.assertIsInstance(~ts, timestamp.Timestamp)
        self.assertEqual(ts.internal, '0000123456.78900')
        self.assertEqual((~ts).internal, '9999876543.21099')

        timestamps = sorted(timestamp.Timestamp(random.random() * 1e10)
                            for _ in range(20))
        self.assertEqual([x.internal for x in timestamps],
                         sorted(x.internal for x in timestamps))
        self.assertEqual([(~x).internal for x in reversed(timestamps)],
                         sorted((~x).internal for x in timestamps))

        ts = timestamp.Timestamp.now()
        self.assertGreater(~ts, ts)  # NB: will break around 2128

        ts = timestamp.Timestamp.now(offset=1)
        with self.assertRaises(ValueError) as caught:
            ~ts
        self.assertEqual(caught.exception.args[0],
                         'Cannot invert timestamps with offsets')


class TestTimestampEncoding(unittest.TestCase):

    def setUp(self):
        t0 = timestamp.Timestamp(0.0)
        t1 = timestamp.Timestamp(997.9996)
        t2 = timestamp.Timestamp(999)
        t3 = timestamp.Timestamp(1000, 24)
        t4 = timestamp.Timestamp(1001)
        t5 = timestamp.Timestamp(1002.00040)

        # encodings that are expected when explicit = False
        self.non_explicit_encodings = (
            ('0000001000.00000_18', (t3, t3, t3)),
            ('0000001000.00000_18', (t3, t3, None)),
        )

        # mappings that are expected when explicit = True
        self.explicit_encodings = (
            ('0000001000.00000_18+0+0', (t3, t3, t3)),
            ('0000001000.00000_18+0', (t3, t3, None)),
        )

        # mappings that are expected when explicit = True or False
        self.encodings = (
            ('0000001000.00000_18+0+186a0', (t3, t3, t4)),
            ('0000001000.00000_18+186a0+186c8', (t3, t4, t5)),
            ('0000001000.00000_18-186a0+0', (t3, t2, t2)),
            ('0000001000.00000_18+0-186a0', (t3, t3, t2)),
            ('0000001000.00000_18-186a0-186c8', (t3, t2, t1)),
            ('0000001000.00000_18', (t3, None, None)),
            ('0000001000.00000_18+186a0', (t3, t4, None)),
            ('0000001000.00000_18-186a0', (t3, t2, None)),
            ('0000001000.00000_18', (t3, None, t1)),
            ('0000001000.00000_18-5f5e100', (t3, t0, None)),
            ('0000001000.00000_18+0-5f5e100', (t3, t3, t0)),
            ('0000001000.00000_18-5f5e100+5f45a60', (t3, t0, t2)),
        )

        # decodings that are expected when explicit = False
        self.non_explicit_decodings = (
            ('0000001000.00000_18', (t3, t3, t3)),
            ('0000001000.00000_18+186a0', (t3, t4, t4)),
            ('0000001000.00000_18-186a0', (t3, t2, t2)),
            ('0000001000.00000_18+186a0', (t3, t4, t4)),
            ('0000001000.00000_18-186a0', (t3, t2, t2)),
            ('0000001000.00000_18-5f5e100', (t3, t0, t0)),
        )

        # decodings that are expected when explicit = True
        self.explicit_decodings = (
            ('0000001000.00000_18+0+0', (t3, t3, t3)),
            ('0000001000.00000_18+0', (t3, t3, None)),
            ('0000001000.00000_18', (t3, None, None)),
            ('0000001000.00000_18+186a0', (t3, t4, None)),
            ('0000001000.00000_18-186a0', (t3, t2, None)),
            ('0000001000.00000_18-5f5e100', (t3, t0, None)),
        )

        # decodings that are expected when explicit = True or False
        self.decodings = (
            ('0000001000.00000_18+0+186a0', (t3, t3, t4)),
            ('0000001000.00000_18+186a0+186c8', (t3, t4, t5)),
            ('0000001000.00000_18-186a0+0', (t3, t2, t2)),
            ('0000001000.00000_18+0-186a0', (t3, t3, t2)),
            ('0000001000.00000_18-186a0-186c8', (t3, t2, t1)),
            ('0000001000.00000_18-5f5e100+5f45a60', (t3, t0, t2)),
        )

    def _assertEqual(self, expected, actual, test):
        self.assertEqual(expected, actual,
                         'Got %s but expected %s for parameters %s'
                         % (actual, expected, test))

    def test_encoding(self):
        for test in self.explicit_encodings:
            actual = timestamp.encode_timestamps(test[1][0], test[1][1],
                                                 test[1][2], True)
            self._assertEqual(test[0], actual, test[1])
        for test in self.non_explicit_encodings:
            actual = timestamp.encode_timestamps(test[1][0], test[1][1],
                                                 test[1][2], False)
            self._assertEqual(test[0], actual, test[1])
        for explicit in (True, False):
            for test in self.encodings:
                actual = timestamp.encode_timestamps(test[1][0], test[1][1],
                                                     test[1][2], explicit)
                self._assertEqual(test[0], actual, test[1])

    def test_decoding(self):
        for test in self.explicit_decodings:
            actual = timestamp.decode_timestamps(test[0], True)
            self._assertEqual(test[1], actual, test[0])
        for test in self.non_explicit_decodings:
            actual = timestamp.decode_timestamps(test[0], False)
            self._assertEqual(test[1], actual, test[0])
        for explicit in (True, False):
            for test in self.decodings:
                actual = timestamp.decode_timestamps(test[0], explicit)
                self._assertEqual(test[1], actual, test[0])
