# Copyright (c) 2021 NVIDIA
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
import unittest
from unittest import mock
from time import time

from swift.common.error_limiter import ErrorLimiter
from test.unit import FakeRing


class TestErrorLimiter(unittest.TestCase):
    def setUp(self):
        self.ring = FakeRing()

    def test_init_config(self):
        config = {'suppression_interval': 100.9,
                  'suppression_limit': 5}
        limiter = ErrorLimiter(**config)
        self.assertEqual(limiter.suppression_interval, 100.9)
        self.assertEqual(limiter.suppression_limit, 5)

        config = {'suppression_interval': '100.9',
                  'suppression_limit': '5'}
        limiter = ErrorLimiter(**config)
        self.assertEqual(limiter.suppression_interval, 100.9)
        self.assertEqual(limiter.suppression_limit, 5)

    def test_init_bad_config(self):
        with self.assertRaises(ValueError):
            ErrorLimiter(suppression_interval='bad',
                         suppression_limit=1)

        with self.assertRaises(TypeError):
            ErrorLimiter(suppression_interval=None,
                         suppression_limit=1)

        with self.assertRaises(ValueError):
            ErrorLimiter(suppression_interval=0,
                         suppression_limit='bad')

        with self.assertRaises(TypeError):
            ErrorLimiter(suppression_interval=0,
                         suppression_limit=None)

    def test_is_limited(self):
        node = self.ring.devs[-1]
        limiter = ErrorLimiter(suppression_interval=60, suppression_limit=10)

        now = time()
        with mock.patch('swift.common.error_limiter.time', return_value=now):
            self.assertFalse(limiter.is_limited(node))
            limiter.limit(node)
            self.assertTrue(limiter.is_limited(node))
            node_key = limiter.node_key(node)
            self.assertEqual(limiter.stats.get(node_key),
                             {'errors': limiter.suppression_limit + 1,
                              'last_error': now})

    def test_increment(self):
        node = self.ring.devs[-1]
        limiter = ErrorLimiter(suppression_interval=60, suppression_limit=10)
        node_key = limiter.node_key(node)
        for i in range(limiter.suppression_limit):
            self.assertFalse(limiter.increment(node))
            self.assertEqual(i + 1, limiter.stats.get(node_key)['errors'])
            self.assertFalse(limiter.is_limited(node))

        # A few more to make sure it is > suppression_limit
        for i in range(1, 4):
            self.assertTrue(limiter.increment(node))
            self.assertEqual(limiter.suppression_limit + i,
                             limiter.stats.get(node_key)['errors'])
            self.assertTrue(limiter.is_limited(node))

        # Simulate time with no errors have gone by.
        last_time = limiter.stats.get(node_key)['last_error']
        now = last_time + limiter.suppression_interval + 1
        with mock.patch('swift.common.error_limiter.time',
                        return_value=now):
            self.assertFalse(limiter.is_limited(node))
            self.assertFalse(limiter.stats.get(node_key))

    def test_node_key(self):
        limiter = ErrorLimiter(suppression_interval=60, suppression_limit=10)
        node = self.ring.devs[0]
        expected = '%s:%s/%s' % (node['ip'], node['port'], node['device'])
        self.assertEqual(expected, limiter.node_key(node))
