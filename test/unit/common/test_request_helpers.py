# Copyright (c) 2010-2012 OpenStack Foundation
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

"""Tests for swift.common.request_helpers"""

import unittest
from swift.common.swob import Request
from swift.common.request_helpers import is_sys_meta, is_user_meta, \
    is_sys_or_user_meta, strip_sys_meta_prefix, strip_user_meta_prefix, \
    remove_items, copy_header_subset

server_types = ['account', 'container', 'object']


class TestRequestHelpers(unittest.TestCase):
    def test_is_user_meta(self):
        m_type = 'meta'
        for st in server_types:
            self.assertTrue(is_user_meta(st, 'x-%s-%s-foo' % (st, m_type)))
            self.assertFalse(is_user_meta(st, 'x-%s-%s-' % (st, m_type)))
            self.assertFalse(is_user_meta(st, 'x-%s-%sfoo' % (st, m_type)))

    def test_is_sys_meta(self):
        m_type = 'sysmeta'
        for st in server_types:
            self.assertTrue(is_sys_meta(st, 'x-%s-%s-foo' % (st, m_type)))
            self.assertFalse(is_sys_meta(st, 'x-%s-%s-' % (st, m_type)))
            self.assertFalse(is_sys_meta(st, 'x-%s-%sfoo' % (st, m_type)))

    def test_is_sys_or_user_meta(self):
        m_types = ['sysmeta', 'meta']
        for mt in m_types:
            for st in server_types:
                self.assertTrue(is_sys_or_user_meta(st, 'x-%s-%s-foo'
                                                    % (st, mt)))
                self.assertFalse(is_sys_or_user_meta(st, 'x-%s-%s-'
                                                     % (st, mt)))
                self.assertFalse(is_sys_or_user_meta(st, 'x-%s-%sfoo'
                                                     % (st, mt)))

    def test_strip_sys_meta_prefix(self):
        mt = 'sysmeta'
        for st in server_types:
            self.assertEquals(strip_sys_meta_prefix(st, 'x-%s-%s-a'
                                                    % (st, mt)), 'a')

    def test_strip_user_meta_prefix(self):
        mt = 'meta'
        for st in server_types:
            self.assertEquals(strip_user_meta_prefix(st, 'x-%s-%s-a'
                                                     % (st, mt)), 'a')

    def test_remove_items(self):
        src = {'a': 'b',
               'c': 'd'}
        test = lambda x: x == 'a'
        rem = remove_items(src, test)
        self.assertEquals(src, {'c': 'd'})
        self.assertEquals(rem, {'a': 'b'})

    def test_copy_header_subset(self):
        src = {'a': 'b',
               'c': 'd'}
        from_req = Request.blank('/path', environ={}, headers=src)
        to_req = Request.blank('/path', {})
        test = lambda x: x.lower() == 'a'
        copy_header_subset(from_req, to_req, test)
        self.assertTrue('A' in to_req.headers)
        self.assertEqual(to_req.headers['A'], 'b')
        self.assertFalse('c' in to_req.headers)
        self.assertFalse('C' in to_req.headers)
