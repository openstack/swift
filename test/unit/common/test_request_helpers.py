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
from swift.common.swob import Request, HTTPException
from swift.common.storage_policy import POLICIES, EC_POLICY, REPL_POLICY
from swift.common.request_helpers import is_sys_meta, is_user_meta, \
    is_sys_or_user_meta, strip_sys_meta_prefix, strip_user_meta_prefix, \
    remove_items, copy_header_subset, get_name_and_placement

from test.unit import patch_policies

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

    @patch_policies(with_ec_default=True)
    def test_get_name_and_placement_object_req(self):
        path = '/device/part/account/container/object'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '0'})
        device, part, account, container, obj, policy = \
            get_name_and_placement(req, 5, 5, True)
        self.assertEqual(device, 'device')
        self.assertEqual(part, 'part')
        self.assertEqual(account, 'account')
        self.assertEqual(container, 'container')
        self.assertEqual(obj, 'object')
        self.assertEqual(policy, POLICIES[0])
        self.assertEqual(policy.policy_type, EC_POLICY)

        req.headers['X-Backend-Storage-Policy-Index'] = 1
        device, part, account, container, obj, policy = \
            get_name_and_placement(req, 5, 5, True)
        self.assertEqual(device, 'device')
        self.assertEqual(part, 'part')
        self.assertEqual(account, 'account')
        self.assertEqual(container, 'container')
        self.assertEqual(obj, 'object')
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)

        req.headers['X-Backend-Storage-Policy-Index'] = 'foo'
        try:
            device, part, account, container, obj, policy = \
                get_name_and_placement(req, 5, 5, True)
        except HTTPException as e:
            self.assertEqual(e.status_int, 503)
            self.assertEqual(str(e), '503 Service Unavailable')
            self.assertEqual(e.body, "No policy with index foo")
        else:
            self.fail('get_name_and_placement did not raise error '
                      'for invalid storage policy index')

    @patch_policies(with_ec_default=True)
    def test_get_name_and_placement_object_replication(self):
        # yup, suffixes are sent '-'.joined in the path
        path = '/device/part/012-345-678-9ab-cde'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '0'})
        device, partition, suffix_parts, policy = \
            get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertEqual(suffix_parts, '012-345-678-9ab-cde')
        self.assertEqual(policy, POLICIES[0])
        self.assertEqual(policy.policy_type, EC_POLICY)

        path = '/device/part'
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '1'})
        device, partition, suffix_parts, policy = \
            get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertEqual(suffix_parts, None)  # false-y
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)

        path = '/device/part/'  # with a trailing slash
        req = Request.blank(path, headers={
            'X-Backend-Storage-Policy-Index': '1'})
        device, partition, suffix_parts, policy = \
            get_name_and_placement(req, 2, 3, True)
        self.assertEqual(device, 'device')
        self.assertEqual(partition, 'part')
        self.assertEqual(suffix_parts, '')  # still false-y
        self.assertEqual(policy, POLICIES[1])
        self.assertEqual(policy.policy_type, REPL_POLICY)
