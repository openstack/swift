# Copyright (c) 2010-2012 OpenStack, LLC.
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

""" Tests for swift.common.storage_policies """
import unittest
import StringIO
from ConfigParser import ConfigParser
from swift.common import storage_policy


class TestStoragePolicies(unittest.TestCase):

    def setUp(self):
        self._policies = storage_policy.StoragePolicyCollection([
            storage_policy.StoragePolicy(0, 'zero', True),
            storage_policy.StoragePolicy(1, 'one', False),
            storage_policy.StoragePolicy(2, 'two', False)])

    def _conf(self, conf_str):
        conf_str = "\n".join(line.strip() for line in conf_str.split("\n"))
        conf = ConfigParser()
        conf.readfp(StringIO.StringIO(conf_str))
        return conf

    def test_defaults(self):
        policies = storage_policy.POLICIES
        self.assertTrue(len(policies) > 0)

        # test class functions
        default_policy = policies.get_default()
        self.assert_(default_policy.is_default)
        zero_policy = policies.get_by_index(0)
        self.assert_(zero_policy.idx == 0)
        zero_policy_by_name = policies.get_by_name(zero_policy.name)
        self.assert_(zero_policy_by_name.idx == 0)

    def test_parse_storage_policies(self):
        conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:5]
        name = one
        default = yes
        [storage-policy:6]
        name = apple
        type = replication
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)

        print repr(stor_pols.__dict__)

        self.assertEquals(stor_pols.get_default(), stor_pols.default)
        self.assertEquals(stor_pols.default.name, 'one')

        self.assertEquals("object", stor_pols.get_by_name("zero").ring_name)
        self.assertEquals("object-5", stor_pols.get_by_name("one").ring_name)
        self.assertEquals("object-6", stor_pols.get_by_name("apple").ring_name)

        self.assertEquals(0, stor_pols.get_by_name("zero").idx)
        self.assertEquals(5, stor_pols.get_by_name("one").idx)
        self.assertEquals(6, stor_pols.get_by_name("apple").idx)

        self.assertEquals("replication",
                          stor_pols.get_by_name("zero").policy_type)
        self.assertEquals("replication",
                          stor_pols.get_by_name("one").policy_type)
        self.assertEquals("replication",
                          stor_pols.get_by_name("apple").policy_type)

        self.assertEquals("zero", stor_pols.get_by_index(0).name)
        self.assertEquals("zero", stor_pols.get_by_index("0").name)
        self.assertEquals("one", stor_pols.get_by_index(5).name)
        self.assertEquals("apple", stor_pols.get_by_index(6).name)
        self.assertEquals("zero", stor_pols.get_by_index(None).name)

        self.assertRaises(ValueError, stor_pols.get_by_index, "")
        self.assertRaises(ValueError, stor_pols.get_by_index, "ein")

    def test_parse_storage_policies_malformed(self):
        conf = self._conf("""
        [storage-policy:chicken]
        name = zero
        [storage-policy:1]
        name = one
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

        conf1 = self._conf("""
        [storage-policy:]
        name = one
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf1)

        conf2 = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = zero
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf2)

        conf3 = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = one
        type = invalid_policy_nonsense
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf3)

    def test_multiple_defaults_is_error(self):
        conf = self._conf("""
        [storage-policy:1]
        name = one
        default = yes
        [storage-policy:2]
        name = two
        default = yes
        [storage-policy:3]
        name = three
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

        conf = self._conf("""
        [storage-policy:0]
        name = zero
        default = no
        [storage-policy:1]
        default = no
        name = one
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)
        self.assertEquals(stor_pols.get_by_index(0).idx, 0)

        conf = self._conf("""
        [storage-policy:1]
        default = no
        name = one
        [storage-policy:2]
        default = no
        name = two
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)
        self.assertEquals(stor_pols.get_by_index(0).idx, 0)

    def test_no_default_specified(self):
        conf = self._conf("""
        [storage-policy:1]
        name = one
        [storage-policy:2]
        name = two
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)
        self.assertEquals(stor_pols.get_by_index(0).idx, 0)

        conf1 = self._conf("""
        [storage-policy:0]
        name = thisOne
        [storage-policy:2]
        name = apple
        """)
        stor_pols = storage_policy.parse_storage_policies(conf1)
        self.assertEqual(stor_pols.default.name, 'thisOne')

    def test_false_default(self):
        conf = self._conf("""
        [storage-policy:0]
        default = no
        name = zero
        [storage-policy:1]
        name = one
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)
        self.assertEquals(stor_pols.get_by_index(0).idx, 0)

        conf = self._conf("""
        [storage-policy:0]
        name = 0
        [storage-policy:1]
        default = no
        name = one
        """)
        stor_pols = storage_policy.parse_storage_policies(conf)
        self.assertEquals(stor_pols.get_by_index(0).idx, 0)

    def test_policy_names(self):
        conf = self._conf("""
        [storage-policy:0]
        [storage-policy:1]
        name = zero
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

        conf = self._conf("""
        [storage-policy:0]
        [storage-policy:1]
        name = one
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)

        conf = self._conf("""
        [storage-policy:0]
        [storage-policy:1]
        """)
        self.assertRaises(
            ValueError, storage_policy.parse_storage_policies, conf)


if __name__ == '__main__':
    unittest.main()
