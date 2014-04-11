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
from test.unit import patch_policies
from swift.common.storage_policy import StoragePolicy, POLICIES, \
    parse_storage_policies, get_policy_string


@patch_policies([StoragePolicy(0, 'zero', True),
                 StoragePolicy(1, 'one', False),
                 StoragePolicy(2, 'two', False)])
class TestStoragePolicies(unittest.TestCase):

    def _conf(self, conf_str):
        conf_str = "\n".join(line.strip() for line in conf_str.split("\n"))
        conf = ConfigParser()
        conf.readfp(StringIO.StringIO(conf_str))
        return conf

    def test_swift_info(self):
        expect = [{'default': True, 'type': 'replication', 'name': 'zero'},
                  {'type': 'replication', 'name': 'two'},
                  {'type': 'replication', 'name': 'one'}]
        swift_info = POLICIES.get_policy_info()
        self.assertEquals(sorted(expect, key=lambda k: k['name']),
                          sorted(swift_info, key=lambda k: k['name']))

    def test_get_policy_string(self):
        self.assertEquals(get_policy_string('something', 0), 'something')
        self.assertEquals(get_policy_string('something', None), 'something')
        self.assertEquals(get_policy_string('something', 1),
                          'something' + '-1')
        self.assertRaises(ValueError, get_policy_string, 'something', 99)

    def test_defaults(self):
        self.assertTrue(len(POLICIES) > 0)

        # test class functions
        default_policy = POLICIES.default
        self.assert_(default_policy.is_default)
        zero_policy = POLICIES.get_by_index(0)
        self.assert_(zero_policy.idx == 0)
        zero_policy_by_name = POLICIES.get_by_name(zero_policy.name)
        self.assert_(zero_policy_by_name.idx == 0)

    def test_validate_policies_defaults(self):
        # 0 explicit default
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        POLICIES._validate_policies(test_policies)
        self.assertEquals(POLICIES.get_default(), test_policies[0])
        self.assertEquals(POLICIES.default.name, 'zero')

        # non-zero explicit default
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', True)]
        POLICIES._validate_policies(test_policies)
        self.assertEquals(POLICIES.get_default(), test_policies[2])
        self.assertEquals(POLICIES.default.name, 'two')

        # multiple defaults
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', True),
                         StoragePolicy(2, 'two', True)]
        self.assertRaises(ValueError, POLICIES._validate_policies,
                          test_policies)

        # no defualt specified
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        POLICIES._validate_policies(test_policies)
        self.assertEquals(POLICIES.get_default(), test_policies[0])
        self.assertEquals(POLICIES.default.name, 'zero')

        # nothing specified
        test_policies = []
        POLICIES._validate_policies(test_policies)
        self.assertEquals(POLICIES.get_default(), test_policies[0])
        self.assertEquals(POLICIES.default.name, 'Policy_0')

    def test_validate_policies_indexes(self):
        # duplicate indexes
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(1, 'two', False)]
        self.assertRaises(ValueError, POLICIES._validate_policies,
                          test_policies)

        # bougs indexes
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy('x', 'two', False)]
        self.assertRaises(ValueError, POLICIES._validate_policies,
                          test_policies)
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(-1, 'two', False)]
        self.assertRaises(ValueError, POLICIES._validate_policies,
                          test_policies)

    def test_validate_policies_names(self):
        # duplicate names
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'zero', False),
                         StoragePolicy(2, 'two', False)]
        self.assertRaises(ValueError, POLICIES._validate_policies,
                          test_policies)

        # missing names
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, '', False)]
        self.assertRaises(ValueError, POLICIES._validate_policies,
                          test_policies)

    def test_validate_policies_types(self):
        # default of replication chosen
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        self.assertEquals(POLICIES.get_by_index(0).policy_type, 'replication')
        self.assertEquals(POLICIES.get_by_index(1).policy_type, 'replication')
        self.assertEquals(POLICIES.get_by_index(2).policy_type, 'replication')

        # bogus types
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False, policy_type='nada')]
        self.assertRaises(ValueError, POLICIES._validate_policies,
                          test_policies)

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
        [storage-policy:6]
        name = apple
        type = replication
        """)
        stor_pols = parse_storage_policies(conf)

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


if __name__ == '__main__':
    unittest.main()
