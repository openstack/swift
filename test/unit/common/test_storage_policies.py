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
import mock
from tempfile import NamedTemporaryFile
from test.unit import patch_policies
from swift.common.storage_policy import (
    StoragePolicy, StoragePolicyCollection, POLICIES,
    parse_storage_policies, reload_storage_policies, get_policy_string)


class TestStoragePolicies(unittest.TestCase):

    def _conf(self, conf_str):
        conf_str = "\n".join(line.strip() for line in conf_str.split("\n"))
        conf = ConfigParser()
        conf.readfp(StringIO.StringIO(conf_str))
        return conf

    @patch_policies([StoragePolicy(0, 'zero', True),
                     StoragePolicy(1, 'one', False),
                     StoragePolicy(2, 'two', False),
                     StoragePolicy(3, 'three', False, is_deprecated=True)])
    def test_swift_info(self):
        # the deprecated 'three' should not exist in expect
        expect = [{'default': True, 'type': 'replication', 'name': 'zero'},
                  {'type': 'replication', 'name': 'two'},
                  {'type': 'replication', 'name': 'one'}]
        swift_info = POLICIES.get_policy_info()
        self.assertEquals(sorted(expect, key=lambda k: k['name']),
                          sorted(swift_info, key=lambda k: k['name']))

    @patch_policies
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
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, test_policies[0])
        self.assertEquals(policies.default.name, 'zero')

        # non-zero explicit default
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', True)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, test_policies[2])
        self.assertEquals(policies.default.name, 'two')

        # multiple defaults
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', True),
                         StoragePolicy(2, 'two', True)]
        self.assertRaises(ValueError, StoragePolicyCollection,
                          test_policies)

        # no defualt specified
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, test_policies[0])
        self.assertEquals(policies.default.name, 'zero')

        # nothing specified
        test_policies = []
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, policies[0])
        self.assertEquals(policies.default.name, 'Policy_0')

    def test_deprecate_policies(self):
        # deprecation specified
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False, is_deprecated=True)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, test_policies[0])
        self.assertEquals(policies.default.name, 'zero')
        self.assertEquals(len(policies), 3)

        # deprecating old default makes policy 0 the default
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', True, is_deprecated=True),
                         StoragePolicy(2, 'two', False)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, test_policies[0])
        self.assertEquals(policies.default.name, 'zero')
        self.assertEquals(len(policies), 3)

    def test_validate_policies_indexes(self):
        # duplicate indexes
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(1, 'two', False)]
        self.assertRaises(ValueError, StoragePolicyCollection,
                          test_policies)

    def test_validate_policy_params(self):
        StoragePolicy(0, 'name')  # sanity
        # bougs indexes
        self.assertRaises(ValueError, StoragePolicy, 'x', 'name')
        self.assertRaises(ValueError, StoragePolicy, -1, 'name')
        # missing name
        self.assertRaises(ValueError, StoragePolicy, 1, '')

        # bogus types
        self.assertRaises(ValueError, StoragePolicy, 2, 'name',
                          policy_type='nada')

    def test_validate_policies_names(self):
        # duplicate names
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'zero', False),
                         StoragePolicy(2, 'two', False)]
        self.assertRaises(ValueError, StoragePolicyCollection,
                          test_policies)

    def test_validate_policies_types(self):
        # default of replication chosen
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.get_by_index(0).policy_type, 'replication')
        self.assertEquals(policies.get_by_index(1).policy_type, 'replication')
        self.assertEquals(policies.get_by_index(2).policy_type, 'replication')

    def assertRaisesWithMessage(self, exc_class, message, f, *args, **kwargs):
        try:
            f(*args, **kwargs)
        except exc_class as err:
            err_msg = str(err)
            self.assert_(message in err_msg, 'Error message %r did not '
                         'have expected substring %r' % (err_msg, message))
        else:
            self.fail('%r did not raise %s' % (message, exc_class.__name__))

    def test_deprecated_default(self):
        bad_conf = self._conf("""
        [storage-policy:1]
        name = one
        deprecated = yes
        default = yes
        """)

        policies = parse_storage_policies(bad_conf)
        self.assertEqual(len(policies), 2)
        self.assertTrue(policies[1].is_deprecated)
        self.assertFalse(policies[1].is_default)
        self.assertEqual(policies.default.idx, 0)
        self.assertTrue(policies[0].is_default)
        self.assertFalse(policies[0].is_deprecated)

    def test_parse_storage_policies(self):
        # ValueError when deprecating policy 0
        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        deprecated = yes
        default = yes

        [storage-policy:1]
        name = one
        deprecated = yes
        """)

        self.assertRaisesWithMessage(
            ValueError, "Unable to find policy that's not deprecated",
            parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:x]
        name = zero
        type = replication
        """)

        self.assertRaisesWithMessage(ValueError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:x-1]
        name = zero
        type = replication
        """)

        self.assertRaisesWithMessage(ValueError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy]
        name = zero
        type = replication
        """)

        self.assertRaisesWithMessage(ValueError,
                                     'Invalid storage-policy section',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policyx:]
        name = zero
        type = replication
        """)

        self.assertRaisesWithMessage(ValueError,
                                     'Invalid storage-policy section',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:x:1]
        name = zero
        type = replication
        """)

        self.assertRaisesWithMessage(ValueError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:1]
        name = zero
        type = replication
        boo = berries
        """)

        self.assertRaisesWithMessage(ValueError, 'Invalid option',
                                     parse_storage_policies, bad_conf)

        conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:5]
        name = one
        default = yes
        [storage-policy:6]
        type = duplicate-sections-are-ignored
        [storage-policy:6]
        name = apple
        type = replication
        """)
        policies = parse_storage_policies(conf)

        self.assertEquals(True, policies.get_by_index(5).is_default)
        self.assertEquals(False, policies.get_by_index(0).is_default)
        self.assertEquals(False, policies.get_by_index(6).is_default)

        class FakeRing(object):

            def __init__(self, *args, **kwargs):
                self.ring_name = kwargs.get('ring_name')

        with mock.patch('swift.common.storage_policy.Ring', FakeRing):
            for policy in policies:
                policy.load_ring('/etc/swift')
            # extra read provides one more line of coverage
            policy.load_ring('/etc/swift')
            self.assertEqual(policies[0].object_ring.ring_name, 'object')
            self.assertEqual(policies[5].object_ring.ring_name, 'object-5')
            self.assertEqual(policies[6].object_ring.ring_name, 'object-6')

        self.assertEqual(0, int(policies.by_name['zero']))
        self.assertEqual(5, int(policies.by_name['one']))
        self.assertEqual(6, int(policies.by_name['apple']))

        self.assertEquals("replication",
                          policies.get_by_name("zero").policy_type)
        self.assertEquals("replication",
                          policies.get_by_name("one").policy_type)
        self.assertEquals("replication",
                          policies.get_by_name("apple").policy_type)

        self.assertEquals("zero", policies.get_by_index(0).name)
        self.assertEquals("zero", policies.get_by_index("0").name)
        self.assertEquals("one", policies.get_by_index(5).name)
        self.assertEquals("apple", policies.get_by_index(6).name)
        self.assertEquals("zero", policies.get_by_index(None).name)

    def test_reload_invalid_storage_policies(self):
        conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:00]
        name = double-zero
        """)
        with NamedTemporaryFile() as f:
            conf.write(f)
            f.flush()
            with mock.patch('swift.common.storage_policy.SWIFT_CONF_FILE',
                            new=f.name):
                try:
                    reload_storage_policies()
                except SystemExit as e:
                    err_msg = str(e)
                else:
                    self.fail('SystemExit not raised')
        parts = [
            'Invalid Storage Policy Configuration',
            'Duplicate index',
        ]
        for expected in parts:
            self.assert_(expected in err_msg, '%s was not in %s' % (expected,
                                                                    err_msg))

    def test_storage_policy_ordering(self):
        test_policies = StoragePolicyCollection([
            StoragePolicy(0, 'zero'),
            StoragePolicy(503, 'error'),
            StoragePolicy(204, 'empty'),
            StoragePolicy(404, 'missing'),
        ])
        self.assertEqual([0, 204, 404, 503], [int(p) for p in
                                              sorted(list(test_policies))])

        p503 = test_policies[503]
        self.assertTrue(501 < p503 < 507)


if __name__ == '__main__':
    unittest.main()
