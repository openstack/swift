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
from test.unit import patch_policies, FakeRing
from swift.common.storage_policy import (
    StoragePolicyCollection, POLICIES, PolicyError, parse_storage_policies,
    reload_storage_policies, get_policy_string, split_policy_string,
    BaseStoragePolicy, StoragePolicy, ECStoragePolicy, REPL_POLICY, EC_POLICY,
    VALID_EC_TYPES, DEFAULT_EC_OBJECT_SEGMENT_SIZE)
from swift.common.exceptions import RingValidationError


@BaseStoragePolicy.register('fake')
class FakeStoragePolicy(BaseStoragePolicy):
    """
    Test StoragePolicy class - the only user at the moment is
    test_validate_policies_type_invalid()
    """
    def __init__(self, idx, name='', is_default=False, is_deprecated=False,
                 object_ring=None):
        super(FakeStoragePolicy, self).__init__(
            idx, name, is_default, is_deprecated, object_ring)


class TestStoragePolicies(unittest.TestCase):

    def _conf(self, conf_str):
        conf_str = "\n".join(line.strip() for line in conf_str.split("\n"))
        conf = ConfigParser()
        conf.readfp(StringIO.StringIO(conf_str))
        return conf

    def assertRaisesWithMessage(self, exc_class, message, f, *args, **kwargs):
        try:
            f(*args, **kwargs)
        except exc_class as err:
            err_msg = str(err)
            self.assert_(message in err_msg, 'Error message %r did not '
                         'have expected substring %r' % (err_msg, message))
        else:
            self.fail('%r did not raise %s' % (message, exc_class.__name__))

    def test_policy_baseclass_instantiate(self):
        self.assertRaisesWithMessage(TypeError,
                                     "Can't instantiate BaseStoragePolicy",
                                     BaseStoragePolicy, 1, 'one')

    @patch_policies([
        StoragePolicy(0, 'zero', is_default=True),
        StoragePolicy(1, 'one'),
        StoragePolicy(2, 'two'),
        StoragePolicy(3, 'three', is_deprecated=True),
        ECStoragePolicy(10, 'ten', ec_type='jerasure_rs_vand',
                        ec_ndata=10, ec_nparity=4),
    ])
    def test_swift_info(self):
        # the deprecated 'three' should not exist in expect
        expect = [{'default': True, 'name': 'zero'},
                  {'name': 'two'},
                  {'name': 'one'},
                  {'name': 'ten'}]
        swift_info = POLICIES.get_policy_info()
        self.assertEquals(sorted(expect, key=lambda k: k['name']),
                          sorted(swift_info, key=lambda k: k['name']))

    @patch_policies
    def test_get_policy_string(self):
        self.assertEquals(get_policy_string('something', 0), 'something')
        self.assertEquals(get_policy_string('something', None), 'something')
        self.assertEquals(get_policy_string('something', ''), 'something')
        self.assertEquals(get_policy_string('something', 1),
                          'something' + '-1')
        self.assertRaises(PolicyError, get_policy_string, 'something', 99)

    @patch_policies
    def test_split_policy_string(self):
        expectations = {
            'something': ('something', POLICIES[0]),
            'something-1': ('something', POLICIES[1]),
            'tmp': ('tmp', POLICIES[0]),
            'objects': ('objects', POLICIES[0]),
            'tmp-1': ('tmp', POLICIES[1]),
            'objects-1': ('objects', POLICIES[1]),
            'objects-': PolicyError,
            'objects-0': PolicyError,
            'objects--1': ('objects-', POLICIES[1]),
            'objects-+1': PolicyError,
            'objects--': PolicyError,
            'objects-foo': PolicyError,
            'objects--bar': PolicyError,
            'objects-+bar': PolicyError,
            # questionable, demonstrated as inverse of get_policy_string
            'objects+0': ('objects+0', POLICIES[0]),
            '': ('', POLICIES[0]),
            '0': ('0', POLICIES[0]),
            '-1': ('', POLICIES[1]),
        }
        for policy_string, expected in expectations.items():
            if expected == PolicyError:
                try:
                    invalid = split_policy_string(policy_string)
                except PolicyError:
                    continue  # good
                else:
                    self.fail('The string %r returned %r '
                              'instead of raising a PolicyError' %
                              (policy_string, invalid))
            self.assertEqual(expected, split_policy_string(policy_string))
            # should be inverse of get_policy_string
            self.assertEqual(policy_string, get_policy_string(*expected))

    def test_defaults(self):
        self.assertTrue(len(POLICIES) > 0)

        # test class functions
        default_policy = POLICIES.default
        self.assert_(default_policy.is_default)
        zero_policy = POLICIES.get_by_index(0)
        self.assert_(zero_policy.idx == 0)
        zero_policy_by_name = POLICIES.get_by_name(zero_policy.name)
        self.assert_(zero_policy_by_name.idx == 0)

    def test_storage_policy_repr(self):
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False),
                         ECStoragePolicy(10, 'ten', ec_type='jerasure_rs_vand',
                                         ec_ndata=10, ec_nparity=3)]
        policies = StoragePolicyCollection(test_policies)
        for policy in policies:
            policy_repr = repr(policy)
            self.assert_(policy.__class__.__name__ in policy_repr)
            self.assert_('is_default=%s' % policy.is_default in policy_repr)
            self.assert_('is_deprecated=%s' % policy.is_deprecated in
                         policy_repr)
            self.assert_(policy.name in policy_repr)
            if policy.policy_type == EC_POLICY:
                self.assert_('ec_type=%s' % policy.ec_type in policy_repr)
                self.assert_('ec_ndata=%s' % policy.ec_ndata in policy_repr)
                self.assert_('ec_nparity=%s' %
                             policy.ec_nparity in policy_repr)
                self.assert_('ec_segment_size=%s' %
                             policy.ec_segment_size in policy_repr)
        collection_repr = repr(policies)
        collection_repr_lines = collection_repr.splitlines()
        self.assert_(policies.__class__.__name__ in collection_repr_lines[0])
        self.assertEqual(len(policies), len(collection_repr_lines[1:-1]))
        for policy, line in zip(policies, collection_repr_lines[1:-1]):
            self.assert_(repr(policy) in line)
        with patch_policies(policies):
            self.assertEqual(repr(POLICIES), collection_repr)

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
        self.assertRaisesWithMessage(
            PolicyError, 'Duplicate default', StoragePolicyCollection,
            test_policies)

        # nothing specified
        test_policies = []
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, policies[0])
        self.assertEquals(policies.default.name, 'Policy-0')

        # no default specified with only policy index 0
        test_policies = [StoragePolicy(0, 'zero')]
        policies = StoragePolicyCollection(test_policies)
        self.assertEqual(policies.default, policies[0])

        # no default specified with multiple policies
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        self.assertRaisesWithMessage(
            PolicyError, 'Unable to find default policy',
            StoragePolicyCollection, test_policies)

    def test_deprecate_policies(self):
        # deprecation specified
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False, is_deprecated=True)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.default, test_policies[0])
        self.assertEquals(policies.default.name, 'zero')
        self.assertEquals(len(policies), 3)

        # multiple policies requires default
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', False, is_deprecated=True),
                         StoragePolicy(2, 'two', False)]
        self.assertRaisesWithMessage(
            PolicyError, 'Unable to find default policy',
            StoragePolicyCollection, test_policies)

    def test_validate_policies_indexes(self):
        # duplicate indexes
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(1, 'two', False)]
        self.assertRaises(PolicyError, StoragePolicyCollection,
                          test_policies)

    def test_validate_policy_params(self):
        StoragePolicy(0, 'name')  # sanity
        # bogus indexes
        self.assertRaises(PolicyError, FakeStoragePolicy, 'x', 'name')
        self.assertRaises(PolicyError, FakeStoragePolicy, -1, 'name')

        # non-zero Policy-0
        self.assertRaisesWithMessage(PolicyError, 'reserved',
                                     FakeStoragePolicy, 1, 'policy-0')
        # deprecate default
        self.assertRaisesWithMessage(
            PolicyError, 'Deprecated policy can not be default',
            FakeStoragePolicy, 1, 'Policy-1', is_default=True,
            is_deprecated=True)
        # weird names
        names = (
            '',
            'name_foo',
            'name\nfoo',
            'name foo',
            u'name \u062a',
            'name \xd8\xaa',
        )
        for name in names:
            self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                         FakeStoragePolicy, 1, name)

    def test_validate_policies_names(self):
        # duplicate names
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'zero', False),
                         StoragePolicy(2, 'two', False)]
        self.assertRaises(PolicyError, StoragePolicyCollection,
                          test_policies)

    def test_validate_policies_type_default(self):
        # no type specified - make sure the policy is initialized to
        # DEFAULT_POLICY_TYPE
        test_policy = FakeStoragePolicy(0, 'zero', True)
        self.assertEquals(test_policy.policy_type, 'fake')

    def test_validate_policies_type_invalid(self):
        class BogusStoragePolicy(FakeStoragePolicy):
            policy_type = 'bogus'
        # unsupported policy type - initialization with FakeStoragePolicy
        self.assertRaisesWithMessage(PolicyError, 'Invalid type',
                                     BogusStoragePolicy, 1, 'one')

    def test_policies_type_attribute(self):
        test_policies = [
            StoragePolicy(0, 'zero', is_default=True),
            StoragePolicy(1, 'one'),
            StoragePolicy(2, 'two'),
            StoragePolicy(3, 'three', is_deprecated=True),
            ECStoragePolicy(10, 'ten', ec_type='jerasure_rs_vand',
                            ec_ndata=10, ec_nparity=3),
        ]
        policies = StoragePolicyCollection(test_policies)
        self.assertEquals(policies.get_by_index(0).policy_type,
                          REPL_POLICY)
        self.assertEquals(policies.get_by_index(1).policy_type,
                          REPL_POLICY)
        self.assertEquals(policies.get_by_index(2).policy_type,
                          REPL_POLICY)
        self.assertEquals(policies.get_by_index(3).policy_type,
                          REPL_POLICY)
        self.assertEquals(policies.get_by_index(10).policy_type,
                          EC_POLICY)

    def test_names_are_normalized(self):
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'ZERO', False)]
        self.assertRaises(PolicyError, StoragePolicyCollection,
                          test_policies)

        policies = StoragePolicyCollection([StoragePolicy(0, 'zEro', True),
                                            StoragePolicy(1, 'One', False)])

        pol0 = policies[0]
        pol1 = policies[1]

        for name in ('zero', 'ZERO', 'zErO', 'ZeRo'):
            self.assertEqual(pol0, policies.get_by_name(name))
            self.assertEqual(policies.get_by_name(name).name, 'zEro')
        for name in ('one', 'ONE', 'oNe', 'OnE'):
            self.assertEqual(pol1, policies.get_by_name(name))
            self.assertEqual(policies.get_by_name(name).name, 'One')

    def test_deprecated_default(self):
        bad_conf = self._conf("""
        [storage-policy:1]
        name = one
        deprecated = yes
        default = yes
        """)

        self.assertRaisesWithMessage(
            PolicyError, "Deprecated policy can not be default",
            parse_storage_policies, bad_conf)

    def test_multiple_policies_with_no_policy_index_zero(self):
        bad_conf = self._conf("""
        [storage-policy:1]
        name = one
        default = yes
        """)

        # Policy-0 will not be implicitly added if other policies are defined
        self.assertRaisesWithMessage(
            PolicyError, "must specify a storage policy section "
            "for policy index 0", parse_storage_policies, bad_conf)

    def test_no_default(self):
        orig_conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = one
        default = yes
        """)

        policies = parse_storage_policies(orig_conf)
        self.assertEqual(policies.default, policies[1])
        self.assert_(policies[0].name, 'Policy-0')

        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = one
        deprecated = yes
        """)

        # multiple polices and no explicit default
        self.assertRaisesWithMessage(
            PolicyError, "Unable to find default",
            parse_storage_policies, bad_conf)

        good_conf = self._conf("""
        [storage-policy:0]
        name = Policy-0
        default = yes
        [storage-policy:1]
        name = one
        deprecated = yes
        """)

        policies = parse_storage_policies(good_conf)
        self.assertEqual(policies.default, policies[0])
        self.assert_(policies[1].is_deprecated, True)

    def test_parse_storage_policies(self):
        # ValueError when deprecating policy 0
        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        deprecated = yes

        [storage-policy:1]
        name = one
        deprecated = yes
        """)

        self.assertRaisesWithMessage(
            PolicyError, "Unable to find policy that's not deprecated",
            parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:]
        name = zero
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:-1]
        name = zero
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:x]
        name = zero
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:x-1]
        name = zero
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:x]
        name = zero
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:x:1]
        name = zero
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid index',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:1]
        name = zero
        boo = berries
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid option',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:0]
        name =
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:3]
        name = Policy-0
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:1]
        name = policY-0
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:0]
        name = one
        [storage-policy:1]
        name = ONE
        """)

        self.assertRaisesWithMessage(PolicyError, 'Duplicate name',
                                     parse_storage_policies, bad_conf)

        bad_conf = self._conf("""
        [storage-policy:0]
        name = good_stuff
        """)

        self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                     parse_storage_policies, bad_conf)

        # policy_type = erasure_coding

        # missing ec_type, ec_num_data_fragments and ec_num_parity_fragments
        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = ec10-4
        policy_type = erasure_coding
        """)

        self.assertRaisesWithMessage(PolicyError, 'Missing ec_type',
                                     parse_storage_policies, bad_conf)

        # missing ec_type, but other options valid...
        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = ec10-4
        policy_type = erasure_coding
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 4
        """)

        self.assertRaisesWithMessage(PolicyError, 'Missing ec_type',
                                     parse_storage_policies, bad_conf)

        # ec_type specified, but invalid...
        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        default = yes
        [storage-policy:1]
        name = ec10-4
        policy_type = erasure_coding
        ec_type = garbage_alg
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 4
        """)

        self.assertRaisesWithMessage(PolicyError,
                                     'Wrong ec_type garbage_alg for policy '
                                     'ec10-4, should be one of "%s"' %
                                     (', '.join(VALID_EC_TYPES)),
                                     parse_storage_policies, bad_conf)

        # missing and invalid ec_num_parity_fragments
        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = ec10-4
        policy_type = erasure_coding
        ec_type = jerasure_rs_vand
        ec_num_data_fragments = 10
        """)

        self.assertRaisesWithMessage(PolicyError,
                                     'Invalid ec_num_parity_fragments',
                                     parse_storage_policies, bad_conf)

        for num_parity in ('-4', '0', 'x'):
            bad_conf = self._conf("""
            [storage-policy:0]
            name = zero
            [storage-policy:1]
            name = ec10-4
            policy_type = erasure_coding
            ec_type = jerasure_rs_vand
            ec_num_data_fragments = 10
            ec_num_parity_fragments = %s
            """ % num_parity)

            self.assertRaisesWithMessage(PolicyError,
                                         'Invalid ec_num_parity_fragments',
                                         parse_storage_policies, bad_conf)

        # missing and invalid ec_num_data_fragments
        bad_conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:1]
        name = ec10-4
        policy_type = erasure_coding
        ec_type = jerasure_rs_vand
        ec_num_parity_fragments = 4
        """)

        self.assertRaisesWithMessage(PolicyError,
                                     'Invalid ec_num_data_fragments',
                                     parse_storage_policies, bad_conf)

        for num_data in ('-10', '0', 'x'):
            bad_conf = self._conf("""
            [storage-policy:0]
            name = zero
            [storage-policy:1]
            name = ec10-4
            policy_type = erasure_coding
            ec_type = jerasure_rs_vand
            ec_num_data_fragments = %s
            ec_num_parity_fragments = 4
            """ % num_data)

            self.assertRaisesWithMessage(PolicyError,
                                         'Invalid ec_num_data_fragments',
                                         parse_storage_policies, bad_conf)

        # invalid ec_object_segment_size
        for segment_size in ('-4', '0', 'x'):
            bad_conf = self._conf("""
            [storage-policy:0]
            name = zero
            [storage-policy:1]
            name = ec10-4
            policy_type = erasure_coding
            ec_object_segment_size = %s
            ec_type = jerasure_rs_vand
            ec_num_data_fragments = 10
            ec_num_parity_fragments = 4
            """ % segment_size)

            self.assertRaisesWithMessage(PolicyError,
                                         'Invalid ec_object_segment_size',
                                         parse_storage_policies, bad_conf)

        # Additional section added to ensure parser ignores other sections
        conf = self._conf("""
        [some-other-section]
        foo = bar
        [storage-policy:0]
        name = zero
        [storage-policy:5]
        name = one
        default = yes
        [storage-policy:6]
        name = duplicate-sections-are-ignored
        [storage-policy:6]
        name = apple
        """)
        policies = parse_storage_policies(conf)

        self.assertEquals(True, policies.get_by_index(5).is_default)
        self.assertEquals(False, policies.get_by_index(0).is_default)
        self.assertEquals(False, policies.get_by_index(6).is_default)

        self.assertEquals("object", policies.get_by_name("zero").ring_name)
        self.assertEquals("object-5", policies.get_by_name("one").ring_name)
        self.assertEquals("object-6", policies.get_by_name("apple").ring_name)

        self.assertEqual(0, int(policies.get_by_name('zero')))
        self.assertEqual(5, int(policies.get_by_name('one')))
        self.assertEqual(6, int(policies.get_by_name('apple')))

        self.assertEquals("zero", policies.get_by_index(0).name)
        self.assertEquals("zero", policies.get_by_index("0").name)
        self.assertEquals("one", policies.get_by_index(5).name)
        self.assertEquals("apple", policies.get_by_index(6).name)
        self.assertEquals("zero", policies.get_by_index(None).name)
        self.assertEquals("zero", policies.get_by_index('').name)

        self.assertEqual(policies.get_by_index(0), policies.legacy)

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
            StoragePolicy(0, 'zero', is_default=True),
            StoragePolicy(503, 'error'),
            StoragePolicy(204, 'empty'),
            StoragePolicy(404, 'missing'),
        ])
        self.assertEqual([0, 204, 404, 503], [int(p) for p in
                                              sorted(list(test_policies))])

        p503 = test_policies[503]
        self.assertTrue(501 < p503 < 507)

    def test_get_object_ring(self):
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False)]
        policies = StoragePolicyCollection(test_policies)

        class NamedFakeRing(FakeRing):

            def __init__(self, swift_dir, ring_name=None):
                self.ring_name = ring_name
                super(NamedFakeRing, self).__init__()

        with mock.patch('swift.common.storage_policy.Ring',
                        new=NamedFakeRing):
            for policy in policies:
                self.assertFalse(policy.object_ring)
                ring = policies.get_object_ring(int(policy), '/path/not/used')
                self.assertEqual(ring.ring_name, policy.ring_name)
                self.assertTrue(policy.object_ring)
                self.assert_(isinstance(policy.object_ring, NamedFakeRing))

        def blow_up(*args, **kwargs):
            raise Exception('kaboom!')

        with mock.patch('swift.common.storage_policy.Ring', new=blow_up):
            for policy in policies:
                policy.load_ring('/path/not/used')
                expected = policies.get_object_ring(int(policy),
                                                    '/path/not/used')
                self.assertEqual(policy.object_ring, expected)

        # bad policy index
        self.assertRaises(PolicyError, policies.get_object_ring, 99,
                          '/path/not/used')

    def test_singleton_passthrough(self):
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False)]
        with patch_policies(test_policies):
            for policy in POLICIES:
                self.assertEqual(POLICIES[int(policy)], policy)

    def test_quorum_size_replication(self):
        expected_sizes = {1: 1,
                          2: 2,
                          3: 2,
                          4: 3,
                          5: 3}
        for n, expected in expected_sizes.items():
            policy = StoragePolicy(0, 'zero',
                                   object_ring=FakeRing(replicas=n))
            self.assertEqual(policy.quorum, expected)

    def test_quorum_size_erasure_coding(self):
        test_ec_policies = [
            ECStoragePolicy(10, 'ec8-2', ec_type='jerasure_rs_vand',
                            ec_ndata=8, ec_nparity=2),
            ECStoragePolicy(11, 'df10-6', ec_type='flat_xor_hd_4',
                            ec_ndata=10, ec_nparity=6),
        ]
        for ec_policy in test_ec_policies:
            k = ec_policy.ec_ndata
            expected_size = \
                k + ec_policy.pyeclib_driver.min_parity_fragments_needed()
            self.assertEqual(expected_size, ec_policy.quorum)

    def test_validate_ring(self):
        test_policies = [
            ECStoragePolicy(0, 'ec8-2', ec_type='jerasure_rs_vand',
                            ec_ndata=8, ec_nparity=2,
                            object_ring=FakeRing(replicas=8),
                            is_default=True),
            ECStoragePolicy(1, 'ec10-4', ec_type='jerasure_rs_vand',
                            ec_ndata=10, ec_nparity=4,
                            object_ring=FakeRing(replicas=10)),
            ECStoragePolicy(2, 'ec4-2', ec_type='jerasure_rs_vand',
                            ec_ndata=4, ec_nparity=2,
                            object_ring=FakeRing(replicas=7)),
        ]
        policies = StoragePolicyCollection(test_policies)

        for policy in policies:
            msg = 'EC ring for policy %s needs to be configured with ' \
                  'exactly %d nodes.' % \
                  (policy.name, policy.ec_ndata + policy.ec_nparity)
            self.assertRaisesWithMessage(
                RingValidationError, msg,
                policy._validate_ring)

    def test_storage_policy_get_info(self):
        test_policies = [
            StoragePolicy(0, 'zero', is_default=True),
            StoragePolicy(1, 'one', is_deprecated=True),
            ECStoragePolicy(10, 'ten',
                            ec_type='jerasure_rs_vand',
                            ec_ndata=10, ec_nparity=3),
            ECStoragePolicy(11, 'done', is_deprecated=True,
                            ec_type='jerasure_rs_vand',
                            ec_ndata=10, ec_nparity=3),
        ]
        policies = StoragePolicyCollection(test_policies)
        expected = {
            # default replication
            (0, True): {
                'name': 'zero',
                'default': True,
                'deprecated': False,
                'policy_type': REPL_POLICY
            },
            (0, False): {
                'name': 'zero',
                'default': True,
            },
            # deprecated replication
            (1, True): {
                'name': 'one',
                'default': False,
                'deprecated': True,
                'policy_type': REPL_POLICY
            },
            (1, False): {
                'name': 'one',
                'deprecated': True,
            },
            # enabled ec
            (10, True): {
                'name': 'ten',
                'default': False,
                'deprecated': False,
                'policy_type': EC_POLICY,
                'ec_type': 'jerasure_rs_vand',
                'ec_num_data_fragments': 10,
                'ec_num_parity_fragments': 3,
                'ec_object_segment_size': DEFAULT_EC_OBJECT_SEGMENT_SIZE,
            },
            (10, False): {
                'name': 'ten',
            },
            # deprecated ec
            (11, True): {
                'name': 'done',
                'default': False,
                'deprecated': True,
                'policy_type': EC_POLICY,
                'ec_type': 'jerasure_rs_vand',
                'ec_num_data_fragments': 10,
                'ec_num_parity_fragments': 3,
                'ec_object_segment_size': DEFAULT_EC_OBJECT_SEGMENT_SIZE,
            },
            (11, False): {
                'name': 'done',
                'deprecated': True,
            },
        }
        self.maxDiff = None
        for policy in policies:
            expected_info = expected[(int(policy), True)]
            self.assertEqual(policy.get_info(config=True), expected_info)
            expected_info = expected[(int(policy), False)]
            self.assertEqual(policy.get_info(config=False), expected_info)


if __name__ == '__main__':
    unittest.main()
