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
from configparser import ConfigParser
import contextlib
import io
import logging
import unittest
import os
from unittest import mock
from functools import partial

from tempfile import NamedTemporaryFile
from test.debug_logger import debug_logger
from test.unit import (
    patch_policies, FakeRing, temptree, DEFAULT_TEST_EC_TYPE)
import swift.common.storage_policy
from swift.common.storage_policy import (
    StoragePolicyCollection, POLICIES, PolicyError, parse_storage_policies,
    reload_storage_policies, get_policy_string, split_policy_string,
    BaseStoragePolicy, StoragePolicy, ECStoragePolicy, REPL_POLICY, EC_POLICY,
    VALID_EC_TYPES, DEFAULT_EC_OBJECT_SEGMENT_SIZE, BindPortsCache)
from swift.common.ring import RingData
from swift.common.exceptions import RingLoadError
from pyeclib.ec_iface import ECDriver


class CapturingHandler(logging.Handler):
    def __init__(self):
        super(CapturingHandler, self).__init__()
        self._records = []

    def emit(self, record):
        self._records.append(record)


@contextlib.contextmanager
def capture_logging(log_name):
    captured = CapturingHandler()
    logger = logging.getLogger(log_name)
    logger.addHandler(captured)
    try:
        yield captured._records
    finally:
        logger.removeHandler(captured)


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
        conf = ConfigParser(strict=False)
        conf.read_file(io.StringIO(conf_str))
        return conf

    def assertRaisesWithMessage(self, exc_class, message, f, *args, **kwargs):
        try:
            f(*args, **kwargs)
        except exc_class as err:
            err_msg = str(err)
            self.assertTrue(message in err_msg, 'Error message %r did not '
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
        ECStoragePolicy(10, 'ten', ec_type=DEFAULT_TEST_EC_TYPE,
                        ec_ndata=10, ec_nparity=4),
    ])
    def test_swift_info(self):
        # the deprecated 'three' should not exist in expect
        expect = [{'aliases': 'zero', 'default': True, 'name': 'zero', },
                  {'aliases': 'two', 'name': 'two'},
                  {'aliases': 'one', 'name': 'one'},
                  {'aliases': 'ten', 'name': 'ten'}]
        swift_info = POLICIES.get_policy_info()
        self.assertEqual(sorted(expect, key=lambda k: k['name']),
                         sorted(swift_info, key=lambda k: k['name']))

    @patch_policies
    def test_get_policy_string(self):
        self.assertEqual(get_policy_string('something', 0), 'something')
        self.assertEqual(get_policy_string('something', None), 'something')
        self.assertEqual(get_policy_string('something', ''), 'something')
        self.assertEqual(get_policy_string('something', 1),
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
        self.assertGreater(len(POLICIES), 0)

        # test class functions
        default_policy = POLICIES.default
        self.assertTrue(default_policy.is_default)
        zero_policy = POLICIES.get_by_index(0)
        self.assertTrue(zero_policy.idx == 0)
        zero_policy_by_name = POLICIES.get_by_name(zero_policy.name)
        self.assertTrue(zero_policy_by_name.idx == 0)

    def test_storage_policy_repr(self):
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False),
                         ECStoragePolicy(10, 'ten',
                                         ec_type=DEFAULT_TEST_EC_TYPE,
                                         ec_ndata=10, ec_nparity=3),
                         ECStoragePolicy(11, 'eleven',
                                         ec_type=DEFAULT_TEST_EC_TYPE,
                                         ec_ndata=10, ec_nparity=3,
                                         ec_duplication_factor=2)]
        policies = StoragePolicyCollection(test_policies)
        for policy in policies:
            policy_repr = repr(policy)
            self.assertTrue(policy.__class__.__name__ in policy_repr)
            self.assertTrue('is_default=%s' % policy.is_default in policy_repr)
            self.assertTrue('is_deprecated=%s' % policy.is_deprecated in
                            policy_repr)
            self.assertTrue(policy.name in policy_repr)
            if policy.policy_type == EC_POLICY:
                self.assertTrue('ec_type=%s' % policy.ec_type in policy_repr)
                self.assertTrue('ec_ndata=%s' % policy.ec_ndata in policy_repr)
                self.assertTrue('ec_nparity=%s' %
                                policy.ec_nparity in policy_repr)
                self.assertTrue('ec_segment_size=%s' %
                                policy.ec_segment_size in policy_repr)
                if policy.ec_duplication_factor > 1:
                    self.assertTrue('ec_duplication_factor=%s' %
                                    policy.ec_duplication_factor in
                                    policy_repr)
        collection_repr = repr(policies)
        collection_repr_lines = collection_repr.splitlines()
        self.assertTrue(
            policies.__class__.__name__ in collection_repr_lines[0])
        self.assertEqual(len(policies), len(collection_repr_lines[1:-1]))
        for policy, line in zip(policies, collection_repr_lines[1:-1]):
            self.assertTrue(repr(policy) in line)
        with patch_policies(policies):
            self.assertEqual(repr(POLICIES), collection_repr)

    def test_validate_policies_defaults(self):
        # 0 explicit default
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEqual(policies.default, test_policies[0])
        self.assertEqual(policies.default.name, 'zero')

        # non-zero explicit default
        test_policies = [StoragePolicy(0, 'zero', False),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', True)]
        policies = StoragePolicyCollection(test_policies)
        self.assertEqual(policies.default, test_policies[2])
        self.assertEqual(policies.default.name, 'two')

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
        self.assertEqual(policies.default, policies[0])
        self.assertEqual(policies.default.name, 'Policy-0')

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
        self.assertEqual(policies.default, test_policies[0])
        self.assertEqual(policies.default.name, 'zero')
        self.assertEqual(len(policies), 3)

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
        self.assertEqual(test_policy.policy_type, 'fake')

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
            ECStoragePolicy(10, 'ten', ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=10, ec_nparity=3),
        ]
        policies = StoragePolicyCollection(test_policies)
        self.assertEqual(policies.get_by_index(0).policy_type,
                         REPL_POLICY)
        self.assertEqual(policies.get_by_index(1).policy_type,
                         REPL_POLICY)
        self.assertEqual(policies.get_by_index(2).policy_type,
                         REPL_POLICY)
        self.assertEqual(policies.get_by_index(3).policy_type,
                         REPL_POLICY)
        self.assertEqual(policies.get_by_index(10).policy_type,
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

    def test_wacky_int_names(self):
        # checking duplicate on insert
        test_policies = [StoragePolicy(0, '1', True, aliases='-1'),
                         StoragePolicy(1, '0', False)]
        policies = StoragePolicyCollection(test_policies)

        with self.assertRaises(PolicyError):
            policies.get_by_name_or_index('0')
        self.assertEqual(policies.get_by_name('1'), test_policies[0])
        self.assertEqual(policies.get_by_index(0), test_policies[0])

        with self.assertRaises(PolicyError):
            policies.get_by_name_or_index('1')
        self.assertEqual(policies.get_by_name('0'), test_policies[1])
        self.assertEqual(policies.get_by_index(1), test_policies[1])

        self.assertIsNone(policies.get_by_index(-1))
        self.assertEqual(policies.get_by_name_or_index('-1'), test_policies[0])

    def test_multiple_names(self):
        # checking duplicate on insert
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False, aliases='zero')]
        self.assertRaises(PolicyError, StoragePolicyCollection,
                          test_policies)

        # checking correct retrival using other names
        test_policies = [StoragePolicy(0, 'zero', True, aliases='cero, kore'),
                         StoragePolicy(1, 'one', False, aliases='uno, tahi'),
                         StoragePolicy(2, 'two', False, aliases='dos, rua')]

        policies = StoragePolicyCollection(test_policies)

        for name in ('zero', 'cero', 'kore'):
            self.assertEqual(policies.get_by_name(name), test_policies[0])
        for name in ('two', 'dos', 'rua'):
            self.assertEqual(policies.get_by_name(name), test_policies[2])

        # Testing parsing of conf files/text
        good_conf = self._conf("""
        [storage-policy:0]
        name = one
        aliases = uno, tahi
        default = yes
        """)

        policies = parse_storage_policies(good_conf)
        self.assertEqual(policies.get_by_name('one'),
                         policies[0])
        self.assertEqual(policies.get_by_name('one'),
                         policies.get_by_name('tahi'))

        name_repeat_conf = self._conf("""
        [storage-policy:0]
        name = one
        aliases = one
        default = yes
        """)
        # Test on line below should not generate errors. Repeat of main
        # name under aliases is permitted during construction
        # but only because automated testing requires it.
        policies = parse_storage_policies(name_repeat_conf)

        extra_commas_conf = self._conf("""
        [storage-policy:0]
        name = one
        aliases = ,,one, ,
        default = yes
        """)
        # Extra blank entries should be silently dropped
        policies = parse_storage_policies(extra_commas_conf)

        bad_conf = self._conf("""
        [storage-policy:0]
        name = one
        aliases = uno, uno
        default = yes
        """)

        self.assertRaisesWithMessage(PolicyError,
                                     'is already assigned to this policy',
                                     parse_storage_policies, bad_conf)

    def test_multiple_names_EC(self):
        # checking duplicate names on insert
        test_policies_ec = [
            ECStoragePolicy(
                0, 'ec8-2',
                aliases='zeus, jupiter',
                ec_type=DEFAULT_TEST_EC_TYPE,
                ec_ndata=8, ec_nparity=2,
                object_ring=FakeRing(replicas=8),
                is_default=True),
            ECStoragePolicy(
                1, 'ec10-4',
                aliases='ec8-2',
                ec_type=DEFAULT_TEST_EC_TYPE,
                ec_ndata=10, ec_nparity=4,
                object_ring=FakeRing(replicas=10))]

        self.assertRaises(PolicyError, StoragePolicyCollection,
                          test_policies_ec)

        # checking correct retrival using other names
        good_test_policies_EC = [
            ECStoragePolicy(0, 'ec8-2', aliases='zeus, jupiter',
                            ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=8, ec_nparity=2,
                            object_ring=FakeRing(replicas=10),
                            is_default=True),
            ECStoragePolicy(1, 'ec10-4', aliases='athena, minerva',
                            ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=10, ec_nparity=4,
                            object_ring=FakeRing(replicas=14)),
            ECStoragePolicy(2, 'ec4-2', aliases='poseidon, neptune',
                            ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=4, ec_nparity=2,
                            object_ring=FakeRing(replicas=6)),
            ECStoragePolicy(3, 'ec4-2-dup', aliases='uzuki, rin',
                            ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=4, ec_nparity=2,
                            ec_duplication_factor=2,
                            object_ring=FakeRing(replicas=12)),
        ]
        ec_policies = StoragePolicyCollection(good_test_policies_EC)

        for name in ('ec8-2', 'zeus', 'jupiter'):
            self.assertEqual(ec_policies.get_by_name(name), ec_policies[0])
        for name in ('ec10-4', 'athena', 'minerva'):
            self.assertEqual(ec_policies.get_by_name(name), ec_policies[1])
        for name in ('ec4-2', 'poseidon', 'neptune'):
            self.assertEqual(ec_policies.get_by_name(name), ec_policies[2])
        for name in ('ec4-2-dup', 'uzuki', 'rin'):
            self.assertEqual(ec_policies.get_by_name(name), ec_policies[3])

        # Testing parsing of conf files/text
        good_ec_conf = self._conf("""
        [storage-policy:0]
        name = ec8-2
        aliases = zeus, jupiter
        policy_type = erasure_coding
        ec_type = %(ec_type)s
        default = yes
        ec_num_data_fragments = 8
        ec_num_parity_fragments = 2
        [storage-policy:1]
        name = ec10-4
        aliases = poseidon, neptune
        policy_type = erasure_coding
        ec_type = %(ec_type)s
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 4
        [storage-policy:2]
        name = ec4-2-dup
        aliases = uzuki, rin
        policy_type = erasure_coding
        ec_type = %(ec_type)s
        ec_num_data_fragments = 4
        ec_num_parity_fragments = 2
        ec_duplication_factor = 2
        """ % {'ec_type': DEFAULT_TEST_EC_TYPE})

        ec_policies = parse_storage_policies(good_ec_conf)
        self.assertEqual(ec_policies.get_by_name('ec8-2'),
                         ec_policies[0])
        self.assertEqual(ec_policies.get_by_name('ec10-4'),
                         ec_policies.get_by_name('poseidon'))
        self.assertEqual(ec_policies.get_by_name('ec4-2-dup'),
                         ec_policies.get_by_name('uzuki'))

        name_repeat_ec_conf = self._conf("""
        [storage-policy:0]
        name = ec8-2
        aliases = ec8-2
        policy_type = erasure_coding
        ec_type = %(ec_type)s
        default = yes
        ec_num_data_fragments = 8
        ec_num_parity_fragments = 2
        """ % {'ec_type': DEFAULT_TEST_EC_TYPE})
        # Test on line below should not generate errors. Repeat of main
        # name under aliases is permitted during construction
        # but only because automated testing requires it.
        ec_policies = parse_storage_policies(name_repeat_ec_conf)

        bad_ec_conf = self._conf("""
        [storage-policy:0]
        name = ec8-2
        aliases = zeus, zeus
        policy_type = erasure_coding
        ec_type = %(ec_type)s
        default = yes
        ec_num_data_fragments = 8
        ec_num_parity_fragments = 2
        """ % {'ec_type': DEFAULT_TEST_EC_TYPE})
        self.assertRaisesWithMessage(PolicyError,
                                     'is already assigned to this policy',
                                     parse_storage_policies, bad_ec_conf)

    def test_add_remove_names(self):
        test_policies = [StoragePolicy(0, 'zero', True),
                         StoragePolicy(1, 'one', False),
                         StoragePolicy(2, 'two', False)]
        policies = StoragePolicyCollection(test_policies)

        # add names
        policies.add_policy_alias(1, 'tahi')
        self.assertEqual(policies.get_by_name('tahi'), test_policies[1])

        policies.add_policy_alias(2, 'rua', 'dos')
        self.assertEqual(policies.get_by_name('rua'), test_policies[2])
        self.assertEqual(policies.get_by_name('dos'), test_policies[2])

        self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                     policies.add_policy_alias, 2, 'double\n')

        self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                     policies.add_policy_alias, 2, '')

        # try to add existing name
        self.assertRaisesWithMessage(PolicyError, 'Duplicate name',
                                     policies.add_policy_alias, 2, 'two')

        self.assertRaisesWithMessage(PolicyError, 'Duplicate name',
                                     policies.add_policy_alias, 1, 'two')

        # remove name
        policies.remove_policy_alias('tahi')
        self.assertIsNone(policies.get_by_name('tahi'))

        # remove only name
        self.assertRaisesWithMessage(PolicyError,
                                     'Policies must have at least one name.',
                                     policies.remove_policy_alias, 'zero')

        # remove non-existent name
        self.assertRaisesWithMessage(PolicyError,
                                     'No policy with name',
                                     policies.remove_policy_alias, 'three')

        # remove default name
        policies.remove_policy_alias('two')
        self.assertIsNone(policies.get_by_name('two'))
        self.assertEqual(policies.get_by_index(2).name, 'rua')

        # change default name to a new name
        policies.change_policy_primary_name(2, 'two')
        self.assertEqual(policies.get_by_name('two'), test_policies[2])
        self.assertEqual(policies.get_by_index(2).name, 'two')

        # change default name to an existing alias
        policies.change_policy_primary_name(2, 'dos')
        self.assertEqual(policies.get_by_index(2).name, 'dos')

        # change default name to a bad new name
        self.assertRaisesWithMessage(PolicyError, 'Invalid name',
                                     policies.change_policy_primary_name,
                                     2, 'bad\nname')

        # change default name to a name belonging to another policy
        self.assertRaisesWithMessage(PolicyError,
                                     'Other policy',
                                     policies.change_policy_primary_name,
                                     1, 'dos')

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

    @mock.patch.object(swift.common.storage_policy, 'VALID_EC_TYPES',
                       ['isa_l_rs_vand', 'isa_l_rs_cauchy'])
    @mock.patch('swift.common.storage_policy.ECDriver')
    def test_known_bad_ec_config(self, mock_driver):
        good_conf = self._conf("""
        [storage-policy:0]
        name = bad-policy
        policy_type = erasure_coding
        ec_type = isa_l_rs_cauchy
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 5
        """)

        with capture_logging('swift.common.storage_policy') as records:
            parse_storage_policies(good_conf)
        mock_driver.assert_called_once()
        mock_driver.reset_mock()
        self.assertFalse([(r.levelname, r.msg) for r in records])

        good_conf = self._conf("""
        [storage-policy:0]
        name = bad-policy
        policy_type = erasure_coding
        ec_type = isa_l_rs_vand
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 4
        """)

        with capture_logging('swift.common.storage_policy') as records:
            parse_storage_policies(good_conf)
        mock_driver.assert_called_once()
        mock_driver.reset_mock()
        self.assertFalse([(r.levelname, r.msg) for r in records])

        bad_conf = self._conf("""
        [storage-policy:0]
        name = bad-policy
        policy_type = erasure_coding
        ec_type = isa_l_rs_vand
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 5
        """)

        with capture_logging('swift.common.storage_policy') as records, \
                self.assertRaises(PolicyError) as exc_mgr:
            parse_storage_policies(bad_conf)
        self.assertEqual(exc_mgr.exception.args[0],
                         'Storage policy bad-policy uses an EC '
                         'configuration known to harm data durability. This '
                         'policy MUST be deprecated.')
        mock_driver.assert_not_called()
        mock_driver.reset_mock()
        self.assertEqual([r.levelname for r in records],
                         ['WARNING'])
        for msg in ('known to harm data durability',
                    'Any data in this policy should be migrated',
                    'https://bugs.launchpad.net/swift/+bug/1639691'):
            self.assertIn(msg, records[0].msg)

        slightly_less_bad_conf = self._conf("""
        [storage-policy:0]
        name = bad-policy
        policy_type = erasure_coding
        ec_type = isa_l_rs_vand
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 5
        deprecated = true

        [storage-policy:1]
        name = good-policy
        policy_type = erasure_coding
        ec_type = isa_l_rs_cauchy
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 5
        default = true
        """)

        with capture_logging('swift.common.storage_policy') as records:
            parse_storage_policies(slightly_less_bad_conf)
        self.assertEqual(2, mock_driver.call_count)
        mock_driver.reset_mock()
        self.assertEqual([r.levelname for r in records],
                         ['WARNING'])
        for msg in ('known to harm data durability',
                    'Any data in this policy should be migrated',
                    'https://bugs.launchpad.net/swift/+bug/1639691'):
            self.assertIn(msg, records[0].msg)

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
        self.assertEqual('zero', policies[0].name)

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
        self.assertTrue(policies[1].is_deprecated)

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
        ec_type = %(ec_type)s
        ec_num_data_fragments = 10
        """ % {'ec_type': DEFAULT_TEST_EC_TYPE})

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
            ec_type = %(ec_type)s
            ec_num_data_fragments = 10
            ec_num_parity_fragments = %(num_parity)s
            """ % {'ec_type': DEFAULT_TEST_EC_TYPE,
                   'num_parity': num_parity})

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
        ec_type = %(ec_type)s
        ec_num_parity_fragments = 4
        """ % {'ec_type': DEFAULT_TEST_EC_TYPE})

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
            ec_type = %(ec_type)s
            ec_num_data_fragments = %(num_data)s
            ec_num_parity_fragments = 4
            """ % {'num_data': num_data, 'ec_type': DEFAULT_TEST_EC_TYPE})

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
            ec_object_segment_size = %(segment_size)s
            ec_type = %(ec_type)s
            ec_num_data_fragments = 10
            ec_num_parity_fragments = 4
            """ % {'segment_size': segment_size,
                   'ec_type': DEFAULT_TEST_EC_TYPE})

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

        self.assertEqual(True, policies.get_by_index(5).is_default)
        self.assertEqual(False, policies.get_by_index(0).is_default)
        self.assertEqual(False, policies.get_by_index(6).is_default)

        self.assertEqual("object", policies.get_by_name("zero").ring_name)
        self.assertEqual("object-5", policies.get_by_name("one").ring_name)
        self.assertEqual("object-6", policies.get_by_name("apple").ring_name)

        self.assertEqual(0, int(policies.get_by_name('zero')))
        self.assertEqual(5, int(policies.get_by_name('one')))
        self.assertEqual(6, int(policies.get_by_name('apple')))

        self.assertEqual("zero", policies.get_by_index(0).name)
        self.assertEqual("zero", policies.get_by_index("0").name)
        self.assertEqual("one", policies.get_by_index(5).name)
        self.assertEqual("apple", policies.get_by_index(6).name)
        self.assertEqual("zero", policies.get_by_index(None).name)
        self.assertEqual("zero", policies.get_by_index('').name)

        self.assertEqual(policies.get_by_index(0), policies.legacy)

    def test_reload_invalid_storage_policies(self):
        conf = self._conf("""
        [storage-policy:0]
        name = zero
        [storage-policy:00]
        name = double-zero
        """)
        with NamedTemporaryFile(mode='w+t') as f:
            conf.write(f)
            f.flush()
            with mock.patch('swift.common.utils.SWIFT_CONF_FILE',
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
            self.assertTrue(
                expected in err_msg, '%s was not in %s' % (expected,
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

    def test_storage_policies_as_dict_keys(self):
        # We have tests that expect to be able to map policies
        # to expected values in a dict; check that we can use
        # policies as keys.
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False)]
        policy_to_name_map = {p: p.name for p in test_policies}
        self.assertEqual(sorted(policy_to_name_map.keys()), test_policies)
        self.assertIs(test_policies[0], next(
            p for p in policy_to_name_map.keys() if p.is_default))
        for p in test_policies:
            self.assertEqual(policy_to_name_map[p], p.name)

    def test_get_object_ring(self):
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False)]
        policies = StoragePolicyCollection(test_policies)

        class NamedFakeRing(FakeRing):

            def __init__(self, swift_dir, reload_time=15, ring_name=None,
                         validation_hook=None):
                self.ring_name = ring_name
                super(NamedFakeRing, self).__init__()

        with mock.patch('swift.common.storage_policy.Ring',
                        new=NamedFakeRing):
            for policy in policies:
                self.assertFalse(policy.object_ring)
                ring = policies.get_object_ring(int(policy), '/path/not/used')
                self.assertEqual(ring.ring_name, policy.ring_name)
                self.assertTrue(policy.object_ring)
                self.assertIsInstance(policy.object_ring, NamedFakeRing)

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

    def test_bind_ports_cache(self):
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False)]

        my_ips = ['1.2.3.4', '2.3.4.5']
        other_ips = ['3.4.5.6', '4.5.6.7']
        bind_ip = my_ips[1]
        devs_by_ring_name1 = {
            'object': [  # 'aay'
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[0],
                 'port': 6006},
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[0],
                 'port': 6007},
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[1],
                 'port': 6008},
                None,
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[1],
                 'port': 6009}],
            'object-1': [  # 'bee'
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[1],
                 'port': 6006},  # dupe
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[0],
                 'port': 6010},
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[1],
                 'port': 6011},
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[1],
                 'port': 6012}],
            'object-2': [  # 'cee'
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[0],
                 'port': 6010},  # on our IP and a not-us IP
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[0],
                 'port': 6013},
                None,
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[1],
                 'port': 6014},
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[1],
                 'port': 6015}],
        }
        devs_by_ring_name2 = {
            'object': [  # 'aay'
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[0],
                 'port': 6016},
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[1],
                 'port': 6019}],
            'object-1': [  # 'bee'
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[1],
                 'port': 6016},  # dupe
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[1],
                 'port': 6022}],
            'object-2': [  # 'cee'
                {'id': 0, 'zone': 0, 'region': 1, 'ip': my_ips[0],
                 'port': 6020},
                {'id': 0, 'zone': 0, 'region': 1, 'ip': other_ips[1],
                 'port': 6025}],
        }
        ring_files = [ring_name + '.ring.gz'
                      for ring_name in sorted(devs_by_ring_name1)]

        def _fake_load(gz_path, stub_objs, metadata_only=False):
            return RingData(
                devs=stub_objs[os.path.basename(gz_path)[:-8]],
                replica2part2dev_id=[],
                part_shift=24)

        with mock.patch(
                'swift.common.storage_policy.RingData.load'
        ) as mock_ld, \
                patch_policies(test_policies), \
                mock.patch('swift.common.storage_policy.whataremyips') \
                as mock_whataremyips, \
                temptree(ring_files) as tempdir:
            mock_whataremyips.return_value = my_ips

            cache = BindPortsCache(tempdir, bind_ip)

            self.assertEqual([
                mock.call(bind_ip),
            ], mock_whataremyips.mock_calls)
            mock_whataremyips.reset_mock()

            mock_ld.side_effect = partial(_fake_load,
                                          stub_objs=devs_by_ring_name1)
            self.assertEqual(set([
                6006, 6008, 6011, 6010, 6014,
            ]), cache.all_bind_ports_for_node())
            self.assertEqual([
                mock.call(os.path.join(tempdir, ring_files[0]),
                          metadata_only=True),
                mock.call(os.path.join(tempdir, ring_files[1]),
                          metadata_only=True),
                mock.call(os.path.join(tempdir, ring_files[2]),
                          metadata_only=True),
            ], mock_ld.mock_calls)
            mock_ld.reset_mock()

            mock_ld.side_effect = partial(_fake_load,
                                          stub_objs=devs_by_ring_name2)
            self.assertEqual(set([
                6006, 6008, 6011, 6010, 6014,
            ]), cache.all_bind_ports_for_node())
            self.assertEqual([], mock_ld.mock_calls)

            # but when all the file mtimes are made different, it'll
            # reload
            for gz_file in [os.path.join(tempdir, n)
                            for n in ring_files]:
                os.utime(gz_file, (88, 88))

            self.assertEqual(set([
                6016, 6020,
            ]), cache.all_bind_ports_for_node())
            self.assertEqual([
                mock.call(os.path.join(tempdir, ring_files[0]),
                          metadata_only=True),
                mock.call(os.path.join(tempdir, ring_files[1]),
                          metadata_only=True),
                mock.call(os.path.join(tempdir, ring_files[2]),
                          metadata_only=True),
            ], mock_ld.mock_calls)
            mock_ld.reset_mock()

            # Don't do something stupid like crash if a ring file is missing.
            os.unlink(os.path.join(tempdir, 'object-2.ring.gz'))

            self.assertEqual(set([
                6016, 6020,
            ]), cache.all_bind_ports_for_node())
            self.assertEqual([], mock_ld.mock_calls)

        # whataremyips() is only called in the constructor
        self.assertEqual([], mock_whataremyips.mock_calls)

    def test_singleton_passthrough(self):
        test_policies = [StoragePolicy(0, 'aay', True),
                         StoragePolicy(1, 'bee', False),
                         StoragePolicy(2, 'cee', False)]
        with patch_policies(test_policies):
            for policy in POLICIES:
                self.assertEqual(POLICIES[int(policy)], policy)

    def test_quorum_size_replication(self):
        expected_sizes = {1: 1,
                          2: 1,
                          3: 2,
                          4: 2,
                          5: 3}
        for n, expected in expected_sizes.items():
            policy = StoragePolicy(0, 'zero',
                                   object_ring=FakeRing(replicas=n))
            self.assertEqual(policy.quorum, expected)

    def test_quorum_size_erasure_coding(self):
        test_ec_policies = [
            ECStoragePolicy(10, 'ec8-2', ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=8, ec_nparity=2),
            ECStoragePolicy(11, 'df10-6', ec_type='flat_xor_hd_4',
                            ec_ndata=10, ec_nparity=6),
            ECStoragePolicy(12, 'ec4-2-dup', ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=4, ec_nparity=2, ec_duplication_factor=2),
        ]
        for ec_policy in test_ec_policies:
            k = ec_policy.ec_ndata
            expected_size = (
                (k + ec_policy.pyeclib_driver.min_parity_fragments_needed())
                * ec_policy.ec_duplication_factor
            )

            self.assertEqual(expected_size, ec_policy.quorum)

    def test_validate_ring(self):
        test_policies = [
            ECStoragePolicy(0, 'ec8-2', ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=8, ec_nparity=2,
                            is_default=True),
            ECStoragePolicy(1, 'ec10-4', ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=10, ec_nparity=4),
            ECStoragePolicy(2, 'ec4-2', ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=4, ec_nparity=2),
            ECStoragePolicy(3, 'ec4-2-2dup', ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=4, ec_nparity=2,
                            ec_duplication_factor=2)
        ]
        policies = StoragePolicyCollection(test_policies)

        class MockRingData(object):
            def __init__(self, num_replica):
                self.replica_count = num_replica

        def do_test(actual_load_ring_replicas):
            for policy, ring_replicas in zip(policies,
                                             actual_load_ring_replicas):
                with mock.patch('swift.common.ring.ring.RingData.load',
                                return_value=MockRingData(ring_replicas)):
                    necessary_replica_num = (policy.ec_n_unique_fragments *
                                             policy.ec_duplication_factor)
                    with mock.patch(
                            'swift.common.ring.ring.validate_configuration'):
                        msg = 'EC ring for policy %s needs to be configured ' \
                              'with exactly %d replicas.' % \
                              (policy.name, necessary_replica_num)
                        self.assertRaisesWithMessage(RingLoadError, msg,
                                                     policy.load_ring, 'mock')

        # first, do somethign completely different
        do_test([8, 10, 7, 11])
        # then again, closer to true, but fractional
        do_test([9.9, 14.1, 5.99999, 12.000000001])

    def test_storage_policy_get_info(self):
        test_policies = [
            StoragePolicy(0, 'zero', is_default=True),
            StoragePolicy(1, 'one', is_deprecated=True,
                          aliases='tahi, uno'),
            ECStoragePolicy(10, 'ten',
                            ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=10, ec_nparity=3),
            ECStoragePolicy(11, 'done', is_deprecated=True,
                            ec_type=DEFAULT_TEST_EC_TYPE,
                            ec_ndata=10, ec_nparity=3),
        ]
        policies = StoragePolicyCollection(test_policies)
        expected = {
            # default replication
            (0, True): {
                'name': 'zero',
                'aliases': 'zero',
                'default': True,
                'deprecated': False,
                'diskfile_module': 'egg:swift#replication.fs',
                'policy_type': REPL_POLICY
            },
            (0, False): {
                'name': 'zero',
                'aliases': 'zero',
                'default': True,
            },
            # deprecated replication
            (1, True): {
                'name': 'one',
                'aliases': 'one, tahi, uno',
                'default': False,
                'deprecated': True,
                'diskfile_module': 'egg:swift#replication.fs',
                'policy_type': REPL_POLICY
            },
            (1, False): {
                'name': 'one',
                'aliases': 'one, tahi, uno',
                'deprecated': True,
            },
            # enabled ec
            (10, True): {
                'name': 'ten',
                'aliases': 'ten',
                'default': False,
                'deprecated': False,
                'diskfile_module': 'egg:swift#erasure_coding.fs',
                'policy_type': EC_POLICY,
                'ec_type': DEFAULT_TEST_EC_TYPE,
                'ec_num_data_fragments': 10,
                'ec_num_parity_fragments': 3,
                'ec_object_segment_size': DEFAULT_EC_OBJECT_SEGMENT_SIZE,
                'ec_duplication_factor': 1,
            },
            (10, False): {
                'name': 'ten',
                'aliases': 'ten',
            },
            # deprecated ec
            (11, True): {
                'name': 'done',
                'aliases': 'done',
                'default': False,
                'deprecated': True,
                'diskfile_module': 'egg:swift#erasure_coding.fs',
                'policy_type': EC_POLICY,
                'ec_type': DEFAULT_TEST_EC_TYPE,
                'ec_num_data_fragments': 10,
                'ec_num_parity_fragments': 3,
                'ec_object_segment_size': DEFAULT_EC_OBJECT_SEGMENT_SIZE,
                'ec_duplication_factor': 1,
            },
            (11, False): {
                'name': 'done',
                'aliases': 'done',
                'deprecated': True,
            },
            # enabled ec with ec_duplication
            (12, True): {
                'name': 'twelve',
                'aliases': 'twelve',
                'default': False,
                'deprecated': False,
                'diskfile_module': 'egg:swift#erasure_coding.fs',
                'policy_type': EC_POLICY,
                'ec_type': DEFAULT_TEST_EC_TYPE,
                'ec_num_data_fragments': 10,
                'ec_num_parity_fragments': 3,
                'ec_object_segment_size': DEFAULT_EC_OBJECT_SEGMENT_SIZE,
                'ec_duplication_factor': 2,
            },
            (12, False): {
                'name': 'twelve',
                'aliases': 'twelve',
            },
        }
        self.maxDiff = None
        for policy in policies:
            expected_info = expected[(int(policy), True)]
            self.assertEqual(policy.get_info(config=True), expected_info)
            expected_info = expected[(int(policy), False)]
            self.assertEqual(policy.get_info(config=False), expected_info)

    def test_ec_fragment_size_cached(self):
        policy = ECStoragePolicy(
            0, 'ec2-1', ec_type=DEFAULT_TEST_EC_TYPE,
            ec_ndata=2, ec_nparity=1, object_ring=FakeRing(replicas=3),
            ec_segment_size=DEFAULT_EC_OBJECT_SEGMENT_SIZE, is_default=True)

        ec_driver = ECDriver(ec_type=DEFAULT_TEST_EC_TYPE,
                             k=2, m=1)
        expected_fragment_size = ec_driver.get_segment_info(
            DEFAULT_EC_OBJECT_SEGMENT_SIZE,
            DEFAULT_EC_OBJECT_SEGMENT_SIZE)['fragment_size']

        with mock.patch.object(
                policy.pyeclib_driver, 'get_segment_info') as fake:
            fake.return_value = {
                'fragment_size': expected_fragment_size}

            for x in range(10):
                self.assertEqual(expected_fragment_size,
                                 policy.fragment_size)
                # pyeclib_driver.get_segment_info is called only once
                self.assertEqual(1, fake.call_count)

    def test_get_diskfile_manager(self):
        # verify unique diskfile manager instances are returned
        policy = StoragePolicy(0, name='zero', is_default=True,
                               diskfile_module='replication.fs')

        dfm = policy.get_diskfile_manager({'devices': 'sdb1'}, debug_logger())
        self.assertEqual('sdb1', dfm.devices)
        dfm = policy.get_diskfile_manager({'devices': 'sdb2'}, debug_logger())
        self.assertEqual('sdb2', dfm.devices)
        dfm2 = policy.get_diskfile_manager({'devices': 'sdb2'}, debug_logger())
        self.assertEqual('sdb2', dfm2.devices)
        self.assertIsNot(dfm, dfm2)

    def test_get_diskfile_manager_custom_diskfile(self):
        calls = []
        is_policy_ok = True

        class DFM(object):
            def __init__(self, *args, **kwargs):
                calls.append((args, kwargs))

            @classmethod
            def check_policy(cls, policy):
                if not is_policy_ok:
                    raise ValueError("I am not ok")

        policy = StoragePolicy(0, name='zero', is_default=True,
                               diskfile_module='thin_air.fs')
        with mock.patch(
                'swift.common.storage_policy.load_pkg_resource',
                side_effect=lambda *a, **kw: DFM) as mock_load_pkg_resource:
            dfm = policy.get_diskfile_manager('arg', kwarg='kwarg')
        self.assertIsInstance(dfm, DFM)
        mock_load_pkg_resource.assert_called_with(
            'swift.diskfile', 'thin_air.fs')
        self.assertEqual([(('arg',), {'kwarg': 'kwarg'})], calls)

        calls = []
        is_policy_ok = False

        with mock.patch(
                'swift.common.storage_policy.load_pkg_resource',
                side_effect=lambda *a, **kw: DFM) as mock_load_pkg_resource:
            with self.assertRaises(PolicyError) as cm:
                policy.get_diskfile_manager('arg', kwarg='kwarg')
        mock_load_pkg_resource.assert_called_with(
            'swift.diskfile', 'thin_air.fs')
        self.assertIn('Invalid diskfile_module thin_air.fs', str(cm.exception))

    def test_get_diskfile_manager_invalid_policy_config(self):
        bad_policy = StoragePolicy(0, name='zero', is_default=True,
                                   diskfile_module='erasure_coding.fs')

        with self.assertRaises(PolicyError) as cm:
            bad_policy.get_diskfile_manager()
        self.assertIn('Invalid diskfile_module erasure_coding.fs',
                      str(cm.exception))

        bad_policy = ECStoragePolicy(0, name='one', is_default=True,
                                     ec_type=DEFAULT_TEST_EC_TYPE,
                                     ec_ndata=10, ec_nparity=4,
                                     diskfile_module='replication.fs')

        with self.assertRaises(PolicyError) as cm:
            bad_policy.get_diskfile_manager()

        self.assertIn('Invalid diskfile_module replication.fs',
                      str(cm.exception))

        bad_policy = StoragePolicy(0, name='zero', is_default=True,
                                   diskfile_module='thin_air.fs')

        with self.assertRaises(PolicyError) as cm:
            bad_policy.get_diskfile_manager()

        self.assertIn('Unable to load diskfile_module thin_air.fs',
                      str(cm.exception))


if __name__ == '__main__':
    unittest.main()
