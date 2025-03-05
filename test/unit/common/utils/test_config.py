# Copyright (c) 2010-2024 OpenStack Foundation
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

"""Tests for swift.common.utils.config"""
import os
import tempfile
from textwrap import dedent
import unittest

from unittest import mock

from swift.common.utils import config

from io import StringIO
from test import annotate_failure
from test.unit import temptree


class TestUtilsConfig(unittest.TestCase):

    def test_TRUE_VALUES(self):
        for v in config.TRUE_VALUES:
            self.assertEqual(v, v.lower())

    @mock.patch.object(config, 'TRUE_VALUES', 'hello world'.split())
    def test_config_true_value(self):
        for val in 'hello world HELLO WORLD'.split():
            self.assertTrue(config.config_true_value(val) is True)
        self.assertTrue(config.config_true_value(True) is True)
        self.assertTrue(config.config_true_value('foo') is False)
        self.assertTrue(config.config_true_value(False) is False)
        self.assertTrue(config.config_true_value(None) is False)

    def test_non_negative_float(self):
        self.assertEqual(0, config.non_negative_float('0.0'))
        self.assertEqual(0, config.non_negative_float(0.0))
        self.assertEqual(1.1, config.non_negative_float(1.1))
        self.assertEqual(1.1, config.non_negative_float('1.1'))
        self.assertEqual(1.0, config.non_negative_float('1'))
        self.assertEqual(1, config.non_negative_float(True))
        self.assertEqual(0, config.non_negative_float(False))

        with self.assertRaises(ValueError) as cm:
            config.non_negative_float(-1.1)
        self.assertEqual(
            'Value must be a non-negative float number, not "-1.1".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_float('-1.1')
        self.assertEqual(
            'Value must be a non-negative float number, not "-1.1".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_float('one')
        self.assertEqual(
            'Value must be a non-negative float number, not "one".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_float(None)
        self.assertEqual(
            'Value must be a non-negative float number, not "None".',
            str(cm.exception))

    def test_non_negative_int(self):
        self.assertEqual(0, config.non_negative_int('0'))
        self.assertEqual(0, config.non_negative_int(0.0))
        self.assertEqual(1, config.non_negative_int(1))
        self.assertEqual(1, config.non_negative_int(1.1))
        self.assertEqual(1, config.non_negative_int('1'))
        self.assertEqual(1, config.non_negative_int(True))
        self.assertEqual(0, config.non_negative_int(False))

        with self.assertRaises(ValueError) as cm:
            config.non_negative_int(-1)
        self.assertEqual(
            'Value must be a non-negative integer, not "-1".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_int('-1')
        self.assertEqual(
            'Value must be a non-negative integer, not "-1".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_int('-1.1')
        self.assertEqual(
            'Value must be a non-negative integer, not "-1.1".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_int('1.1')
        self.assertEqual(
            'Value must be a non-negative integer, not "1.1".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_int('1.0')
        self.assertEqual(
            'Value must be a non-negative integer, not "1.0".',
            str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            config.non_negative_int('one')
        self.assertEqual(
            'Value must be a non-negative integer, not "one".',
            str(cm.exception))

    def test_config_positive_int_value(self):
        expectations = {
            # value : expected,
            u'1': 1,
            b'1': 1,
            1: 1,
            1.1: 1,
            u'2': 2,
            b'2': 2,
            u'1024': 1024,
            b'1024': 1024,
            u'0': ValueError,
            b'0': ValueError,
            u'-1': ValueError,
            b'-1': ValueError,
            u'0x01': ValueError,
            b'0x01': ValueError,
            u'asdf': ValueError,
            b'asdf': ValueError,
            None: ValueError,
            0: ValueError,
            -1: ValueError,
            u'1.2': ValueError,  # string expresses float should be value error
            b'1.2': ValueError,  # string expresses float should be value error
        }
        for value, expected in expectations.items():
            try:
                rv = config.config_positive_int_value(value)
            except Exception as e:
                if e.__class__ is not expected:
                    raise
                else:
                    self.assertEqual(
                        'Config option must be an positive int number, '
                        'not "%s".' % value, e.args[0])
            else:
                self.assertEqual(expected, rv)

    def test_config_float_value(self):
        for args, expected in (
                ((99, None, None), 99.0),
                ((99.01, None, None), 99.01),
                (('99', None, None), 99.0),
                (('99.01', None, None), 99.01),
                ((99, 99, None), 99.0),
                ((99.01, 99.01, None), 99.01),
                (('99', 99, None), 99.0),
                (('99.01', 99.01, None), 99.01),
                ((99, None, 99), 99.0),
                ((99.01, None, 99.01), 99.01),
                (('99', None, 99), 99.0),
                (('99.01', None, 99.01), 99.01),
                ((-99, -99, -99), -99.0),
                ((-99.01, -99.01, -99.01), -99.01),
                (('-99', -99, -99), -99.0),
                (('-99.01', -99.01, -99.01), -99.01),):
            actual = config.config_float_value(*args)
            self.assertEqual(expected, actual)

        for val, minimum in ((99, 100),
                             ('99', 100),
                             (-99, -98),
                             ('-98.01', -98)):
            with self.assertRaises(ValueError) as cm:
                config.config_float_value(val, minimum=minimum)
            self.assertIn('greater than %s' % minimum, cm.exception.args[0])
            self.assertNotIn('less than', cm.exception.args[0])

        for val, maximum in ((99, 98),
                             ('99', 98),
                             (-99, -100),
                             ('-97.9', -98)):
            with self.assertRaises(ValueError) as cm:
                config.config_float_value(val, maximum=maximum)
            self.assertIn('less than %s' % maximum, cm.exception.args[0])
            self.assertNotIn('greater than', cm.exception.args[0])

        for val, minimum, maximum in ((99, 99, 98),
                                      ('99', 100, 100),
                                      (99, 98, 98),):
            with self.assertRaises(ValueError) as cm:
                config.config_float_value(
                    val, minimum=minimum, maximum=maximum)
            self.assertIn('greater than %s' % minimum, cm.exception.args[0])
            self.assertIn('less than %s' % maximum, cm.exception.args[0])

    def test_config_percent_value(self):
        for arg, expected in (
                (99, 0.99),
                (25.5, 0.255),
                ('99', 0.99),
                ('25.5', 0.255),
                (0, 0.0),
                ('0', 0.0),
                ('100', 1.0),
                (100, 1.0),
                (1, 0.01),
                ('1', 0.01),
                (25, 0.25)):
            actual = config.config_percent_value(arg)
            self.assertEqual(expected, actual)

        # bad values
        for val in (-1, '-1', 101, '101'):
            with self.assertRaises(ValueError) as cm:
                config.config_percent_value(val)
            self.assertIn('Config option must be a number, greater than 0, '
                          'less than 100, not "{}"'.format(val),
                          cm.exception.args[0])

    def test_config_request_node_count_value(self):
        def do_test(value, replicas, expected):
            self.assertEqual(
                expected,
                config.config_request_node_count_value(value)(replicas))

        do_test('0', 10, 0)
        do_test('1 * replicas', 3, 3)
        do_test('1 * replicas', 11, 11)
        do_test('2 * replicas', 3, 6)
        do_test('2 * replicas', 11, 22)
        do_test('11', 11, 11)
        do_test('10', 11, 10)
        do_test('12', 11, 12)

        for bad in ('1.1', 1.1, 'auto', 'bad',
                    '2.5 * replicas', 'two * replicas'):
            with annotate_failure(bad):
                with self.assertRaises(ValueError):
                    config.config_request_node_count_value(bad)

    def test_config_auto_int_value(self):
        expectations = {
            # (value, default) : expected,
            ('1', 0): 1,
            (1, 0): 1,
            ('asdf', 0): ValueError,
            ('auto', 1): 1,
            ('AutO', 1): 1,
            ('Aut0', 1): ValueError,
            (None, 1): 1,
        }
        for (value, default), expected in expectations.items():
            try:
                rv = config.config_auto_int_value(value, default)
            except Exception as e:
                if e.__class__ is not expected:
                    raise
            else:
                self.assertEqual(expected, rv)

    def test_config_fallocate_value(self):
        fallocate_value, is_percent = config.config_fallocate_value('10%')
        self.assertEqual(fallocate_value, 10)
        self.assertTrue(is_percent)
        fallocate_value, is_percent = config.config_fallocate_value('10')
        self.assertEqual(fallocate_value, 10)
        self.assertFalse(is_percent)
        try:
            fallocate_value, is_percent = config.config_fallocate_value('ab%')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: ab% is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = config.config_fallocate_value('ab')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: ab is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = config.config_fallocate_value('1%%')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: 1%% is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = config.config_fallocate_value('10.0')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: 10.0 is an invalid value for '
                                   'fallocate_reserve.')
        fallocate_value, is_percent = config.config_fallocate_value('10.5%')
        self.assertEqual(fallocate_value, 10.5)
        self.assertTrue(is_percent)
        fallocate_value, is_percent = config.config_fallocate_value('10.000%')
        self.assertEqual(fallocate_value, 10.000)
        self.assertTrue(is_percent)


class ResellerConfReader(unittest.TestCase):

    def setUp(self):
        self.default_rules = {'operator_roles': ['admin', 'swiftoperator'],
                              'service_roles': [],
                              'require_group': ''}

    def test_defaults(self):
        conf = {}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_'])
        self.assertEqual(options['AUTH_'], self.default_rules)

    def test_same_as_default(self):
        conf = {'reseller_prefix': 'AUTH',
                'operator_roles': 'admin, swiftoperator'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_'])
        self.assertEqual(options['AUTH_'], self.default_rules)

    def test_single_blank_reseller(self):
        conf = {'reseller_prefix': ''}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])
        self.assertEqual(options[''], self.default_rules)

    def test_single_blank_reseller_with_conf(self):
        conf = {'reseller_prefix': '',
                "''operator_roles": 'role1, role2'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])
        self.assertEqual(options[''].get('operator_roles'),
                         ['role1', 'role2'])
        self.assertEqual(options[''].get('service_roles'),
                         self.default_rules.get('service_roles'))
        self.assertEqual(options[''].get('require_group'),
                         self.default_rules.get('require_group'))

    def test_multiple_same_resellers(self):
        conf = {'reseller_prefix': " '' , '' "}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])

        conf = {'reseller_prefix': '_, _'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['_'])

        conf = {'reseller_prefix': 'AUTH, PRE2, AUTH, PRE2'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', 'PRE2_'])

    def test_several_resellers_with_conf(self):
        conf = {'reseller_prefix': 'PRE1, PRE2',
                'PRE1_operator_roles': 'role1, role2',
                'PRE1_service_roles': 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['PRE1_', 'PRE2_'])

        self.assertEqual(set(['role1', 'role2']),
                         set(options['PRE1_'].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options['PRE1_'].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options['PRE1_'].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

    def test_several_resellers_first_blank(self):
        conf = {'reseller_prefix': " '' , PRE2",
                "''operator_roles": 'role1, role2',
                "''service_roles": 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['', 'PRE2_'])

        self.assertEqual(set(['role1', 'role2']),
                         set(options[''].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options[''].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options[''].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

    def test_several_resellers_with_blank_comma(self):
        conf = {'reseller_prefix': "AUTH , '', PRE2",
                "''operator_roles": 'role1, role2',
                "''service_roles": 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', '', 'PRE2_'])
        self.assertEqual(set(['admin', 'swiftoperator']),
                         set(options['AUTH_'].get('operator_roles')))
        self.assertEqual(set(['role1', 'role2']),
                         set(options[''].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual([],
                         options['AUTH_'].get('service_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options[''].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options['AUTH_'].get('require_group'))
        self.assertEqual('', options[''].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

    def test_stray_comma(self):
        conf = {'reseller_prefix': "AUTH ,, PRE2",
                "''operator_roles": 'role1, role2',
                "''service_roles": 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', 'PRE2_'])
        self.assertEqual(set(['admin', 'swiftoperator']),
                         set(options['AUTH_'].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual([],
                         options['AUTH_'].get('service_roles'))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options['AUTH_'].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

    def test_multiple_stray_commas_resellers(self):
        conf = {'reseller_prefix': ' , , ,'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])
        self.assertEqual(options[''], self.default_rules)

    def test_unprefixed_options(self):
        conf = {'reseller_prefix': "AUTH , '', PRE2",
                "operator_roles": 'role1, role2',
                "service_roles": 'role3, role4',
                'require_group': 'auth_blank_group',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = config.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', '', 'PRE2_'])
        self.assertEqual(set(['role1', 'role2']),
                         set(options['AUTH_'].get('operator_roles')))
        self.assertEqual(set(['role1', 'role2']),
                         set(options[''].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options['AUTH_'].get('service_roles')))
        self.assertEqual(set(['role3', 'role4']),
                         set(options[''].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('auth_blank_group',
                         options['AUTH_'].get('require_group'))
        self.assertEqual('auth_blank_group', options[''].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))


class TestAffinityKeyFunction(unittest.TestCase):
    def setUp(self):
        self.nodes = [dict(id=0, region=1, zone=1),
                      dict(id=1, region=1, zone=2),
                      dict(id=2, region=2, zone=1),
                      dict(id=3, region=2, zone=2),
                      dict(id=4, region=3, zone=1),
                      dict(id=5, region=3, zone=2),
                      dict(id=6, region=4, zone=0),
                      dict(id=7, region=4, zone=1)]

    def test_single_region(self):
        keyfn = config.affinity_key_function("r3=1")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([4, 5, 0, 1, 2, 3, 6, 7], ids)

    def test_bogus_value(self):
        self.assertRaises(ValueError,
                          config.affinity_key_function, "r3")
        self.assertRaises(ValueError,
                          config.affinity_key_function, "r3=elephant")

    def test_empty_value(self):
        # Empty's okay, it just means no preference
        keyfn = config.affinity_key_function("")
        self.assertTrue(callable(keyfn))
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([0, 1, 2, 3, 4, 5, 6, 7], ids)

    def test_all_whitespace_value(self):
        # Empty's okay, it just means no preference
        keyfn = config.affinity_key_function("  \n")
        self.assertTrue(callable(keyfn))
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([0, 1, 2, 3, 4, 5, 6, 7], ids)

    def test_with_zone_zero(self):
        keyfn = config.affinity_key_function("r4z0=1")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([6, 0, 1, 2, 3, 4, 5, 7], ids)

    def test_multiple(self):
        keyfn = config.affinity_key_function("r1=100, r4=200, r3z1=1")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([4, 0, 1, 6, 7, 2, 3, 5], ids)

    def test_more_specific_after_less_specific(self):
        keyfn = config.affinity_key_function("r2=100, r2z2=50")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([3, 2, 0, 1, 4, 5, 6, 7], ids)


class TestAffinityLocalityPredicate(unittest.TestCase):
    def setUp(self):
        self.nodes = [dict(id=0, region=1, zone=1),
                      dict(id=1, region=1, zone=2),
                      dict(id=2, region=2, zone=1),
                      dict(id=3, region=2, zone=2),
                      dict(id=4, region=3, zone=1),
                      dict(id=5, region=3, zone=2),
                      dict(id=6, region=4, zone=0),
                      dict(id=7, region=4, zone=1)]

    def test_empty(self):
        pred = config.affinity_locality_predicate('')
        self.assertTrue(pred is None)

    def test_region(self):
        pred = config.affinity_locality_predicate('r1')
        self.assertTrue(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0, 1], ids)

    def test_zone(self):
        pred = config.affinity_locality_predicate('r1z1')
        self.assertTrue(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0], ids)

    def test_multiple(self):
        pred = config.affinity_locality_predicate('r1, r3, r4z0')
        self.assertTrue(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0, 1, 4, 5, 6], ids)

    def test_invalid(self):
        self.assertRaises(ValueError,
                          config.affinity_locality_predicate, 'falafel')
        self.assertRaises(ValueError,
                          config.affinity_locality_predicate, 'r8zQ')
        self.assertRaises(ValueError,
                          config.affinity_locality_predicate, 'r2d2')
        self.assertRaises(ValueError,
                          config.affinity_locality_predicate, 'r1z1=1')


class TestReadConf(unittest.TestCase):

    def test_readconf(self):
        conf = '''[section1]
foo = bar

[section2]
log_name = yarr'''
        # setup a real file
        fd, temppath = tempfile.mkstemp()
        with os.fdopen(fd, 'w') as f:
            f.write(conf)
        make_filename = lambda: temppath
        # setup a file stream
        make_fp = lambda: StringIO(conf)
        for conf_object_maker in (make_filename, make_fp):
            conffile = conf_object_maker()
            result = config.readconf(conffile)
            expected = {'__file__': conffile,
                        'log_name': None,
                        'section1': {'foo': 'bar'},
                        'section2': {'log_name': 'yarr'}}
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = config.readconf(conffile, 'section1')
            expected = {'__file__': conffile, 'log_name': 'section1',
                        'foo': 'bar'}
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = config.readconf(conffile, 'section2').get('log_name')
            expected = 'yarr'
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = config.readconf(conffile, 'section1',
                                     log_name='foo').get('log_name')
            expected = 'foo'
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = config.readconf(conffile, 'section1',
                                     defaults={'bar': 'baz'})
            expected = {'__file__': conffile, 'log_name': 'section1',
                        'foo': 'bar', 'bar': 'baz'}
            self.assertEqual(result, expected)

        self.assertRaisesRegex(
            ValueError, 'Unable to find section3 config section in.*',
            config.readconf, temppath, 'section3')
        os.unlink(temppath)
        self.assertRaises(IOError, config.readconf, temppath)

    def test_readconf_raw(self):
        conf = '''[section1]
foo = bar

[section2]
log_name = %(yarr)s'''
        # setup a real file
        fd, temppath = tempfile.mkstemp()
        with os.fdopen(fd, 'w') as f:
            f.write(conf)
        make_filename = lambda: temppath
        # setup a file stream
        make_fp = lambda: StringIO(conf)
        for conf_object_maker in (make_filename, make_fp):
            conffile = conf_object_maker()
            result = config.readconf(conffile, raw=True)
            expected = {'__file__': conffile,
                        'log_name': None,
                        'section1': {'foo': 'bar'},
                        'section2': {'log_name': '%(yarr)s'}}
            self.assertEqual(result, expected)
        os.unlink(temppath)
        self.assertRaises(IOError, config.readconf, temppath)

    def test_readconf_dir(self):
        config_dir = {
            'server.conf.d/01.conf': """
            [DEFAULT]
            port = 8080
            foo = bar

            [section1]
            name=section1
            """,
            'server.conf.d/section2.conf': """
            [DEFAULT]
            port = 8081
            bar = baz

            [section2]
            name=section2
            """,
            'other-server.conf.d/01.conf': """
            [DEFAULT]
            port = 8082

            [section3]
            name=section3
            """
        }
        # strip indent from test config contents
        config_dir = dict((f, dedent(c)) for (f, c) in config_dir.items())
        with temptree(*zip(*config_dir.items())) as path:
            conf_dir = os.path.join(path, 'server.conf.d')
            conf = config.readconf(conf_dir)
        expected = {
            '__file__': os.path.join(path, 'server.conf.d'),
            'log_name': None,
            'section1': {
                'port': '8081',
                'foo': 'bar',
                'bar': 'baz',
                'name': 'section1',
            },
            'section2': {
                'port': '8081',
                'foo': 'bar',
                'bar': 'baz',
                'name': 'section2',
            },
        }
        self.assertEqual(conf, expected)

    def test_readconf_dir_ignores_hidden_and_nondotconf_files(self):
        config_dir = {
            'server.conf.d/01.conf': """
            [section1]
            port = 8080
            """,
            'server.conf.d/.01.conf.swp': """
            [section]
            port = 8081
            """,
            'server.conf.d/01.conf-bak': """
            [section]
            port = 8082
            """,
        }
        # strip indent from test config contents
        config_dir = dict((f, dedent(c)) for (f, c) in config_dir.items())
        with temptree(*zip(*config_dir.items())) as path:
            conf_dir = os.path.join(path, 'server.conf.d')
            conf = config.readconf(conf_dir)
        expected = {
            '__file__': os.path.join(path, 'server.conf.d'),
            'log_name': None,
            'section1': {
                'port': '8080',
            },
        }
        self.assertEqual(conf, expected)
