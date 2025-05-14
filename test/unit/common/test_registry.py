# Copyright (c) 2022 NVIDIA
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

from swift.common import registry, utils

from unittest import mock
import unittest


class TestSwiftInfo(unittest.TestCase):

    def setUp(self):
        patcher = mock.patch.object(registry, '_swift_info', dict())
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(registry, '_swift_admin_info', dict())
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_register_swift_info(self):
        registry.register_swift_info(foo='bar')
        registry.register_swift_info(lorem='ipsum')
        registry.register_swift_info('cap1', cap1_foo='cap1_bar')
        registry.register_swift_info('cap1', cap1_lorem='cap1_ipsum')

        self.assertTrue('swift' in registry._swift_info)
        self.assertTrue('foo' in registry._swift_info['swift'])
        self.assertEqual(registry._swift_info['swift']['foo'], 'bar')
        self.assertTrue('lorem' in registry._swift_info['swift'])
        self.assertEqual(registry._swift_info['swift']['lorem'], 'ipsum')

        self.assertTrue('cap1' in registry._swift_info)
        self.assertTrue('cap1_foo' in registry._swift_info['cap1'])
        self.assertEqual(registry._swift_info['cap1']['cap1_foo'], 'cap1_bar')
        self.assertTrue('cap1_lorem' in registry._swift_info['cap1'])
        self.assertEqual(registry._swift_info['cap1']['cap1_lorem'],
                         'cap1_ipsum')

        self.assertRaises(ValueError,
                          registry.register_swift_info, 'admin', foo='bar')

        self.assertRaises(ValueError,
                          registry.register_swift_info, 'disallowed_sections',
                          disallowed_sections=None)

        registry.register_swift_info('goodkey', foo='5.6')
        self.assertRaises(ValueError,
                          registry.register_swift_info, 'bad.key', foo='5.6')
        data = {'bad.key': '5.6'}
        self.assertRaises(ValueError,
                          registry.register_swift_info, 'goodkey', **data)

    def test_get_swift_info(self):
        registry._swift_info = {'swift': {'foo': 'bar'},
                                'cap1': {'cap1_foo': 'cap1_bar'}}
        registry._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = registry.get_swift_info()

        self.assertNotIn('admin', info)

        self.assertIn('swift', info)
        self.assertIn('foo', info['swift'])
        self.assertEqual(registry._swift_info['swift']['foo'], 'bar')

        self.assertIn('cap1', info)
        self.assertIn('cap1_foo', info['cap1'])
        self.assertEqual(registry._swift_info['cap1']['cap1_foo'], 'cap1_bar')

    def test_get_swift_info_with_disallowed_sections(self):
        registry._swift_info = {'swift': {'foo': 'bar'},
                                'cap1': {'cap1_foo': 'cap1_bar'},
                                'cap2': {'cap2_foo': 'cap2_bar'},
                                'cap3': {'cap3_foo': 'cap3_bar'}}
        registry._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = registry.get_swift_info(disallowed_sections=['cap1', 'cap3'])

        self.assertNotIn('admin', info)

        self.assertIn('swift', info)
        self.assertIn('foo', info['swift'])
        self.assertEqual(info['swift']['foo'], 'bar')

        self.assertNotIn('cap1', info)

        self.assertIn('cap2', info)
        self.assertIn('cap2_foo', info['cap2'])
        self.assertEqual(info['cap2']['cap2_foo'], 'cap2_bar')

        self.assertNotIn('cap3', info)

    def test_register_swift_admin_info(self):
        registry.register_swift_info(admin=True, admin_foo='admin_bar')
        registry.register_swift_info(admin=True, admin_lorem='admin_ipsum')
        registry.register_swift_info('cap1', admin=True, ac1_foo='ac1_bar')
        registry.register_swift_info('cap1', admin=True, ac1_lorem='ac1_ipsum')

        self.assertIn('swift', registry._swift_admin_info)
        self.assertIn('admin_foo', registry._swift_admin_info['swift'])
        self.assertEqual(
            registry._swift_admin_info['swift']['admin_foo'], 'admin_bar')
        self.assertIn('admin_lorem', registry._swift_admin_info['swift'])
        self.assertEqual(
            registry._swift_admin_info['swift']['admin_lorem'], 'admin_ipsum')

        self.assertIn('cap1', registry._swift_admin_info)
        self.assertIn('ac1_foo', registry._swift_admin_info['cap1'])
        self.assertEqual(
            registry._swift_admin_info['cap1']['ac1_foo'], 'ac1_bar')
        self.assertIn('ac1_lorem', registry._swift_admin_info['cap1'])
        self.assertEqual(
            registry._swift_admin_info['cap1']['ac1_lorem'], 'ac1_ipsum')

        self.assertNotIn('swift', registry._swift_info)
        self.assertNotIn('cap1', registry._swift_info)

    def test_get_swift_admin_info(self):
        registry._swift_info = {'swift': {'foo': 'bar'},
                                'cap1': {'cap1_foo': 'cap1_bar'}}
        registry._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = registry.get_swift_info(admin=True)

        self.assertIn('admin', info)
        self.assertIn('admin_cap1', info['admin'])
        self.assertIn('ac1_foo', info['admin']['admin_cap1'])
        self.assertEqual(info['admin']['admin_cap1']['ac1_foo'], 'ac1_bar')

        self.assertIn('swift', info)
        self.assertIn('foo', info['swift'])
        self.assertEqual(registry._swift_info['swift']['foo'], 'bar')

        self.assertIn('cap1', info)
        self.assertIn('cap1_foo', info['cap1'])
        self.assertEqual(registry._swift_info['cap1']['cap1_foo'], 'cap1_bar')

    def test_get_swift_admin_info_with_disallowed_sections(self):
        registry._swift_info = {'swift': {'foo': 'bar'},
                                'cap1': {'cap1_foo': 'cap1_bar'},
                                'cap2': {'cap2_foo': 'cap2_bar'},
                                'cap3': {'cap3_foo': 'cap3_bar'}}
        registry._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = registry.get_swift_info(
            admin=True, disallowed_sections=['cap1', 'cap3'])

        self.assertIn('admin', info)
        self.assertIn('admin_cap1', info['admin'])
        self.assertIn('ac1_foo', info['admin']['admin_cap1'])
        self.assertEqual(info['admin']['admin_cap1']['ac1_foo'], 'ac1_bar')
        self.assertIn('disallowed_sections', info['admin'])
        self.assertIn('cap1', info['admin']['disallowed_sections'])
        self.assertNotIn('cap2', info['admin']['disallowed_sections'])
        self.assertIn('cap3', info['admin']['disallowed_sections'])

        self.assertIn('swift', info)
        self.assertIn('foo', info['swift'])
        self.assertEqual(info['swift']['foo'], 'bar')

        self.assertNotIn('cap1', info)

        self.assertIn('cap2', info)
        self.assertIn('cap2_foo', info['cap2'])
        self.assertEqual(info['cap2']['cap2_foo'], 'cap2_bar')

        self.assertNotIn('cap3', info)

    def test_get_swift_admin_info_with_disallowed_sub_sections(self):
        registry._swift_info = {'swift': {'foo': 'bar'},
                                'cap1': {'cap1_foo': 'cap1_bar',
                                         'cap1_moo': 'cap1_baa'},
                                'cap2': {'cap2_foo': 'cap2_bar'},
                                'cap3': {'cap2_foo': 'cap2_bar'},
                                'cap4': {'a': {'b': {'c': 'c'},
                                               'b.c': 'b.c'}}}
        registry._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = registry.get_swift_info(
            admin=True, disallowed_sections=['cap1.cap1_foo', 'cap3',
                                             'cap4.a.b.c'])
        self.assertNotIn('cap3', info)
        self.assertEqual(info['cap1']['cap1_moo'], 'cap1_baa')
        self.assertNotIn('cap1_foo', info['cap1'])
        self.assertNotIn('c', info['cap4']['a']['b'])
        self.assertEqual(info['cap4']['a']['b.c'], 'b.c')

    def test_get_swift_info_with_unmatched_disallowed_sections(self):
        cap1 = {'cap1_foo': 'cap1_bar',
                'cap1_moo': 'cap1_baa'}
        registry._swift_info = {'swift': {'foo': 'bar'},
                                'cap1': cap1}
        # expect no exceptions
        info = registry.get_swift_info(
            disallowed_sections=['cap2.cap1_foo', 'cap1.no_match',
                                 'cap1.cap1_foo.no_match.no_match'])
        self.assertEqual(info['cap1'], cap1)

    def test_register_swift_info_import_from_utils(self):
        # verify that the functions are available to import from utils
        utils.register_swift_info(foo='bar')
        self.assertTrue('swift' in registry._swift_info)
        self.assertTrue('foo' in registry._swift_info['swift'])
        self.assertEqual(registry._swift_info['swift']['foo'], 'bar')
        self.assertEqual(registry.get_swift_info(admin=True),
                         utils.get_swift_info(admin=True))


class TestSensitiveRegistry(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(registry, '_sensitive_headers', set())
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(registry, '_sensitive_params', set())
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_register_sensitive_header(self):
        self.assertFalse(registry._sensitive_headers)

        registry.register_sensitive_header('Some-Header')
        expected_headers = {'some-header'}
        self.assertEqual(expected_headers, registry._sensitive_headers)

        expected_headers.add("new-header")
        registry.register_sensitive_header("New-Header")
        self.assertEqual(expected_headers, registry._sensitive_headers)

        for header_not_str in (1, None, 1.1):
            with self.assertRaises(TypeError):
                registry.register_sensitive_header(header_not_str)
            self.assertEqual(expected_headers, registry._sensitive_headers)

        with self.assertRaises(UnicodeError):
            registry.register_sensitive_header('\xe2\x98\x83')
        self.assertEqual(expected_headers, registry._sensitive_headers)

    def test_register_sensitive_param(self):
        self.assertFalse(registry._sensitive_params)

        registry.register_sensitive_param('some_param')
        expected_params = {'some_param'}
        self.assertEqual(expected_params, registry._sensitive_params)

        expected_params.add("another")
        registry.register_sensitive_param("another")
        self.assertEqual(expected_params, registry._sensitive_params)

        for param_not_str in (1, None, 1.1):
            with self.assertRaises(TypeError):
                registry.register_sensitive_param(param_not_str)
            self.assertEqual(expected_params, registry._sensitive_params)

        with self.assertRaises(UnicodeError):
            registry.register_sensitive_param('\xe2\x98\x83')
        self.assertEqual(expected_params, registry._sensitive_params)

    def test_get_sensitive_headers(self):
        self.assertFalse(registry.get_sensitive_headers())

        registry.register_sensitive_header('Header1')
        self.assertEqual(registry.get_sensitive_headers(), {'header1'})
        self.assertEqual(registry.get_sensitive_headers(),
                         registry._sensitive_headers)

        registry.register_sensitive_header('Header2')
        self.assertEqual(registry.get_sensitive_headers(),
                         {'header1', 'header2'})
        self.assertEqual(registry.get_sensitive_headers(),
                         registry._sensitive_headers)

    def test_get_sensitive_params(self):
        self.assertFalse(registry.get_sensitive_params())

        registry.register_sensitive_param('Param1')
        self.assertEqual(registry.get_sensitive_params(), {'Param1'})
        self.assertEqual(registry.get_sensitive_params(),
                         registry._sensitive_params)

        registry.register_sensitive_param('param')
        self.assertEqual(registry.get_sensitive_params(),
                         {'Param1', 'param'})
        self.assertEqual(registry.get_sensitive_params(),
                         registry._sensitive_params)
