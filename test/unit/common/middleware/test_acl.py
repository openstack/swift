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

import unittest

from swift.common.middleware import acl


class TestACL(unittest.TestCase):

    def test_clean_acl(self):
        value = acl.clean_acl('header', '.r:*')
        self.assertEqual(value, '.r:*')
        value = acl.clean_acl('header', '.r:specific.host')
        self.assertEqual(value, '.r:specific.host')
        value = acl.clean_acl('header', '.r:.ending.with')
        self.assertEqual(value, '.r:.ending.with')
        value = acl.clean_acl('header', '.r:*.ending.with')
        self.assertEqual(value, '.r:.ending.with')
        value = acl.clean_acl('header', '.r:-*.ending.with')
        self.assertEqual(value, '.r:-.ending.with')
        value = acl.clean_acl('header', '.r:one,.r:two')
        self.assertEqual(value, '.r:one,.r:two')
        value = acl.clean_acl('header', '.r:*,.r:-specific.host')
        self.assertEqual(value, '.r:*,.r:-specific.host')
        value = acl.clean_acl('header', '.r:*,.r:-.ending.with')
        self.assertEqual(value, '.r:*,.r:-.ending.with')
        value = acl.clean_acl('header', '.r:one,.r:-two')
        self.assertEqual(value, '.r:one,.r:-two')
        value = acl.clean_acl('header', '.r:one,.r:-two,account,account:user')
        self.assertEqual(value, '.r:one,.r:-two,account,account:user')
        value = acl.clean_acl('header', 'TEST_account')
        self.assertEqual(value, 'TEST_account')
        value = acl.clean_acl('header', '.ref:*')
        self.assertEqual(value, '.r:*')
        value = acl.clean_acl('header', '.referer:*')
        self.assertEqual(value, '.r:*')
        value = acl.clean_acl('header', '.referrer:*')
        self.assertEqual(value, '.r:*')
        value = acl.clean_acl('header',
                              ' .r : one , ,, .r:two , .r : - three ')
        self.assertEqual(value, '.r:one,.r:two,.r:-three')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.unknown:test')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.r:')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.r:*.')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.r : * . ')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.r:-*.')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.r : - * . ')
        self.assertRaises(ValueError, acl.clean_acl, 'header', ' .r : ')
        self.assertRaises(ValueError, acl.clean_acl, 'header', 'user , .r : ')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.r:-')
        self.assertRaises(ValueError, acl.clean_acl, 'header', ' .r : - ')
        self.assertRaises(ValueError, acl.clean_acl, 'header',
                          'user , .r : - ')
        self.assertRaises(ValueError, acl.clean_acl, 'write-header', '.r:r')

    def test_parse_acl(self):
        self.assertEqual(acl.parse_acl(None), ([], []))
        self.assertEqual(acl.parse_acl(''), ([], []))
        self.assertEqual(acl.parse_acl('.r:ref1'), (['ref1'], []))
        self.assertEqual(acl.parse_acl('.r:-ref1'), (['-ref1'], []))
        self.assertEqual(acl.parse_acl('account:user'),
                         ([], ['account:user']))
        self.assertEqual(acl.parse_acl('account'), ([], ['account']))
        self.assertEqual(acl.parse_acl('acc1,acc2:usr2,.r:ref3,.r:-ref4'),
                         (['ref3', '-ref4'], ['acc1', 'acc2:usr2']))
        self.assertEqual(acl.parse_acl(
            'acc1,acc2:usr2,.r:ref3,acc3,acc4:usr4,.r:ref5,.r:-ref6'),
            (['ref3', 'ref5', '-ref6'],
             ['acc1', 'acc2:usr2', 'acc3', 'acc4:usr4']))

    def test_parse_v2_acl(self):
        # For all these tests, the header name will be "hdr".
        tests = [
            # Simple case: all ACL data in one header line
            ({'hdr': '{"a":1,"b":"foo"}'}, {'a': 1, 'b': 'foo'}),

            # No header "hdr" exists -- should return None
            ({}, None),
            ({'junk': 'junk'}, None),

            # Empty ACLs should return empty dict
            ({'hdr': ''}, {}),
            ({'hdr': '{}'}, {}),
            ({'hdr': '{ }'}, {}),

            # Bad input -- should return None
            ({'hdr': '["array"]'}, None),
            ({'hdr': 'null'}, None),
            ({'hdr': '"some_string"'}, None),
            ({'hdr': '123'}, None),
        ]

        for hdrs_in, expected in tests:
            result = acl.parse_acl(version=2, data=hdrs_in.get('hdr'))
            self.assertEqual(expected, result,
                             '%r: %r != %r' % (hdrs_in, result, expected))

    def test_format_v1_acl(self):
        tests = [
            ((['a', 'b'], ['c.com']), 'a,b,.r:c.com'),
            ((['a', 'b'], ['c.com', '-x.c.com']), 'a,b,.r:c.com,.r:-x.c.com'),
            ((['a', 'b'], None), 'a,b'),
            ((None, ['c.com']), '.r:c.com'),
            ((None, None), ''),
        ]

        for (groups, refs), expected in tests:
            result = acl.format_acl(
                version=1, groups=groups, referrers=refs, header_name='hdr')
            self.assertEqual(expected, result, 'groups=%r, refs=%r: %r != %r'
                             % (groups, refs, result, expected))

    def test_format_v2_acl(self):
        tests = [
            ({}, '{}'),
            ({'foo': 'bar'}, '{"foo":"bar"}'),
            ({'groups': ['a', 'b'], 'referrers': ['c.com', '-x.c.com']},
             '{"groups":["a","b"],"referrers":["c.com","-x.c.com"]}'),
        ]

        for data, expected in tests:
            result = acl.format_acl(version=2, acl_dict=data)
            self.assertEqual(expected, result,
                             'data=%r: %r *!=* %r' % (data, result, expected))

    def test_acls_from_account_info(self):
        test_data = [
            ({}, None),
            ({'sysmeta': {}}, None),
            ({'sysmeta':
              {'core-access-control': '{"VERSION":1,"admin":["a","b"]}'}},
             {'admin': ['a', 'b'], 'read-write': [], 'read-only': []}),
            ({
                'some-key': 'some-value',
                'other-key': 'other-value',
                'sysmeta': {
                    'core-access-control': '{"VERSION":1,"admin":["a","b"],"r'
                                           'ead-write":["c"],"read-only":[]}',
                }},
             {'admin': ['a', 'b'], 'read-write': ['c'], 'read-only': []}),
        ]

        for args, expected in test_data:
            result = acl.acls_from_account_info(args)
            self.assertEqual(expected, result, "%r: Got %r, expected %r" %
                             (args, result, expected))

    def test_referrer_allowed(self):
        self.assertTrue(not acl.referrer_allowed('host', None))
        self.assertTrue(not acl.referrer_allowed('host', []))
        self.assertTrue(acl.referrer_allowed(None, ['*']))
        self.assertTrue(acl.referrer_allowed('', ['*']))
        self.assertTrue(not acl.referrer_allowed(None, ['specific.host']))
        self.assertTrue(not acl.referrer_allowed('', ['specific.host']))
        self.assertTrue(
            acl.referrer_allowed('http://www.example.com/index.html',
                                 ['.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://user@www.example.com/index.html', ['.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://user:pass@www.example.com/index.html', ['.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://www.example.com:8080/index.html', ['.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://user@www.example.com:8080/index.html', ['.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://user:pass@www.example.com:8080/index.html',
            ['.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://user:pass@www.example.com:8080', ['.example.com']))
        self.assertTrue(acl.referrer_allowed('http://www.example.com',
                                             ['.example.com']))
        self.assertTrue(not acl.referrer_allowed(
            'http://thief.example.com',
            ['.example.com', '-thief.example.com']))
        self.assertTrue(not acl.referrer_allowed(
            'http://thief.example.com',
            ['*', '-thief.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://www.example.com',
            ['.other.com', 'www.example.com']))
        self.assertTrue(acl.referrer_allowed(
            'http://www.example.com',
            ['-.example.com', 'www.example.com']))
        # This is considered a relative uri to the request uri, a mode not
        # currently supported.
        self.assertTrue(not acl.referrer_allowed('www.example.com',
                                                 ['.example.com']))
        self.assertTrue(not acl.referrer_allowed('../index.html',
                                                 ['.example.com']))
        self.assertTrue(acl.referrer_allowed('www.example.com', ['*']))


if __name__ == '__main__':
    unittest.main()
