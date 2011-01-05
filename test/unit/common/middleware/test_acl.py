# Copyright (c) 2010-2011 OpenStack, LLC.
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
        self.assertEquals(value, '.r:*')
        value = acl.clean_acl('header', '.r:specific.host')
        self.assertEquals(value, '.r:specific.host')
        value = acl.clean_acl('header', '.r:.ending.with')
        self.assertEquals(value, '.r:.ending.with')
        value = acl.clean_acl('header', '.r:*.ending.with')
        self.assertEquals(value, '.r:.ending.with')
        value = acl.clean_acl('header', '.r:-*.ending.with')
        self.assertEquals(value, '.r:-.ending.with')
        value = acl.clean_acl('header', '.r:one,.r:two')
        self.assertEquals(value, '.r:one,.r:two')
        value = acl.clean_acl('header', '.r:*,.r:-specific.host')
        self.assertEquals(value, '.r:*,.r:-specific.host')
        value = acl.clean_acl('header', '.r:*,.r:-.ending.with')
        self.assertEquals(value, '.r:*,.r:-.ending.with')
        value = acl.clean_acl('header', '.r:one,.r:-two')
        self.assertEquals(value, '.r:one,.r:-two')
        value = acl.clean_acl('header', '.r:one,.r:-two,account,account:user')
        self.assertEquals(value, '.r:one,.r:-two,account,account:user')
        value = acl.clean_acl('header', 'TEST_account')
        self.assertEquals(value, 'TEST_account')
        value = acl.clean_acl('header', '.ref:*')
        self.assertEquals(value, '.r:*')
        value = acl.clean_acl('header', '.referer:*')
        self.assertEquals(value, '.r:*')
        value = acl.clean_acl('header', '.referrer:*')
        self.assertEquals(value, '.r:*')
        value = acl.clean_acl('header', 
                              ' .r : one , ,, .r:two , .r : - three ')
        self.assertEquals(value, '.r:one,.r:two,.r:-three')
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
        self.assertEquals(acl.parse_acl(None), ([], []))
        self.assertEquals(acl.parse_acl(''), ([], []))
        self.assertEquals(acl.parse_acl('.r:ref1'), (['ref1'], []))
        self.assertEquals(acl.parse_acl('.r:-ref1'), (['-ref1'], []))
        self.assertEquals(acl.parse_acl('account:user'),
                          ([], ['account:user']))
        self.assertEquals(acl.parse_acl('account'), ([], ['account']))
        self.assertEquals(acl.parse_acl('acc1,acc2:usr2,.r:ref3,.r:-ref4'),
                          (['ref3', '-ref4'], ['acc1', 'acc2:usr2']))
        self.assertEquals(acl.parse_acl(
            'acc1,acc2:usr2,.r:ref3,acc3,acc4:usr4,.r:ref5,.r:-ref6'),
            (['ref3', 'ref5', '-ref6'],
             ['acc1', 'acc2:usr2', 'acc3', 'acc4:usr4']))

    def test_referrer_allowed(self):
        self.assert_(not acl.referrer_allowed('host', None))
        self.assert_(not acl.referrer_allowed('host', []))
        self.assert_(acl.referrer_allowed(None, ['*']))
        self.assert_(acl.referrer_allowed('', ['*']))
        self.assert_(not acl.referrer_allowed(None, ['specific.host']))
        self.assert_(not acl.referrer_allowed('', ['specific.host']))
        self.assert_(acl.referrer_allowed('http://www.example.com/index.html',
                                          ['.example.com']))
        self.assert_(acl.referrer_allowed(
            'http://user@www.example.com/index.html', ['.example.com']))
        self.assert_(acl.referrer_allowed(
            'http://user:pass@www.example.com/index.html', ['.example.com']))
        self.assert_(acl.referrer_allowed(
            'http://www.example.com:8080/index.html', ['.example.com']))
        self.assert_(acl.referrer_allowed(
            'http://user@www.example.com:8080/index.html', ['.example.com']))
        self.assert_(acl.referrer_allowed(
            'http://user:pass@www.example.com:8080/index.html',
            ['.example.com']))
        self.assert_(acl.referrer_allowed(
            'http://user:pass@www.example.com:8080', ['.example.com']))
        self.assert_(acl.referrer_allowed('http://www.example.com',
                                          ['.example.com']))
        self.assert_(not acl.referrer_allowed('http://thief.example.com',
            ['.example.com', '-thief.example.com']))
        self.assert_(not acl.referrer_allowed('http://thief.example.com',
            ['*', '-thief.example.com']))
        self.assert_(acl.referrer_allowed('http://www.example.com',
            ['.other.com', 'www.example.com']))
        self.assert_(acl.referrer_allowed('http://www.example.com',
            ['-.example.com', 'www.example.com']))
        # This is considered a relative uri to the request uri, a mode not
        # currently supported.
        self.assert_(not acl.referrer_allowed('www.example.com',
                                              ['.example.com']))
        self.assert_(not acl.referrer_allowed('../index.html',
                                              ['.example.com']))
        self.assert_(acl.referrer_allowed('www.example.com', ['*']))


if __name__ == '__main__':
    unittest.main()
