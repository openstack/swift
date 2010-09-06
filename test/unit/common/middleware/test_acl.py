# Copyright (c) 2010 OpenStack, LLC.
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
        value = acl.clean_acl('header', '.ref:any')
        self.assertEquals(value, '.ref:any')
        value = acl.clean_acl('header', '.ref:specific.host')
        self.assertEquals(value, '.ref:specific.host')
        value = acl.clean_acl('header', '.ref:.ending.with')
        self.assertEquals(value, '.ref:.ending.with')
        value = acl.clean_acl('header', '.ref:one,.ref:two')
        self.assertEquals(value, '.ref:one,.ref:two')
        value = acl.clean_acl('header', '.ref:any,.ref:-specific.host')
        self.assertEquals(value, '.ref:any,.ref:-specific.host')
        value = acl.clean_acl('header', '.ref:any,.ref:-.ending.with')
        self.assertEquals(value, '.ref:any,.ref:-.ending.with')
        value = acl.clean_acl('header', '.ref:one,.ref:-two')
        self.assertEquals(value, '.ref:one,.ref:-two')
        value = acl.clean_acl('header',
                              '.ref:one,.ref:-two,account,account:user')
        self.assertEquals(value, '.ref:one,.ref:-two,account,account:user')
        value = acl.clean_acl('header', 
                              ' .ref : one , ,, .ref:two , .ref : - three ')
        self.assertEquals(value, '.ref:one,.ref:two,.ref:-three')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.ref:')
        self.assertRaises(ValueError, acl.clean_acl, 'header', ' .ref : ')
        self.assertRaises(ValueError, acl.clean_acl, 'header',
                          'user , .ref : ')
        self.assertRaises(ValueError, acl.clean_acl, 'header', '.ref:-')
        self.assertRaises(ValueError, acl.clean_acl, 'header', ' .ref : - ')
        self.assertRaises(ValueError, acl.clean_acl, 'header',
                          'user , .ref : - ')
        self.assertRaises(ValueError, acl.clean_acl, 'write-header', '.ref:r')

    def test_parse_acl(self):
        self.assertEquals(acl.parse_acl(None), ([], []))
        self.assertEquals(acl.parse_acl(''), ([], []))
        self.assertEquals(acl.parse_acl('.ref:ref1'), (['ref1'], []))
        self.assertEquals(acl.parse_acl('.ref:-ref1'), (['-ref1'], []))
        self.assertEquals(acl.parse_acl('account:user'),
                          ([], ['account:user']))
        self.assertEquals(acl.parse_acl('account'), ([], ['account']))
        self.assertEquals(acl.parse_acl('acc1,acc2:usr2,.ref:ref3,.ref:-ref4'),
                          (['ref3', '-ref4'], ['acc1', 'acc2:usr2']))
        self.assertEquals(acl.parse_acl(
            'acc1,acc2:usr2,.ref:ref3,acc3,acc4:usr4,.ref:ref5,.ref:-ref6'),
            (['ref3', 'ref5', '-ref6'],
             ['acc1', 'acc2:usr2', 'acc3', 'acc4:usr4']))

    def test_referrer_allowed(self):
        self.assert_(not acl.referrer_allowed('host', None))
        self.assert_(not acl.referrer_allowed('host', []))
        self.assert_(acl.referrer_allowed(None, ['any']))
        self.assert_(acl.referrer_allowed('', ['any']))
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
            ['any', '-thief.example.com']))
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
        self.assert_(acl.referrer_allowed('www.example.com', ['any']))


if __name__ == '__main__':
    unittest.main()
