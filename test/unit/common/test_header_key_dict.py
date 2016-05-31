# Copyright (c) 2012 OpenStack Foundation
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
from swift.common.header_key_dict import HeaderKeyDict


class TestHeaderKeyDict(unittest.TestCase):
    def test_case_insensitive(self):
        headers = HeaderKeyDict()
        headers['Content-Length'] = 0
        headers['CONTENT-LENGTH'] = 10
        headers['content-length'] = 20
        self.assertEqual(headers['Content-Length'], '20')
        self.assertEqual(headers['content-length'], '20')
        self.assertEqual(headers['CONTENT-LENGTH'], '20')

    def test_setdefault(self):
        headers = HeaderKeyDict()

        # it gets set
        headers.setdefault('x-rubber-ducky', 'the one')
        self.assertEqual(headers['X-Rubber-Ducky'], 'the one')

        # it has the right return value
        ret = headers.setdefault('x-boat', 'dinghy')
        self.assertEqual(ret, 'dinghy')

        ret = headers.setdefault('x-boat', 'yacht')
        self.assertEqual(ret, 'dinghy')

        # shouldn't crash
        headers.setdefault('x-sir-not-appearing-in-this-request', None)

    def test_del_contains(self):
        headers = HeaderKeyDict()
        headers['Content-Length'] = 0
        self.assertTrue('Content-Length' in headers)
        del headers['Content-Length']
        self.assertTrue('Content-Length' not in headers)

    def test_update(self):
        headers = HeaderKeyDict()
        headers.update({'Content-Length': '0'})
        headers.update([('Content-Type', 'text/plain')])
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['Content-Type'], 'text/plain')

    def test_get(self):
        headers = HeaderKeyDict()
        headers['content-length'] = 20
        self.assertEqual(headers.get('CONTENT-LENGTH'), '20')
        self.assertEqual(headers.get('something-else'), None)
        self.assertEqual(headers.get('something-else', True), True)

    def test_keys(self):
        headers = HeaderKeyDict()
        headers['content-length'] = 20
        headers['cOnTent-tYpe'] = 'text/plain'
        headers['SomeThing-eLse'] = 'somevalue'
        self.assertEqual(
            set(headers.keys()),
            set(('Content-Length', 'Content-Type', 'Something-Else')))
