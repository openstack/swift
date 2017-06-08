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
        self.assertIn('Content-Length', headers)
        del headers['Content-Length']
        self.assertNotIn('Content-Length', headers)

    def test_update(self):
        headers = HeaderKeyDict()
        headers.update({'Content-Length': '0'})
        headers.update([('Content-Type', 'text/plain')])
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['Content-Type'], 'text/plain')

    def test_set_none(self):
        headers = HeaderKeyDict()
        headers['test'] = None
        self.assertNotIn('test', headers)
        headers['test'] = 'something'
        self.assertEqual('something', headers['test'])  # sanity check
        headers['test'] = None
        self.assertNotIn('test', headers)

    def test_init_from_dict(self):
        headers = HeaderKeyDict({'Content-Length': 20,
                                 'Content-Type': 'text/plain'})
        self.assertEqual('20', headers['Content-Length'])
        self.assertEqual('text/plain', headers['Content-Type'])
        headers = HeaderKeyDict(headers)
        self.assertEqual('20', headers['Content-Length'])
        self.assertEqual('text/plain', headers['Content-Type'])

    def test_set(self):
        # mappings = ((<tuple of input vals>, <expected output val>), ...)
        mappings = (((1.618, '1.618', b'1.618', u'1.618'), '1.618'),
                    ((20, '20', b'20', u'20'), '20'),
                    ((True, 'True', b'True', u'True'), 'True'),
                    ((False, 'False', b'False', u'False'), 'False'))
        for vals, expected in mappings:
            for val in vals:
                headers = HeaderKeyDict(test=val)
                actual = headers['test']
                self.assertEqual(expected, actual,
                                 'Expected %s but got %s for val %s' %
                                 (expected, actual, val))
                self.assertIsInstance(
                    actual, str,
                    'Expected type str but got %s for val %s of type %s' %
                    (type(actual), val, type(val)))

    def test_get(self):
        headers = HeaderKeyDict()
        headers['content-length'] = 20
        self.assertEqual(headers.get('CONTENT-LENGTH'), '20')
        self.assertIsNone(headers.get('something-else'))
        self.assertEqual(headers.get('something-else', True), True)

    def test_keys(self):
        headers = HeaderKeyDict()
        headers['content-length'] = 20
        headers['cOnTent-tYpe'] = 'text/plain'
        headers['SomeThing-eLse'] = 'somevalue'
        self.assertEqual(
            set(headers.keys()),
            set(('Content-Length', 'Content-Type', 'Something-Else')))

    def test_pop(self):
        headers = HeaderKeyDict()
        headers['content-length'] = 20
        headers['cOntent-tYpe'] = 'text/plain'
        self.assertEqual(headers.pop('content-Length'), '20')
        self.assertEqual(headers.pop('Content-type'), 'text/plain')
        self.assertEqual(headers.pop('Something-Else', 'somevalue'),
                         'somevalue')
