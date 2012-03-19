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

import unittest
from test.unit import MockTrue

from webob import Request
from webob.exc import HTTPBadRequest, HTTPLengthRequired, \
    HTTPRequestEntityTooLarge

from swift.common import constraints


class TestConstraints(unittest.TestCase):

    def test_check_metadata_empty(self):
        headers = {}
        self.assertEquals(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), None)

    def test_check_metadata_good(self):
        headers = {'X-Object-Meta-Name': 'Value'}
        self.assertEquals(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), None)

    def test_check_metadata_empty_name(self):
        headers = {'X-Object-Meta-': 'Value'}
        self.assert_(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), HTTPBadRequest)

    def test_check_metadata_name_length(self):
        name = 'a' * constraints.MAX_META_NAME_LENGTH
        headers = {'X-Object-Meta-%s' % name: 'v'}
        self.assertEquals(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), None)
        name = 'a' * (constraints.MAX_META_NAME_LENGTH + 1)
        headers = {'X-Object-Meta-%s' % name: 'v'}
        self.assert_(isinstance(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), HTTPBadRequest))

    def test_check_metadata_value_length(self):
        value = 'a' * constraints.MAX_META_VALUE_LENGTH
        headers = {'X-Object-Meta-Name': value}
        self.assertEquals(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), None)
        value = 'a' * (constraints.MAX_META_VALUE_LENGTH + 1)
        headers = {'X-Object-Meta-Name': value}
        self.assert_(isinstance(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), HTTPBadRequest))

    def test_check_metadata_count(self):
        headers = {}
        for x in xrange(constraints.MAX_META_COUNT):
            headers['X-Object-Meta-%d' % x] = 'v'
        self.assertEquals(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), None)
        headers['X-Object-Meta-Too-Many'] = 'v'
        self.assert_(isinstance(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), HTTPBadRequest))

    def test_check_metadata_size(self):
        headers = {}
        size = 0
        chunk = constraints.MAX_META_NAME_LENGTH + \
                constraints.MAX_META_VALUE_LENGTH
        x = 0
        while size + chunk < constraints.MAX_META_OVERALL_SIZE:
            headers['X-Object-Meta-%04d%s' %
                    (x, 'a' * (constraints.MAX_META_NAME_LENGTH - 4))] = \
                        'v' * constraints.MAX_META_VALUE_LENGTH
            size += chunk
            x += 1
        self.assertEquals(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), None)
        headers['X-Object-Meta-9999%s' %
                ('a' * (constraints.MAX_META_NAME_LENGTH - 4))] = \
                    'v' * constraints.MAX_META_VALUE_LENGTH
        self.assert_(isinstance(constraints.check_metadata(Request.blank('/',
            headers=headers), 'object'), HTTPBadRequest))

    def test_check_object_creation_content_length(self):
        headers = {'Content-Length': str(constraints.MAX_FILE_SIZE),
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank('/',
            headers=headers), 'object_name'), None)
        headers = {'Content-Length': str(constraints.MAX_FILE_SIZE + 1),
                   'Content-Type': 'text/plain'}
        self.assert_(isinstance(constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name'),
            HTTPRequestEntityTooLarge))
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank('/',
            headers=headers), 'object_name'), None)
        headers = {'Content-Type': 'text/plain'}
        self.assert_(isinstance(constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name'),
            HTTPLengthRequired))

    def test_check_object_creation_name_length(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain'}
        name = 'o' * constraints.MAX_OBJECT_NAME_LENGTH
        self.assertEquals(constraints.check_object_creation(Request.blank('/',
            headers=headers), name), None)
        name = 'o' * (constraints.MAX_OBJECT_NAME_LENGTH + 1)
        self.assert_(isinstance(constraints.check_object_creation(
            Request.blank('/', headers=headers), name),
            HTTPBadRequest))

    def test_check_object_creation_content_type(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': 'text/plain'}
        self.assertEquals(constraints.check_object_creation(Request.blank('/',
            headers=headers), 'object_name'), None)
        headers = {'Transfer-Encoding': 'chunked'}
        self.assert_(isinstance(constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name'),
            HTTPBadRequest))

    def test_check_object_creation_bad_content_type(self):
        headers = {'Transfer-Encoding': 'chunked',
                   'Content-Type': '\xff\xff'}
        resp = constraints.check_object_creation(
            Request.blank('/', headers=headers), 'object_name')
        self.assert_(isinstance(resp, HTTPBadRequest))
        self.assert_('Content-Type' in resp.body)

    def test_check_object_manifest_header(self):
        resp = constraints.check_object_creation(Request.blank('/',
            headers={'X-Object-Manifest': 'container/prefix', 'Content-Length':
            '0', 'Content-Type': 'text/plain'}), 'manifest')
        self.assert_(not resp)
        resp = constraints.check_object_creation(Request.blank('/',
            headers={'X-Object-Manifest': 'container', 'Content-Length': '0',
            'Content-Type': 'text/plain'}), 'manifest')
        self.assert_(isinstance(resp, HTTPBadRequest))
        resp = constraints.check_object_creation(Request.blank('/',
            headers={'X-Object-Manifest': '/container/prefix',
            'Content-Length': '0', 'Content-Type': 'text/plain'}), 'manifest')
        self.assert_(isinstance(resp, HTTPBadRequest))
        resp = constraints.check_object_creation(Request.blank('/',
            headers={'X-Object-Manifest': 'container/prefix?query=param',
            'Content-Length': '0', 'Content-Type': 'text/plain'}), 'manifest')
        self.assert_(isinstance(resp, HTTPBadRequest))
        resp = constraints.check_object_creation(Request.blank('/',
            headers={'X-Object-Manifest': 'container/prefix&query=param',
            'Content-Length': '0', 'Content-Type': 'text/plain'}), 'manifest')
        self.assert_(isinstance(resp, HTTPBadRequest))
        resp = constraints.check_object_creation(Request.blank('/',
            headers={'X-Object-Manifest': 'http://host/container/prefix',
            'Content-Length': '0', 'Content-Type': 'text/plain'}), 'manifest')
        self.assert_(isinstance(resp, HTTPBadRequest))

    def test_check_mount(self):
        self.assertFalse(constraints.check_mount('', ''))
        constraints.os = MockTrue() # mock os module
        self.assertTrue(constraints.check_mount('/srv', '1'))
        reload(constraints) # put it back

    def test_check_float(self):
        self.assertFalse(constraints.check_float(''))
        self.assertTrue(constraints.check_float('0'))

if __name__ == '__main__':
    unittest.main()
