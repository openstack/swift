# Copyright (c) 2017 OpenStack Foundation
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

from swift.common.middleware.s3api.etree import fromstring, tostring, \
    Element, SubElement
import test.functional as tf
from test.functional.s3api import S3ApiBase
from test.functional.s3api.utils import get_error_code


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiVersioning(S3ApiBase):
    def setUp(self):
        super(TestS3ApiVersioning, self).setUp()
        if 'object_versioning' not in tf.cluster_info:
            # Alternatively, maybe we should assert we get 501s...
            self.skipTest('S3 versioning requires that Swift object '
                          'versioning be enabled')
        status, headers, body = self.conn.make_request('PUT', 'bucket')
        self.assertEqual(status, 200)

    def tearDown(self):
        # TODO: is this necessary on AWS? or can you delete buckets while
        # versioning is enabled?
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Suspended'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 200)

        status, headers, body = self.conn.make_request('DELETE', 'bucket')
        self.assertEqual(status, 204)
        super(TestS3ApiVersioning, self).tearDown()

    def test_versioning_put(self):
        # Versioning not configured
        status, headers, body = self.conn.make_request(
            'GET', 'bucket', query='versioning')
        self.assertEqual(status, 200)
        elem = fromstring(body)
        self.assertEqual(list(elem), [])

        # Enable versioning
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 200)

        status, headers, body = self.conn.make_request(
            'GET', 'bucket', query='versioning')
        self.assertEqual(status, 200)
        elem = fromstring(body)
        self.assertEqual(elem.find('./Status').text, 'Enabled')

        # Suspend versioning
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Suspended'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 200)

        status, headers, body = self.conn.make_request(
            'GET', 'bucket', query='versioning')
        self.assertEqual(status, 200)
        elem = fromstring(body)
        self.assertEqual(elem.find('./Status').text, 'Suspended')

        # Resume versioning
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 200)

        status, headers, body = self.conn.make_request(
            'GET', 'bucket', query='versioning')
        self.assertEqual(status, 200)
        elem = fromstring(body)
        self.assertEqual(elem.find('./Status').text, 'Enabled')

    def test_versioning_immediately_suspend(self):
        # Versioning not configured
        status, headers, body = self.conn.make_request(
            'GET', 'bucket', query='versioning')
        self.assertEqual(status, 200)
        elem = fromstring(body)
        self.assertEqual(list(elem), [])

        # Suspend versioning
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Suspended'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 200)

        status, headers, body = self.conn.make_request(
            'GET', 'bucket', query='versioning')
        self.assertEqual(status, 200)
        elem = fromstring(body)
        self.assertEqual(elem.find('./Status').text, 'Suspended')

        # Enable versioning
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 200)

        status, headers, body = self.conn.make_request(
            'GET', 'bucket', query='versioning')
        self.assertEqual(status, 200)
        elem = fromstring(body)
        self.assertEqual(elem.find('./Status').text, 'Enabled')

    def test_versioning_put_error(self):
        # Root tag is not VersioningConfiguration
        elem = Element('foo')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 400)
        self.assertEqual(get_error_code(body), 'MalformedXML')

        # Status is not "Enabled" or "Suspended"
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = '...'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 400)
        self.assertEqual(get_error_code(body), 'MalformedXML')

        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = ''
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', 'bucket', body=xml, query='versioning')
        self.assertEqual(status, 400)
        self.assertEqual(get_error_code(body), 'MalformedXML')
