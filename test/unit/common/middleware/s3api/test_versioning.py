# Copyright (c) 2014 OpenStack Foundation
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

from unittest.mock import patch

from swift.common.swob import Request, HTTPNoContent
from swift.common.middleware.s3api.etree import fromstring, tostring, \
    Element, SubElement

from test.unit.common.middleware.s3api import S3ApiTestCase


class TestS3ApiVersioning(S3ApiTestCase):

    def _versioning_GET(self, path):
        req = Request.blank('%s?versioning' % path,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        status, headers, body = self.call_s3api(req)
        return status, headers, body

    def _versioning_GET_not_configured(self, path):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            HTTPNoContent, {}, None)

        status, headers, body = self._versioning_GET(path)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'VersioningConfiguration')
        self.assertEqual(list(elem), [])

    def _versioning_GET_enabled(self, path):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, {
            'X-Container-Sysmeta-Versions-Enabled': 'True',
        }, None)

        status, headers, body = self._versioning_GET(path)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'VersioningConfiguration')
        status = elem.find('./Status').text
        self.assertEqual(status, 'Enabled')

    def _versioning_GET_suspended(self, path):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, {
            'X-Container-Sysmeta-Versions-Enabled': 'False',
        }, None)

        status, headers, body = self._versioning_GET('/bucket/object')
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'VersioningConfiguration')
        status = elem.find('./Status').text
        self.assertEqual(status, 'Suspended')

    def _versioning_PUT_error(self, path):
        # Root tag is not VersioningConfiguration
        elem = Element('foo')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)

        req = Request.blank('%s?versioning' % path,
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')

        # Status is not "Enabled" or "Suspended"
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'enabled'
        xml = tostring(elem)

        req = Request.blank('%s?versioning' % path,
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')

    def _versioning_PUT_enabled(self, path):
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)

        self.swift.register('POST', '/v1/AUTH_test/bucket', HTTPNoContent,
                            {'X-Container-Sysmeta-Versions-Enabled': 'True'},
                            None)

        req = Request.blank('%s?versioning' % path,
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        calls = self.swift.calls_with_headers
        self.assertEqual(calls[-1][0], 'POST')
        self.assertIn(('X-Versions-Enabled', 'true'),
                      list(calls[-1][2].items()))

    def _versioning_PUT_suspended(self, path):
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Suspended'
        xml = tostring(elem)

        self.swift.register('POST', '/v1/AUTH_test/bucket', HTTPNoContent,
                            {'x-container-sysmeta-versions-enabled': 'False'},
                            None)

        req = Request.blank('%s?versioning' % path,
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        calls = self.swift.calls_with_headers
        self.assertEqual(calls[-1][0], 'POST')
        self.assertIn(('X-Versions-Enabled', 'false'),
                      list(calls[-1][2].items()))

    def test_object_versioning_GET_not_configured(self):
        self._versioning_GET_not_configured('/bucket/object')

    def test_object_versioning_GET_enabled(self):
        self._versioning_GET_enabled('/bucket/object')

    def test_object_versioning_GET_suspended(self):
        self._versioning_GET_suspended('/bucket/object')

    def test_object_versioning_PUT_error(self):
        self._versioning_PUT_error('/bucket/object')

    def test_object_versioning_PUT_enabled(self):
        self._versioning_PUT_enabled('/bucket/object')

    def test_object_versioning_PUT_suspended(self):
        self._versioning_PUT_suspended('/bucket/object')

    def test_bucket_versioning_GET_not_configured(self):
        self._versioning_GET_not_configured('/bucket')

    def test_bucket_versioning_GET_enabled(self):
        self._versioning_GET_enabled('/bucket')

    def test_bucket_versioning_GET_suspended(self):
        self._versioning_GET_suspended('/bucket')

    def test_bucket_versioning_PUT_error(self):
        self._versioning_PUT_error('/bucket')

    def test_object_versioning_PUT_not_implemented(self):
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)

        req = Request.blank('/bucket?versioning',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)

        with patch('swift.common.middleware.s3api.controllers.versioning.'
                   'get_swift_info', return_value={}):
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '501', body)

    def test_bucket_versioning_PUT_enabled(self):
        self._versioning_PUT_enabled('/bucket')

    def test_bucket_versioning_PUT_suspended(self):
        self._versioning_PUT_suspended('/bucket')


if __name__ == '__main__':
    unittest.main()
