# Copyright (c) 2015 OpenStack Foundation
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
import os

from swift3.test.functional.s3_test_client import Connection
from swift3.test.functional.utils import get_error_code
from swift3.etree import fromstring
from swift3.test.functional import Swift3FunctionalTestCase


class TestSwift3Service(Swift3FunctionalTestCase):
    def setUp(self):
        super(TestSwift3Service, self).setUp()

    def test_service(self):
        # GET Service(without bucket)
        status, headers, body = self.conn.make_request('GET')
        self.assertEqual(status, 200)

        self.assertCommonResponseHeaders(headers)
        self.assertTrue(headers['content-type'] is not None)
        # TODO; requires consideration
        # self.assertEqual(headers['transfer-encoding'], 'chunked')

        elem = fromstring(body, 'ListAllMyBucketsResult')
        buckets = elem.findall('./Buckets/Bucket')
        self.assertEqual(list(buckets), [])
        owner = elem.find('Owner')
        self.assertEqual(self.conn.user_id, owner.find('ID').text)
        self.assertEqual(self.conn.user_id, owner.find('DisplayName').text)

        # GET Service(with Bucket)
        req_buckets = ('bucket', 'bucket2')
        for bucket in req_buckets:
            self.conn.make_request('PUT', bucket)
        status, headers, body = self.conn.make_request('GET')
        self.assertEqual(status, 200)

        elem = fromstring(body, 'ListAllMyBucketsResult')
        resp_buckets = elem.findall('./Buckets/Bucket')
        self.assertEqual(len(list(resp_buckets)), 2)
        for b in resp_buckets:
            self.assertTrue(b.find('Name').text in req_buckets)
            self.assertTrue(b.find('CreationDate') is not None)

    def test_service_error_signature_not_match(self):
        auth_error_conn = Connection(aws_secret_key='invalid')
        status, headers, body = auth_error_conn.make_request('GET')
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')
        self.assertEqual(headers['content-type'], 'application/xml')

    def test_service_error_no_date_header(self):
        # Without x-amz-date/Date header, that makes 403 forbidden
        status, headers, body = self.conn.make_request(
            'GET', headers={'Date': '', 'x-amz-date': ''})
        self.assertEqual(status, 403)
        self.assertEqual(get_error_code(body), 'AccessDenied')
        self.assertIn('AWS authentication requires a valid Date '
                      'or x-amz-date header', body)


@unittest.skipIf(os.environ['AUTH'] == 'tempauth',
                 'v4 is supported only in keystone')
class TestSwift3ServiceSigV4(TestSwift3Service):
    @classmethod
    def setUpClass(cls):
        os.environ['S3_USE_SIGV4'] = "True"

    @classmethod
    def tearDownClass(cls):
        del os.environ['S3_USE_SIGV4']


if __name__ == '__main__':
    unittest.main()
