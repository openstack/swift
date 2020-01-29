# Copyright (c) 2019 SwiftStack, Inc.
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

from test.s3api import BaseS3TestCase, ConfigError


class TestGetServiceSigV4(BaseS3TestCase):
    def test_empty_service(self):
        def do_test(client):
            access_key = client._request_signer._credentials.access_key
            resp = client.list_buckets()
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
            self.assertEqual([], resp['Buckets'])
            self.assertIn('x-amz-request-id',
                          resp['ResponseMetadata']['HTTPHeaders'])
            self.assertIn('DisplayName', resp['Owner'])
            self.assertEqual(access_key, resp['Owner']['DisplayName'])
            self.assertIn('ID', resp['Owner'])

        client = self.get_s3_client(1)
        do_test(client)
        try:
            client = self.get_s3_client(3)
        except ConfigError:
            pass
        else:
            do_test(client)

    def test_service_with_buckets(self):
        c = self.get_s3_client(1)
        buckets = [self.create_name('bucket%s' % i) for i in range(5)]
        for bucket in buckets:
            c.create_bucket(Bucket=bucket)

        resp = c.list_buckets()
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(sorted(buckets), [
            bucket['Name'] for bucket in resp['Buckets']])
        self.assertTrue(all('CreationDate' in bucket
                            for bucket in resp['Buckets']))
        self.assertIn('x-amz-request-id',
                      resp['ResponseMetadata']['HTTPHeaders'])
        self.assertIn('DisplayName', resp['Owner'])
        access_key = c._request_signer._credentials.access_key
        self.assertEqual(access_key, resp['Owner']['DisplayName'])
        self.assertIn('ID', resp['Owner'])

        # Second user can only see its own buckets
        try:
            c2 = self.get_s3_client(2)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        buckets2 = [self.create_name('bucket%s' % i) for i in range(2)]
        for bucket in buckets2:
            c2.create_bucket(Bucket=bucket)
        self.assertEqual(sorted(buckets2), [
            bucket['Name'] for bucket in c2.list_buckets()['Buckets']])

        # Unprivileged user can't see anything
        try:
            c3 = self.get_s3_client(3)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        self.assertEqual([], c3.list_buckets()['Buckets'])


class TestGetServiceSigV2(TestGetServiceSigV4):
    signature_version = 's3'


class TestGetServicePresignedV2(TestGetServiceSigV4):
    signature_version = 's3-query'


class TestGetServicePresignedV4(TestGetServiceSigV4):
    signature_version = 's3v4-query'
