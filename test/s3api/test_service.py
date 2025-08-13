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

from test.s3api import BaseS3TestCase, ConfigError, \
    skip_if_s3_acl_tests_disabled


class TestGetServiceSigV4(BaseS3TestCase):
    def _do_test_empty_service(self, client):
        resp = client.list_buckets()
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual([], resp['Buckets'])
        self.assertIn('x-amz-request-id',
                      resp['ResponseMetadata']['HTTPHeaders'])
        self.check_owner(resp['Owner'])
        self.assertIn('ID', resp['Owner'])

    def test_empty_service(self):
        client1 = self.get_s3_client(1)
        self._do_test_empty_service(client1)

    @skip_if_s3_acl_tests_disabled
    def test_empty_service_client3(self):
        try:
            client3 = self.get_s3_client(3)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        else:
            self._do_test_empty_service(client3)

    def _create_buckets(self, client):
        buckets = [self.create_name('bucket%s' % i) for i in range(5)]
        for bucket in buckets:
            client.create_bucket(Bucket=bucket)
        return buckets

    def _do_test_service_with_buckets(self, client, buckets):
        resp = client.list_buckets()
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(sorted(buckets), [
            bucket['Name'] for bucket in resp['Buckets']])
        self.assertTrue(all('CreationDate' in bucket
                            for bucket in resp['Buckets']))
        self.assertIn('x-amz-request-id',
                      resp['ResponseMetadata']['HTTPHeaders'])
        self.check_owner(resp['Owner'])

    def test_service_with_buckets(self):
        client = self.get_s3_client(1)
        buckets = self._create_buckets(client)
        self._do_test_service_with_buckets(client, buckets)

    @skip_if_s3_acl_tests_disabled
    def test_service_with_buckets_client2(self):
        # Second user can only see its own buckets
        try:
            client2 = self.get_s3_client(2)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        client1 = self.get_s3_client(1)
        self._create_buckets(client1)
        buckets2 = self._create_buckets(client2)
        self.assertEqual(sorted(buckets2), [
            bucket['Name'] for bucket in client2.list_buckets()['Buckets']])

    @skip_if_s3_acl_tests_disabled
    def test_service_with_buckets_client3(self):
        # Unprivileged user can't see anything
        try:
            client3 = self.get_s3_client(3)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        client1 = self.get_s3_client(1)
        self._create_buckets(client1)
        self.assertEqual([], client3.list_buckets()['Buckets'])


class TestGetServiceSigV2(TestGetServiceSigV4):
    signature_version = 's3'


class TestGetServicePresignedV2(TestGetServiceSigV4):
    signature_version = 's3-query'


class TestGetServicePresignedV4(TestGetServiceSigV4):
    signature_version = 's3v4-query'
