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
from collections import defaultdict
import botocore.exceptions

from test.s3api import BaseS3TestCase, ConfigError, \
    skip_if_s3_acl_tests_disabled, is_s3_acl_tests_enabled


class TestGetServiceSigV4(BaseS3TestCase):
    def setUp(self):
        super().setUp()
        # Capture existing buckets before running tests
        self.existing_buckets = defaultdict(list)
        self.existing_buckets[1] = self._get_buckets(1)
        if is_s3_acl_tests_enabled():
            # client2 seems to be always able to list buckets
            try:
                self.get_s3_client(2)
            except ConfigError:
                pass
            else:
                self.existing_buckets[2] = self._get_buckets(2)
            # client3 gets AccessDenied unless s3_acl = True
            try:
                self.get_s3_client(3)
            except ConfigError:
                pass
            else:
                try:
                    self.existing_buckets[3] = self._get_buckets(3)
                except botocore.exceptions.ClientError as e:
                    # but the lack of the existing_buckets doesn't really
                    # matter to most tests
                    if e.response['Error']['Code'] == 'AccessDenied':
                        pass
                    else:
                        raise

    def _get_buckets(self, client_num):
        client = self.get_s3_client(client_num)
        resp = client.list_buckets()
        return [bucket['Name'] for bucket in resp['Buckets']]

    def _do_test_existing_service(self, client_num):
        client = self.get_s3_client(client_num)
        resp = client.list_buckets()
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        found_buckets = [bucket['Name'] for bucket in resp['Buckets']]
        self.assertEqual(self.existing_buckets[client_num], found_buckets)
        self.assertIn('x-amz-request-id',
                      resp['ResponseMetadata']['HTTPHeaders'])
        self.check_owner(resp['Owner'])
        self.assertIn('ID', resp['Owner'])

    def test_existing_service(self):
        self._do_test_existing_service(1)

    @skip_if_s3_acl_tests_disabled
    def test_existing_service_client3(self):
        try:
            self.get_s3_client(3)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        else:
            self._do_test_existing_service(3)

    def _create_buckets(self, client_num):
        client = self.get_s3_client(client_num)
        buckets = [self.create_name('bucket%s' % i) for i in range(5)]
        for bucket in buckets:
            client.create_bucket(Bucket=bucket)
        return buckets

    def _do_test_service_with_buckets(self, client_num, buckets):
        client = self.get_s3_client(client_num)
        resp = client.list_buckets()
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        expected_buckets = buckets + self.existing_buckets[client_num]
        self.assertEqual(sorted(expected_buckets), [
            bucket['Name'] for bucket in resp['Buckets']])
        self.assertTrue(all('CreationDate' in bucket
                            for bucket in resp['Buckets']))
        self.assertIn('x-amz-request-id',
                      resp['ResponseMetadata']['HTTPHeaders'])
        self.check_owner(resp['Owner'])

    def test_service_with_buckets(self):
        client_num = 1
        buckets = self._create_buckets(client_num)
        self._do_test_service_with_buckets(client_num, buckets)

    @skip_if_s3_acl_tests_disabled
    def test_service_with_buckets_client2(self):
        # Second user can only see its own buckets
        try:
            client2 = self.get_s3_client(2)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        self._create_buckets(1)
        buckets2 = self._create_buckets(2)
        expected_buckets = buckets2 + self.existing_buckets[2]
        resp = client2.list_buckets()
        found_buckets = [bucket['Name'] for bucket in resp['Buckets']]
        self.assertEqual(sorted(expected_buckets), found_buckets)

    @skip_if_s3_acl_tests_disabled
    def test_service_with_buckets_client3(self):
        # Unprivileged user can only see its own buckets
        # (which should be empty)
        try:
            client3 = self.get_s3_client(3)
        except ConfigError as err:
            raise unittest.SkipTest(str(err))
        self._create_buckets(1)
        resp = client3.list_buckets()
        found_buckets = [bucket['Name'] for bucket in resp['Buckets']]
        self.assertEqual(self.existing_buckets[3], found_buckets)


class TestGetServiceSigV2(TestGetServiceSigV4):
    signature_version = 's3'


class TestGetServicePresignedV2(TestGetServiceSigV4):
    signature_version = 's3-query'


class TestGetServicePresignedV4(TestGetServiceSigV4):
    signature_version = 's3v4-query'
