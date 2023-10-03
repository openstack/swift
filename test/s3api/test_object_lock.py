# Copyright (c) 2010-2023 OpenStack Foundation
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

import botocore

from test.s3api import BaseS3TestCase


class TestObjectLockConfiguration(BaseS3TestCase):

    maxDiff = None

    def setUp(self):
        self.client = self.get_s3_client(1)
        self.bucket_name = self.create_name('objlock')
        resp = self.client.create_bucket(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def tearDown(self):
        self.clear_bucket(self.client, self.bucket_name)
        super(TestObjectLockConfiguration, self).tearDown()

    def test_get_object_lock_configuration(self):
        with self.assertRaises(botocore.exceptions.ClientError) as ce:
            self.client.get_object_lock_configuration(
                Bucket=self.bucket_name)

        self.assertEqual(
            ce.exception.response['ResponseMetadata']['HTTPStatusCode'],
            404)
        self.assertEqual(
            ce.exception.response['Error']['Code'],
            'ObjectLockConfigurationNotFoundError')

        self.assertEqual(
            str(ce.exception),
            'An error occurred (ObjectLockConfigurationNotFoundError) when '
            'calling the GetObjectLockConfiguration operation: Object Lock '
            'configuration does not exist for this bucket')
