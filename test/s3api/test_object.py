# Copyright (c) 2026 Nvidia
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
import time

from botocore.exceptions import ClientError

from test.s3api import BaseS3TestCase, code_from_error, status_from_error


class TestObject(BaseS3TestCase):
    def setUp(self):
        self.client = self.get_s3_client(1)
        self.bucket_name = self.create_name('object-tests')
        resp = self.client.create_bucket(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def test_object_copy(self):
        orig_name = self.create_name('original')
        put_resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=orig_name,
            Body=b'123456789',
        )
        self.assertEqual(200, put_resp['ResponseMetadata']['HTTPStatusCode'])

        src_head_resp = self.client.head_object(
            Bucket=self.bucket_name,
            Key=orig_name
        )
        self.assertEqual(
            200, src_head_resp['ResponseMetadata']['HTTPStatusCode'])

        # sleep to ensure distinct last-modified times
        time.sleep(1)
        copy_name = self.create_name('copy')
        copy_resp = self.client.copy_object(
            Bucket=self.bucket_name,
            Key=copy_name,
            CopySource={'Bucket': self.bucket_name, 'Key': orig_name},
        )
        self.assertEqual(200, copy_resp['ResponseMetadata']['HTTPStatusCode'])

        # the copy gets a fresh timestamp...
        self.assertGreater(
            copy_resp['CopyObjectResult']['LastModified'],
            src_head_resp['LastModified'])

    def test_object_server_side_encryption_header(self):
        obj_name = self.create_name('sse')
        body = b'abcd'
        try:
            put_resp = self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=body,
                ServerSideEncryption='AES256',
            )
        except ClientError as err:
            if (status_from_error(err) == 501 and
                    code_from_error(err) == 'NotImplemented'):
                self.skipTest('x-amz-server-side-encryption is not supported')
            raise
        self.assertEqual(200, put_resp['ResponseMetadata']['HTTPStatusCode'])

        head_resp = self.client.head_object(
            Bucket=self.bucket_name,
            Key=obj_name,
        )
        self.assertEqual(200, head_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('AES256', head_resp['ServerSideEncryption'])
        self.assertEqual(
            'AES256',
            head_resp['ResponseMetadata']['HTTPHeaders'].get(
                'x-amz-server-side-encryption'))

        get_resp = self.client.get_object(
            Bucket=self.bucket_name,
            Key=obj_name,
        )
        self.assertEqual(200, get_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('AES256', get_resp['ServerSideEncryption'])
        self.assertEqual(
            'AES256',
            get_resp['ResponseMetadata']['HTTPHeaders'].get(
                'x-amz-server-side-encryption'))
        self.assertEqual(body, get_resp['Body'].read())
