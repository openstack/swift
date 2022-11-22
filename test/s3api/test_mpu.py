# Copyright (c) 2021 Nvidia
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

from test.s3api import BaseS3TestCase
from botocore.exceptions import ClientError


class TestMultiPartUploads(BaseS3TestCase):

    maxDiff = None

    def setUp(self):
        self.client = self.get_s3_client(1)
        self.bucket_name = self.create_name('test-mpu')
        resp = self.client.create_bucket(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def tearDown(self):
        self.clear_bucket(self.client, self.bucket_name)
        super(TestMultiPartUploads, self).tearDown()

    def test_basic_upload(self):
        key_name = self.create_name('key')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        parts = []
        for i in range(1, 3):
            body = ('%d' % i) * 5 * (2 ** 20)
            part_resp = self.client.upload_part(
                Body=body, Bucket=self.bucket_name, Key=key_name,
                PartNumber=i, UploadId=upload_id)
            self.assertEqual(200, part_resp[
                'ResponseMetadata']['HTTPStatusCode'])
            parts.append({
                'ETag': part_resp['ETag'],
                'PartNumber': i,
            })
        list_parts_resp = self.client.list_parts(
            Bucket=self.bucket_name, Key=key_name,
            UploadId=upload_id,
        )
        self.assertEqual(200, list_parts_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(parts, [{k: p[k] for k in ('ETag', 'PartNumber')}
                                 for p in list_parts_resp['Parts']])
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=key_name,
            MultipartUpload={
                'Parts': parts,
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])

    def test_create_list_abort_multipart_uploads(self):
        key_name = self.create_name('key')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']

        # our upload is in progress
        list_mpu_resp = self.client.list_multipart_uploads(
            Bucket=self.bucket_name)
        self.assertEqual(200, list_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        found_uploads = list_mpu_resp.get('Uploads', [])
        self.assertEqual(1, len(found_uploads), found_uploads)
        self.assertEqual(upload_id, found_uploads[0]['UploadId'])

        abort_resp = self.client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=key_name,
            UploadId=upload_id,
        )
        self.assertEqual(204, abort_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        # no more inprogress uploads
        list_mpu_resp = self.client.list_multipart_uploads(
            Bucket=self.bucket_name)
        self.assertEqual(200, list_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual([], list_mpu_resp.get('Uploads', []))

    def test_complete_multipart_upload_malformed_request(self):
        key_name = self.create_name('key')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        parts = []
        for i in range(1, 3):
            body = ('%d' % i) * 5 * (2 ** 20)
            part_resp = self.client.upload_part(
                Body=body, Bucket=self.bucket_name, Key=key_name,
                PartNumber=i, UploadId=upload_id)
            self.assertEqual(200, part_resp[
                'ResponseMetadata']['HTTPStatusCode'])
            parts.append({
                'PartNumber': i,
                'ETag': '',
            })
        with self.assertRaises(ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=key_name,
                MultipartUpload={
                    'Parts': parts,
                },
                UploadId=upload_id,
            )
        complete_mpu_resp = caught.exception.response
        self.assertEqual(400, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('InvalidPart', complete_mpu_resp[
            'Error']['Code'])
        self.assertTrue(complete_mpu_resp['Error']['Message'].startswith(
            'One or more of the specified parts could not be found.'
        ), complete_mpu_resp['Error']['Message'])
        self.assertEqual(complete_mpu_resp['Error']['UploadId'], upload_id)
        self.assertIn(complete_mpu_resp['Error']['PartNumber'], ('1', '2'))
        self.assertEqual(complete_mpu_resp['Error']['ETag'], None)
