# Copyright (c) 2025 Nvidia
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

from test.s3api import BaseS3TestCaseWithBucket, status_from_error, \
    code_from_error
from botocore.exceptions import ClientError


class TestConditionalWrites(BaseS3TestCaseWithBucket):
    def test_if_none_match_star_simple_put(self):
        client = self.get_s3_client(1)
        key_name = self.create_name('if-none-match-simple')
        # Can create new object fine
        resp = client.put_object(
            Bucket=self.bucket_name,
            Key=key_name,
            IfNoneMatch='*',
            Body=b'',
        )
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        # But overwrite is blocked
        with self.assertRaises(ClientError) as caught:
            client.put_object(
                Bucket=self.bucket_name,
                Key=key_name,
                IfNoneMatch='*',
                Body=b'',
            )
        self.assertEqual(412, status_from_error(caught.exception))
        self.assertEqual('PreconditionFailed',
                         code_from_error(caught.exception))

    def test_if_none_match_star_mpu(self):
        client = self.get_s3_client(1)
        key_name = self.create_name('if-none-match-mpu')

        create_mpu_resp = client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        parts = []
        for part_num in range(1, 4):
            part_resp = client.upload_part(
                Body=b'x' * 5 * 1024 * 1024,
                Bucket=self.bucket_name, Key=key_name,
                PartNumber=part_num, UploadId=upload_id)
            self.assertEqual(200, part_resp[
                'ResponseMetadata']['HTTPStatusCode'])
            parts.append({
                'ETag': part_resp['ETag'],
                'PartNumber': part_num,
            })

        # Nothing there, so complete succeeds
        complete_mpu_resp = client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key_name,
            MultipartUpload={'Parts': parts[:2]},
            UploadId=upload_id,
            IfNoneMatch='*',
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        # Retrying with more parts fails
        with self.assertRaises(ClientError) as caught:
            client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key_name,
                MultipartUpload={'Parts': parts},
                UploadId=upload_id,
                IfNoneMatch='*',
            )
        self.assertEqual(404, status_from_error(caught.exception))
        self.assertEqual('NoSuchUpload',
                         code_from_error(caught.exception))

        # Ditto fewer
        with self.assertRaises(ClientError) as caught:
            client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key_name,
                MultipartUpload={'Parts': parts[:1]},
                UploadId=upload_id,
                IfNoneMatch='*',
            )
        self.assertEqual(404, status_from_error(caught.exception))
        self.assertEqual('NoSuchUpload',
                         code_from_error(caught.exception))

        # Can retry with all the same parts and 200 though
        complete_mpu_resp = client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key_name,
            MultipartUpload={'Parts': parts[:2]},
            UploadId=upload_id,
            IfNoneMatch='*',
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        # Can still start a new upload
        create_mpu_resp = client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        # And upload parts
        part_resp = client.upload_part(
            Body=b'', Bucket=self.bucket_name, Key=key_name,
            PartNumber=1, UploadId=upload_id)
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        parts = [{
            'ETag': part_resp['ETag'],
            'PartNumber': 1,
        }]
        # But completion will be blocked
        with self.assertRaises(ClientError) as caught:
            client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key_name,
                MultipartUpload={'Parts': parts},
                UploadId=upload_id,
                IfNoneMatch='*',
            )
        self.assertEqual(412, status_from_error(caught.exception))
        self.assertEqual('PreconditionFailed',
                         code_from_error(caught.exception))
