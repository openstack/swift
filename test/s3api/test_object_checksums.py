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

import binascii
import botocore
import hashlib
from unittest import SkipTest

from swift.common.utils import base64_str
from swift.common.utils.checksum import crc32c
from test.s3api import BaseS3TestCaseWithBucket

TEST_BODY = b'123456789'


def boto_at_least(*version):
    return tuple(int(x) for x in botocore.__version__.split('.')) >= version


class ObjectChecksumMixin(object):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')
        cls.CHECKSUM_HDR = 'x-amz-checksum-' + cls.ALGORITHM.lower()

    def assert_error(self, resp, err_code, err_msg, obj_name, **extra):
        self.assertEqual(400, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(err_code, resp['Error']['Code'])
        self.assertEqual(err_msg, resp['Error']['Message'])
        self.assertEqual({k: resp['Error'].get(k) for k in extra}, extra)

        # Sanity check: object was not created
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name, Key=obj_name)
        resp = caught.exception.response
        self.assertEqual(404, resp['ResponseMetadata']['HTTPStatusCode'])

    def test_let_sdk_compute(self):
        obj_name = self.create_name(self.ALGORITHM + '-sdk')
        resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=TEST_BODY,
            ChecksumAlgorithm=self.ALGORITHM,
        )
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def test_good_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-with-algo-header')
        resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=TEST_BODY,
            ChecksumAlgorithm=self.ALGORITHM,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED}
        )
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def test_good_checksum_no_algorithm_header(self):
        obj_name = self.create_name(self.ALGORITHM + '-no-algo-header')
        resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=TEST_BODY,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED}
        )
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def test_invalid_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-invalid')
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=TEST_BODY,
                ChecksumAlgorithm=self.ALGORITHM,
                **{'Checksum' + self.ALGORITHM: self.INVALID}
            )
        self.assert_error(
            caught.exception.response,
            'InvalidRequest',
            'Value for %s header is invalid.' % self.CHECKSUM_HDR,
            obj_name,
        )

    def test_bad_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-bad')
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=TEST_BODY,
                ChecksumAlgorithm=self.ALGORITHM,
                **{'Checksum' + self.ALGORITHM: self.BAD}
            )
        self.assert_error(
            caught.exception.response,
            'BadDigest',
            'The %s you specified did not match the calculated checksum.'
            % self.ALGORITHM,
            obj_name,
        )

    def test_mpu_upload_part_invalid_checksum(self):
        obj_name = self.create_name(
            self.ALGORITHM + '-mpu-upload-part-invalid-checksum')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm=self.ALGORITHM)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                **{'Checksum' + self.ALGORITHM: self.INVALID},
            )
        self.assert_error(
            caught.exception.response,
            'InvalidRequest',
            'Value for %s header is invalid.' % self.CHECKSUM_HDR,
            obj_name,
        )

    def test_mpu_upload_part_bad_checksum(self):
        obj_name = self.create_name(
            self.ALGORITHM + '-mpu-upload-part-bad-checksum')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm=self.ALGORITHM)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                **{'Checksum' + self.ALGORITHM: self.BAD},
            )
        self.assert_error(
            caught.exception.response,
            'BadDigest',
            'The %s you specified did not match the calculated '
            'checksum.' % self.ALGORITHM,
            obj_name,
        )

    def test_mpu_upload_part_good_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-mpu-upload-part-good')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm=self.ALGORITHM)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=TEST_BODY,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED},
        )
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])

    def test_mpu_complete_good_checksum(self):
        checksum_kwargs = {
            'ChecksumAlgorithm': self.ALGORITHM,
        }
        if boto_at_least(1, 36):
            if self.ALGORITHM == 'CRC64NVME':
                # crc64nvme only allows full-object
                checksum_kwargs['ChecksumType'] = 'FULL_OBJECT'
            else:
                checksum_kwargs['ChecksumType'] = 'COMPOSITE'

        obj_name = self.create_name(self.ALGORITHM + '-mpu-complete-good')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            **checksum_kwargs)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=TEST_BODY,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED},
        )
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={
                'Parts': [
                    {
                        'ETag': part_resp['ETag'],
                        'PartNumber': 1,
                        'Checksum' + self.ALGORITHM: self.EXPECTED,
                    },
                ],
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])


class TestObjectChecksumCRC32(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'CRC32'
    EXPECTED = 'y/Q5Jg=='
    INVALID = 'y/Q5Jh=='
    BAD = 'z/Q5Jg=='


class TestObjectChecksumCRC32C(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'CRC32C'
    EXPECTED = '4waSgw=='
    INVALID = '4waSgx=='
    BAD = '5waSgw=='

    @classmethod
    def setUpClass(cls):
        if not botocore.httpchecksum.HAS_CRT:
            raise SkipTest('botocore cannot crc32c (run `pip install awscrt`)')
        super().setUpClass()


class TestObjectChecksumCRC64NVME(ObjectChecksumMixin,
                                  BaseS3TestCaseWithBucket):
    ALGORITHM = 'CRC64NVME'
    EXPECTED = 'rosUhgp5mIg='
    INVALID = 'rosUhgp5mIh='
    BAD = 'sosUhgp5mIg='

    @classmethod
    def setUpClass(cls):
        if [int(x) for x in botocore.__version__.split('.')] < [1, 36]:
            raise SkipTest('botocore cannot crc64nvme (run '
                           '`pip install -U boto3 botocore`)')
        if not botocore.httpchecksum.HAS_CRT:
            raise SkipTest('botocore cannot crc64nvme (run '
                           '`pip install awscrt`)')
        super().setUpClass()


class TestObjectChecksumSHA1(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'SHA1'
    EXPECTED = '98O8HYCOBHMq32eZZczDTKeuNEE='
    INVALID = '98O8HYCOBHMq32eZZczDTKeuNEF='
    BAD = '+8O8HYCOBHMq32eZZczDTKeuNEE='


class TestObjectChecksumSHA256(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'SHA256'
    EXPECTED = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='
    INVALID = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiV='
    BAD = 'GeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='


class TestObjectChecksums(BaseS3TestCaseWithBucket):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')

    def test_multi_checksum(self):
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=self.create_name('multi-checksum'),
                Body=TEST_BODY,
                # Note: Both valid! Ought to be able to validate & store both
                ChecksumCRC32='y/Q5Jg==',
                ChecksumSHA1='98O8HYCOBHMq32eZZczDTKeuNEE=',
            )
        resp = caught.exception.response
        code = resp['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(400, code)
        self.assertEqual('InvalidRequest', resp['Error']['Code'])
        self.assertEqual(
            resp['Error']['Message'],
            'Expecting a single x-amz-checksum- header. '
            'Multiple checksum Types are not allowed.')

    def test_different_checksum_requested(self):
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=self.create_name('different-checksum'),
                Body=TEST_BODY,
                ChecksumCRC32='y/Q5Jg==',
                ChecksumAlgorithm='SHA1',
            )
        resp = caught.exception.response
        code = resp['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(400, code)
        self.assertEqual('InvalidRequest', resp['Error']['Code'])
        if boto_at_least(1, 36):
            self.assertEqual(
                resp['Error']['Message'],
                'Value for x-amz-sdk-checksum-algorithm header is invalid.')
        else:
            self.assertEqual(
                resp['Error']['Message'],
                'Expecting a single x-amz-checksum- header')

    def assert_invalid(self, resp):
        code = resp['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(400, code)
        self.assertEqual('InvalidRequest', resp['Error']['Code'])
        self.assertEqual(
            resp['Error']['Message'],
            'Value for x-amz-checksum-crc32 header is invalid.')

    def test_invalid_base64_invalid_length(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('invalid-bad-length'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'short===',  # invalid length for base64
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_too_short(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('invalid-short'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'shrt',  # only 3 bytes
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_too_long(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('invalid-long'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'toolong=',  # 5 bytes
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_all_invalid_chars(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('purely-invalid'),
            'Body': TEST_BODY,
            'ChecksumCRC32': '^^^^^^==',  # all invalid char
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_includes_invalid_chars(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('contains-invalid'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'y^/^Q5^J^g==',  # spaced out with invalid chars
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_mpu_no_checksum_upload_part_invalid_checksum(self):
        obj_name = self.create_name('no-checksum-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                ChecksumCRC32=TestObjectChecksumCRC32.INVALID,
            )
        self.assert_invalid(caught.exception.response)

    def test_mpu_has_no_checksum(self):
        # Clients don't need to be thinking about checksums at all
        obj_name = self.create_name('no-checksum-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=TEST_BODY,
        )
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={
                'Parts': [
                    {
                        'ETag': part_resp['ETag'],
                        'PartNumber': 1,
                    },
                ],
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        head_resp = self.client.head_object(
            Bucket=self.bucket_name, Key=obj_name)
        self.assertFalse([k for k in head_resp
                          if k.startswith('Checksum')])

    def test_mpu_upload_part_multi_checksum(self):
        obj_name = self.create_name('multi-checksum-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32C')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                # Both valid!
                ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED,
                ChecksumCRC32C=TestObjectChecksumCRC32C.EXPECTED,
            )
        resp = caught.exception.response
        self.assertEqual(400, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Error'], {
            'Code': 'InvalidRequest',
            'Message': ('Expecting a single x-amz-checksum- header. '
                        'Multiple checksum Types are not allowed.'),
        })
        # You'd think we ought to be able to validate & store both...

    def test_multipart_mpu(self):
        obj_name = self.create_name('multipart-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32C')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_body = b'\x00' * 5 * 1024 * 1024
        part_crc32c = base64_str(crc32c(part_body).digest())

        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        # then do another
        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=2,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={
                'Parts': [
                    {
                        'PartNumber': 1,
                        'ETag': upload_part_resp['ETag'],
                        'ChecksumCRC32C': part_crc32c,
                    },
                    {
                        'PartNumber': 2,
                        'ETag': upload_part_resp['ETag'],
                        'ChecksumCRC32C': part_crc32c,
                    },
                ],
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        mpu_etag = '"' + hashlib.md5(binascii.unhexlify(
            upload_part_resp['ETag'].strip('"')) * 2).hexdigest() + '-2"'
        self.assertEqual(mpu_etag,
                         complete_mpu_resp['ETag'])

    def test_multipart_mpu_no_etags(self):
        obj_name = self.create_name('multipart-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32C')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_body = b'\x00' * 5 * 1024 * 1024
        part_crc32c = base64_str(crc32c(part_body).digest())

        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        # then do another
        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=2,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={
                    'Parts': [
                        {
                            'PartNumber': 1,
                            'ChecksumCRC32C': part_crc32c,
                        },
                        {
                            'PartNumber': 2,
                            'ChecksumCRC32C': part_crc32c,
                        },
                    ],
                },
                UploadId=upload_id,
            )
        resp = caught.exception.response
        self.assertEqual(400, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Error']['Code'], 'MalformedXML')
        self.assertEqual(
            resp['Error']['Message'],
            'The XML you provided was not well-formed or did not validate '
            'against our published schema'
        )
        abort_resp = self.client.abort_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            UploadId=upload_id,
        )
        self.assertEqual(204, abort_resp[
            'ResponseMetadata']['HTTPStatusCode'])
