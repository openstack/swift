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

from test.s3api import BaseS3TestCase, status_from_error, code_from_error, \
    etag_from_resp
from botocore.exceptions import ClientError


class BaseMultiPartUploadTestCase(BaseS3TestCase):
    maxDiff = None

    def setUp(self):
        self.client = self.get_s3_client(1)
        self.bucket_name = self.create_name('mpu-bucket')
        resp = self.client.create_bucket(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.num_parts = 3
        self.part_size = 5 * (2 ** 20)  # 5 MB

    def tearDown(self):
        self.clear_bucket(self.client, self.bucket_name)
        super(BaseMultiPartUploadTestCase, self).tearDown()

    def _make_part_bodies(self):
        return [
            ('%d' % i) * self.part_size
            for i in range(self.num_parts)
        ]

    def _iter_part_num_ranges(self):
        for i in range(self.num_parts):
            start = self.part_size * i
            end = start + self.part_size
            # part_num is 1 indexed
            yield i + 1, start, end

    def _upload_mpu(self, key_name):
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']

        part_bodies = self._make_part_bodies()
        parts = []
        for i, body in enumerate(part_bodies, 1):
            part_resp = self.client.upload_part(
                Body=body, Bucket=self.bucket_name, Key=key_name,
                PartNumber=i, UploadId=upload_id)
            self.assertEqual(200, part_resp[
                'ResponseMetadata']['HTTPStatusCode'])
            parts.append({
                'ETag': part_resp['ETag'],
                'PartNumber': i,
            })
        # this helper doesn't bother calling list-parts, it's not required
        # and we know what we uploaded
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=key_name,
            MultipartUpload={
                'Parts': parts,
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        return complete_mpu_resp

    def upload_mpu_version(self, key_name):
        complete_mpu_resp = self._upload_mpu(key_name)
        # AWS returns the version_id *in* the MPU-complete response but s3api
        # does NOT (see https://bugs.launchpad.net/swift/+bug/2043619), so we
        # do an extra HEAD to get the version
        head_object_resp = self.client.head_object(
            Bucket=self.bucket_name, Key=key_name)

        self.assertEqual(200, head_object_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        return complete_mpu_resp['ETag'], head_object_resp.get('VersionId')

    def upload_mpu(self, key_name):
        complete_mpu_resp = self._upload_mpu(key_name)
        return complete_mpu_resp['ETag']

    def _verify_part_num_response(self, method, key_name, mpu_etag,
                                  version=None):
        part_bodies = self._make_part_bodies()
        total_size = self.num_parts * self.part_size

        for part_num, start, end in self._iter_part_num_ranges():
            extra_kwargs = {}
            if version is not None:
                extra_kwargs['VersionId'] = version
            resp = method(Bucket=self.bucket_name, Key=key_name,
                          PartNumber=part_num, **extra_kwargs)
            self.assertEqual(206, resp['ResponseMetadata'][
                'HTTPStatusCode'])
            self.assertEqual(self.part_size, resp['ContentLength'])
            if method == self.client.get_object:
                resp_body = b''.join(resp['Body']).decode()
                # our part_bodies are zero indexed
                self.assertEqual(resp_body, part_bodies[part_num - 1])
                expected_range = 'bytes %s-%s/%s' % (
                    start, end - 1, total_size)
                self.assertEqual(expected_range, resp['ContentRange'])
            # ETag and PartsCount are from the MPU
            self.assertEqual(mpu_etag, resp['ETag'], mpu_etag)
            self.assertEqual(self.num_parts, resp['PartsCount'])
            self.assertEqual('bytes', resp['AcceptRanges'])
            if version is None:
                self.assertNotIn('VersionId', resp)
            else:
                self.assertEqual(version, resp['VersionId'])

    def _verify_copy_parts(self, key_src, key_dest, upload_id):
        parts = []
        for part_num, start, end in self._iter_part_num_ranges():
            copy_range = 'bytes=%d-%d' % (start, end - 1)
            copy_resp = self.client.\
                upload_part_copy(Bucket=self.bucket_name,
                                 Key=key_dest, PartNumber=part_num,
                                 CopySource={
                                     'Bucket': self.bucket_name,
                                     'Key': key_src,
                                 }, CopySourceRange=copy_range,
                                 UploadId=upload_id)
            self.assertEqual(200, copy_resp[
                'ResponseMetadata']['HTTPStatusCode'])
            self.assertTrue(copy_resp['CopyPartResult']['ETag'])
            self.assertTrue(copy_resp['CopyPartResult']['LastModified'])
            parts.append({
                'ETag': copy_resp['CopyPartResult']['ETag'],
                'PartNumber': part_num,
            })

        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=key_dest,
            MultipartUpload={
                'Parts': parts,
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        return complete_mpu_resp['ETag']


class TestMultiPartUpload(BaseMultiPartUploadTestCase):

    def setUp(self):
        super(TestMultiPartUpload, self).setUp()

    def _discover_max_part_num(self):
        key_name = self.create_name('discover-max-part-num')
        self.upload_mpu(key_name)
        with self.assertRaises(ClientError) as cm:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name, PartNumber=0)
        err_resp = cm.exception.response
        self.assertEqual(400, err_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('InvalidArgument', err_resp['Error']['Code'])
        err_msg = err_resp['Error']['Message']
        preamble = 'Part number must be an integer between 1 and '
        self.assertIn(preamble, err_msg)
        return int(err_msg[len(preamble):].split(',')[0])

    def create_mpu(self, key_name):
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        return create_mpu_resp['UploadId']

    def list_mpus(self):
        list_mpu_resp = self.client.list_multipart_uploads(
            Bucket=self.bucket_name)
        self.assertEqual(200, list_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        mpus = list_mpu_resp.get('Uploads', [])
        return [(mpu['Key'], mpu['UploadId']) for mpu in mpus]

    def list_parts(self, key_name, upload_id):
        list_parts_resp = self.client.list_parts(
            Bucket=self.bucket_name, Key=key_name,
            UploadId=upload_id,
        )
        self.assertEqual(200, list_parts_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        return [{k: p[k] for k in ('ETag', 'PartNumber')}
                for p in list_parts_resp.get('Parts', [])]

    def upload_part_indexes(self, key_name, upload_id, part_indexes):
        parts = []
        for i in part_indexes:
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
        self.assertEqual(parts, self.list_parts(key_name, upload_id))
        return parts

    def upload_parts(self, key_name, upload_id, num_parts):
        return self.upload_part_indexes(key_name, upload_id,
                                        range(1, num_parts + 1))

    def complete_mpu(self, key_name, upload_id, parts):
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=key_name,
            MultipartUpload={
                'Parts': parts,
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        return complete_mpu_resp

    def abort_mpu(self, key_name, upload_id):
        abort_resp = self.client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=key_name,
            UploadId=upload_id,
        )
        self.assertEqual(204, abort_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        return abort_resp

    def head_part(self, key_name, part_number):
        resp = self.client.head_object(Bucket=self.bucket_name, Key=key_name,
                                       PartNumber=part_number)
        self.assertEqual(206, resp[
            'ResponseMetadata']['HTTPStatusCode'])
        return resp

    def get_part(self, key_name, part_number):
        resp = self.client.get_object(Bucket=self.bucket_name, Key=key_name,
                                      PartNumber=part_number)
        self.assertEqual(206, resp[
            'ResponseMetadata']['HTTPStatusCode'])
        return resp

    def delete_object(self, key_name):
        delete_resp = self.client.delete_object(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(204, delete_resp[
            'ResponseMetadata']['HTTPStatusCode'])

    def assert_object_not_found(self, key_name):
        with self.assertRaises(ClientError) as cm:
            self.client.head_object(
                Bucket=self.bucket_name, Key=key_name,
            )
        self.assertEqual(404, status_from_error(cm.exception))

    def test_basic_upload(self):
        key_name = self.create_name('key')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']

        list_mpu_resp = self.client.list_multipart_uploads(
            Bucket=self.bucket_name)
        self.assertEqual(200, list_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        found_uploads = list_mpu_resp.get('Uploads', [])
        self.assertEqual(1, len(found_uploads), found_uploads)
        self.assertEqual(upload_id, found_uploads[0]['UploadId'])

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

    def test_zero_byte_segment_upload(self):
        key_name = self.create_name('key')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_resp = self.client.upload_part(
            Body='', Bucket=self.bucket_name, Key=key_name,
            PartNumber=1, UploadId=upload_id)
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        parts = [{
            'ETag': part_resp['ETag'],
            'PartNumber': 1,
        }]
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

    def _check_part_num_invalid_exc(self, exc, val, max_part_num,
                                    is_head=False):
        err_resp = exc.response
        self.assertEqual(400, err_resp['ResponseMetadata']['HTTPStatusCode'])
        if is_head:
            err_code = '400'
            err_msg = 'Bad Request'
        else:
            err_code = 'InvalidArgument'
            err_msg = 'Part number must be an integer between ' \
                      '1 and %d, inclusive' % max_part_num
        self.assertEqual(err_code, err_resp['Error']['Code'], err_resp)
        self.assertEqual(err_msg, err_resp['Error']['Message'])
        if is_head:
            self.assertNotIn('ArgumentName', err_resp['Error'])
            self.assertNotIn('ArgumentValue', err_resp['Error'])
        else:
            self.assertEqual('partNumber', err_resp['Error']['ArgumentName'])
            self.assertEqual(str(val), err_resp['Error']['ArgumentValue'])

    def _check_part_num_out_of_range_exc(self, exc, is_head=False):
        err_resp = exc.response
        self.assertEqual(416, err_resp['ResponseMetadata']['HTTPStatusCode'])
        if is_head:
            err_code = '416'
            err_msg = 'Requested Range Not Satisfiable'
        else:
            err_code = 'InvalidPartNumber'
            err_msg = 'The requested partnumber is not satisfiable'
        self.assertEqual(err_code, err_resp['Error']['Code'], err_resp)
        self.assertEqual(err_msg, err_resp['Error']['Message'], err_resp)

    def test_get_object_partNumber_errors(self):
        max_part_num = self._discover_max_part_num()
        key_name = self.create_name('invalid-part-num-test')
        mpu_etag = self.upload_mpu(key_name)

        # partNumber argument is 1 indexed
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name, PartNumber=0)
        self._check_part_num_invalid_exc(caught.exception, 0, max_part_num)

        # all other partNumber args are valid
        self._verify_part_num_response(
            self.client.get_object, key_name, mpu_etag)

        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name, PartNumber=self.num_parts + 1)
        self._check_part_num_out_of_range_exc(caught.exception)

        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name, PartNumber=max_part_num)
        self._check_part_num_out_of_range_exc(caught.exception)

        # because of ParamValidationError we can't test 'foo'
        val = -1
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name, PartNumber=val)
        self._check_part_num_invalid_exc(caught.exception, val, max_part_num)
        val = max_part_num + 1
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name, PartNumber=val)
        self._check_part_num_invalid_exc(caught.exception, val, max_part_num)

    def test_head_object_partNumber_errors(self):
        max_part_num = self._discover_max_part_num()
        key_name = self.create_name('invalid-part-num-head')
        mpu_etag = self.upload_mpu(key_name)

        # partNumber argument is 1 indexed
        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name,
                                    Key=key_name, PartNumber=0)
        self._check_part_num_invalid_exc(caught.exception, 0, max_part_num,
                                         is_head=True)

        # all other partNumber args are valid
        self._verify_part_num_response(
            self.client.head_object, key_name, mpu_etag)

        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name, Key=key_name,
                                    PartNumber=self.num_parts + 1)
        self._check_part_num_out_of_range_exc(caught.exception, is_head=True)

        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name, Key=key_name,
                                    PartNumber=max_part_num)
        self._check_part_num_out_of_range_exc(caught.exception, is_head=True)

        # because of ParamValidationError we can't test 'foo'
        val = -1
        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name, Key=key_name,
                                    PartNumber=val)
        self._check_part_num_invalid_exc(caught.exception, val, max_part_num,
                                         is_head=True)
        val = max_part_num + 1
        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name, Key=key_name,
                                    PartNumber=val)
        self._check_part_num_invalid_exc(caught.exception, val, max_part_num,
                                         is_head=True)

    def test_part_number_non_mpu(self):
        max_part_num = self._discover_max_part_num()
        key_name = self.create_name('part-num-non-mpu')
        self.client.put_object(Bucket=self.bucket_name,
                               Key=key_name,
                               Body=b'non-mpu-object')
        head_resp = self.client.head_object(Bucket=self.bucket_name,
                                            Key=key_name)
        # sanity check
        self.assertEqual(200,
                         head_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(head_resp['AcceptRanges'], 'bytes')
        self.assertEqual(head_resp['ContentLength'], 14)

        head_resp = self.client.head_object(Bucket=self.bucket_name,
                                            Key=key_name,
                                            PartNumber=1)
        self.assertEqual(206,
                         head_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(head_resp['ContentLength'], 14)

        get_resp = self.client.get_object(Bucket=self.bucket_name,
                                          Key=key_name,
                                          PartNumber=1)
        self.assertEqual(206,
                         get_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(get_resp['ContentLength'], 14)
        self.assertEqual(get_resp['ContentRange'], 'bytes 0-13/14')
        self.assertEqual(b'non-mpu-object', b''.join(get_resp['Body']))

        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name,
                                   PartNumber=4)
        self._check_part_num_out_of_range_exc(caught.exception)

        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name,
                                    Key=key_name,
                                    PartNumber=4)
        self._check_part_num_out_of_range_exc(caught.exception, is_head=True)

        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name,
                                   PartNumber=0)
        self._check_part_num_invalid_exc(caught.exception, 0, max_part_num)

        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name,
                                    Key=key_name,
                                    PartNumber=0)
        self._check_part_num_invalid_exc(caught.exception, 0, max_part_num,
                                         is_head=True)

        invalid_part_num = 10001
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name,
                                   PartNumber=invalid_part_num)
        self._check_part_num_invalid_exc(caught.exception, invalid_part_num,
                                         max_part_num)

        with self.assertRaises(ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name,
                                    Key=key_name,
                                    PartNumber=invalid_part_num)
        self._check_part_num_invalid_exc(caught.exception, invalid_part_num,
                                         max_part_num, is_head=True)

    def test_get_object_partNumber_and_range(self):
        # partNumber not allowed with Range even for non-mpu object
        key_name = self.create_name('part-num-mpu')
        self._upload_mpu(key_name)
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name,
                                   PartNumber=1,
                                   Range='bytes=1-2')
        err_resp = caught.exception.response
        self.assertEqual(400, err_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('InvalidRequest', err_resp['Error']['Code'], err_resp)
        self.assertEqual('Cannot specify both Range header and partNumber '
                         'query parameter', err_resp['Error']['Message'])

        key_name = self.create_name('part-num-non-mpu')
        self.client.put_object(Bucket=self.bucket_name,
                               Key=key_name,
                               Body=b'non-mpu-object')
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name,
                                   PartNumber=1,
                                   Range='bytes=1-2')
        err_resp = caught.exception.response
        self.assertEqual(400, err_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('InvalidRequest', err_resp['Error']['Code'], err_resp)
        self.assertEqual('Cannot specify both Range header and partNumber '
                         'query parameter', err_resp['Error']['Message'])

        # partNumber + Range error trumps bad partNumber
        with self.assertRaises(ClientError) as caught:
            self.client.get_object(Bucket=self.bucket_name,
                                   Key=key_name,
                                   PartNumber=0,
                                   Range='bytes=1-2')
        err_resp = caught.exception.response
        self.assertEqual(400, err_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual('InvalidRequest', err_resp['Error']['Code'], err_resp)
        self.assertEqual('Cannot specify both Range header and partNumber '
                         'query parameter', err_resp['Error']['Message'])

    def test_upload_part_copy(self):
        self.num_parts = 4
        key_src = self.create_name('part-copy-src')
        key_dest = self.create_name('part-copy-dest')
        mpu_etag_src = self.upload_mpu(key_src)
        self._verify_part_num_response(
            self.client.get_object, key_src, mpu_etag_src)
        self._verify_part_num_response(
            self.client.head_object, key_src, mpu_etag_src)

        create_mpu_dest = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_dest)
        self.assertEqual(200, create_mpu_dest[
            'ResponseMetadata']['HTTPStatusCode'])

        upload_id = create_mpu_dest['UploadId']
        mpu_etag_dst = self._verify_copy_parts(key_src, key_dest, upload_id)
        self._verify_part_num_response(
            self.client.get_object, key_dest, mpu_etag_dst)
        self._verify_part_num_response(
            self.client.head_object, key_dest, mpu_etag_dst)

    def test_copy_mpu_from_parts(self):
        key_src = self.create_name('copy-from-from-src')
        mpu_etag_src = self.upload_mpu(key_src)

        # client wanting to copy object would first HEAD
        head_object_resp = self.client.head_object(
            Bucket=self.bucket_name, Key=key_src)
        # the client will know it's an mpu and how many parts
        self.assertEqual(mpu_etag_src, head_object_resp['ETag'])
        self.assertIn('-', mpu_etag_src)
        num_parts = int(mpu_etag_src.strip('"').rsplit('-')[-1])

        # create new mpu
        key_dest = self.create_name('copy-from-from-dest')
        create_mpu_dest = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key_dest)
        self.assertEqual(200, create_mpu_dest[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_dest['UploadId']

        parts = []
        start = 0
        # do HEAD?partNumber to get copy range
        for part_num in range(1, num_parts + 1):
            part_head_resp = self.client.head_object(
                Bucket=self.bucket_name, Key=key_src, PartNumber=part_num)
            end = start + part_head_resp['ContentLength']
            copy_range = 'bytes=%s-%s' % (start, end - 1)
            copy_resp = self.client.upload_part_copy(
                Bucket=self.bucket_name, Key=key_dest, PartNumber=part_num,
                CopySource={
                    'Bucket': self.bucket_name,
                    'Key': key_src,
                },
                CopySourceRange=copy_range, UploadId=upload_id)
            self.assertEqual(200, copy_resp[
                'ResponseMetadata']['HTTPStatusCode'])
            parts.append({
                'ETag': copy_resp['CopyPartResult']['ETag'],
                'PartNumber': part_num,
            })
            start = end

        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=key_dest,
            MultipartUpload={
                'Parts': parts,
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(complete_mpu_resp['ETag'], mpu_etag_src)

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

    def test_create_upload_complete_complete(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        parts = self.upload_parts(key_name, upload_id, 2)
        self.complete_mpu(key_name, upload_id, parts)
        # repeat complete gets 200
        self.complete_mpu(key_name, upload_id, parts)

    def test_create_upload_complete_delete_complete(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        parts = self.upload_parts(key_name, upload_id, 2)
        self.complete_mpu(key_name, upload_id, parts)
        self.delete_object(key_name)
        self.assert_object_not_found(key_name)
        # repeat complete gets 404
        with self.assertRaises(ClientError) as cm:
            self.complete_mpu(key_name, upload_id, parts)
        self.assertEqual(404, status_from_error(cm.exception))
        self.assertEqual('NoSuchUpload', code_from_error(cm.exception))

    def test_create_upload_abort_complete(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        parts = self.upload_parts(key_name, upload_id, 1)
        self.abort_mpu(key_name, upload_id)
        with self.assertRaises(ClientError) as cm:
            self.complete_mpu(key_name, upload_id, parts)
        self.assertEqual(404, status_from_error(cm.exception))
        self.assertEqual('NoSuchUpload', code_from_error(cm.exception))

    def test_abort_bogus_id(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        with self.assertRaises(ClientError) as cm:
            self.abort_mpu(key_name, upload_id + 'x')
        self.assertEqual(404, status_from_error(cm.exception))
        self.assertEqual('NoSuchUpload', code_from_error(cm.exception))

    def test_create_upload_abort_list_parts(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        self.upload_parts(key_name, upload_id, 1)
        self.abort_mpu(key_name, upload_id)
        with self.assertRaises(ClientError) as cm:
            self.list_parts(key_name, upload_id)
        self.assertEqual(404, status_from_error(cm.exception))
        self.assertEqual('NoSuchUpload', code_from_error(cm.exception))

    def test_create_upload_abort_upload(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        self.upload_parts(key_name, upload_id, 1)
        self.abort_mpu(key_name, upload_id)
        with self.assertRaises(ClientError) as cm:
            self.upload_parts(key_name, upload_id, 1)
        self.assertEqual(404, status_from_error(cm.exception))
        self.assertEqual('NoSuchUpload', code_from_error(cm.exception))

    def test_create_upload_complete_subset_of_parts_list(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        parts = self.upload_parts(key_name, upload_id, 3)
        subset_parts = parts[:2]
        self.complete_mpu(key_name, upload_id, subset_parts)

        response = self.head_part(key_name, 1)
        self.assertTrue(etag_from_resp(response).endswith('-2"'))
        response2 = self.head_part(key_name, 2)
        self.assertEqual(etag_from_resp(response), etag_from_resp(response2))

        with self.assertRaises(ClientError) as cm:
            self.get_part(key_name, 3)
        self.assertEqual(416, status_from_error(cm.exception))
        self.assertEqual('InvalidPartNumber', code_from_error(cm.exception))

    def test_create_upload_complete_subset_of_parts_list_with_gaps(self):
        # only a subset of uploaded parts are referenced in complete
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        parts = self.upload_parts(key_name, upload_id, 3)
        subset_parts = [parts[0], parts[2]]
        self.complete_mpu(key_name, upload_id, subset_parts)
        # GET partNumbers are not same as uploaded part numbers!
        self.head_part(key_name, 1)
        response = self.head_part(key_name, 1)
        self.assertTrue(etag_from_resp(response).endswith('-2"'))
        response2 = self.head_part(key_name, 2)
        self.assertEqual(etag_from_resp(response), etag_from_resp(response2))

        with self.assertRaises(ClientError) as cm:
            self.get_part(key_name, 3)
        self.assertEqual(416, status_from_error(cm.exception))
        self.assertEqual('InvalidPartNumber', code_from_error(cm.exception))

    def test_create_upload_complete_parts_list_with_gaps(self):
        # only a subset of part indexes are uploaded
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        parts = self.upload_part_indexes(key_name, upload_id, [1, 1000])
        actual_parts = self.list_parts(key_name, upload_id)
        self.assertEqual([1, 1000], [p['PartNumber'] for p in actual_parts])
        self.complete_mpu(key_name, upload_id, parts)
        # GET partNumbers are not same as uploaded part numbers!
        self.head_part(key_name, 1)
        response = self.head_part(key_name, 1)
        self.assertTrue(etag_from_resp(response).endswith('-2"'))
        response2 = self.head_part(key_name, 2)
        self.assertEqual(etag_from_resp(response), etag_from_resp(response2))

        with self.assertRaises(ClientError) as cm:
            self.get_part(key_name, 3)
        self.assertEqual(416, status_from_error(cm.exception), cm.exception)
        self.assertEqual('InvalidPartNumber', code_from_error(cm.exception))

    def test_create_upload_complete_misordered_parts(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        parts = self.upload_parts(key_name, upload_id, 3)
        with self.assertRaises(ClientError) as cm:
            self.complete_mpu(key_name, upload_id, list(reversed(parts)))
        self.assertEqual(400, status_from_error(cm.exception))
        self.assertEqual('InvalidPartOrder', code_from_error(cm.exception))

    def test_create_list_mpus_abort_list_mpus(self):
        key_name = self.create_name('key')
        upload_id = self.create_mpu(key_name)
        # our upload is in progress
        found_uploads = self.list_mpus()
        self.assertEqual([(key_name, upload_id)], found_uploads)
        self.assertEqual([], self.list_parts(key_name, upload_id))
        self.abort_mpu(key_name, upload_id)
        # no more inprogress uploads
        self.assertEqual([], self.list_mpus())

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


class TestVersionedMultiPartUpload(BaseMultiPartUploadTestCase):

    def setUp(self):
        super(TestVersionedMultiPartUpload, self).setUp()
        resp = self.client.put_bucket_versioning(
            Bucket=self.bucket_name,
            VersioningConfiguration={'Status': 'Enabled'})
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def tearDown(self):
        resp = self.client.put_bucket_versioning(
            Bucket=self.bucket_name,
            VersioningConfiguration={'Status': 'Suspended'})
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        super(TestVersionedMultiPartUpload, self).tearDown()

    def test_get_by_part_number_with_versioning(self):
        # create 3 version with progressively larger sizes
        parts_counts = [2, 3, 4]
        key_name = self.create_name('part-num-versions')
        version_vars = []
        for num_parts in parts_counts:
            self.num_parts = num_parts
            etag, version_id = self.upload_mpu_version(key_name)
            version_vars.append((num_parts, etag, version_id))
        for num_parts, mpu_etag, version in version_vars:
            self.num_parts = num_parts
            self._verify_part_num_response(
                self.client.get_object, key_name, mpu_etag, version)
            self._verify_part_num_response(
                self.client.head_object, key_name, mpu_etag, version)
