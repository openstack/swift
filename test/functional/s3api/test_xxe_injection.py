#!/usr/bin/env python
# Copyright (c) 2022 OpenStack Foundation
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

import requests

import botocore

import test.functional as tf
from test.functional.s3api import S3ApiBaseBoto3


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiXxeInjection(S3ApiBaseBoto3):

    def setUp(self):
        super(TestS3ApiXxeInjection, self).setUp()
        self.bucket = 'test-s3api-xxe-injection'

    def _create_bucket(self, **kwargs):
        resp = self.conn.create_bucket(Bucket=self.bucket, **kwargs)
        response_metadata = resp.pop('ResponseMetadata', {})
        self.assertEqual(200, response_metadata.get('HTTPStatusCode'))

    @staticmethod
    def _clear_data(request, **_kwargs):
        request.data = b''

    def _presign_url(self, method, key=None, **kwargs):
        params = {
            'Bucket': self.bucket
        }
        if key:
            params['Key'] = key
        params.update(kwargs)
        try:
            # https://github.com/boto/boto3/issues/2192
            self.conn.meta.events.register(
                'before-sign.s3.*', self._clear_data)
            url = self.conn.generate_presigned_url(
                method, Params=params, ExpiresIn=60)
        finally:
            self.conn.meta.events.unregister(
                'before-sign.s3.*', self._clear_data)
        if not params.get('Key') and '/?' not in url:
            # Some combination of dependencies seems to cause bucket requests
            # to not get the trailing slash despite signing with it? But only
            # new-enough versions sign with the trailing slash
            url = url.replace('?', '/?')
        return url

    def test_put_bucket_acl(self):
        if not tf.cluster_info['s3api'].get('s3_acl'):
            self.skipTest('s3_acl must be enabled')

        self._create_bucket()

        url = self._presign_url('put_bucket_acl')
        resp = requests.put(url, data="""
<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/swift/swift.conf"> ]>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Owner>
    <DisplayName>test:tester</DisplayName>
    <ID>test:tester</ID>
</Owner>
<AccessControlList>
    <Grant>
        <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
            <DisplayName>name&xxe;</DisplayName>
            <ID>id&xxe;</ID>
        </Grantee>
        <Permission>WRITE</Permission>
    </Grant>
</AccessControlList>
</AccessControlPolicy>
""")  # noqa: E501
        self.assertEqual(200, resp.status_code)
        self.assertNotIn(b'xxe', resp.content)
        self.assertNotIn(b'[swift-hash]', resp.content)

        acl = self.conn.get_bucket_acl(Bucket=self.bucket)
        response_metadata = acl.pop('ResponseMetadata', {})
        self.assertEqual(200, response_metadata.get('HTTPStatusCode'))
        self.assertDictEqual({
            'Owner': {
                'DisplayName': 'test:tester',
                'ID': 'test:tester'
            },
            'Grants': [
                {
                    'Grantee': {
                        'DisplayName': 'id',
                        'ID': 'id',
                        'Type': 'CanonicalUser'
                    },
                    'Permission': 'WRITE'
                }
            ]
        }, acl)

    def test_create_bucket(self):
        url = self._presign_url('create_bucket')
        resp = requests.put(url, data="""
<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/swift/swift.conf"> ]>
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LocationConstraint>&xxe;</LocationConstraint>
</CreateBucketConfiguration>
""")  # noqa: E501
        self.assertEqual(400, resp.status_code)
        self.assertNotIn(b'xxe', resp.content)
        self.assertNotIn(b'[swift-hash]', resp.content)

        self.assertRaisesRegex(
            botocore.exceptions.ClientError, 'Not Found',
            self.conn.head_bucket, Bucket=self.bucket)

    def test_delete_objects(self):
        self._create_bucket()

        url = self._presign_url(
            'delete_objects',
            Delete={
                'Objects': [
                    {
                        'Key': 'string',
                        'VersionId': 'string'
                    }
                ]
            })
        body = """
<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/swift/swift.conf"> ]>
<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Object>
        <Key>&xxe;</Key>
    </Object>
</Delete>
"""
        body = body.encode('utf-8')
        resp = requests.post(url, data=body)
        self.assertEqual(400, resp.status_code, resp.content)
        self.assertNotIn(b'xxe', resp.content)
        self.assertNotIn(b'[swift-hash]', resp.content)

    def test_complete_multipart_upload(self):
        self._create_bucket()

        resp = self.conn.create_multipart_upload(
            Bucket=self.bucket, Key='test')
        response_metadata = resp.pop('ResponseMetadata', {})
        self.assertEqual(200, response_metadata.get('HTTPStatusCode'))
        uploadid = resp.get('UploadId')

        try:
            url = self._presign_url(
                'complete_multipart_upload',
                Key='key',
                MultipartUpload={
                    'Parts': [
                        {
                            'ETag': 'string',
                            'PartNumber': 1
                        }
                    ],
                },
                UploadId=uploadid)
            resp = requests.post(url, data="""
<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/swift/swift.conf"> ]>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Part>
      <ETag>"{uploadid}"</ETag>
      <PartNumber>&xxe;</PartNumber>
   </Part>
</CompleteMultipartUpload>
""")  # noqa: E501
            self.assertEqual(404, resp.status_code)
            self.assertNotIn(b'xxe', resp.content)
            self.assertNotIn(b'[swift-hash]', resp.content)

            resp = requests.post(url, data="""
<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/swift/swift.conf"> ]>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Part>
      <ETag>"&xxe;"</ETag>
      <PartNumber>1</PartNumber>
   </Part>
</CompleteMultipartUpload>
""")  # noqa: E501
            self.assertEqual(404, resp.status_code)
            self.assertNotIn(b'xxe', resp.content)
            self.assertNotIn(b'[swift-hash]', resp.content)
        finally:
            resp = self.conn.abort_multipart_upload(
                Bucket=self.bucket, Key='test', UploadId=uploadid)
            response_metadata = resp.pop('ResponseMetadata', {})
            self.assertEqual(204, response_metadata.get('HTTPStatusCode'))

    def test_put_bucket_versioning(self):
        if 'object_versioning' not in tf.cluster_info:
            raise tf.SkipTest('S3 versioning requires that Swift object '
                              'versioning be enabled')
        self._create_bucket()

        url = self._presign_url(
            'put_bucket_versioning',
            VersioningConfiguration={
                'Status': 'Enabled'
            })
        resp = requests.put(url, data="""
<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/swift/swift.conf"> ]>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Status>&xxe;</Status>
</VersioningConfiguration>
""")  # noqa: E501
        self.assertEqual(400, resp.status_code)
        self.assertNotIn(b'xxe', resp.content)
        self.assertNotIn(b'[swift-hash]', resp.content)

        versioning = self.conn.get_bucket_versioning(Bucket=self.bucket)
        response_metadata = versioning.pop('ResponseMetadata', {})
        self.assertEqual(200, response_metadata.get('HTTPStatusCode'))
        self.assertDictEqual({}, versioning)
