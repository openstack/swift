# Copyright (c) 2015 OpenStack Foundation
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
import os
import test.functional as tf
from swift.common.middleware.s3api.etree import fromstring
from unittest import SkipTest
from test.functional.s3api import S3ApiBase
from test.functional.s3api.s3_test_client import Connection
from test.functional.s3api.utils import get_error_code


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3Acl(S3ApiBase):
    def setUp(self):
        super(TestS3Acl, self).setUp()
        self.bucket = 'bucket'
        self.obj = 'object'
        if 's3_access_key3' not in tf.config or \
                's3_secret_key3' not in tf.config:
            raise SkipTest(
                'TestS3Acl requires s3_access_key3 and s3_secret_key3 '
                'configured for reduced-access user')
        status, headers, body = self.conn.make_request('PUT', self.bucket)
        self.assertEqual(status, 200, body)
        access_key3 = tf.config['s3_access_key3']
        secret_key3 = tf.config['s3_secret_key3']
        self.conn3 = Connection(access_key3, secret_key3, access_key3)

    def test_acl(self):
        self.conn.make_request('PUT', self.bucket, self.obj)
        query = 'acl'

        # PUT Bucket ACL
        headers = {'x-amz-acl': 'public-read'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, headers=headers,
                                   query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertEqual(headers['content-length'], '0')

        # GET Bucket ACL
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        # TODO: Fix the response that last-modified must be in the response.
        # self.assertTrue(headers['last-modified'] is not None)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertTrue(headers['content-type'] is not None)
        elem = fromstring(body, 'AccessControlPolicy')
        owner = elem.find('Owner')
        self.assertEqual(owner.find('ID').text, self.conn.user_id)
        self.assertEqual(owner.find('DisplayName').text, self.conn.user_id)
        acl = elem.find('AccessControlList')
        self.assertTrue(acl.find('Grant') is not None)

        # GET Object ACL
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, self.obj, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        # TODO: Fix the response that last-modified must be in the response.
        # self.assertTrue(headers['last-modified'] is not None)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertTrue(headers['content-type'] is not None)
        elem = fromstring(body, 'AccessControlPolicy')
        owner = elem.find('Owner')
        self.assertEqual(owner.find('ID').text, self.conn.user_id)
        self.assertEqual(owner.find('DisplayName').text, self.conn.user_id)
        acl = elem.find('AccessControlList')
        self.assertTrue(acl.find('Grant') is not None)

    def test_put_bucket_acl_error(self):
        req_headers = {'x-amz-acl': 'public-read'}
        aws_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            aws_error_conn.make_request('PUT', self.bucket,
                                        headers=req_headers, query='acl')
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('PUT', 'nothing',
                                   headers=req_headers, query='acl')
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        status, headers, body = \
            self.conn3.make_request('PUT', self.bucket,
                                    headers=req_headers, query='acl')
        self.assertEqual(get_error_code(body), 'AccessDenied')

    def test_get_bucket_acl_error(self):
        aws_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            aws_error_conn.make_request('GET', self.bucket, query='acl')
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('GET', 'nothing', query='acl')
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        status, headers, body = \
            self.conn3.make_request('GET', self.bucket, query='acl')
        self.assertEqual(get_error_code(body), 'AccessDenied')

    def test_get_object_acl_error(self):
        self.conn.make_request('PUT', self.bucket, self.obj)

        aws_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            aws_error_conn.make_request('GET', self.bucket, self.obj,
                                        query='acl')
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('GET', self.bucket, 'nothing', query='acl')
        self.assertEqual(get_error_code(body), 'NoSuchKey')

        status, headers, body = \
            self.conn3.make_request('GET', self.bucket, self.obj, query='acl')
        self.assertEqual(get_error_code(body), 'AccessDenied')


class TestS3AclSigV4(TestS3Acl):
    @classmethod
    def setUpClass(cls):
        os.environ['S3_USE_SIGV4'] = "True"

    @classmethod
    def tearDownClass(cls):
        del os.environ['S3_USE_SIGV4']

    def setUp(self):
        super(TestS3AclSigV4, self).setUp()


if __name__ == '__main__':
    unittest.main()
