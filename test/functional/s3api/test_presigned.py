# Copyright (c) 2016 SwiftStack, Inc.
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

import os

import requests

from swift.common.middleware.s3api.etree import fromstring

import test.functional as tf

from test.functional.s3api import S3ApiBase
from test.functional.s3api.utils import get_error_code, get_error_msg


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiPresignedUrls(S3ApiBase):
    def test_bucket(self):
        bucket = 'test-bucket'
        req_objects = ('object', 'object2')
        max_bucket_listing = tf.cluster_info['s3api'].get(
            'max_bucket_listing', 1000)

        # GET Bucket (Without Object)
        status, _junk, _junk = self.conn.make_request('PUT', bucket)
        self.assertEqual(status, 200)

        url, headers = self.conn.generate_url_and_headers('GET', bucket)
        resp = requests.get(url, headers=headers)
        self.assertEqual(resp.status_code, 200,
                         'Got %d %s' % (resp.status_code, resp.content))
        self.assertCommonResponseHeaders(resp.headers)
        self.assertIsNotNone(resp.headers['content-type'])
        self.assertEqual(resp.headers['content-length'],
                         str(len(resp.content)))

        elem = fromstring(resp.content, 'ListBucketResult')
        self.assertEqual(elem.find('Name').text, bucket)
        self.assertIsNone(elem.find('Prefix').text)
        self.assertIsNone(elem.find('Marker').text)
        self.assertEqual(elem.find('MaxKeys').text,
                         str(max_bucket_listing))
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        objects = elem.findall('./Contents')
        self.assertEqual(list(objects), [])

        # GET Bucket (With Object)
        for obj in req_objects:
            status, _junk, _junk = self.conn.make_request('PUT', bucket, obj)
            self.assertEqual(
                status, 200,
                'Got %d response while creating %s' % (status, obj))

        resp = requests.get(url, headers=headers)
        self.assertEqual(resp.status_code, 200,
                         'Got %d %s' % (resp.status_code, resp.content))
        self.assertCommonResponseHeaders(resp.headers)
        self.assertIsNotNone(resp.headers['content-type'])
        self.assertEqual(resp.headers['content-length'],
                         str(len(resp.content)))

        elem = fromstring(resp.content, 'ListBucketResult')
        self.assertEqual(elem.find('Name').text, bucket)
        self.assertIsNone(elem.find('Prefix').text)
        self.assertIsNone(elem.find('Marker').text)
        self.assertEqual(elem.find('MaxKeys').text,
                         str(max_bucket_listing))
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        resp_objects = elem.findall('./Contents')
        self.assertEqual(len(list(resp_objects)), 2)
        for o in resp_objects:
            self.assertIn(o.find('Key').text, req_objects)
            self.assertIsNotNone(o.find('LastModified').text)
            self.assertRegexpMatches(
                o.find('LastModified').text,
                r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$')
            self.assertIsNotNone(o.find('ETag').text)
            self.assertEqual(o.find('Size').text, '0')
            self.assertIsNotNone(o.find('StorageClass').text is not None)
            self.assertEqual(o.find('Owner/ID').text, self.conn.user_id)
            self.assertEqual(o.find('Owner/DisplayName').text,
                             self.conn.user_id)
        # DELETE Bucket
        for obj in req_objects:
            self.conn.make_request('DELETE', bucket, obj)
        url, headers = self.conn.generate_url_and_headers('DELETE', bucket)
        resp = requests.delete(url, headers=headers)
        self.assertEqual(resp.status_code, 204,
                         'Got %d %s' % (resp.status_code, resp.content))

    def test_expiration_limits(self):
        if os.environ.get('S3_USE_SIGV4'):
            self._test_expiration_limits_v4()
        else:
            self._test_expiration_limits_v2()

    def _test_expiration_limits_v2(self):
        bucket = 'test-bucket'

        # Expiration date is too far in the future
        url, headers = self.conn.generate_url_and_headers(
            'GET', bucket, expires_in=2 ** 32)
        resp = requests.get(url, headers=headers)
        self.assertEqual(resp.status_code, 403,
                         'Got %d %s' % (resp.status_code, resp.content))
        self.assertEqual(get_error_code(resp.content),
                         'AccessDenied')
        self.assertIn('Invalid date (should be seconds since epoch)',
                      get_error_msg(resp.content))

    def _test_expiration_limits_v4(self):
        bucket = 'test-bucket'

        # Expiration is negative
        url, headers = self.conn.generate_url_and_headers(
            'GET', bucket, expires_in=-1)
        resp = requests.get(url, headers=headers)
        self.assertEqual(resp.status_code, 400,
                         'Got %d %s' % (resp.status_code, resp.content))
        self.assertEqual(get_error_code(resp.content),
                         'AuthorizationQueryParametersError')
        self.assertIn('X-Amz-Expires must be non-negative',
                      get_error_msg(resp.content))

        # Expiration date is too far in the future
        for exp in (7 * 24 * 60 * 60 + 1,
                    2 ** 63 - 1):
            url, headers = self.conn.generate_url_and_headers(
                'GET', bucket, expires_in=exp)
            resp = requests.get(url, headers=headers)
            self.assertEqual(resp.status_code, 400,
                             'Got %d %s' % (resp.status_code, resp.content))
            self.assertEqual(get_error_code(resp.content),
                             'AuthorizationQueryParametersError')
            self.assertIn('X-Amz-Expires must be less than 604800 seconds',
                          get_error_msg(resp.content))

        # Expiration date is *way* too far in the future, or isn't a number
        for exp in (2 ** 63, 'foo'):
            url, headers = self.conn.generate_url_and_headers(
                'GET', bucket, expires_in=2 ** 63)
            resp = requests.get(url, headers=headers)
            self.assertEqual(resp.status_code, 400,
                             'Got %d %s' % (resp.status_code, resp.content))
            self.assertEqual(get_error_code(resp.content),
                             'AuthorizationQueryParametersError')
            self.assertEqual('X-Amz-Expires should be a number',
                             get_error_msg(resp.content))

    def test_object(self):
        bucket = 'test-bucket'
        obj = 'object'

        status, _junk, _junk = self.conn.make_request('PUT', bucket)
        self.assertEqual(status, 200)

        # HEAD/missing object
        head_url, headers = self.conn.generate_url_and_headers(
            'HEAD', bucket, obj)
        resp = requests.head(head_url, headers=headers)
        self.assertEqual(resp.status_code, 404,
                         'Got %d %s' % (resp.status_code, resp.content))

        # Wrong verb
        resp = requests.get(head_url)
        self.assertEqual(resp.status_code, 403,
                         'Got %d %s' % (resp.status_code, resp.content))
        self.assertEqual(get_error_code(resp.content),
                         'SignatureDoesNotMatch')

        # PUT empty object
        put_url, headers = self.conn.generate_url_and_headers(
            'PUT', bucket, obj)
        resp = requests.put(put_url, data='', headers=headers)
        self.assertEqual(resp.status_code, 200,
                         'Got %d %s' % (resp.status_code, resp.content))
        # GET empty object
        get_url, headers = self.conn.generate_url_and_headers(
            'GET', bucket, obj)
        resp = requests.get(get_url, headers=headers)
        self.assertEqual(resp.status_code, 200,
                         'Got %d %s' % (resp.status_code, resp.content))
        self.assertEqual(resp.content, '')

        # PUT over object
        resp = requests.put(put_url, data='foobar', headers=headers)
        self.assertEqual(resp.status_code, 200,
                         'Got %d %s' % (resp.status_code, resp.content))

        # GET non-empty object
        resp = requests.get(get_url, headers=headers)
        self.assertEqual(resp.status_code, 200,
                         'Got %d %s' % (resp.status_code, resp.content))
        self.assertEqual(resp.content, 'foobar')

        # DELETE Object
        delete_url, headers = self.conn.generate_url_and_headers(
            'DELETE', bucket, obj)
        resp = requests.delete(delete_url, headers=headers)
        self.assertEqual(resp.status_code, 204,
                         'Got %d %s' % (resp.status_code, resp.content))

        # Final cleanup
        status, _junk, _junk = self.conn.make_request('DELETE', bucket)
        self.assertEqual(status, 204)


class TestS3ApiPresignedUrlsSigV4(TestS3ApiPresignedUrls):
    @classmethod
    def setUpClass(cls):
        os.environ['S3_USE_SIGV4'] = "True"

    @classmethod
    def tearDownClass(cls):
        del os.environ['S3_USE_SIGV4']

    def setUp(self):
        super(TestS3ApiPresignedUrlsSigV4, self).setUp()
