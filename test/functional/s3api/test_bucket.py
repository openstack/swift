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

import botocore
import datetime
import unittest
import os
from unittest import SkipTest

import test.functional as tf
from swift.common.utils import config_true_value
from test.functional.s3api import S3ApiBaseBoto3
from test.functional.s3api.s3_test_client import get_boto3_conn
from test.functional.swift_test_client import Connection


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiBucket(S3ApiBaseBoto3):
    def _validate_object_listing(self, resp_objects, req_objects,
                                 expect_owner=True):
        self.assertEqual(len(resp_objects), len(req_objects))
        for i, obj in enumerate(resp_objects):
            self.assertEqual(obj['Key'], req_objects[i])
            self.assertIsInstance(obj['LastModified'], datetime.datetime)
            self.assertIn('ETag', obj)
            self.assertIn('Size', obj)
            self.assertEqual(obj['StorageClass'], 'STANDARD')
            if not expect_owner:
                self.assertNotIn('Owner', obj)
            elif tf.cluster_info['s3api'].get('s3_acl'):
                self.assertEqual(obj['Owner']['ID'], self.access_key)
                self.assertEqual(obj['Owner']['DisplayName'], self.access_key)
            else:
                self.assertIn('Owner', obj)
                self.assertIn('ID', obj['Owner'])
                self.assertIn('DisplayName', obj['Owner'])

    def test_bucket(self):
        bucket = 'bucket'
        max_bucket_listing = int(tf.cluster_info['s3api'].get(
            'max_bucket_listing', 1000))

        # PUT Bucket
        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        headers = resp['ResponseMetadata']['HTTPHeaders']

        self.assertCommonResponseHeaders(headers)
        self.assertIn(headers['location'], (
            '/' + bucket,  # swob won't touch it...
            # but webob (which we get because of auth_token) *does*
            '%s/%s' % (self.endpoint_url, bucket),
        ))
        self.assertEqual(headers['content-length'], '0')

        # GET Bucket(Without Object)
        resp = self.conn.list_objects(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        headers = resp['ResponseMetadata']['HTTPHeaders']

        self.assertCommonResponseHeaders(headers)
        self.assertIsNotNone(headers['content-type'])
        # TODO; requires consideration
        # self.assertEqual(headers['transfer-encoding'], 'chunked')

        self.assertEqual(resp['Name'], bucket)
        self.assertEqual(resp['Prefix'], '')
        self.assertEqual(resp['Marker'], '')
        self.assertEqual(resp['MaxKeys'], max_bucket_listing)
        self.assertFalse(resp['IsTruncated'])
        self.assertNotIn('Contents', bucket)

        # GET Bucket(With Object)
        req_objects = ['object', 'object2']
        for obj in req_objects:
            self.conn.put_object(Bucket=bucket, Key=obj, Body=b'')
        resp = self.conn.list_objects(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        self.assertEqual(resp['Name'], bucket)
        self.assertEqual(resp['Prefix'], '')
        self.assertEqual(resp['Marker'], '')
        self.assertEqual(resp['MaxKeys'], max_bucket_listing)
        self.assertFalse(resp['IsTruncated'])
        self._validate_object_listing(resp['Contents'], req_objects)

        # HEAD Bucket
        resp = self.conn.head_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        headers = resp['ResponseMetadata']['HTTPHeaders']

        self.assertCommonResponseHeaders(headers)
        self.assertIsNotNone(headers['content-type'])
        # TODO; requires consideration
        # self.assertEqual(headers['transfer-encoding'], 'chunked')

        # DELETE Bucket
        for obj in req_objects:
            self.conn.delete_object(Bucket=bucket, Key=obj)
        resp = self.conn.delete_bucket(Bucket=bucket)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        self.assertCommonResponseHeaders(
            resp['ResponseMetadata']['HTTPHeaders'])

    def test_bucket_listing_with_staticweb(self):
        if 'staticweb' not in tf.cluster_info:
            raise SkipTest('Staticweb not enabled')
        bucket = 'bucket'

        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        resp = self.conn.list_objects(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # enable staticweb listings; make publicly-readable
        conn = Connection(tf.config)
        conn.authenticate()
        post_status = conn.make_request('POST', [bucket], hdrs={
            'X-Container-Read': '.r:*,.rlistings',
            'X-Container-Meta-Web-Listings': 'true',
        })
        self.assertEqual(post_status, 204)

        resp = self.conn.list_objects(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

    def test_put_bucket_error(self):
        event_system = self.conn.meta.events
        event_system.unregister(
            'before-parameter-build.s3',
            botocore.handlers.validate_bucket_name)
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.create_bucket(Bucket='bucket+invalid')
        self.assertEqual(
            ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 400)
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'InvalidBucketName')

        auth_error_conn = get_boto3_conn(tf.config['s3_access_key'], 'invalid')
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            auth_error_conn.create_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 403)
        self.assertEqual(ctx.exception.response['Error']['Code'],
                         'SignatureDoesNotMatch')

        self.conn.create_bucket(Bucket='bucket')
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.create_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 409)
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'BucketAlreadyOwnedByYou')

    def test_put_bucket_error_key2(self):
        if config_true_value(tf.cluster_info['s3api'].get('s3_acl')):
            if 's3_access_key2' not in tf.config or \
                    's3_secret_key2' not in tf.config:
                raise SkipTest(
                    'Cannot test for BucketAlreadyExists with second user; '
                    'need s3_access_key2 and s3_secret_key2 configured')

            self.conn.create_bucket(Bucket='bucket')

            # Other users of the same account get the same 409 error
            conn2 = get_boto3_conn(tf.config['s3_access_key2'],
                                   tf.config['s3_secret_key2'])
            with self.assertRaises(botocore.exceptions.ClientError) as ctx:
                conn2.create_bucket(Bucket='bucket')
            self.assertEqual(
                ctx.exception.response['ResponseMetadata']['HTTPStatusCode'],
                409)
            self.assertEqual(
                ctx.exception.response['Error']['Code'], 'BucketAlreadyExists')

    def test_put_bucket_error_key3(self):
        if 's3_access_key3' not in tf.config or \
                's3_secret_key3' not in tf.config:
            raise SkipTest('Cannot test for AccessDenied; need '
                           's3_access_key3 and s3_secret_key3 configured')

        self.conn.create_bucket(Bucket='bucket')
        # If the user can't create buckets, they shouldn't even know
        # whether the bucket exists.
        conn3 = get_boto3_conn(tf.config['s3_access_key3'],
                               tf.config['s3_secret_key3'])
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            conn3.create_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 403)
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'AccessDenied')

    def test_put_bucket_with_LocationConstraint(self):
        resp = self.conn.create_bucket(
            Bucket='bucket',
            CreateBucketConfiguration={'LocationConstraint': self.region})
        self.assertEqual(resp['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_get_bucket_error(self):
        event_system = self.conn.meta.events
        event_system.unregister(
            'before-parameter-build.s3',
            botocore.handlers.validate_bucket_name)
        self.conn.create_bucket(Bucket='bucket')

        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.list_objects(Bucket='bucket+invalid')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'InvalidBucketName')

        auth_error_conn = get_boto3_conn(tf.config['s3_access_key'], 'invalid')
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            auth_error_conn.list_objects(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'SignatureDoesNotMatch')

        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.list_objects(Bucket='nothing')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'NoSuchBucket')

    def _prepare_test_get_bucket(self, bucket, objects):
        try:
            self.conn.create_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            err_code = e.response.get('Error', {}).get('Code')
            if err_code != 'BucketAlreadyOwnedByYou':
                raise

        for obj in objects:
            self.conn.put_object(Bucket=bucket, Key=obj, Body=b'')

    def test_blank_params(self):
        bucket = 'bucket'
        self._prepare_test_get_bucket(bucket, ())

        resp = self.conn.list_objects(
            Bucket=bucket, Delimiter='', Marker='', Prefix='')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('Delimiter', resp)
        self.assertIn('Marker', resp)
        self.assertEqual('', resp['Marker'])
        self.assertIn('Prefix', resp)
        self.assertEqual('', resp['Prefix'])

        resp = self.conn.list_objects_v2(
            Bucket=bucket, Delimiter='', StartAfter='', Prefix='')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('Delimiter', resp)
        self.assertIn('StartAfter', resp)
        self.assertEqual('', resp['StartAfter'])
        self.assertIn('Prefix', resp)
        self.assertEqual('', resp['Prefix'])

        resp = self.conn.list_object_versions(
            Bucket=bucket, Delimiter='', KeyMarker='', Prefix='')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertIn('Delimiter', resp)
        self.assertEqual('', resp['Delimiter'])
        self.assertIn('KeyMarker', resp)
        self.assertEqual('', resp['KeyMarker'])
        self.assertIn('Prefix', resp)
        self.assertEqual('', resp['Prefix'])

    def test_get_bucket_with_delimiter(self):
        bucket = 'bucket'
        put_objects = ('object', 'object2', 'subdir/object', 'subdir2/object',
                       'dir/subdir/object')
        self._prepare_test_get_bucket(bucket, put_objects)

        delimiter = '/'
        expect_objects = ('object', 'object2')
        expect_prefixes = ('dir/', 'subdir/', 'subdir2/')
        resp = self.conn.list_objects(Bucket=bucket, Delimiter=delimiter)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Delimiter'], delimiter)
        self._validate_object_listing(resp['Contents'], expect_objects)
        resp_prefixes = resp['CommonPrefixes']
        self.assertEqual(
            resp_prefixes,
            [{'Prefix': p} for p in expect_prefixes])

    def test_get_bucket_with_multi_char_delimiter(self):
        bucket = 'bucket'
        put_objects = ('object', 'object2', 'subdir/object', 'subdir2/object',
                       'dir/subdir/object')
        self._prepare_test_get_bucket(bucket, put_objects)

        delimiter = '/obj'
        expect_objects = ('object', 'object2')
        expect_prefixes = ('dir/subdir/obj', 'subdir/obj', 'subdir2/obj')
        resp = self.conn.list_objects(Bucket=bucket, Delimiter=delimiter)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Delimiter'], delimiter)
        self._validate_object_listing(resp['Contents'], expect_objects)
        resp_prefixes = resp['CommonPrefixes']
        self.assertEqual(
            resp_prefixes,
            [{'Prefix': p} for p in expect_prefixes])

    def test_get_bucket_with_non_ascii_delimiter(self):
        bucket = 'bucket'
        put_objects = (
            'bar',
            'foo',
            u'foobar\N{SNOWMAN}baz',
            u'foo\N{SNOWMAN}bar',
            u'foo\N{SNOWMAN}bar\N{SNOWMAN}baz',
        )
        self._prepare_test_get_bucket(bucket, put_objects)
        # boto3 doesn't always unquote everything it should; see
        # https://github.com/boto/botocore/pull/1901
        # Fortunately, we can just drop the encoding-type=url param
        self.conn.meta.events.unregister(
            'before-parameter-build.s3.ListObjects',
            botocore.handlers.set_list_objects_encoding_type_url)

        delimiter = u'\N{SNOWMAN}'
        expect_objects = ('bar', 'foo')
        expect_prefixes = (u'foobar\N{SNOWMAN}', u'foo\N{SNOWMAN}')
        resp = self.conn.list_objects(Bucket=bucket, Delimiter=delimiter)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Delimiter'], delimiter)
        self._validate_object_listing(resp['Contents'], expect_objects)
        resp_prefixes = resp['CommonPrefixes']
        self.assertEqual(
            resp_prefixes,
            [{'Prefix': p} for p in expect_prefixes])

        prefix = u'foo\N{SNOWMAN}'
        expect_objects = (u'foo\N{SNOWMAN}bar',)
        expect_prefixes = (u'foo\N{SNOWMAN}bar\N{SNOWMAN}',)
        resp = self.conn.list_objects(
            Bucket=bucket, Delimiter=delimiter, Prefix=prefix)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Delimiter'], delimiter)
        self.assertEqual(resp['Prefix'], prefix)
        self._validate_object_listing(resp['Contents'], expect_objects)
        resp_prefixes = resp['CommonPrefixes']
        self.assertEqual(
            resp_prefixes,
            [{'Prefix': p} for p in expect_prefixes])

    def test_get_bucket_with_encoding_type(self):
        bucket = 'bucket'
        put_objects = ('object', 'object2')
        self._prepare_test_get_bucket(bucket, put_objects)

        encoding_type = 'url'
        resp = self.conn.list_objects(
            Bucket=bucket, EncodingType=encoding_type)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['EncodingType'], encoding_type)

    def test_get_bucket_with_marker(self):
        bucket = 'bucket'
        put_objects = ('object', 'object2', 'subdir/object', 'subdir2/object',
                       'dir/subdir/object')
        self._prepare_test_get_bucket(bucket, put_objects)

        marker = 'object'
        expect_objects = ('object2', 'subdir/object', 'subdir2/object')
        resp = self.conn.list_objects(Bucket=bucket, Marker=marker)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Marker'], marker)
        self._validate_object_listing(resp['Contents'], expect_objects)

    def test_get_bucket_with_max_keys(self):
        bucket = 'bucket'
        put_objects = ('object', 'object2', 'subdir/object', 'subdir2/object',
                       'dir/subdir/object')
        self._prepare_test_get_bucket(bucket, put_objects)

        max_keys = 2
        expect_objects = ('dir/subdir/object', 'object')
        resp = self.conn.list_objects(Bucket=bucket, MaxKeys=max_keys)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['MaxKeys'], max_keys)
        self._validate_object_listing(resp['Contents'], expect_objects)

    def test_get_bucket_with_prefix(self):
        bucket = 'bucket'
        req_objects = ('object', 'object2', 'subdir/object', 'subdir2/object',
                       'dir/subdir/object')
        self._prepare_test_get_bucket(bucket, req_objects)

        prefix = 'object'
        expect_objects = ('object', 'object2')
        resp = self.conn.list_objects(Bucket=bucket, Prefix=prefix)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Prefix'], prefix)
        self._validate_object_listing(resp['Contents'], expect_objects)

    def test_get_bucket_v2_with_start_after(self):
        bucket = 'bucket'
        put_objects = ('object', 'object2', 'subdir/object', 'subdir2/object',
                       'dir/subdir/object')
        self._prepare_test_get_bucket(bucket, put_objects)

        marker = 'object'
        expect_objects = ('object2', 'subdir/object', 'subdir2/object')
        resp = self.conn.list_objects_v2(Bucket=bucket, StartAfter=marker)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['StartAfter'], marker)
        self.assertEqual(resp['KeyCount'], 3)
        self._validate_object_listing(resp['Contents'], expect_objects,
                                      expect_owner=False)

    def test_get_bucket_v2_with_fetch_owner(self):
        bucket = 'bucket'
        put_objects = ('object', 'object2', 'subdir/object', 'subdir2/object',
                       'dir/subdir/object')
        self._prepare_test_get_bucket(bucket, put_objects)

        expect_objects = ('dir/subdir/object', 'object', 'object2',
                          'subdir/object', 'subdir2/object')
        resp = self.conn.list_objects_v2(Bucket=bucket, FetchOwner=True)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['KeyCount'], 5)
        self._validate_object_listing(resp['Contents'], expect_objects)

    def test_get_bucket_v2_with_continuation_token_and_delimiter(self):
        bucket = 'bucket'
        put_objects = ('object', u'object2-\u062a', 'subdir/object',
                       u'subdir2-\u062a/object', 'dir/subdir/object',
                       'x', 'y', 'z')
        self._prepare_test_get_bucket(bucket, put_objects)

        expected = [{'objects': ['object', u'object2-\u062a'],
                     'subdirs': ['dir/']},
                    {'objects': ['x'],
                     'subdirs': ['subdir/', u'subdir2-\u062a/']},
                    {'objects': ['y', 'z'],
                     'subdirs': []}]

        continuation_token = ''

        for i in range(len(expected)):
            resp = self.conn.list_objects_v2(
                Bucket=bucket,
                MaxKeys=3,
                Delimiter='/',
                ContinuationToken=continuation_token)
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
            self.assertEqual(resp['MaxKeys'], 3)
            self.assertEqual(
                resp['KeyCount'],
                len(expected[i]['objects']) + len(expected[i]['subdirs']))
            expect_truncated = i < len(expected) - 1
            self.assertEqual(resp['IsTruncated'], expect_truncated)
            if expect_truncated:
                self.assertIsNotNone(resp['NextContinuationToken'])
                continuation_token = resp['NextContinuationToken']
            self._validate_object_listing(resp['Contents'],
                                          expected[i]['objects'],
                                          expect_owner=False)
            resp_subdirs = resp.get('CommonPrefixes', [])
            self.assertEqual(
                resp_subdirs,
                [{'Prefix': p} for p in expected[i]['subdirs']])

    def test_head_bucket_error(self):
        event_system = self.conn.meta.events
        event_system.unregister(
            'before-parameter-build.s3',
            botocore.handlers.validate_bucket_name)

        self.conn.create_bucket(Bucket='bucket')

        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.head_bucket(Bucket='bucket+invalid')
        self.assertEqual(
            ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 400)
        self.assertEqual(ctx.exception.response['Error']['Code'], '400')
        self.assertEqual(
            ctx.exception.response[
                'ResponseMetadata']['HTTPHeaders']['content-length'], '0')

        auth_error_conn = get_boto3_conn(tf.config['s3_access_key'], 'invalid')
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            auth_error_conn.head_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 403)
        self.assertEqual(
            ctx.exception.response['Error']['Code'], '403')
        self.assertEqual(
            ctx.exception.response[
                'ResponseMetadata']['HTTPHeaders']['content-length'], '0')

        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.head_bucket(Bucket='nothing')
        self.assertEqual(
            ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 404)
        self.assertEqual(
            ctx.exception.response['Error']['Code'], '404')
        self.assertEqual(
            ctx.exception.response[
                'ResponseMetadata']['HTTPHeaders']['content-length'], '0')

    def test_delete_bucket_error(self):
        event_system = self.conn.meta.events
        event_system.unregister(
            'before-parameter-build.s3',
            botocore.handlers.validate_bucket_name)
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.delete_bucket(Bucket='bucket+invalid')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'InvalidBucketName')

        auth_error_conn = get_boto3_conn(tf.config['s3_access_key'], 'invalid')
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            auth_error_conn.delete_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'SignatureDoesNotMatch')

        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.delete_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'NoSuchBucket')

    def test_bucket_invalid_method_error(self):
        def _mangle_req_method(request, **kwargs):
            request.method = 'GETPUT'

        def _mangle_req_controller_method(request, **kwargs):
            request.method = '_delete_segments_bucket'

        event_system = self.conn.meta.events
        event_system.register(
            'request-created.s3.CreateBucket',
            _mangle_req_method)
        # non existed verb in the controller
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.create_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'MethodNotAllowed')

        event_system.unregister('request-created.s3.CreateBucket',
                                _mangle_req_method)
        event_system.register('request-created.s3.CreateBucket',
                              _mangle_req_controller_method)

        try:
            import awscrt  # noqa: F401
        except ImportError:
            raise SkipTest('lower-case request methods require awscrt for '
                           'proper signing (try `pip install awscrt`)')
        # the method exists in the controller but deny as MethodNotAllowed
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.conn.create_bucket(Bucket='bucket')
        self.assertEqual(
            ctx.exception.response['Error']['Code'], 'MethodNotAllowed')

    def test_bucket_get_object_lock_configuration(self):
        bucket = 'bucket'

        # PUT Bucket
        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        headers = resp['ResponseMetadata']['HTTPHeaders']
        self.assertCommonResponseHeaders(headers)

        # now attempt to get object_lock_configuration from new bucket.
        with self.assertRaises(botocore.exceptions.ClientError) as ce:
            self.conn.get_object_lock_configuration(
                Bucket=bucket)
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

    def test_bucket_put_object_lock_configuration(self):
        bucket = 'bucket'

        # PUT Bucket
        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        headers = resp['ResponseMetadata']['HTTPHeaders']
        self.assertCommonResponseHeaders(headers)

        # now attempt to get object_lock_configuration from new bucket.
        with self.assertRaises(botocore.exceptions.ClientError) as ce:
            self.conn.put_object_lock_configuration(
                Bucket=bucket, ObjectLockConfiguration={})

        self.assertEqual(
            ce.exception.response['ResponseMetadata']['HTTPStatusCode'],
            501)
        self.assertEqual(
            ce.exception.response['Error']['Code'],
            'NotImplemented')

        self.assertEqual(str(ce.exception),
                         'An error occurred (NotImplemented) when calling '
                         'the PutObjectLockConfiguration operation: The '
                         'requested resource is not implemented')


class TestS3ApiBucketSigV4(TestS3ApiBucket):
    @classmethod
    def setUpClass(cls):
        os.environ['S3_USE_SIGV4'] = "True"

    @classmethod
    def tearDownClass(cls):
        del os.environ['S3_USE_SIGV4']

    def setUp(self):
        super(TestS3ApiBucket, self).setUp()


if __name__ == '__main__':
    unittest.main()
