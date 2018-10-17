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

import unittest2
import os
import boto

# For an issue with venv and distutils, disable pylint message here
# pylint: disable-msg=E0611,F0401
from distutils.version import StrictVersion

import email.parser
from email.utils import formatdate, parsedate
from time import mktime
from hashlib import md5
from urllib import quote

import test.functional as tf

from swift.common.middleware.s3api.etree import fromstring

from test.functional.s3api import S3ApiBase
from test.functional.s3api.s3_test_client import Connection
from test.functional.s3api.utils import get_error_code, calculate_md5

DAY = 86400.0  # 60 * 60 * 24 (sec)


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiObject(S3ApiBase):
    def setUp(self):
        super(TestS3ApiObject, self).setUp()
        self.bucket = 'bucket'
        self.conn.make_request('PUT', self.bucket)

    def _assertObjectEtag(self, bucket, obj, etag):
        status, headers, _ = self.conn.make_request('HEAD', bucket, obj)
        self.assertEqual(status, 200)  # sanity
        self.assertCommonResponseHeaders(headers, etag)

    def test_object(self):
        obj = 'object name with %-sign'
        content = 'abc123'
        etag = md5(content).hexdigest()

        # PUT Object
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, body=content)
        self.assertEqual(status, 200)

        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-length' in headers)  # sanity
        self.assertEqual(headers['content-length'], '0')
        self._assertObjectEtag(self.bucket, obj, etag)

        # PUT Object Copy
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_obj'
        self.conn.make_request('PUT', dst_bucket)
        headers = {'x-amz-copy-source': '/%s/%s' % (self.bucket, obj)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj,
                                   headers=headers)
        self.assertEqual(status, 200)

        # PUT Object Copy with URL-encoded Source
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_obj'
        self.conn.make_request('PUT', dst_bucket)
        headers = {'x-amz-copy-source': quote('/%s/%s' % (self.bucket, obj))}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj,
                                   headers=headers)
        self.assertEqual(status, 200)

        self.assertCommonResponseHeaders(headers)
        self.assertEqual(headers['content-length'], str(len(body)))

        elem = fromstring(body, 'CopyObjectResult')
        self.assertTrue(elem.find('LastModified').text is not None)
        last_modified_xml = elem.find('LastModified').text
        self.assertTrue(elem.find('ETag').text is not None)
        self.assertEqual(etag, elem.find('ETag').text.strip('"'))
        self._assertObjectEtag(dst_bucket, dst_obj, etag)

        # Check timestamp on Copy:
        status, headers, body = \
            self.conn.make_request('GET', dst_bucket)
        self.assertEqual(status, 200)
        elem = fromstring(body, 'ListBucketResult')

        # FIXME: COPY result drops milli/microseconds but GET doesn't
        self.assertEqual(
            elem.find('Contents').find("LastModified").text.rsplit('.', 1)[0],
            last_modified_xml.rsplit('.', 1)[0])

        # GET Object
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj)
        self.assertEqual(status, 200)

        self.assertCommonResponseHeaders(headers, etag)
        self.assertTrue(headers['last-modified'] is not None)
        self.assertTrue(headers['content-type'] is not None)
        self.assertEqual(headers['content-length'], str(len(content)))

        # HEAD Object
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj)
        self.assertEqual(status, 200)

        self.assertCommonResponseHeaders(headers, etag)
        self.assertTrue(headers['last-modified'] is not None)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-length'], str(len(content)))

        # DELETE Object
        status, headers, body = \
            self.conn.make_request('DELETE', self.bucket, obj)
        self.assertEqual(status, 204)
        self.assertCommonResponseHeaders(headers)

        # DELETE Non-Existent Object
        status, headers, body = \
            self.conn.make_request('DELETE', self.bucket, 'does-not-exist')
        self.assertEqual(status, 204)
        self.assertCommonResponseHeaders(headers)

    def test_put_object_error(self):
        auth_error_conn = Connection(aws_secret_key='invalid')
        status, headers, body = \
            auth_error_conn.make_request('PUT', self.bucket, 'object')
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')
        self.assertEqual(headers['content-type'], 'application/xml')

        status, headers, body = \
            self.conn.make_request('PUT', 'bucket2', 'object')
        self.assertEqual(get_error_code(body), 'NoSuchBucket')
        self.assertEqual(headers['content-type'], 'application/xml')

    def test_put_object_copy_error(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)
        dst_bucket = 'dst-bucket'
        self.conn.make_request('PUT', dst_bucket)
        dst_obj = 'dst_object'

        headers = {'x-amz-copy-source': '/%s/%s' % (self.bucket, obj)}
        auth_error_conn = Connection(aws_secret_key='invalid')
        status, headers, body = \
            auth_error_conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')
        self.assertEqual(headers['content-type'], 'application/xml')

        # /src/nothing -> /dst/dst
        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, 'nothing')}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(get_error_code(body), 'NoSuchKey')
        self.assertEqual(headers['content-type'], 'application/xml')

        # /nothing/src -> /dst/dst
        headers = {'X-Amz-Copy-Source': '/%s/%s' % ('nothing', obj)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        # TODO: source bucket is not check.
        # self.assertEqual(get_error_code(body), 'NoSuchBucket')

        # /src/src -> /nothing/dst
        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj)}
        status, headers, body = \
            self.conn.make_request('PUT', 'nothing', dst_obj, headers)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')
        self.assertEqual(headers['content-type'], 'application/xml')

    def test_get_object_error(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        auth_error_conn = Connection(aws_secret_key='invalid')
        status, headers, body = \
            auth_error_conn.make_request('GET', self.bucket, obj)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')
        self.assertEqual(headers['content-type'], 'application/xml')

        status, headers, body = \
            self.conn.make_request('GET', self.bucket, 'invalid')
        self.assertEqual(get_error_code(body), 'NoSuchKey')
        self.assertEqual(headers['content-type'], 'application/xml')

        status, headers, body = self.conn.make_request('GET', 'invalid', obj)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')
        self.assertEqual(headers['content-type'], 'application/xml')

    def test_head_object_error(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        auth_error_conn = Connection(aws_secret_key='invalid')
        status, headers, body = \
            auth_error_conn.make_request('HEAD', self.bucket, obj)
        self.assertEqual(status, 403)
        self.assertEqual(body, '')  # sanity
        self.assertEqual(headers['content-type'], 'application/xml')

        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, 'invalid')
        self.assertEqual(status, 404)
        self.assertEqual(body, '')  # sanity
        self.assertEqual(headers['content-type'], 'application/xml')

        status, headers, body = \
            self.conn.make_request('HEAD', 'invalid', obj)
        self.assertEqual(status, 404)
        self.assertEqual(body, '')  # sanity
        self.assertEqual(headers['content-type'], 'application/xml')

    def test_delete_object_error(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        auth_error_conn = Connection(aws_secret_key='invalid')
        status, headers, body = \
            auth_error_conn.make_request('DELETE', self.bucket, obj)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')
        self.assertEqual(headers['content-type'], 'application/xml')

        status, headers, body = \
            self.conn.make_request('DELETE', 'invalid', obj)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')
        self.assertEqual(headers['content-type'], 'application/xml')

    def test_put_object_content_encoding(self):
        obj = 'object'
        etag = md5().hexdigest()
        headers = {'Content-Encoding': 'gzip'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers)
        self.assertEqual(status, 200)
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj)
        self.assertTrue('content-encoding' in headers)  # sanity
        self.assertEqual(headers['content-encoding'], 'gzip')
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_content_md5(self):
        obj = 'object'
        content = 'abcdefghij'
        etag = md5(content).hexdigest()
        headers = {'Content-MD5': calculate_md5(content)}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_content_type(self):
        obj = 'object'
        content = 'abcdefghij'
        etag = md5(content).hexdigest()
        headers = {'Content-Type': 'text/plain'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 200)
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj)
        self.assertEqual(headers['content-type'], 'text/plain')
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_conditional_requests(self):
        obj = 'object'
        content = 'abcdefghij'
        headers = {'If-None-Match': '*'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 501)

        headers = {'If-Match': '*'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 501)

        headers = {'If-Modified-Since': 'Sat, 27 Jun 2015 00:00:00 GMT'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 501)

        headers = {'If-Unmodified-Since': 'Sat, 27 Jun 2015 00:00:00 GMT'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 501)

        # None of the above should actually have created an object
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, {}, '')
        self.assertEqual(status, 404)

    def test_put_object_expect(self):
        obj = 'object'
        content = 'abcdefghij'
        etag = md5(content).hexdigest()
        headers = {'Expect': '100-continue'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def _test_put_object_headers(self, req_headers, expected_headers=None):
        if expected_headers is None:
            expected_headers = req_headers
        obj = 'object'
        content = 'abcdefghij'
        etag = md5(content).hexdigest()
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj,
                                   req_headers, content)
        self.assertEqual(status, 200)
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj)
        for header, value in expected_headers.items():
            self.assertIn(header.lower(), headers)
            self.assertEqual(headers[header.lower()], value)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_metadata(self):
        self._test_put_object_headers({
            'X-Amz-Meta-Bar': 'foo',
            'X-Amz-Meta-Bar2': 'foo2'})

    def test_put_object_weird_metadata(self):
        req_headers = dict(
            ('x-amz-meta-' + c, c)
            for c in '!"#$%&\'()*+-./<=>?@[\\]^`{|}~')
        exp_headers = dict(
            ('x-amz-meta-' + c, c)
            for c in '!#$%&\'(*+-.^`|~')
        self._test_put_object_headers(req_headers, exp_headers)

    def test_put_object_underscore_in_metadata(self):
        # Break this out separately for ease of testing pre-0.19.0 eventlet
        self._test_put_object_headers({
            'X-Amz-Meta-Foo-Bar': 'baz',
            'X-Amz-Meta-Foo_Bar': 'also baz'})

    def test_put_object_content_headers(self):
        self._test_put_object_headers({
            'Content-Type': 'foo/bar',
            'Content-Encoding': 'baz',
            'Content-Disposition': 'attachment',
            'Content-Language': 'en'})

    def test_put_object_cache_control(self):
        self._test_put_object_headers({
            'Cache-Control': 'private, some-extension'})

    def test_put_object_expires(self):
        self._test_put_object_headers({
            # We don't validate that the Expires header is a valid date
            'Expires': 'a valid HTTP-date timestamp'})

    def test_put_object_robots_tag(self):
        self._test_put_object_headers({
            'X-Robots-Tag': 'googlebot: noarchive'})

    def test_put_object_storage_class(self):
        obj = 'object'
        content = 'abcdefghij'
        etag = md5(content).hexdigest()
        headers = {'X-Amz-Storage-Class': 'STANDARD'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers, content)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_copy_source_params(self):
        obj = 'object'
        src_headers = {'X-Amz-Meta-Test': 'src'}
        src_body = 'some content'
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_object'
        self.conn.make_request('PUT', self.bucket, obj, src_headers, src_body)
        self.conn.make_request('PUT', dst_bucket)

        headers = {'X-Amz-Copy-Source': '/%s/%s?nonsense' % (
            self.bucket, obj)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 400)
        self.assertEqual(get_error_code(body), 'InvalidArgument')

        headers = {'X-Amz-Copy-Source': '/%s/%s?versionId=null&nonsense' % (
            self.bucket, obj)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 400)
        self.assertEqual(get_error_code(body), 'InvalidArgument')

        headers = {'X-Amz-Copy-Source': '/%s/%s?versionId=null' % (
            self.bucket, obj)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        status, headers, body = \
            self.conn.make_request('GET', dst_bucket, dst_obj)
        self.assertEqual(status, 200)
        self.assertEqual(headers['x-amz-meta-test'], 'src')
        self.assertEqual(body, src_body)

    def test_put_object_copy_source(self):
        obj = 'object'
        content = 'abcdefghij'
        etag = md5(content).hexdigest()
        self.conn.make_request('PUT', self.bucket, obj, body=content)

        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_object'
        self.conn.make_request('PUT', dst_bucket)

        # /src/src -> /dst/dst
        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(dst_bucket, dst_obj, etag)

        # /src/src -> /src/dst
        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj)}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, dst_obj, headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, dst_obj, etag)

        # /src/src -> /src/src
        # need changes to copy itself (e.g. metadata)
        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Meta-Foo': 'bar',
                   'X-Amz-Metadata-Directive': 'REPLACE'}
        status, headers, body = \
            self.conn.make_request('PUT', self.bucket, obj, headers)
        self.assertEqual(status, 200)
        self._assertObjectEtag(self.bucket, obj, etag)
        self.assertCommonResponseHeaders(headers)

    def test_put_object_copy_metadata_directive(self):
        obj = 'object'
        src_headers = {'X-Amz-Meta-Test': 'src'}
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_object'
        self.conn.make_request('PUT', self.bucket, obj, headers=src_headers)
        self.conn.make_request('PUT', dst_bucket)

        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Metadata-Directive': 'REPLACE',
                   'X-Amz-Meta-Test': 'dst'}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        status, headers, body = \
            self.conn.make_request('HEAD', dst_bucket, dst_obj)
        self.assertEqual(headers['x-amz-meta-test'], 'dst')

        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Metadata-Directive': 'COPY',
                   'X-Amz-Meta-Test': 'dst'}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        status, headers, body = \
            self.conn.make_request('HEAD', dst_bucket, dst_obj)
        self.assertEqual(headers['x-amz-meta-test'], 'src')

        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Meta-Test2': 'dst',
                   'X-Amz-Metadata-Directive': 'REPLACE'}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        status, headers, body = \
            self.conn.make_request('HEAD', dst_bucket, dst_obj)
        self.assertNotIn('x-amz-meta-test', headers)
        self.assertEqual(headers['x-amz-meta-test2'], 'dst')

        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Metadata-Directive': 'BAD'}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers)
        self.assertEqual(status, 400)

    def test_put_object_copy_source_if_modified_since(self):
        obj = 'object'
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_object'
        etag = md5().hexdigest()
        self.conn.make_request('PUT', self.bucket, obj)
        self.conn.make_request('PUT', dst_bucket)

        _, headers, _ = self.conn.make_request('HEAD', self.bucket, obj)
        src_datetime = mktime(parsedate(headers['last-modified']))
        src_datetime = src_datetime - DAY
        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Copy-Source-If-Modified-Since':
                   formatdate(src_datetime)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_copy_source_if_unmodified_since(self):
        obj = 'object'
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_object'
        etag = md5().hexdigest()
        self.conn.make_request('PUT', self.bucket, obj)
        self.conn.make_request('PUT', dst_bucket)

        _, headers, _ = self.conn.make_request('HEAD', self.bucket, obj)
        src_datetime = mktime(parsedate(headers['last-modified']))
        src_datetime = src_datetime + DAY
        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Copy-Source-If-Unmodified-Since':
                   formatdate(src_datetime)}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_copy_source_if_match(self):
        obj = 'object'
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_object'
        etag = md5().hexdigest()
        self.conn.make_request('PUT', self.bucket, obj)
        self.conn.make_request('PUT', dst_bucket)

        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj)

        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Copy-Source-If-Match': etag}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_put_object_copy_source_if_none_match(self):
        obj = 'object'
        dst_bucket = 'dst-bucket'
        dst_obj = 'dst_object'
        etag = md5().hexdigest()
        self.conn.make_request('PUT', self.bucket, obj)
        self.conn.make_request('PUT', dst_bucket)

        headers = {'X-Amz-Copy-Source': '/%s/%s' % (self.bucket, obj),
                   'X-Amz-Copy-Source-If-None-Match': 'none-match'}
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self._assertObjectEtag(self.bucket, obj, etag)

    def test_get_object_response_content_type(self):
        obj = 'obj'
        self.conn.make_request('PUT', self.bucket, obj)

        query = 'response-content-type=text/plain'
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertEqual(headers['content-type'], 'text/plain')

    def test_get_object_response_content_language(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        query = 'response-content-language=en'
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertEqual(headers['content-language'], 'en')

    def test_get_object_response_cache_control(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        query = 'response-cache-control=private'
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertEqual(headers['cache-control'], 'private')

    def test_get_object_response_content_disposition(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        query = 'response-content-disposition=inline'
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertEqual(headers['content-disposition'], 'inline')

    def test_get_object_response_content_encoding(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        query = 'response-content-encoding=gzip'
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertEqual(headers['content-encoding'], 'gzip')

    def test_get_object_range(self):
        obj = 'object'
        content = 'abcdefghij'
        headers = {'x-amz-meta-test': 'swift'}
        self.conn.make_request(
            'PUT', self.bucket, obj, headers=headers, body=content)

        headers = {'Range': 'bytes=1-5'}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 206)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('x-amz-meta-test' in headers)
        self.assertEqual('swift', headers['x-amz-meta-test'])
        self.assertEqual(body, 'bcdef')

        headers = {'Range': 'bytes=5-'}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 206)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('x-amz-meta-test' in headers)
        self.assertEqual('swift', headers['x-amz-meta-test'])
        self.assertEqual(body, 'fghij')

        headers = {'Range': 'bytes=-5'}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 206)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('x-amz-meta-test' in headers)
        self.assertEqual('swift', headers['x-amz-meta-test'])
        self.assertEqual(body, 'fghij')

        ranges = ['1-2', '4-5']

        headers = {'Range': 'bytes=%s' % ','.join(ranges)}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 206)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-length' in headers)

        self.assertTrue('content-type' in headers)  # sanity
        content_type, boundary = headers['content-type'].split(';')

        self.assertEqual('multipart/byteranges', content_type)
        self.assertTrue(boundary.startswith('boundary='))  # sanity
        boundary_str = boundary[len('boundary='):]

        # TODO: Using swift.common.utils.multipart_byteranges_to_document_iters
        #       could be easy enough.
        parser = email.parser.FeedParser()
        parser.feed(
            "Content-Type: multipart/byterange; boundary=%s\r\n\r\n" %
            boundary_str)
        parser.feed(body)
        message = parser.close()

        self.assertTrue(message.is_multipart())  # sanity check
        mime_parts = message.get_payload()
        self.assertEqual(len(mime_parts), len(ranges))  # sanity

        for index, range_value in enumerate(ranges):
            start, end = map(int, range_value.split('-'))
            # go to next section and check sanity
            self.assertTrue(mime_parts[index])

            part = mime_parts[index]
            self.assertEqual(
                'application/octet-stream', part.get_content_type())
            expected_range = 'bytes %s/%s' % (range_value, len(content))
            self.assertEqual(
                expected_range, part.get('Content-Range'))
            # rest
            payload = part.get_payload().strip()
            self.assertEqual(content[start:end + 1], payload)

    def test_get_object_if_modified_since(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        _, headers, _ = self.conn.make_request('HEAD', self.bucket, obj)
        src_datetime = mktime(parsedate(headers['last-modified']))
        src_datetime = src_datetime - DAY
        headers = {'If-Modified-Since': formatdate(src_datetime)}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

    def test_get_object_if_unmodified_since(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        _, headers, _ = self.conn.make_request('HEAD', self.bucket, obj)
        src_datetime = mktime(parsedate(headers['last-modified']))
        src_datetime = src_datetime + DAY
        headers = \
            {'If-Unmodified-Since': formatdate(src_datetime)}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

    def test_get_object_if_match(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj)
        etag = headers['etag']

        headers = {'If-Match': etag}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

    def test_get_object_if_none_match(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        headers = {'If-None-Match': 'none-match'}
        status, headers, body = \
            self.conn.make_request('GET', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

    def test_head_object_range(self):
        obj = 'object'
        content = 'abcdefghij'
        self.conn.make_request('PUT', self.bucket, obj, body=content)

        headers = {'Range': 'bytes=1-5'}
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, headers=headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertCommonResponseHeaders(headers)

        headers = {'Range': 'bytes=5-'}
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, headers=headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertCommonResponseHeaders(headers)

        headers = {'Range': 'bytes=-5'}
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, headers=headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertCommonResponseHeaders(headers)

    def test_head_object_if_modified_since(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        _, headers, _ = self.conn.make_request('HEAD', self.bucket, obj)
        dt = mktime(parsedate(headers['last-modified']))
        dt = dt - DAY

        headers = {'If-Modified-Since': formatdate(dt)}
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

    def test_head_object_if_unmodified_since(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        _, headers, _ = self.conn.make_request('HEAD', self.bucket, obj)
        dt = mktime(parsedate(headers['last-modified']))
        dt = dt + DAY

        headers = {'If-Unmodified-Since': formatdate(dt)}
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

    def test_head_object_if_match(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj)
        etag = headers['etag']

        headers = {'If-Match': etag}
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

    def test_head_object_if_none_match(self):
        obj = 'object'
        self.conn.make_request('PUT', self.bucket, obj)

        headers = {'If-None-Match': 'none-match'}
        status, headers, body = \
            self.conn.make_request('HEAD', self.bucket, obj, headers=headers)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)


class TestS3ApiObjectSigV4(TestS3ApiObject):
    @classmethod
    def setUpClass(cls):
        os.environ['S3_USE_SIGV4'] = "True"

    @classmethod
    def tearDownClass(cls):
        del os.environ['S3_USE_SIGV4']

    def setUp(self):
        super(TestS3ApiObjectSigV4, self).setUp()

    @unittest2.skipIf(StrictVersion(boto.__version__) < StrictVersion('3.0'),
                      'This stuff got the signing issue of boto<=2.x')
    def test_put_object_metadata(self):
        super(TestS3ApiObjectSigV4, self).test_put_object_metadata()

    @unittest2.skipIf(StrictVersion(boto.__version__) < StrictVersion('3.0'),
                      'This stuff got the signing issue of boto<=2.x')
    def test_put_object_copy_source_if_modified_since(self):
        super(TestS3ApiObjectSigV4, self).\
            test_put_object_copy_source_if_modified_since()

    @unittest2.skipIf(StrictVersion(boto.__version__) < StrictVersion('3.0'),
                      'This stuff got the signing issue of boto<=2.x')
    def test_put_object_copy_source_if_unmodified_since(self):
        super(TestS3ApiObjectSigV4, self).\
            test_put_object_copy_source_if_unmodified_since()

    @unittest2.skipIf(StrictVersion(boto.__version__) < StrictVersion('3.0'),
                      'This stuff got the signing issue of boto<=2.x')
    def test_put_object_copy_source_if_match(self):
        super(TestS3ApiObjectSigV4,
              self).test_put_object_copy_source_if_match()

    @unittest2.skipIf(StrictVersion(boto.__version__) < StrictVersion('3.0'),
                      'This stuff got the signing issue of boto<=2.x')
    def test_put_object_copy_source_if_none_match(self):
        super(TestS3ApiObjectSigV4,
              self).test_put_object_copy_source_if_none_match()


if __name__ == '__main__':
    unittest2.main()
