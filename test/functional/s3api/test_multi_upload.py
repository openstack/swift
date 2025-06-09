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

import base64
import binascii
import unittest

import urllib.parse
from itertools import zip_longest

import test.functional as tf
from swift.common.middleware.s3api.etree import fromstring, tostring, \
    Element, SubElement
from swift.common.middleware.s3api.utils import mktime, S3Timestamp
from swift.common.utils import md5
from swift.common import swob

from test.functional.s3api import S3ApiBase, SigV4Mixin, \
    skip_boto2_sort_header_bug
from test.functional.s3api.s3_test_client import Connection
from test.functional.s3api.utils import get_error_code, get_error_msg, \
    calculate_md5
from test.functional.swift_test_client import Connection as SwiftConnection


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiMultiUpload(S3ApiBase):
    def setUp(self):
        super(TestS3ApiMultiUpload, self).setUp()
        if not tf.cluster_info['s3api'].get('allow_multipart_uploads', False):
            self.skipTest('multipart upload is not enebled')

        self.min_segment_size = int(tf.cluster_info['s3api'].get(
            'min_segment_size', 5242880))

    def _gen_comp_xml(self, etags, step=1):
        elem = Element('CompleteMultipartUpload')
        for i, etag in enumerate(etags):
            elem_part = SubElement(elem, 'Part')
            SubElement(elem_part, 'PartNumber').text = str(i * step + 1)
            SubElement(elem_part, 'ETag').text = etag
        return tostring(elem)

    def _initiate_multi_uploads_result_generator(self, bucket, keys,
                                                 headers=None, trials=1):
        if headers is None:
            headers = [None] * len(keys)
        self.conn.make_request('PUT', bucket)
        query = 'uploads'
        for key, key_headers in zip_longest(keys, headers):
            for i in range(trials):
                status, resp_headers, body = \
                    self.conn.make_request('POST', bucket, key,
                                           headers=key_headers, query=query)
                yield status, resp_headers, body

    def _upload_part(self, bucket, key, upload_id, content=None, part_num=1):
        query = 'partNumber=%s&uploadId=%s' % (part_num, upload_id)
        content = content if content else b'a' * self.min_segment_size
        with self.quiet_boto_logging():
            status, headers, body = self.conn.make_request(
                'PUT', bucket, key, body=content, query=query)
        return status, headers, body

    def _upload_part_copy(self, src_bucket, src_obj, dst_bucket, dst_key,
                          upload_id, part_num=1, src_range=None,
                          src_version_id=None):

        src_path = '%s/%s' % (src_bucket, src_obj)
        if src_version_id:
            src_path += '?versionId=%s' % src_version_id
        query = 'partNumber=%s&uploadId=%s' % (part_num, upload_id)
        req_headers = {'X-Amz-Copy-Source': src_path}
        if src_range:
            req_headers['X-Amz-Copy-Source-Range'] = src_range
        status, headers, body = \
            self.conn.make_request('PUT', dst_bucket, dst_key,
                                   headers=req_headers,
                                   query=query)
        elem = fromstring(body, 'CopyPartResult')
        etag = elem.find('ETag').text.strip('"')
        return status, headers, body, etag

    def _complete_multi_upload(self, bucket, key, upload_id, xml):
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        return status, headers, body

    def test_object_multi_upload(self):
        bucket = 'bucket'
        keys = [u'obj1\N{SNOWMAN}', u'obj2\N{SNOWMAN}', 'obj3']
        bad_content_md5 = base64.b64encode(b'a' * 16).strip().decode('ascii')
        headers = [{'Content-Type': 'foo/bar', 'x-amz-meta-baz': 'quux',
                    'Content-Encoding': 'gzip', 'Content-Language': 'en-US',
                    'Expires': 'Thu, 01 Dec 1994 16:00:00 GMT',
                    'Cache-Control': 'no-cache',
                    'Content-Disposition': 'attachment'},
                   {'Content-MD5': bad_content_md5},
                   {'Etag': 'nonsense'}]
        uploads = []

        results_generator = self._initiate_multi_uploads_result_generator(
            bucket, keys, headers=headers)

        # Initiate Multipart Upload
        for expected_key, (status, headers, body) in \
                zip(keys, results_generator):
            self.assertEqual(status, 200, body)
            self.assertCommonResponseHeaders(headers)
            self.assertIn('content-type', headers)
            self.assertEqual(headers['content-type'], 'application/xml')
            self.assertIn('content-length', headers)
            self.assertEqual(headers['content-length'], str(len(body)))
            elem = fromstring(body, 'InitiateMultipartUploadResult')
            self.assertEqual(elem.find('Bucket').text, bucket)
            key = elem.find('Key').text
            self.assertEqual(expected_key, key)
            upload_id = elem.find('UploadId').text
            self.assertIsNotNone(upload_id)
            self.assertNotIn((key, upload_id), uploads)
            uploads.append((key, upload_id))

        self.assertEqual(len(uploads), len(keys))  # sanity

        # List Multipart Uploads
        expected_uploads_list = [uploads]
        for upload in uploads:
            expected_uploads_list.append([upload])
        for expected_uploads in expected_uploads_list:
            query = 'uploads'
            if len(expected_uploads) == 1:
                query += '&' + urllib.parse.urlencode(
                    {'prefix': expected_uploads[0][0]})
            status, headers, body = \
                self.conn.make_request('GET', bucket, query=query)
            self.assertEqual(status, 200)
            self.assertCommonResponseHeaders(headers)
            self.assertTrue('content-type' in headers)
            self.assertEqual(headers['content-type'], 'application/xml')
            self.assertTrue('content-length' in headers)
            self.assertEqual(headers['content-length'], str(len(body)))
            elem = fromstring(body, 'ListMultipartUploadsResult')
            self.assertEqual(elem.find('Bucket').text, bucket)
            self.assertIsNone(elem.find('KeyMarker').text)
            if len(expected_uploads) > 1:
                self.assertEqual(elem.find('NextKeyMarker').text,
                                 expected_uploads[-1][0])
            else:
                self.assertIsNone(elem.find('NextKeyMarker').text)
            self.assertIsNone(elem.find('UploadIdMarker').text)
            if len(expected_uploads) > 1:
                self.assertEqual(elem.find('NextUploadIdMarker').text,
                                 expected_uploads[-1][1])
            else:
                self.assertIsNone(elem.find('NextUploadIdMarker').text)
            self.assertEqual(elem.find('MaxUploads').text, '1000')
            self.assertTrue(elem.find('EncodingType') is None)
            self.assertEqual(elem.find('IsTruncated').text, 'false')
            self.assertEqual(len(elem.findall('Upload')),
                             len(expected_uploads))
            for (expected_key, expected_upload_id), u in \
                    zip(expected_uploads, elem.findall('Upload')):
                key = u.find('Key').text
                upload_id = u.find('UploadId').text
                self.assertEqual(expected_key, key)
                self.assertEqual(expected_upload_id, upload_id)
                self.assertEqual(u.find('Initiator/ID').text,
                                 self.conn.user_id)
                self.assertEqual(u.find('Initiator/DisplayName').text,
                                 self.conn.user_id)
                self.assertEqual(u.find('Owner/ID').text, self.conn.user_id)
                self.assertEqual(u.find('Owner/DisplayName').text,
                                 self.conn.user_id)
                self.assertEqual(u.find('StorageClass').text, 'STANDARD')
                self.assertTrue(u.find('Initiated').text is not None)

        # Upload Part
        key, upload_id = uploads[0]
        content = b'a' * self.min_segment_size
        etag = md5(content, usedforsecurity=False).hexdigest()
        status, headers, body = \
            self._upload_part(bucket, key, upload_id, content)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers, etag)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'text/html; charset=UTF-8')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '0')
        expected_parts_list = [(headers['etag'],
                                mktime(headers['last-modified']))]

        # Upload Part Copy
        key, upload_id = uploads[1]
        src_bucket = 'bucket2'
        src_obj = 'obj3'
        src_content = b'b' * self.min_segment_size
        etag = md5(src_content, usedforsecurity=False).hexdigest()

        # prepare src obj
        self.conn.make_request('PUT', src_bucket)
        with self.quiet_boto_logging():
            self.conn.make_request('PUT', src_bucket, src_obj,
                                   body=src_content)
        _, headers, _ = self.conn.make_request('HEAD', src_bucket, src_obj)
        self.assertCommonResponseHeaders(headers)

        status, headers, body, resp_etag = \
            self._upload_part_copy(src_bucket, src_obj, bucket,
                                   key, upload_id)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertTrue('etag' not in headers)
        elem = fromstring(body, 'CopyPartResult')

        copy_resp_last_modified = elem.find('LastModified').text
        self.assertIsNotNone(copy_resp_last_modified)

        self.assertEqual(resp_etag, etag)

        # Check last-modified timestamp
        key, upload_id = uploads[1]
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('GET', bucket, key, query=query)

        self.assertEqual(200, status)
        elem = fromstring(body, 'ListPartsResult')

        listing_last_modified = [p.find('LastModified').text
                                 for p in elem.iterfind('Part')]
        # There should be *exactly* one parts in the result
        self.assertEqual(listing_last_modified, [copy_resp_last_modified])

        # List Parts
        key, upload_id = uploads[0]
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('GET', bucket, key, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Bucket').text, bucket)
        self.assertEqual(elem.find('Key').text, key)
        self.assertEqual(elem.find('UploadId').text, upload_id)
        self.assertEqual(elem.find('Initiator/ID').text, self.conn.user_id)
        self.assertEqual(elem.find('Initiator/DisplayName').text,
                         self.conn.user_id)
        self.assertEqual(elem.find('Owner/ID').text, self.conn.user_id)
        self.assertEqual(elem.find('Owner/DisplayName').text,
                         self.conn.user_id)
        self.assertEqual(elem.find('StorageClass').text, 'STANDARD')
        self.assertEqual(elem.find('PartNumberMarker').text, '0')
        self.assertEqual(elem.find('NextPartNumberMarker').text, '1')
        self.assertEqual(elem.find('MaxParts').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Part')), 1)

        # etags will be used to generate xml for Complete Multipart Upload
        etags = []
        for (expected_etag, expected_date), p in \
                zip(expected_parts_list, elem.findall('Part')):
            last_modified = p.find('LastModified').text
            self.assertIsNotNone(last_modified)
            last_modified_from_xml = S3Timestamp.from_s3xmlformat(
                last_modified)
            self.assertEqual(expected_date, float(last_modified_from_xml))
            self.assertEqual(expected_etag, p.find('ETag').text)
            self.assertEqual(self.min_segment_size, int(p.find('Size').text))
            etags.append(p.find('ETag').text)

        # Complete Multipart Upload
        key, upload_id = uploads[0]
        xml = self._gen_comp_xml(etags)
        status, headers, body = \
            self._complete_multi_upload(bucket, key, upload_id, xml)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertIn('content-type', headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        if 'content-length' in headers:
            self.assertEqual(headers['content-length'], str(len(body)))
        else:
            self.assertIn('transfer-encoding', headers)
            self.assertEqual(headers['transfer-encoding'], 'chunked')
        lines = body.split(b'\n')
        self.assertTrue(lines[0].startswith(b'<?xml'), body)
        self.assertTrue(lines[0].endswith(b'?>'), body)
        elem = fromstring(body, 'CompleteMultipartUploadResult')
        exp_location = '%s/bucket/%s' % \
                       (tf.config['s3_storage_url'].rstrip('/'),
                        swob.wsgi_quote(swob.str_to_wsgi(key)))
        self.assertEqual(exp_location, elem.find('Location').text)
        self.assertEqual(elem.find('Bucket').text, bucket)
        self.assertEqual(elem.find('Key').text, key)
        concatted_etags = b''.join(
            etag.strip('"').encode('ascii') for etag in etags)
        exp_etag = '"%s-%s"' % (
            md5(binascii.unhexlify(concatted_etags),
                usedforsecurity=False).hexdigest(), len(etags))
        etag = elem.find('ETag').text
        self.assertEqual(etag, exp_etag)

        exp_size = self.min_segment_size * len(etags)
        status, headers, body = \
            self.conn.make_request('HEAD', bucket, key)
        self.assertEqual(status, 200)
        self.assertEqual(headers['content-length'], str(exp_size))
        self.assertEqual(headers['content-type'], 'foo/bar')
        self.assertEqual(headers['content-encoding'], 'gzip')
        self.assertEqual(headers['content-language'], 'en-US')
        self.assertEqual(headers['content-disposition'], 'attachment')
        self.assertEqual(headers['expires'], 'Thu, 01 Dec 1994 16:00:00 GMT')
        self.assertEqual(headers['cache-control'], 'no-cache')
        self.assertEqual(headers['x-amz-meta-baz'], 'quux')

        swift_etag = '"%s"' % md5(
            concatted_etags, usedforsecurity=False).hexdigest()
        # TODO: GET via swift api, check against swift_etag

        # Should be safe to retry
        status, headers, body = \
            self._complete_multi_upload(bucket, key, upload_id, xml)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertIn('content-type', headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        if 'content-length' in headers:
            self.assertEqual(headers['content-length'], str(len(body)))
        else:
            self.assertIn('transfer-encoding', headers)
            self.assertEqual(headers['transfer-encoding'], 'chunked')
        lines = body.split(b'\n')
        self.assertTrue(lines[0].startswith(b'<?xml'), body)
        self.assertTrue(lines[0].endswith(b'?>'), body)
        elem = fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(exp_location, elem.find('Location').text)
        self.assertEqual(elem.find('Bucket').text, bucket)
        self.assertEqual(elem.find('Key').text, key)
        self.assertEqual(elem.find('ETag').text, exp_etag)

        status, headers, body = \
            self.conn.make_request('HEAD', bucket, key)
        self.assertEqual(status, 200)
        self.assertEqual(headers['content-length'], str(exp_size))
        self.assertEqual(headers['content-type'], 'foo/bar')
        self.assertEqual(headers['x-amz-meta-baz'], 'quux')

        # Upload Part Copy -- MU as source
        key, upload_id = uploads[1]
        status, headers, body, resp_etag = \
            self._upload_part_copy(bucket, keys[0], bucket,
                                   key, upload_id, part_num=2)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertIn('content-type', headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertIn('content-length', headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertNotIn('etag', headers)
        elem = fromstring(body, 'CopyPartResult')

        last_modified = elem.find('LastModified').text
        self.assertIsNotNone(last_modified)

        exp_content = b'a' * self.min_segment_size
        etag = md5(exp_content, usedforsecurity=False).hexdigest()
        self.assertEqual(resp_etag, etag)

        # Also check that the etag is correct in part listings
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('GET', bucket, key, query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(len(elem.findall('Part')), 2)
        self.assertEqual(elem.findall('Part')[1].find('PartNumber').text, '2')
        self.assertEqual(elem.findall('Part')[1].find('ETag').text,
                         '"%s"' % etag)

        # Abort Multipart Uploads
        # note that uploads[1] has part data while uploads[2] does not
        sw_conn = SwiftConnection(tf.config)
        sw_conn.authenticate()
        for key, upload_id in uploads[1:]:
            query = 'uploadId=%s' % upload_id
            status, headers, body = \
                self.conn.make_request('DELETE', bucket, key, query=query)
            self.assertEqual(status, 204)
            self.assertCommonResponseHeaders(headers)
            self.assertTrue('content-type' in headers)
            self.assertEqual(headers['content-type'],
                             'text/html; charset=UTF-8')
            self.assertTrue('content-length' in headers)
            self.assertEqual(headers['content-length'], '0')

        # Check object
        def check_obj(req_headers, exp_status):
            status, headers, body = \
                self.conn.make_request('HEAD', bucket, keys[0], req_headers)
            self.assertEqual(status, exp_status)
            self.assertCommonResponseHeaders(headers)
            self.assertIn('content-length', headers)
            if exp_status == 412:
                self.assertNotIn('etag', headers)
                self.assertEqual(headers['content-length'], '0')
            else:
                self.assertIn('etag', headers)
                self.assertEqual(headers['etag'], exp_etag)
                if exp_status == 304:
                    self.assertEqual(headers['content-length'], '0')
                else:
                    self.assertEqual(headers['content-length'], str(exp_size))

        check_obj({}, 200)

        # Sanity check conditionals
        check_obj({'If-Match': 'some other thing'}, 412)
        check_obj({'If-None-Match': 'some other thing'}, 200)

        # More interesting conditional cases
        check_obj({'If-Match': exp_etag}, 200)
        check_obj({'If-Match': swift_etag}, 412)
        check_obj({'If-None-Match': swift_etag}, 200)
        check_obj({'If-None-Match': exp_etag}, 304)

        # Check listings
        status, headers, body = self.conn.make_request('GET', bucket)
        self.assertEqual(status, 200)

        elem = fromstring(body, 'ListBucketResult')
        resp_objects = list(elem.findall('./Contents'))
        self.assertEqual(len(resp_objects), 1)
        o = resp_objects[0]
        expected_key = keys[0]
        self.assertEqual(o.find('Key').text, expected_key)
        self.assertIsNotNone(o.find('LastModified').text)
        self.assertRegex(
            o.find('LastModified').text,
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.000Z$')
        self.assertEqual(o.find('ETag').text, exp_etag)
        self.assertEqual(o.find('Size').text, str(exp_size))
        self.assertIsNotNone(o.find('StorageClass').text)
        self.assertEqual(o.find('Owner/ID').text, self.conn.user_id)
        self.assertEqual(o.find('Owner/DisplayName').text,
                         self.conn.user_id)

    def test_initiate_multi_upload_error(self):
        bucket = 'bucket'
        key = 'obj'
        self.conn.make_request('PUT', bucket)
        query = 'uploads'

        auth_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            auth_error_conn.make_request('POST', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, resp_headers, body = \
            self.conn.make_request('POST', 'nothing', key, query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        status, resp_headers, body = self.conn.make_request(
            'POST', bucket,
            'x' * (tf.cluster_info['swift']['max_object_name_length'] + 1),
            query=query)
        self.assertEqual(get_error_code(body), 'KeyTooLongError')

    def test_list_multi_uploads_error(self):
        bucket = 'bucket'
        self.conn.make_request('PUT', bucket)
        query = 'uploads'

        auth_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            auth_error_conn.make_request('GET', bucket, query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('GET', 'nothing', query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

    def test_upload_part_error(self):
        bucket = 'bucket'
        self.conn.make_request('PUT', bucket)
        query = 'uploads'
        key = 'obj'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        query = 'partNumber=%s&uploadId=%s' % (1, upload_id)
        auth_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            auth_error_conn.make_request('PUT', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('PUT', 'nothing', key, query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        query = 'partNumber=%s&uploadId=%s' % (1, 'nothing')
        status, headers, body = \
            self.conn.make_request('PUT', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'NoSuchUpload')

        query = 'partNumber=%s&uploadId=%s' % (0, upload_id)
        status, headers, body = \
            self.conn.make_request('PUT', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'InvalidArgument')
        err_msg = 'Part number must be an integer between 1 and'
        self.assertTrue(err_msg in get_error_msg(body))

    def test_upload_part_copy_error(self):
        src_bucket = 'src'
        src_obj = 'src'
        self.conn.make_request('PUT', src_bucket)
        self.conn.make_request('PUT', src_bucket, src_obj)
        src_path = '%s/%s' % (src_bucket, src_obj)

        bucket = 'bucket'
        self.conn.make_request('PUT', bucket)
        key = 'obj'
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        query = 'partNumber=%s&uploadId=%s' % (1, upload_id)
        auth_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            auth_error_conn.make_request('PUT', bucket, key,
                                         headers={
                                             'X-Amz-Copy-Source': src_path
                                         },
                                         query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('PUT', 'nothing', key,
                                   headers={'X-Amz-Copy-Source': src_path},
                                   query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        query = 'partNumber=%s&uploadId=%s' % (1, 'nothing')
        status, headers, body = \
            self.conn.make_request('PUT', bucket, key,
                                   headers={'X-Amz-Copy-Source': src_path},
                                   query=query)
        self.assertEqual(get_error_code(body), 'NoSuchUpload')

        src_path = '%s/%s' % (src_bucket, 'nothing')
        query = 'partNumber=%s&uploadId=%s' % (1, upload_id)
        status, headers, body = \
            self.conn.make_request('PUT', bucket, key,
                                   headers={'X-Amz-Copy-Source': src_path},
                                   query=query)
        self.assertEqual(get_error_code(body), 'NoSuchKey')

    def test_list_parts_error(self):
        bucket = 'bucket'
        self.conn.make_request('PUT', bucket)
        key = 'obj'
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        query = 'uploadId=%s' % upload_id
        auth_error_conn = Connection(tf.config['s3_access_key'], 'invalid')

        status, headers, body = \
            auth_error_conn.make_request('GET', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('GET', 'nothing', key, query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        query = 'uploadId=%s' % 'nothing'
        status, headers, body = \
            self.conn.make_request('GET', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'NoSuchUpload')

    def test_abort_multi_upload_error(self):
        bucket = 'bucket'
        self.conn.make_request('PUT', bucket)
        key = 'obj'
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text
        self._upload_part(bucket, key, upload_id)

        query = 'uploadId=%s' % upload_id
        auth_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            auth_error_conn.make_request('DELETE', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        status, headers, body = \
            self.conn.make_request('DELETE', 'nothing', key, query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        status, headers, body = \
            self.conn.make_request('DELETE', bucket, 'nothing', query=query)
        self.assertEqual(status, 404)
        self.assertEqual(get_error_code(body), 'NoSuchUpload')

        query = 'uploadId=%s' % 'nothing'
        status, headers, body = \
            self.conn.make_request('DELETE', bucket, key, query=query)
        self.assertEqual(get_error_code(body), 'NoSuchUpload')

    def test_complete_multi_upload_error(self):
        bucket = 'bucket'
        keys = ['obj', 'obj2']
        self.conn.make_request('PUT', bucket)
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, keys[0], query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        etags = []
        for i in range(1, 3):
            query = 'partNumber=%s&uploadId=%s' % (i, upload_id)
            status, headers, body = \
                self.conn.make_request('PUT', bucket, keys[0], query=query)
            self.assertEqual(status, 200)
            etags.append(headers['etag'])
        xml = self._gen_comp_xml(etags)

        # part 1 too small
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('POST', bucket, keys[0], body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'EntityTooSmall')

        # invalid credentials
        auth_error_conn = Connection(tf.config['s3_access_key'], 'invalid')
        status, headers, body = \
            auth_error_conn.make_request('POST', bucket, keys[0], body=xml,
                                         query=query)
        self.assertEqual(get_error_code(body), 'SignatureDoesNotMatch')

        # wrong/missing bucket
        status, headers, body = \
            self.conn.make_request('POST', 'nothing', keys[0], query=query)
        self.assertEqual(get_error_code(body), 'NoSuchBucket')

        # wrong upload ID
        query = 'uploadId=%s' % 'nothing'
        status, headers, body = \
            self.conn.make_request('POST', bucket, keys[0], body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'NoSuchUpload')

        # without Part tag in xml
        query = 'uploadId=%s' % upload_id
        xml = self._gen_comp_xml([])
        status, headers, body = \
            self.conn.make_request('POST', bucket, keys[0], body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'MalformedXML')

        # with invalid etag in xml
        invalid_etag = 'invalid'
        xml = self._gen_comp_xml([invalid_etag])
        status, headers, body = \
            self.conn.make_request('POST', bucket, keys[0], body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'InvalidPart')

        # without part in Swift
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, keys[1], query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text
        query = 'uploadId=%s' % upload_id
        xml = self._gen_comp_xml([etags[0]])
        status, headers, body = \
            self.conn.make_request('POST', bucket, keys[1], body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'InvalidPart')

    def test_complete_multi_upload_conditional(self):
        bucket = 'bucket'
        key = 'obj'
        self.conn.make_request('PUT', bucket)
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        query = 'partNumber=1&uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('PUT', bucket, key, query=query)
        part_etag = headers['etag']
        xml = self._gen_comp_xml([part_etag])

        for headers in [
            {'If-Match': part_etag},
            {'If-Match': '*'},
            {'If-None-Match': part_etag},
            {'If-Modified-Since': 'Wed, 21 Oct 2015 07:28:00 GMT'},
            {'If-Unmodified-Since': 'Wed, 21 Oct 2015 07:28:00 GMT'},
        ]:
            with self.subTest(headers=headers):
                query = 'uploadId=%s' % upload_id
                status, _, body = self.conn.make_request(
                    'POST', bucket, key, body=xml,
                    query=query, headers=headers)
                self.assertEqual(status, 501)
                self.assertEqual(get_error_code(body), 'NotImplemented')

        # Can do basic existence checks, though
        headers = {'If-None-Match': '*'}
        query = 'uploadId=%s' % upload_id
        status, _, body = self.conn.make_request(
            'POST', bucket, key, body=xml,
            query=query, headers=headers)
        self.assertEqual(status, 200)

        # And it'll prevent overwrites
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        query = 'partNumber=1&uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('PUT', bucket, key, query=query)
        part_etag = headers['etag']
        xml = self._gen_comp_xml([part_etag])

        headers = {'If-None-Match': '*'}
        query = 'uploadId=%s' % upload_id
        status, _, body = self.conn.make_request(
            'POST', bucket, key, body=xml,
            query=query, headers=headers)
        self.assertEqual(status, 412)
        self.assertEqual(get_error_code(body), 'PreconditionFailed')

    def test_complete_upload_min_segment_size(self):
        bucket = 'bucket'
        key = 'obj'
        self.conn.make_request('PUT', bucket)
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        # multi parts with no body
        etags = []
        for i in range(1, 3):
            query = 'partNumber=%s&uploadId=%s' % (i, upload_id)
            status, headers, body = \
                self.conn.make_request('PUT', bucket, key, query=query)
            self.assertEqual(status, 200)
            etags.append(headers['etag'])
            xml = self._gen_comp_xml(etags)

        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'EntityTooSmall')

        # multi parts with all parts less than min segment size
        etags = []
        for i in range(1, 3):
            query = 'partNumber=%s&uploadId=%s' % (i, upload_id)
            status, headers, body = \
                self.conn.make_request('PUT', bucket, key, query=query,
                                       body='AA')
            self.assertEqual(status, 200)
            etags.append(headers['etag'])
            xml = self._gen_comp_xml(etags)

        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'EntityTooSmall')

        # one part and less than min segment size
        etags = []
        query = 'partNumber=1&uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('PUT', bucket, key, query=query,
                                   body='AA')
        etags.append(headers['etag'])
        xml = self._gen_comp_xml(etags)

        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        self.assertEqual(status, 200)

        # multi parts with all parts except the first part less than min
        # segment size
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        etags = []
        body_size = [self.min_segment_size, self.min_segment_size - 1, 2]
        for i in range(1, 3):
            query = 'partNumber=%s&uploadId=%s' % (i, upload_id)
            status, headers, body = \
                self.conn.make_request('PUT', bucket, key, query=query,
                                       body=b'A' * body_size[i])
            etags.append(headers['etag'])
            xml = self._gen_comp_xml(etags)

        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        self.assertEqual(get_error_code(body), 'EntityTooSmall')

        # multi parts with all parts except last part more than min segment
        # size
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        etags = []
        body_size = [self.min_segment_size, self.min_segment_size, 2]
        for i in range(1, 3):
            query = 'partNumber=%s&uploadId=%s' % (i, upload_id)
            status, headers, body = \
                self.conn.make_request('PUT', bucket, key, query=query,
                                       body=b'A' * body_size[i])
            etags.append(headers['etag'])
            xml = self._gen_comp_xml(etags)

        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        self.assertEqual(status, 200)

    def test_complete_upload_with_fewer_etags(self):
        bucket = 'bucket'
        key = 'obj'
        self.conn.make_request('PUT', bucket)
        query = 'uploads'
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, query=query)
        elem = fromstring(body, 'InitiateMultipartUploadResult')
        upload_id = elem.find('UploadId').text

        etags = []
        for i in range(1, 4):
            query = 'partNumber=%s&uploadId=%s' % (2 * i - 1, upload_id)
            status, headers, body = self.conn.make_request(
                'PUT', bucket, key, body=b'A' * 1024 * 1024 * 5,
                query=query)
            etags.append(headers['etag'])
        query = 'uploadId=%s' % upload_id
        xml = self._gen_comp_xml(etags[:-1], step=2)
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        self.assertEqual(status, 200)

    def _initiate_mpu_upload(self, bucket, key):
        keys = [key]
        uploads = []

        results_generator = self._initiate_multi_uploads_result_generator(
            bucket, keys)

        # Initiate Multipart Upload
        for expected_key, (status, headers, body) in \
                zip(keys, results_generator):
            self.assertEqual(status, 200)
            self.assertCommonResponseHeaders(headers)
            self.assertTrue('content-type' in headers)
            self.assertEqual(headers['content-type'], 'application/xml')
            self.assertTrue('content-length' in headers)
            self.assertEqual(headers['content-length'], str(len(body)))
            elem = fromstring(body, 'InitiateMultipartUploadResult')
            self.assertEqual(elem.find('Bucket').text, bucket)
            key = elem.find('Key').text
            self.assertEqual(expected_key, key)
            upload_id = elem.find('UploadId').text
            self.assertTrue(upload_id is not None)
            self.assertTrue((key, upload_id) not in uploads)
            uploads.append((key, upload_id))

        # sanity, there's just one multi-part upload
        self.assertEqual(1, len(uploads))
        self.assertEqual(1, len(keys))
        _, upload_id = uploads[0]
        return upload_id

    def _copy_part_from_new_src_range(self, bucket, key, upload_id):
        src_bucket = 'bucket2'
        src_obj = 'obj4'
        src_content = b'y' * (self.min_segment_size // 2) + b'z' * \
            self.min_segment_size
        src_range = 'bytes=0-%d' % (self.min_segment_size - 1)
        etag = md5(
            src_content[:self.min_segment_size],
            usedforsecurity=False).hexdigest()

        # prepare src obj
        self.conn.make_request('PUT', src_bucket)
        self.conn.make_request('PUT', src_bucket, src_obj, body=src_content)
        _, headers, _ = self.conn.make_request('HEAD', src_bucket, src_obj)
        self.assertCommonResponseHeaders(headers)

        status, headers, body, resp_etag = \
            self._upload_part_copy(src_bucket, src_obj, bucket,
                                   key, upload_id, 1, src_range)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertTrue('etag' not in headers)
        elem = fromstring(body, 'CopyPartResult')
        etags = [elem.find('ETag').text]

        copy_resp_last_modified = elem.find('LastModified').text
        self.assertIsNotNone(copy_resp_last_modified)

        self.assertEqual(resp_etag, etag)

        # Check last-modified timestamp
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('GET', bucket, key, query=query)

        elem = fromstring(body, 'ListPartsResult')

        listing_last_modified = [p.find('LastModified').text
                                 for p in elem.iterfind('Part')]
        # There should be *exactly* one parts in the result
        self.assertEqual(listing_last_modified, [copy_resp_last_modified])

        # sanity, there's just one etag
        self.assertEqual(1, len(etags))
        return etags[0]

    def _complete_mpu_upload(self, bucket, key, upload_id, etags):
        # Complete Multipart Upload
        query = 'uploadId=%s' % upload_id
        xml = self._gen_comp_xml(etags)
        status, headers, body = \
            self.conn.make_request('POST', bucket, key, body=xml,
                                   query=query)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        if 'content-length' in headers:
            self.assertEqual(headers['content-length'], str(len(body)))
        else:
            self.assertIn('transfer-encoding', headers)
            self.assertEqual(headers['transfer-encoding'], 'chunked')
        lines = body.split(b'\n')
        self.assertTrue(lines[0].startswith(b'<?xml'), body)
        self.assertTrue(lines[0].endswith(b'?>'), body)
        elem = fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(
            '%s/%s/%s' %
            (tf.config['s3_storage_url'].rstrip('/'), bucket, key),
            elem.find('Location').text)
        self.assertEqual(elem.find('Bucket').text, bucket)
        self.assertEqual(elem.find('Key').text, key)
        concatted_etags = b''.join(
            etag.strip('"').encode('ascii') for etag in etags)
        exp_etag = '"%s-%s"' % (
            md5(binascii.unhexlify(concatted_etags),
                usedforsecurity=False).hexdigest(), len(etags))
        etag = elem.find('ETag').text
        self.assertEqual(etag, exp_etag)

    @skip_boto2_sort_header_bug
    def test_mpu_copy_part_from_range_then_complete(self):
        bucket = 'mpu-copy-range'
        key = 'obj-complete'
        upload_id = self._initiate_mpu_upload(bucket, key)
        etag = self._copy_part_from_new_src_range(bucket, key, upload_id)
        self._complete_mpu_upload(bucket, key, upload_id, [etag])

    @skip_boto2_sort_header_bug
    def test_mpu_copy_part_from_range_then_abort(self):
        bucket = 'mpu-copy-range'
        key = 'obj-abort'
        upload_id = self._initiate_mpu_upload(bucket, key)
        self._copy_part_from_new_src_range(bucket, key, upload_id)

        # Abort Multipart Upload
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('DELETE', bucket, key, query=query)

        # sanity checks
        self.assertEqual(status, 204)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'text/html; charset=UTF-8')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '0')

    def _copy_part_from_new_mpu_range(self, bucket, key, upload_id):
        src_bucket = 'bucket2'
        src_obj = 'mpu2'
        src_upload_id = self._initiate_mpu_upload(src_bucket, src_obj)
        # upload parts
        etags = []
        for part_num in range(2):
            # Upload Part
            content = (chr(97 + part_num) * self.min_segment_size).encode()
            etag = md5(content, usedforsecurity=False).hexdigest()
            status, headers, body = \
                self._upload_part(src_bucket, src_obj, src_upload_id,
                                  content, part_num=part_num + 1)
            self.assertEqual(status, 200)
            self.assertCommonResponseHeaders(headers, etag)
            self.assertTrue('content-type' in headers)
            self.assertEqual(headers['content-type'],
                             'text/html; charset=UTF-8')
            self.assertTrue('content-length' in headers)
            self.assertEqual(headers['content-length'], '0')
            self.assertEqual(headers['etag'], '"%s"' % etag)
            etags.append(etag)
        self._complete_mpu_upload(src_bucket, src_obj, src_upload_id, etags)

        # Upload Part Copy -- MPU as source
        src_range = 'bytes=0-%d' % (self.min_segment_size - 1)
        status, headers, body, resp_etag = \
            self._upload_part_copy(src_bucket, src_obj, bucket,
                                   key, upload_id, part_num=1,
                                   src_range=src_range)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertIn('content-type', headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertIn('content-length', headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertNotIn('etag', headers)
        elem = fromstring(body, 'CopyPartResult')

        last_modified = elem.find('LastModified').text
        self.assertIsNotNone(last_modified)
        # use copied with src_range from src_obj?part-number=1
        self.assertEqual(resp_etag, etags[0])

        return resp_etag

    @skip_boto2_sort_header_bug
    def test_mpu_copy_part_from_mpu_part_number_then_complete(self):
        bucket = 'mpu-copy-range'
        key = 'obj-complete'
        upload_id = self._initiate_mpu_upload(bucket, key)
        etag = self._copy_part_from_new_mpu_range(bucket, key, upload_id)
        self._complete_mpu_upload(bucket, key, upload_id, [etag])

    @skip_boto2_sort_header_bug
    def test_mpu_copy_part_from_mpu_part_number_then_abort(self):
        bucket = 'mpu-copy-range'
        key = 'obj-abort'
        upload_id = self._initiate_mpu_upload(bucket, key)
        self._copy_part_from_new_mpu_range(bucket, key, upload_id)

        # Abort Multipart Upload
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('DELETE', bucket, key, query=query)

        # sanity checks
        self.assertEqual(status, 204)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'text/html; charset=UTF-8')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '0')

    def test_object_multi_upload_part_copy_version(self):
        if 'object_versioning' not in tf.cluster_info:
            self.skipTest('Object Versioning not enabled')
        bucket = 'bucket'
        keys = ['obj1']
        uploads = []

        results_generator = self._initiate_multi_uploads_result_generator(
            bucket, keys)

        # Initiate Multipart Upload
        for expected_key, (status, headers, body) in \
                zip(keys, results_generator):
            self.assertEqual(status, 200)
            self.assertCommonResponseHeaders(headers)
            self.assertTrue('content-type' in headers)
            self.assertEqual(headers['content-type'], 'application/xml')
            self.assertTrue('content-length' in headers)
            self.assertEqual(headers['content-length'], str(len(body)))
            elem = fromstring(body, 'InitiateMultipartUploadResult')
            self.assertEqual(elem.find('Bucket').text, bucket)
            key = elem.find('Key').text
            self.assertEqual(expected_key, key)
            upload_id = elem.find('UploadId').text
            self.assertTrue(upload_id is not None)
            self.assertTrue((key, upload_id) not in uploads)
            uploads.append((key, upload_id))

        self.assertEqual(len(uploads), len(keys))  # sanity

        key, upload_id = uploads[0]
        src_bucket = 'bucket2'
        src_obj = 'obj4'
        src_content = b'y' * (self.min_segment_size // 2) + b'z' * \
            self.min_segment_size
        etags = [md5(src_content, usedforsecurity=False).hexdigest()]

        # prepare null-version src obj
        self.conn.make_request('PUT', src_bucket)
        self.conn.make_request('PUT', src_bucket, src_obj, body=src_content)
        _, headers, _ = self.conn.make_request('HEAD', src_bucket, src_obj)
        self.assertCommonResponseHeaders(headers)

        # Turn on versioning
        elem = Element('VersioningConfiguration')
        SubElement(elem, 'Status').text = 'Enabled'
        xml = tostring(elem)
        status, headers, body = self.conn.make_request(
            'PUT', src_bucket, body=xml, query='versioning')
        self.assertEqual(status, 200)

        src_obj2 = 'obj5'
        src_content2 = b'stub'
        etags.append(md5(src_content2, usedforsecurity=False).hexdigest())

        # prepare src obj w/ real version
        self.conn.make_request('PUT', src_bucket, src_obj2, body=src_content2)
        _, headers, _ = self.conn.make_request('HEAD', src_bucket, src_obj2)
        self.assertCommonResponseHeaders(headers)
        version_id2 = headers['x-amz-version-id']

        status, headers, body, resp_etag = \
            self._upload_part_copy(src_bucket, src_obj, bucket,
                                   key, upload_id, 1,
                                   src_version_id='null')
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertTrue('etag' not in headers)
        elem = fromstring(body, 'CopyPartResult')

        copy_resp_last_modifieds = [elem.find('LastModified').text]
        self.assertTrue(copy_resp_last_modifieds[0] is not None)

        self.assertEqual(resp_etag, etags[0])

        status, headers, body, resp_etag = \
            self._upload_part_copy(src_bucket, src_obj2, bucket,
                                   key, upload_id, 2,
                                   src_version_id=version_id2)
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'application/xml')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], str(len(body)))
        self.assertTrue('etag' not in headers)
        elem = fromstring(body, 'CopyPartResult')

        copy_resp_last_modifieds.append(elem.find('LastModified').text)
        self.assertTrue(copy_resp_last_modifieds[1] is not None)

        self.assertEqual(resp_etag, etags[1])

        # Check last-modified timestamp
        key, upload_id = uploads[0]
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('GET', bucket, key, query=query)

        elem = fromstring(body, 'ListPartsResult')

        listing_last_modified = [p.find('LastModified').text
                                 for p in elem.iterfind('Part')]
        self.assertEqual(listing_last_modified, copy_resp_last_modifieds)

        # Abort Multipart Upload
        key, upload_id = uploads[0]
        query = 'uploadId=%s' % upload_id
        status, headers, body = \
            self.conn.make_request('DELETE', bucket, key, query=query)

        # sanity checks
        self.assertEqual(status, 204)
        self.assertCommonResponseHeaders(headers)
        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'text/html; charset=UTF-8')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '0')

    def test_delete_bucket_multi_upload_object_exisiting(self):
        bucket = 'bucket'
        keys = ['obj1']
        uploads = []

        results_generator = self._initiate_multi_uploads_result_generator(
            bucket, keys)

        # Initiate Multipart Upload
        for expected_key, (status, _, body) in \
                zip(keys, results_generator):
            self.assertEqual(status, 200)  # sanity
            elem = fromstring(body, 'InitiateMultipartUploadResult')
            key = elem.find('Key').text
            self.assertEqual(expected_key, key)  # sanity
            upload_id = elem.find('UploadId').text
            self.assertTrue(upload_id is not None)  # sanity
            self.assertTrue((key, upload_id) not in uploads)
            uploads.append((key, upload_id))

        self.assertEqual(len(uploads), len(keys))  # sanity

        # Upload Part
        key, upload_id = uploads[0]
        content = b'a' * self.min_segment_size
        status, headers, body = \
            self._upload_part(bucket, key, upload_id, content)
        self.assertEqual(status, 200)

        # Complete Multipart Upload
        key, upload_id = uploads[0]
        etags = [md5(content, usedforsecurity=False).hexdigest()]
        xml = self._gen_comp_xml(etags)
        status, headers, body = \
            self._complete_multi_upload(bucket, key, upload_id, xml)
        self.assertEqual(status, 200)  # sanity

        # GET multipart object
        status, headers, body = \
            self.conn.make_request('GET', bucket, key)
        self.assertEqual(status, 200)  # sanity
        self.assertEqual(content, body)  # sanity

        # DELETE bucket while the object existing
        status, headers, body = \
            self.conn.make_request('DELETE', bucket)
        self.assertEqual(status, 409)  # sanity

        # The object must still be there.
        status, headers, body = \
            self.conn.make_request('GET', bucket, key)
        self.assertEqual(status, 200)  # sanity
        self.assertEqual(content, body)  # sanity

        # Can delete it with DeleteMultipleObjects request
        elem = Element('Delete')
        SubElement(elem, 'Quiet').text = 'true'
        obj_elem = SubElement(elem, 'Object')
        SubElement(obj_elem, 'Key').text = key
        body = tostring(elem, use_s3ns=False)

        status, headers, body = self.conn.make_request(
            'POST', bucket, body=body, query='delete',
            headers={'Content-MD5': calculate_md5(body)})
        self.assertEqual(status, 200)
        self.assertCommonResponseHeaders(headers)

        status, headers, body = \
            self.conn.make_request('GET', bucket, key)
        self.assertEqual(status, 404)  # sanity

        # Now we can delete
        status, headers, body = \
            self.conn.make_request('DELETE', bucket)
        self.assertEqual(status, 204)  # sanity


class TestS3ApiMultiUploadSigV4(TestS3ApiMultiUpload, SigV4Mixin):
    pass


if __name__ == '__main__':
    unittest.main()
