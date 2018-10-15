# Copyright (c) 2014 OpenStack Foundation
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
import hashlib
from mock import patch
import os
import time
import unittest
from urllib import quote

from swift.common import swob
from swift.common.swob import Request
from swift.common.utils import json

from test.unit.common.middleware.s3api import S3ApiTestCase
from test.unit.common.middleware.s3api.helpers import UnreadableInput
from swift.common.middleware.s3api.etree import fromstring, tostring
from swift.common.middleware.s3api.subresource import Owner, Grant, User, \
    ACL, encode_acl, decode_acl, ACLPublicRead
from test.unit.common.middleware.s3api.test_s3_acl import s3acl
from swift.common.middleware.s3api.utils import sysmeta_header, mktime, \
    S3Timestamp
from swift.common.middleware.s3api.s3request import MAX_32BIT_INT

xml = '<CompleteMultipartUpload>' \
    '<Part>' \
    '<PartNumber>1</PartNumber>' \
    '<ETag>0123456789abcdef0123456789abcdef</ETag>' \
    '</Part>' \
    '<Part>' \
    '<PartNumber>2</PartNumber>' \
    '<ETag>"fedcba9876543210fedcba9876543210"</ETag>' \
    '</Part>' \
    '</CompleteMultipartUpload>'

objects_template = \
    (('object/X/1', '2014-05-07T19:47:51.592270', '0123456789abcdef', 100),
     ('object/X/2', '2014-05-07T19:47:52.592270', 'fedcba9876543210', 200))

multiparts_template = \
    (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
     ('object/X/1', '2014-05-07T19:47:51.592270', '0123456789abcdef', 11),
     ('object/X/2', '2014-05-07T19:47:52.592270', 'fedcba9876543210', 21),
     ('object/Y', '2014-05-07T19:47:53.592270', 'HASH', 2),
     ('object/Y/1', '2014-05-07T19:47:54.592270', '0123456789abcdef', 12),
     ('object/Y/2', '2014-05-07T19:47:55.592270', 'fedcba9876543210', 22),
     ('object/Z', '2014-05-07T19:47:56.592270', 'HASH', 3),
     ('object/Z/1', '2014-05-07T19:47:57.592270', '0123456789abcdef', 13),
     ('object/Z/2', '2014-05-07T19:47:58.592270', 'fedcba9876543210', 23),
     ('subdir/object/Z', '2014-05-07T19:47:58.592270', 'HASH', 4),
     ('subdir/object/Z/1', '2014-05-07T19:47:58.592270', '0123456789abcdef',
      41),
     ('subdir/object/Z/2', '2014-05-07T19:47:58.592270', 'fedcba9876543210',
      41))

s3_etag = '"%s-2"' % hashlib.md5((
    '0123456789abcdef0123456789abcdef'
    'fedcba9876543210fedcba9876543210').decode('hex')).hexdigest()


class TestS3ApiMultiUpload(S3ApiTestCase):

    def setUp(self):
        super(TestS3ApiMultiUpload, self).setUp()

        segment_bucket = '/v1/AUTH_test/bucket+segments'
        self.etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        self.last_modified = 'Fri, 01 Apr 2014 12:00:00 GMT'
        put_headers = {'etag': self.etag, 'last-modified': self.last_modified}

        self.s3api.conf.min_segment_size = 1

        objects = map(lambda item: {'name': item[0], 'last_modified': item[1],
                                    'hash': item[2], 'bytes': item[3]},
                      objects_template)
        object_list = json.dumps(objects)

        self.swift.register('PUT', segment_bucket,
                            swob.HTTPAccepted, {}, None)
        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            object_list)
        self.swift.register('HEAD', segment_bucket + '/object/X',
                            swob.HTTPOk,
                            {'x-object-meta-foo': 'bar',
                             'content-type': 'application/directory',
                             'x-object-sysmeta-s3api-has-content-type': 'yes',
                             'x-object-sysmeta-s3api-content-type':
                             'baz/quux'}, None)
        self.swift.register('PUT', segment_bucket + '/object/X',
                            swob.HTTPCreated, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('GET', segment_bucket + '/object/invalid',
                            swob.HTTPNotFound, {}, None)
        self.swift.register('PUT', segment_bucket + '/object/X/1',
                            swob.HTTPCreated, put_headers, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/2',
                            swob.HTTPNoContent, {}, None)

    @s3acl
    def test_bucket_upload_part(self):
        req = Request.blank('/bucket?partNumber=1&uploadId=x',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_object_multipart_uploads_list(self):
        req = Request.blank('/bucket/object?uploads',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_bucket_multipart_uploads_initiate(self):
        req = Request.blank('/bucket?uploads',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_bucket_list_parts(self):
        req = Request.blank('/bucket?uploadId=x',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_bucket_multipart_uploads_abort(self):
        req = Request.blank('/bucket?uploadId=x',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(self._get_error_message(body),
                         'A key must be specified')

    @s3acl
    def test_bucket_multipart_uploads_complete(self):
        req = Request.blank('/bucket?uploadId=x',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    def _test_bucket_multipart_uploads_GET(self, query=None,
                                           multiparts=None):
        segment_bucket = '/v1/AUTH_test/bucket+segments'
        objects = multiparts or multiparts_template
        objects = map(lambda item: {'name': item[0], 'last_modified': item[1],
                                    'hash': item[2], 'bytes': item[3]},
                      objects)
        object_list = json.dumps(objects)
        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            object_list)

        query = '?uploads&' + query if query else '?uploads'
        req = Request.blank('/bucket/%s' % query,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

    @s3acl
    def test_bucket_multipart_uploads_GET(self):
        status, headers, body = self._test_bucket_multipart_uploads_GET()
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertIsNone(elem.find('KeyMarker').text)
        self.assertIsNone(elem.find('UploadIdMarker').text)
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Z')
        self.assertEqual(elem.find('MaxUploads').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Upload')), 4)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts_template]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
            self.assertEqual(u.find('Initiator/ID').text, 'test:tester')
            self.assertEqual(u.find('Initiator/DisplayName').text,
                             'test:tester')
            self.assertEqual(u.find('Owner/ID').text, 'test:tester')
            self.assertEqual(u.find('Owner/DisplayName').text, 'test:tester')
            self.assertEqual(u.find('StorageClass').text, 'STANDARD')
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_bucket_multipart_uploads_GET_without_segment_bucket(self):
        segment_bucket = '/v1/AUTH_test/bucket+segments'
        self.swift.register('GET', segment_bucket, swob.HTTPNotFound, {}, '')

        req = Request.blank('/bucket?uploads',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        status, haeaders, body = self.call_s3api(req)

        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertIsNone(elem.find('KeyMarker').text)
        self.assertIsNone(elem.find('UploadIdMarker').text)
        self.assertIsNone(elem.find('NextUploadIdMarker').text)
        self.assertEqual(elem.find('MaxUploads').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Upload')), 0)

    @s3acl
    @patch('swift.common.middleware.s3api.s3request.get_container_info',
           lambda x, y: {'status': 404})
    def test_bucket_multipart_uploads_GET_without_bucket(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNotFound, {}, '')
        req = Request.blank('/bucket?uploads',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, haeaders, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_bucket_multipart_uploads_GET_encoding_type_error(self):
        query = 'encoding-type=xml'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_maxuploads(self):
        query = 'max-uploads=2'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload/UploadId')), 2)
        self.assertEqual(elem.find('NextKeyMarker').text, 'object')
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Y')
        self.assertEqual(elem.find('MaxUploads').text, '2')
        self.assertEqual(elem.find('IsTruncated').text, 'true')
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_bucket_multipart_uploads_GET_str_maxuploads(self):
        query = 'max-uploads=invalid'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_negative_maxuploads(self):
        query = 'max-uploads=-1'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_maxuploads_over_default(self):
        query = 'max-uploads=1001'
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload/UploadId')), 4)
        self.assertEqual(elem.find('NextKeyMarker').text, 'subdir/object')
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Z')
        self.assertEqual(elem.find('MaxUploads').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_bucket_multipart_uploads_GET_maxuploads_over_max_32bit_int(self):
        query = 'max-uploads=%s' % (MAX_32BIT_INT + 1)
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    @s3acl
    def test_bucket_multipart_uploads_GET_with_id_and_key_marker(self):
        query = 'upload-id-marker=Y&key-marker=object'
        multiparts = \
            (('object/Y', '2014-05-07T19:47:53.592270', 'HASH', 2),
             ('object/Y/1', '2014-05-07T19:47:54.592270', 'HASH', 12),
             ('object/Y/2', '2014-05-07T19:47:55.592270', 'HASH', 22))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('KeyMarker').text, 'object')
        self.assertEqual(elem.find('UploadIdMarker').text, 'Y')
        self.assertEqual(len(elem.findall('Upload')), 1)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        self.assertEqual(status.split()[0], '200')

        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['marker'], 'object/Y')

    @s3acl
    def test_bucket_multipart_uploads_GET_with_key_marker(self):
        query = 'key-marker=object'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21),
             ('object/Y', '2014-05-07T19:47:53.592270', 'HASH', 2),
             ('object/Y/1', '2014-05-07T19:47:54.592270', 'HASH', 12),
             ('object/Y/2', '2014-05-07T19:47:55.592270', 'HASH', 22))
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(elem.find('KeyMarker').text, 'object')
        self.assertEqual(elem.find('NextKeyMarker').text, 'object')
        self.assertEqual(elem.find('NextUploadIdMarker').text, 'Y')
        self.assertEqual(len(elem.findall('Upload')), 2)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        self.assertEqual(status.split()[0], '200')

        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['marker'], quote('object/~'))

    @s3acl
    def test_bucket_multipart_uploads_GET_with_prefix(self):
        query = 'prefix=X'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21))
        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 1)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts]
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        self.assertEqual(status.split()[0], '200')

        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['prefix'], 'X')

    @s3acl
    def test_bucket_multipart_uploads_GET_with_delimiter(self):
        query = 'delimiter=/'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21),
             ('object/Y', '2014-05-07T19:47:50.592270', 'HASH', 2),
             ('object/Y/1', '2014-05-07T19:47:51.592270', 'HASH', 21),
             ('object/Y/2', '2014-05-07T19:47:52.592270', 'HASH', 22),
             ('object/Z', '2014-05-07T19:47:50.592270', 'HASH', 3),
             ('object/Z/1', '2014-05-07T19:47:51.592270', 'HASH', 31),
             ('object/Z/2', '2014-05-07T19:47:52.592270', 'HASH', 32),
             ('subdir/object/X', '2014-05-07T19:47:50.592270', 'HASH', 4),
             ('subdir/object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 41),
             ('subdir/object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 42),
             ('subdir/object/Y', '2014-05-07T19:47:50.592270', 'HASH', 5),
             ('subdir/object/Y/1', '2014-05-07T19:47:51.592270', 'HASH', 51),
             ('subdir/object/Y/2', '2014-05-07T19:47:52.592270', 'HASH', 52),
             ('subdir2/object/Z', '2014-05-07T19:47:50.592270', 'HASH', 6),
             ('subdir2/object/Z/1', '2014-05-07T19:47:51.592270', 'HASH', 61),
             ('subdir2/object/Z/2', '2014-05-07T19:47:52.592270', 'HASH', 62))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 3)
        self.assertEqual(len(elem.findall('CommonPrefixes')), 2)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts
                   if o[0].startswith('o')]
        prefixes = set([o[0].split('/')[0] + '/' for o in multiparts
                       if o[0].startswith('s')])
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        for p in elem.findall('CommonPrefixes'):
            prefix = p.find('Prefix').text
            self.assertTrue(prefix in prefixes)

        self.assertEqual(status.split()[0], '200')
        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertTrue(query.get('delimiter') is None)

    @s3acl
    def test_bucket_multipart_uploads_GET_with_multi_chars_delimiter(self):
        query = 'delimiter=subdir'
        multiparts = \
            (('object/X', '2014-05-07T19:47:50.592270', 'HASH', 1),
             ('object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 11),
             ('object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 21),
             ('dir/subdir/object/X', '2014-05-07T19:47:50.592270',
              'HASH', 3),
             ('dir/subdir/object/X/1', '2014-05-07T19:47:51.592270',
              'HASH', 31),
             ('dir/subdir/object/X/2', '2014-05-07T19:47:52.592270',
              'HASH', 32),
             ('subdir/object/X', '2014-05-07T19:47:50.592270', 'HASH', 4),
             ('subdir/object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 41),
             ('subdir/object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 42),
             ('subdir/object/Y', '2014-05-07T19:47:50.592270', 'HASH', 5),
             ('subdir/object/Y/1', '2014-05-07T19:47:51.592270', 'HASH', 51),
             ('subdir/object/Y/2', '2014-05-07T19:47:52.592270', 'HASH', 52),
             ('subdir2/object/Z', '2014-05-07T19:47:50.592270', 'HASH', 6),
             ('subdir2/object/Z/1', '2014-05-07T19:47:51.592270', 'HASH', 61),
             ('subdir2/object/Z/2', '2014-05-07T19:47:52.592270', 'HASH', 62))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 1)
        self.assertEqual(len(elem.findall('CommonPrefixes')), 2)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts
                   if o[0].startswith('object')]
        prefixes = ('dir/subdir', 'subdir')
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        for p in elem.findall('CommonPrefixes'):
            prefix = p.find('Prefix').text
            self.assertTrue(prefix in prefixes)

        self.assertEqual(status.split()[0], '200')
        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertTrue(query.get('delimiter') is None)

    @s3acl
    def test_bucket_multipart_uploads_GET_with_prefix_and_delimiter(self):
        query = 'prefix=dir/&delimiter=/'
        multiparts = \
            (('dir/subdir/object/X', '2014-05-07T19:47:50.592270',
              'HASH', 4),
             ('dir/subdir/object/X/1', '2014-05-07T19:47:51.592270',
              'HASH', 41),
             ('dir/subdir/object/X/2', '2014-05-07T19:47:52.592270',
              'HASH', 42),
             ('dir/object/X', '2014-05-07T19:47:50.592270', 'HASH', 5),
             ('dir/object/X/1', '2014-05-07T19:47:51.592270', 'HASH', 51),
             ('dir/object/X/2', '2014-05-07T19:47:52.592270', 'HASH', 52))

        status, headers, body = \
            self._test_bucket_multipart_uploads_GET(query, multiparts)
        elem = fromstring(body, 'ListMultipartUploadsResult')
        self.assertEqual(len(elem.findall('Upload')), 1)
        self.assertEqual(len(elem.findall('CommonPrefixes')), 1)
        objects = [(o[0], o[1][:-3] + 'Z') for o in multiparts
                   if o[0].startswith('dir/o')]
        prefixes = ['dir/subdir/']
        for u in elem.findall('Upload'):
            name = u.find('Key').text + '/' + u.find('UploadId').text
            initiated = u.find('Initiated').text
            self.assertTrue((name, initiated) in objects)
        for p in elem.findall('CommonPrefixes'):
            prefix = p.find('Prefix').text
            self.assertTrue(prefix in prefixes)

        self.assertEqual(status.split()[0], '200')
        _, path, _ = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['format'], 'json')
        self.assertEqual(query['limit'], '1001')
        self.assertEqual(query['prefix'], 'dir/')
        self.assertTrue(query.get('delimiter') is None)

    @patch('swift.common.middleware.s3api.controllers.'
           'multi_upload.unique_id', lambda: 'X')
    def _test_object_multipart_upload_initiate(self, headers):
        headers.update({
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header(),
            'x-amz-meta-foo': 'bar',
        })
        req = Request.blank('/bucket/object?uploads',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers=headers)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'InitiateMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, req_headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req_headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertNotIn('Etag', req_headers)
        self.assertNotIn('Content-MD5', req_headers)
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('PUT', '/v1/AUTH_test/bucket+segments'),
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket+segments'),
            ('PUT', '/v1/AUTH_test/bucket+segments/object/X'),
        ], self.swift.calls)
        self.swift.clear_calls()

    def test_object_multipart_upload_initiate(self):
        self._test_object_multipart_upload_initiate({})
        self._test_object_multipart_upload_initiate({'Etag': 'blahblahblah'})
        self._test_object_multipart_upload_initiate({
            'Content-MD5': base64.b64encode('blahblahblahblah').strip()})

    @s3acl(s3acl_only=True)
    @patch('swift.common.middleware.s3api.controllers.multi_upload.'
           'unique_id', lambda: 'X')
    def test_object_multipart_upload_initiate_s3acl(self):
        req = Request.blank('/bucket/object?uploads',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization':
                                     'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'public-read',
                                     'x-amz-meta-foo': 'bar',
                                     'Content-Type': 'cat/picture'})
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'InitiateMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, req_headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req_headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(req_headers.get(
            'X-Object-Sysmeta-S3api-Has-Content-Type'), 'yes')
        self.assertEqual(req_headers.get(
            'X-Object-Sysmeta-S3api-Content-Type'), 'cat/picture')
        tmpacl_header = req_headers.get(sysmeta_header('object', 'tmpacl'))
        self.assertTrue(tmpacl_header)
        acl_header = encode_acl('object',
                                ACLPublicRead(Owner('test:tester',
                                                    'test:tester')))
        self.assertEqual(acl_header.get(sysmeta_header('object', 'acl')),
                         tmpacl_header)

    @s3acl(s3acl_only=True)
    @patch('swift.common.middleware.s3api.controllers.'
           'multi_upload.unique_id', lambda: 'X')
    def test_object_multipart_upload_initiate_no_content_type(self):
        req = Request.blank('/bucket/object?uploads',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization':
                                     'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'public-read',
                                     'x-amz-meta-foo': 'bar'})
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'InitiateMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, req_headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req_headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(req_headers.get(
            'X-Object-Sysmeta-S3api-Has-Content-Type'), 'no')
        tmpacl_header = req_headers.get(sysmeta_header('object', 'tmpacl'))
        self.assertTrue(tmpacl_header)
        acl_header = encode_acl('object',
                                ACLPublicRead(Owner('test:tester',
                                                    'test:tester')))
        self.assertEqual(acl_header.get(sysmeta_header('object', 'acl')),
                         tmpacl_header)

    @patch('swift.common.middleware.s3api.controllers.'
           'multi_upload.unique_id', lambda: 'X')
    def test_object_multipart_upload_initiate_without_bucket(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNotFound, {}, None)
        req = Request.blank('/bucket/object?uploads',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization':
                                     'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_multipart_upload_complete_error(self):
        malformed_xml = 'malformed_XML'
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=malformed_xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedXML')

        # without target bucket
        req = Request.blank('/nobucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        with patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    def test_object_multipart_upload_complete(self):
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'CompleteMultipartUploadResult')
        self.assertNotIn('Etag', headers)
        self.assertEqual(elem.find('ETag').text, s3_etag)
        self.assertEqual(status.split()[0], '200')

        self.assertEqual(self.swift.calls, [
            # Bucket exists
            ('HEAD', '/v1/AUTH_test/bucket'),
            # Segment container exists
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            # Create the SLO
            ('PUT', '/v1/AUTH_test/bucket/object'
                    '?heartbeat=on&multipart-manifest=put'),
            # Delete the in-progress-upload marker
            ('DELETE', '/v1/AUTH_test/bucket+segments/object/X')
        ])

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(headers.get('Content-Type'), 'baz/quux')
        # SLO will provide a base value
        override_etag = '; s3_etag=%s' % s3_etag.strip('"')
        h = 'X-Object-Sysmeta-Container-Update-Override-Etag'
        self.assertEqual(headers.get(h), override_etag)

    @patch('swift.common.middleware.s3api.controllers.multi_upload.time')
    def test_object_multipart_upload_complete_with_heartbeat(self, mock_time):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket+segments/heartbeat-ok/X',
            swob.HTTPOk, {}, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket+segments', swob.HTTPOk, {},
            json.dumps([
                {'name': item[0].replace('object', 'heartbeat-ok'),
                 'last_modified': item[1], 'hash': item[2], 'bytes': item[3]}
                for item in objects_template
            ]))
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/heartbeat-ok',
            swob.HTTPAccepted, {}, [' ', ' ', ' ', json.dumps({
                'Etag': '"slo-etag"',
                'Response Status': '201 Created',
                'Errors': [],
            })])
        mock_time.return_value.time.side_effect = (
            1,  # start_time
            12,  # first whitespace
            13,  # second...
            14,  # third...
            15,  # JSON body
        )
        self.swift.register(
            'DELETE', '/v1/AUTH_test/bucket+segments/heartbeat-ok/X',
            swob.HTTPNoContent, {}, None)

        req = Request.blank('/bucket/heartbeat-ok?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        lines = body.split('\n')
        self.assertTrue(lines[0].startswith('<?xml '))
        self.assertTrue(lines[1])
        self.assertFalse(lines[1].strip())
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')
        # NB: s3_etag includes quotes
        self.assertIn('<ETag>%s</ETag>' % s3_etag, body)
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/heartbeat-ok/X'),
            ('PUT', '/v1/AUTH_test/bucket/heartbeat-ok?'
                    'heartbeat=on&multipart-manifest=put'),
            ('DELETE', '/v1/AUTH_test/bucket+segments/heartbeat-ok/X'),
        ])

    @patch('swift.common.middleware.s3api.controllers.multi_upload.time')
    def test_object_multipart_upload_complete_failure_with_heartbeat(
            self, mock_time):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket+segments/heartbeat-fail/X',
            swob.HTTPOk, {}, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket+segments', swob.HTTPOk, {},
            json.dumps([
                {'name': item[0].replace('object', 'heartbeat-fail'),
                 'last_modified': item[1], 'hash': item[2], 'bytes': item[3]}
                for item in objects_template
            ]))
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/heartbeat-fail',
            swob.HTTPAccepted, {}, [' ', ' ', ' ', json.dumps({
                'Response Status': '400 Bad Request',
                'Errors': [['some/object', '403 Forbidden']],
            })])
        mock_time.return_value.time.side_effect = (
            1,  # start_time
            12,  # first whitespace
            13,  # second...
            14,  # third...
            15,  # JSON body
        )

        req = Request.blank('/bucket/heartbeat-fail?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        lines = body.split('\n')
        self.assertTrue(lines[0].startswith('<?xml '), (status, lines))
        self.assertTrue(lines[1])
        self.assertFalse(lines[1].strip())
        fromstring(body, 'Error')
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(self._get_error_message(body),
                         'some/object: 403 Forbidden')
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/heartbeat-fail/X'),
            ('PUT', '/v1/AUTH_test/bucket/heartbeat-fail?'
                    'heartbeat=on&multipart-manifest=put'),
        ])

    @patch('swift.common.middleware.s3api.controllers.multi_upload.time')
    def test_object_multipart_upload_missing_part_with_heartbeat(
            self, mock_time):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket+segments/heartbeat-fail/X',
            swob.HTTPOk, {}, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket+segments', swob.HTTPOk, {},
            json.dumps([
                {'name': item[0].replace('object', 'heartbeat-fail'),
                 'last_modified': item[1], 'hash': item[2], 'bytes': item[3]}
                for item in objects_template
            ]))
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/heartbeat-fail',
            swob.HTTPAccepted, {}, [' ', ' ', ' ', json.dumps({
                'Response Status': '400 Bad Request',
                'Errors': [['some/object', '404 Not Found']],
            })])
        mock_time.return_value.time.side_effect = (
            1,  # start_time
            12,  # first whitespace
            13,  # second...
            14,  # third...
            15,  # JSON body
        )

        req = Request.blank('/bucket/heartbeat-fail?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        lines = body.split('\n')
        self.assertTrue(lines[0].startswith('<?xml '))
        self.assertTrue(lines[1])
        self.assertFalse(lines[1].strip())
        fromstring(body, 'Error')
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(self._get_error_code(body), 'InvalidPart')
        self.assertIn('One or more of the specified parts could not be found',
                      self._get_error_message(body))
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/heartbeat-fail/X'),
            ('PUT', '/v1/AUTH_test/bucket/heartbeat-fail?'
                    'heartbeat=on&multipart-manifest=put'),
        ])

    def test_object_multipart_upload_complete_404_on_marker_delete(self):
        segment_bucket = '/v1/AUTH_test/bucket+segments'
        self.swift.register('DELETE', segment_bucket + '/object/X',
                            swob.HTTPNotFound, {}, None)
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        fromstring(body, 'CompleteMultipartUploadResult')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(headers.get('Content-Type'), 'baz/quux')

    def test_object_multipart_upload_complete_old_content_type(self):
        self.swift.register_unconditionally(
            'HEAD', '/v1/AUTH_test/bucket+segments/object/X',
            swob.HTTPOk, {"Content-Type": "thingy/dingy"}, None)

        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('Content-Type'), 'thingy/dingy')

    def test_object_multipart_upload_complete_no_content_type(self):
        self.swift.register_unconditionally(
            'HEAD', '/v1/AUTH_test/bucket+segments/object/X',
            swob.HTTPOk, {"X-Object-Sysmeta-S3api-Has-Content-Type": "no"},
            None)

        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertNotIn('Content-Type', headers)

    def test_object_multipart_upload_complete_weird_host_name(self):
        # This happens via boto signature v4
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST',
                                     'HTTP_HOST': 'localhost:8080:8080'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')

    def test_object_multipart_upload_complete_segment_too_small(self):
        msg = ('some/path: s3api requires that each segment be at least '
               '%d bytes') % self.s3api.conf.min_segment_size
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPBadRequest, {}, msg)
        req = Request.blank(
            '/bucket/object?uploadId=X',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header(), },
            body=xml)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'EntityTooSmall')
        self.assertEqual(self._get_error_message(body), msg)
        # We punt to SLO to do the validation
        self.assertEqual([method for method, _ in self.swift.calls],
                         ['HEAD', 'HEAD', 'PUT'])

        self.swift.clear_calls()
        self.s3api.conf.min_segment_size = 5242880
        msg = ('some/path: s3api requires that each segment be at least '
               '%d bytes') % self.s3api.conf.min_segment_size
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPBadRequest, {}, msg)
        req = Request.blank(
            '/bucket/object?uploadId=X',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header(), },
            body=xml)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'EntityTooSmall')
        self.assertEqual(self._get_error_message(body), msg)
        # Again, we punt to SLO to do the validation
        self.assertEqual([method for method, _ in self.swift.calls],
                         ['HEAD', 'HEAD', 'PUT'])

    def test_object_multipart_upload_complete_zero_segments(self):
        segment_bucket = '/v1/AUTH_test/empty-bucket+segments'

        object_list = [{
            'name': 'object/X/1',
            'last_modified': self.last_modified,
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': '0',
        }]

        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            json.dumps(object_list))
        self.swift.register('HEAD', '/v1/AUTH_test/empty-bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('HEAD', segment_bucket + '/object/X',
                            swob.HTTPOk, {'x-object-meta-foo': 'bar',
                                          'content-type': 'baz/quux'}, None)
        self.swift.register('PUT', '/v1/AUTH_test/empty-bucket/object',
                            swob.HTTPCreated, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/1',
                            swob.HTTPOk, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X',
                            swob.HTTPOk, {}, None)

        xml = '<CompleteMultipartUpload></CompleteMultipartUpload>'

        req = Request.blank('/empty-bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        fromstring(body, 'Error')

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/empty-bucket'),
            ('HEAD', '/v1/AUTH_test/empty-bucket+segments/object/X'),
        ])

    def test_object_multipart_upload_complete_single_zero_length_segment(self):
        segment_bucket = '/v1/AUTH_test/empty-bucket+segments'
        put_headers = {'etag': self.etag, 'last-modified': self.last_modified}

        object_list = [{
            'name': 'object/X/1',
            'last_modified': self.last_modified,
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': '0',
        }]

        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            json.dumps(object_list))
        self.swift.register('HEAD', '/v1/AUTH_test/empty-bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('HEAD', segment_bucket + '/object/X',
                            swob.HTTPOk, {'x-object-meta-foo': 'bar',
                                          'content-type': 'baz/quux'}, None)
        self.swift.register('PUT', '/v1/AUTH_test/empty-bucket/object',
                            swob.HTTPCreated, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/1',
                            swob.HTTPOk, {}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X',
                            swob.HTTPOk, {}, None)

        xml = '<CompleteMultipartUpload>' \
            '<Part>' \
            '<PartNumber>1</PartNumber>' \
            '<ETag>d41d8cd98f00b204e9800998ecf8427e</ETag>' \
            '</Part>' \
            '</CompleteMultipartUpload>'

        req = Request.blank('/empty-bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/empty-bucket'),
            ('HEAD', '/v1/AUTH_test/empty-bucket+segments/object/X'),
            ('PUT', '/v1/AUTH_test/empty-bucket/object?'
                    'heartbeat=on&multipart-manifest=put'),
            ('DELETE', '/v1/AUTH_test/empty-bucket+segments/object/X'),
        ])
        _, _, put_headers = self.swift.calls_with_headers[-2]
        self.assertEqual(put_headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(put_headers.get('Content-Type'), 'baz/quux')

    def test_object_multipart_upload_complete_zero_length_final_segment(self):
        segment_bucket = '/v1/AUTH_test/bucket+segments'

        object_list = [{
            'name': 'object/X/1',
            'last_modified': self.last_modified,
            'hash': '0123456789abcdef0123456789abcdef',
            'bytes': '100',
        }, {
            'name': 'object/X/2',
            'last_modified': self.last_modified,
            'hash': 'fedcba9876543210fedcba9876543210',
            'bytes': '1',
        }, {
            'name': 'object/X/3',
            'last_modified': self.last_modified,
            'hash': 'd41d8cd98f00b204e9800998ecf8427e',
            'bytes': '0',
        }]

        self.swift.register('GET', segment_bucket, swob.HTTPOk, {},
                            json.dumps(object_list))
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('HEAD', segment_bucket + '/object/X',
                            swob.HTTPOk, {'x-object-meta-foo': 'bar',
                                          'content-type': 'baz/quux'}, None)
        self.swift.register('DELETE', segment_bucket + '/object/X/3',
                            swob.HTTPNoContent, {}, None)

        xml = '<CompleteMultipartUpload>' \
            '<Part>' \
            '<PartNumber>1</PartNumber>' \
            '<ETag>0123456789abcdef0123456789abcdef</ETag>' \
            '</Part>' \
            '<Part>' \
            '<PartNumber>2</PartNumber>' \
            '<ETag>fedcba9876543210fedcba9876543210</ETag>' \
            '</Part>' \
            '<Part>' \
            '<PartNumber>3</PartNumber>' \
            '<ETag>d41d8cd98f00b204e9800998ecf8427e</ETag>' \
            '</Part>' \
            '</CompleteMultipartUpload>'

        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(), },
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'CompleteMultipartUploadResult')
        self.assertNotIn('Etag', headers)
        expected_etag = '"%s-3"' % hashlib.md5(''.join(
            x['hash'] for x in object_list).decode('hex')).hexdigest()
        self.assertEqual(elem.find('ETag').text, expected_etag)

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('PUT', '/v1/AUTH_test/bucket/object?'
                    'heartbeat=on&multipart-manifest=put'),
            ('DELETE', '/v1/AUTH_test/bucket+segments/object/X'),
        ])

        _, _, headers = self.swift.calls_with_headers[-2]
        # SLO will provide a base value
        override_etag = '; s3_etag=%s' % expected_etag.strip('"')
        h = 'X-Object-Sysmeta-Container-Update-Override-Etag'
        self.assertEqual(headers.get(h), override_etag)

    @s3acl(s3acl_only=True)
    def test_object_multipart_upload_complete_s3acl(self):
        acl_headers = encode_acl('object', ACLPublicRead(Owner('test:tester',
                                                               'test:tester')))
        headers = {}
        headers[sysmeta_header('object', 'tmpacl')] = \
            acl_headers.get(sysmeta_header('object', 'acl'))
        headers['X-Object-Meta-Foo'] = 'bar'
        headers['Content-Type'] = 'baz/quux'
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/object/X',
                            swob.HTTPOk, headers, None)
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        fromstring(body, 'CompleteMultipartUploadResult')
        self.assertEqual(status.split()[0], '200')

        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers.get('X-Object-Meta-Foo'), 'bar')
        self.assertEqual(headers.get('Content-Type'), 'baz/quux')
        self.assertEqual(
            tostring(ACLPublicRead(Owner('test:tester',
                                         'test:tester')).elem()),
            tostring(decode_acl('object', headers, False).elem()))

    @s3acl
    def test_object_multipart_upload_abort_error(self):
        req = Request.blank('/bucket/object?uploadId=invalid',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchUpload')

        # without target bucket
        req = Request.blank('/nobucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_multipart_upload_abort(self):
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

    @s3acl
    @patch('swift.common.middleware.s3api.s3request.'
           'get_container_info', lambda x, y: {'status': 204})
    def test_object_upload_part_error(self):
        # without upload id
        req = Request.blank('/bucket/object?partNumber=1',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # invalid part number
        req = Request.blank('/bucket/object?partNumber=invalid&uploadId=X',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # part number must be > 0
        req = Request.blank('/bucket/object?partNumber=0&uploadId=X',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # part number must be < 1001
        req = Request.blank('/bucket/object?partNumber=1001&uploadId=X',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

        # without target bucket
        req = Request.blank('/nobucket/object?partNumber=1&uploadId=X',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='part object')
        with patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_upload_part(self):
        req = Request.blank('/bucket/object?partNumber=1&uploadId=X',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='part object')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_object_list_parts_error(self):
        req = Request.blank('/bucket/object?uploadId=invalid',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchUpload')

        # without target bucket
        req = Request.blank('/nobucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                lambda x, y: {'status': 404}):
            self.swift.register('HEAD', '/v1/AUTH_test/nobucket',
                                swob.HTTPNotFound, {}, None)
            status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NoSuchBucket')

    @s3acl
    def test_object_list_parts(self):
        req = Request.blank('/bucket/object?uploadId=X',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertEqual(elem.find('Key').text, 'object')
        self.assertEqual(elem.find('UploadId').text, 'X')
        self.assertEqual(elem.find('Initiator/ID').text, 'test:tester')
        self.assertEqual(elem.find('Initiator/ID').text, 'test:tester')
        self.assertEqual(elem.find('Owner/ID').text, 'test:tester')
        self.assertEqual(elem.find('Owner/ID').text, 'test:tester')
        self.assertEqual(elem.find('StorageClass').text, 'STANDARD')
        self.assertEqual(elem.find('PartNumberMarker').text, '0')
        self.assertEqual(elem.find('NextPartNumberMarker').text, '2')
        self.assertEqual(elem.find('MaxParts').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Part')), 2)
        for p in elem.findall('Part'):
            partnum = int(p.find('PartNumber').text)
            self.assertEqual(p.find('LastModified').text,
                             objects_template[partnum - 1][1][:-3] + 'Z')
            self.assertEqual(p.find('ETag').text.strip(),
                             '"%s"' % objects_template[partnum - 1][2])
            self.assertEqual(p.find('Size').text,
                             str(objects_template[partnum - 1][3]))
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_encoding_type(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/object@@/X',
                            swob.HTTPOk, {}, None)
        req = Request.blank('/bucket/object@@?uploadId=X&encoding-type=url',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Key').text, quote('object@@'))
        self.assertEqual(elem.find('EncodingType').text, 'url')
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_without_encoding_type(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/object@@/X',
                            swob.HTTPOk, {}, None)
        req = Request.blank('/bucket/object@@?uploadId=X',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Key').text, 'object@@')
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_encoding_type_error(self):
        req = Request.blank('/bucket/object?uploadId=X&encoding-type=xml',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_max_parts(self):
        req = Request.blank('/bucket/object?uploadId=X&max-parts=1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('IsTruncated').text, 'true')
        self.assertEqual(len(elem.findall('Part')), 1)
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_str_max_parts(self):
        req = Request.blank('/bucket/object?uploadId=X&max-parts=invalid',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_negative_max_parts(self):
        req = Request.blank('/bucket/object?uploadId=X&max-parts=-1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_over_max_parts(self):
        req = Request.blank('/bucket/object?uploadId=X&max-parts=%d' %
                            (self.s3api.conf.max_parts_listing + 1),
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(elem.find('Bucket').text, 'bucket')
        self.assertEqual(elem.find('Key').text, 'object')
        self.assertEqual(elem.find('UploadId').text, 'X')
        self.assertEqual(elem.find('Initiator/ID').text, 'test:tester')
        self.assertEqual(elem.find('Owner/ID').text, 'test:tester')
        self.assertEqual(elem.find('StorageClass').text, 'STANDARD')
        self.assertEqual(elem.find('PartNumberMarker').text, '0')
        self.assertEqual(elem.find('NextPartNumberMarker').text, '2')
        self.assertEqual(elem.find('MaxParts').text, '1000')
        self.assertEqual(elem.find('IsTruncated').text, 'false')
        self.assertEqual(len(elem.findall('Part')), 2)
        for p in elem.findall('Part'):
            partnum = int(p.find('PartNumber').text)
            self.assertEqual(p.find('LastModified').text,
                             objects_template[partnum - 1][1][:-3] + 'Z')
            self.assertEqual(p.find('ETag').text,
                             '"%s"' % objects_template[partnum - 1][2])
            self.assertEqual(p.find('Size').text,
                             str(objects_template[partnum - 1][3]))
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_over_max_32bit_int(self):
        req = Request.blank('/bucket/object?uploadId=X&max-parts=%d' %
                            (MAX_32BIT_INT + 1),
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_with_part_number_marker(self):
        req = Request.blank('/bucket/object?uploadId=X&'
                            'part-number-marker=1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(len(elem.findall('Part')), 1)
        self.assertEqual(elem.find('Part/PartNumber').text, '2')
        self.assertEqual(elem.find('PartNumberMarker').text, '1')
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_str_part_number_marker(self):
        req = Request.blank('/bucket/object?uploadId=X&part-number-marker='
                            'invalid',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_negative_part_number_marker(self):
        req = Request.blank('/bucket/object?uploadId=X&part-number-marker='
                            '-1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_over_part_number_marker(self):
        part_number_marker = str(self.s3api.conf.max_upload_part_num + 1)
        req = Request.blank('/bucket/object?uploadId=X&'
                            'part-number-marker=%s' % part_number_marker,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(len(elem.findall('Part')), 0)
        self.assertEqual(elem.find('PartNumberMarker').text,
                         part_number_marker)
        self.assertEqual(status.split()[0], '200')

    def test_object_list_parts_over_max_32bit_int_part_number_marker(self):
        req = Request.blank('/bucket/object?uploadId=X&part-number-marker='
                            '%s' % ((MAX_32BIT_INT + 1)),
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_object_list_parts_same_max_marts_as_objects_num(self):
        req = Request.blank('/bucket/object?uploadId=X&max-parts=2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'ListPartsResult')
        self.assertEqual(len(elem.findall('Part')), 2)
        self.assertEqual(status.split()[0], '200')

    def _test_for_s3acl(self, method, query, account, hasObj=True, body=None):
        path = '/bucket%s' % ('/object' + query if hasObj else query)
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()},
                            body=body)
        return self.call_s3api(req)

    @s3acl(s3acl_only=True)
    def test_upload_part_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('PUT', '?partNumber=1&uploadId=X',
                                 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_upload_part_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('PUT', '?partNumber=1&uploadId=X',
                                 'test:write')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('PUT', '?partNumber=1&uploadId=X',
                                 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_multipart_uploads_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploads', 'test:other',
                                 hasObj=False)
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_list_multipart_uploads_acl_with_read_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploads', 'test:read',
                                 hasObj=False)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_multipart_uploads_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploads', 'test:full_control',
                                 hasObj=False)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    @patch('swift.common.middleware.s3api.controllers.'
           'multi_upload.unique_id', lambda: 'X')
    def test_initiate_multipart_upload_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploads', 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    @patch('swift.common.middleware.s3api.controllers.'
           'multi_upload.unique_id', lambda: 'X')
    def test_initiate_multipart_upload_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploads', 'test:write')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    @patch('swift.common.middleware.s3api.controllers.'
           'multi_upload.unique_id', lambda: 'X')
    def test_initiate_multipart_upload_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploads', 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_parts_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploadId=X', 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_list_parts_acl_with_read_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploadId=X', 'test:read')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_list_parts_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('GET', '?uploadId=X', 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_abort_multipart_upload_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('DELETE', '?uploadId=X', 'test:other')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_abort_multipart_upload_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('DELETE', '?uploadId=X', 'test:write')
        self.assertEqual(status.split()[0], '204')

    @s3acl(s3acl_only=True)
    def test_abort_multipart_upload_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('DELETE', '?uploadId=X', 'test:full_control')
        self.assertEqual(status.split()[0], '204')

    @s3acl(s3acl_only=True)
    def test_complete_multipart_upload_acl_without_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploadId=X', 'test:other',
                                 body=xml)
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_complete_multipart_upload_acl_with_write_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploadId=X', 'test:write',
                                 body=xml)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_complete_multipart_upload_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_for_s3acl('POST', '?uploadId=X', 'test:full_control',
                                 body=xml)
        self.assertEqual(status.split()[0], '200')

    def _test_copy_for_s3acl(self, account, src_permission=None,
                             src_path='/src_bucket/src_obj', src_headers=None,
                             head_resp=swob.HTTPOk, put_header=None,
                             timestamp=None):
        owner = 'test:tester'
        grants = [Grant(User(account), src_permission)] \
            if src_permission else [Grant(User(owner), 'FULL_CONTROL')]
        src_o_headers = encode_acl('object', ACL(Owner(owner, owner), grants))
        src_o_headers.update({'last-modified': self.last_modified})
        src_o_headers.update(src_headers or {})
        self.swift.register('HEAD', '/v1/AUTH_test/%s' % src_path.lstrip('/'),
                            head_resp, src_o_headers, None)
        put_header = put_header or {}
        put_headers = {'Authorization': 'AWS %s:hmac' % account,
                       'Date': self.get_date_header(),
                       'X-Amz-Copy-Source': src_path}
        put_headers.update(put_header)
        req = Request.blank(
            '/bucket/object?partNumber=1&uploadId=X',
            environ={'REQUEST_METHOD': 'PUT'},
            headers=put_headers)
        timestamp = timestamp or time.time()
        with patch('swift.common.middleware.s3api.utils.time.time',
                   return_value=timestamp):
            return self.call_s3api(req)

    @s3acl
    def test_upload_part_copy(self):
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        last_modified = S3Timestamp(timestamp).s3xmlformat
        status, headers, body = self._test_copy_for_s3acl(
            'test:tester', put_header={'Date': date_header},
            timestamp=timestamp)
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Content-Type'], 'application/xml')
        self.assertTrue(headers.get('etag') is None)
        elem = fromstring(body, 'CopyPartResult')
        self.assertEqual(elem.find('LastModified').text, last_modified)
        self.assertEqual(elem.find('ETag').text, '"%s"' % self.etag)

        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(headers['X-Copy-From'], '/src_bucket/src_obj')
        self.assertEqual(headers['Content-Length'], '0')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_with_owner_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:tester')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_without_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:other', 'READ')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_with_write_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'READ')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:full_control', 'READ')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_without_src_permission(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_acl_invalid_source(self):
        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '')
        self.assertEqual(status.split()[0], '400')

        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '/')
        self.assertEqual(status.split()[0], '400')

        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '/bucket')
        self.assertEqual(status.split()[0], '400')

        status, headers, body = \
            self._test_copy_for_s3acl('test:write', 'WRITE', '/bucket/')
        self.assertEqual(status.split()[0], '400')

    @s3acl
    def test_upload_part_copy_headers_error(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPPreconditionFailed,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = {'X-Amz-Copy-Source-If-None-Match': etag}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPNotModified,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = {'X-Amz-Copy-Source-If-Modified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPNotModified,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = \
            {'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account,
                                      head_resp=swob.HTTPPreconditionFailed,
                                      put_header=header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

    def test_upload_part_copy_headers_with_match(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 11:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag,
                  'X-Amz-Copy-Source-If-Modified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')

        self.assertEqual(len(self.swift.calls_with_headers), 4)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-Match'], etag)
        self.assertEqual(headers['If-Modified-Since'], last_modified_since)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_headers_with_match_and_s3acl(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 11:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag,
                  'X-Amz-Copy-Source-If-Modified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 4)
        # Before the check of the copy source in the case of s3acl is valid,
        # s3api check the bucket write permissions and the object existence
        # of the destination.
        _, _, headers = self.swift.calls_with_headers[-3]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-Match'], etag)
        self.assertEqual(headers['If-Modified-Since'], last_modified_since)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)

    def test_upload_part_copy_headers_with_not_match(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-None-Match': etag,
                  'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 4)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-None-Match'], etag)
        self.assertEqual(headers['If-Unmodified-Since'], last_modified_since)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)

    @s3acl(s3acl_only=True)
    def test_upload_part_copy_headers_with_not_match_and_s3acl(self):
        account = 'test:tester'
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-None-Match': etag,
                  'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 4)
        # Before the check of the copy source in the case of s3acl is valid,
        # s3api check the bucket write permissions and the object existence
        # of the destination.
        _, _, headers = self.swift.calls_with_headers[-3]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertEqual(headers['If-None-Match'], etag)
        self.assertEqual(headers['If-Unmodified-Since'], last_modified_since)
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]

    def test_upload_part_copy_range_unsatisfiable(self):
        account = 'test:tester'

        header = {'X-Amz-Copy-Source-Range': 'bytes=1000-'}
        status, header, body = self._test_copy_for_s3acl(
            account, src_headers={'Content-Length': '10'}, put_header=header)

        self.assertEqual(status.split()[0], '400')
        self.assertIn('Range specified is not valid for '
                      'source object of size: 10', body)

        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('HEAD', '/v1/AUTH_test/src_bucket/src_obj'),
        ], self.swift.calls)

    def test_upload_part_copy_range_invalid(self):
        account = 'test:tester'

        header = {'X-Amz-Copy-Source-Range': '0-9'}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '400', body)

        header = {'X-Amz-Copy-Source-Range': 'asdf'}
        status, header, body = \
            self._test_copy_for_s3acl(account, put_header=header)

        self.assertEqual(status.split()[0], '400', body)

    def test_upload_part_copy_range(self):
        account = 'test:tester'

        header = {'X-Amz-Copy-Source-Range': 'bytes=0-9'}
        status, header, body = self._test_copy_for_s3acl(
            account, src_headers={'Content-Length': '20'}, put_header=header)

        self.assertEqual(status.split()[0], '200', body)

        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket+segments/object/X'),
            ('HEAD', '/v1/AUTH_test/src_bucket/src_obj'),
            ('PUT', '/v1/AUTH_test/bucket+segments/object/X/1'),
        ], self.swift.calls)
        put_headers = self.swift.calls_with_headers[-1][2]
        self.assertEqual('bytes=0-9', put_headers['Range'])
        self.assertEqual('/src_bucket/src_obj', put_headers['X-Copy-From'])

    def _test_no_body(self, use_content_length=False,
                      use_transfer_encoding=False, string_to_md5=''):
        raw_md5 = hashlib.md5(string_to_md5).digest()
        content_md5 = raw_md5.encode('base64').strip()
        with UnreadableInput(self) as fake_input:
            req = Request.blank(
                '/bucket/object?uploadId=X',
                environ={
                    'REQUEST_METHOD': 'POST',
                    'wsgi.input': fake_input},
                headers={
                    'Authorization': 'AWS test:tester:hmac',
                    'Date': self.get_date_header(),
                    'Content-MD5': content_md5},
                body='')
            if not use_content_length:
                req.environ.pop('CONTENT_LENGTH')
            if use_transfer_encoding:
                req.environ['HTTP_TRANSFER_ENCODING'] = 'chunked'
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(self._get_error_message(body),
                         'You must specify at least one part')

    @s3acl
    def test_object_multi_upload_empty_body(self):
        self._test_no_body()
        self._test_no_body(string_to_md5='test')
        self._test_no_body(use_content_length=True)
        self._test_no_body(use_content_length=True, string_to_md5='test')
        self._test_no_body(use_transfer_encoding=True)
        self._test_no_body(use_transfer_encoding=True, string_to_md5='test')


class TestS3ApiMultiUploadNonUTC(TestS3ApiMultiUpload):
    def setUp(self):
        self.orig_tz = os.environ.get('TZ', '')
        os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
        time.tzset()
        super(TestS3ApiMultiUploadNonUTC, self).setUp()

    def tearDown(self):
        super(TestS3ApiMultiUploadNonUTC, self).tearDown()
        os.environ['TZ'] = self.orig_tz
        time.tzset()


if __name__ == '__main__':
    unittest.main()
