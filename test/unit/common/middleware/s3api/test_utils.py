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

import os
import time
import unittest

from swift.common.swob import Request
from swift.common.middleware.s3api import utils, s3request
from swift.common.middleware.s3api.exception import InvalidBucketNameParseError
from swift.common.middleware.s3api.utils import make_header_label

strs = [
    ('Owner', 'owner'),
    ('DisplayName', 'display_name'),
    ('AccessControlPolicy', 'access_control_policy'),
]


class TestS3ApiUtils(unittest.TestCase):
    def test_camel_to_snake(self):
        for s1, s2 in strs:
            self.assertEqual(utils.camel_to_snake(s1), s2)

    def test_snake_to_camel(self):
        for s1, s2 in strs:
            self.assertEqual(s1, utils.snake_to_camel(s2))

    def test_make_header_label(self):
        self.assertEqual('header_aa_b_c', make_header_label('Aa-B-C'))
        self.assertEqual('header_aa_b_c', make_header_label('AA_B_C'))
        self.assertEqual('header_aa_b_c', make_header_label('aA-b-c'))

    def test_classify_checksum_header_value(self):
        self.assertEqual(
            utils.classify_checksum_header_value('00000000'), 'hash_8')
        self.assertEqual(
            utils.classify_checksum_header_value('a' * 64), 'hash_64')
        self.assertEqual(
            utils.classify_checksum_header_value('STUVWXYZ'), 'b64_8')
        self.assertEqual(
            utils.classify_checksum_header_value('abcdef&1'), 'unknown')
        self.assertEqual(
            utils.classify_checksum_header_value('z'), 'unknown')

    def test_validate_bucket_name(self):
        # good cases
        self.assertTrue(utils.validate_bucket_name('bucket', True))
        self.assertTrue(utils.validate_bucket_name('bucket1', True))
        self.assertTrue(utils.validate_bucket_name('bucket-1', True))
        self.assertTrue(utils.validate_bucket_name('b.u.c.k.e.t', True))
        self.assertTrue(utils.validate_bucket_name('a' * 63, True))
        self.assertTrue(utils.validate_bucket_name('v1.0', True))
        # bad cases
        self.assertFalse(utils.validate_bucket_name('a', True))
        self.assertFalse(utils.validate_bucket_name('aa', True))
        self.assertFalse(utils.validate_bucket_name('a+a', True))
        self.assertFalse(utils.validate_bucket_name('a_a', True))
        self.assertFalse(utils.validate_bucket_name('Bucket', True))
        self.assertFalse(utils.validate_bucket_name('BUCKET', True))
        self.assertFalse(utils.validate_bucket_name('bucket-', True))
        self.assertFalse(utils.validate_bucket_name('bucket.', True))
        self.assertFalse(utils.validate_bucket_name('bucket_', True))
        self.assertFalse(utils.validate_bucket_name('bucket.-bucket', True))
        self.assertFalse(utils.validate_bucket_name('bucket-.bucket', True))
        self.assertFalse(utils.validate_bucket_name('bucket..bucket', True))
        self.assertFalse(utils.validate_bucket_name('a' * 64, True))
        self.assertFalse(utils.validate_bucket_name('v1', False))

    def test_validate_bucket_name_with_dns_compliant_bucket_names_false(self):
        # good cases
        self.assertTrue(utils.validate_bucket_name('bucket', False))
        self.assertTrue(utils.validate_bucket_name('bucket1', False))
        self.assertTrue(utils.validate_bucket_name('bucket-1', False))
        self.assertTrue(utils.validate_bucket_name('b.u.c.k.e.t', False))
        self.assertTrue(utils.validate_bucket_name('a' * 63, False))
        self.assertTrue(utils.validate_bucket_name('a' * 255, False))
        self.assertTrue(utils.validate_bucket_name('a_a', False))
        self.assertTrue(utils.validate_bucket_name('Bucket', False))
        self.assertTrue(utils.validate_bucket_name('BUCKET', False))
        self.assertTrue(utils.validate_bucket_name('bucket-', False))
        self.assertTrue(utils.validate_bucket_name('bucket_', False))
        self.assertTrue(utils.validate_bucket_name('bucket.-bucket', False))
        self.assertTrue(utils.validate_bucket_name('bucket-.bucket', False))
        self.assertTrue(utils.validate_bucket_name('bucket..bucket', False))
        # bad cases
        self.assertFalse(utils.validate_bucket_name('a', False))
        self.assertFalse(utils.validate_bucket_name('aa', False))
        self.assertFalse(utils.validate_bucket_name('a+a', False))
        # ending with dot seems invalid in US standard, too
        self.assertFalse(utils.validate_bucket_name('bucket.', False))
        self.assertFalse(utils.validate_bucket_name('a' * 256, False))

    def test_extract_bucket_and_key(self):
        req = Request.blank(
            '/bucket/object',
            environ={
                'REQUEST_METHOD': 'GET',
            },
            headers={
                'Authorization': 'AWS test:tester:hmac',
            },
        )

        cont, obj = utils.extract_bucket_and_key(req, [], False)
        self.assertEqual(cont, 'bucket')
        self.assertEqual(obj, 'object')

    def test_extract_bucket_and_key_invalid_character(self):
        req = Request.blank(
            '/bucket/\x00object',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac'},
        )
        self.assertEqual((None, None),
                         utils.extract_bucket_and_key(req, [], False))

    def test_extract_bucket_and_key_invalid_bucket(self):
        req = Request.blank(
            '/b/object',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac'},
        )
        self.assertEqual((None, None),
                         utils.extract_bucket_and_key(req, [], False))

    def test_extract_bucket_and_key_invalid_dns_compliant(self):
        req = Request.blank(
            '/BUCKET/object',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac'},
        )
        self.assertEqual(('BUCKET', 'object'),
                         utils.extract_bucket_and_key(req, [], False))

        self.assertEqual((None, None),
                         utils.extract_bucket_and_key(req, [], True))

    def test_extract_bucket_and_key_bucket_in_host(self):
        req = Request.blank(
            '/object/xyz',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_HOST': 'bucket.localhost'},
            headers={'Authorization': 'AWS test:atester:hmac'},
        )
        self.assertEqual(
            ('bucket', 'object/xyz'),
            utils.extract_bucket_and_key(req, ['localhost'], False))

    def test_parse_host(self):
        req = Request.blank(
            '/bucket/object',
            environ={
                'REQUEST_METHOD': 'GET',
                'SERVER_NAME': 'foo.boo'
            },
        )
        del req.environ['HTTP_HOST']
        self.assertEqual(utils.parse_host(req.environ, []), None)
        self.assertEqual(utils.parse_host(req.environ, ['boo']), 'foo')
        req = Request.blank(
            '/bucket/object',
            environ={
                'REQUEST_METHOD': 'GET',
                'HTTP_HOST': 'buckets.localhost',
                'SERVER_NAME': 'foo.localhost',
            },
        )
        self.assertEqual(utils.parse_host(req.environ, []), None)
        self.assertEqual(utils.parse_host(
            req.environ, ['notlocalhost']), None)
        self.assertEqual(utils.parse_host(
            req.environ, ['localhost']), 'buckets')
        self.assertEqual(utils.parse_host(
            req.environ, ['.localhost']), 'buckets')
        self.assertEqual(utils.parse_host(
            req.environ, ['notlocalhost', '.localhost']), 'buckets')

    def test_parse_path(self):
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'GET'},
        )
        bucket, obj = utils.parse_path(req, None, False)
        self.assertEqual(bucket, 'bucket')
        self.assertEqual(obj, 'object')
        bucket, obj = utils.parse_path(req, None, True)
        self.assertEqual(bucket, 'bucket')
        self.assertEqual(obj, 'object')
        bucket, obj = utils.parse_path(req, 'boo', True)
        self.assertEqual(bucket, 'boo')
        self.assertEqual(obj, 'bucket/object')

    def test_parse_path_dns_compliant_bucket_names(self):
        req = Request.blank(
            '/BUCKET/object',
            environ={'REQUEST_METHOD': 'GET'},
        )
        with self.assertRaises(InvalidBucketNameParseError):
            utils.parse_path(req, None, True)
        # non-compliant is ok if it somehow came in the host??
        bucket, obj = utils.parse_path(req, 'BUCKET', True)
        self.assertEqual(bucket, 'BUCKET')
        self.assertEqual(obj, 'BUCKET/object')

    def test_get_s3_access_key_id_not_s3_req(self):
        headers = {'Authorization': 'not AWS my_access_key_id:signature'}
        req = Request.blank('/v1/a/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers=headers)
        self.assertIsNone(utils.get_s3_access_key_id(req))

    def test_get_s3_access_key_id_v2_header(self):
        headers = {'Authorization': 'AWS my_access_key_id:signature'}
        req = Request.blank('/v1/a/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers=headers)
        self.assertEqual('my_access_key_id', utils.get_s3_access_key_id(req))

    def test_get_s3_access_key_id_v2_param(self):
        params = {'AWSAccessKeyId': 'my_access_key_id'}
        req = Request.blank('/v1/a/',
                            environ={'REQUEST_METHOD': 'GET'},
                            params=params)
        self.assertEqual('my_access_key_id', utils.get_s3_access_key_id(req))

    def test_get_s3_access_key_id_v4_header(self):
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=my_access_key_id/20130524/us-east-1/s3/'
                'aws4_request,'
                'SignedHeaders=host;range;x-amz-date,'
                'Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963'
                '176630326f1024'}
        req = Request.blank('/v1/a/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers=headers)
        self.assertEqual('my_access_key_id', utils.get_s3_access_key_id(req))

    def test_get_s3_access_key_id_v4_param(self):
        params = {'X-Amz-Credential':
                  'my_access_key_id/20130721/us-east-1/s3/aws4_request'}
        req = Request.blank('/v1/a/',
                            environ={'REQUEST_METHOD': 'GET'},
                            params=params)
        self.assertEqual('my_access_key_id', utils.get_s3_access_key_id(req))

    def test_is_s3_req(self):
        headers = {'Authorization': 'not AWS my_access_key_id:signature'}
        req = Request.blank('/v1/a/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers=headers)
        self.assertIs(False, utils.is_s3_req(req))

        headers = {'Authorization': 'AWS my_access_key_id:signature'}
        req = Request.blank('/v1/a/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers=headers)
        self.assertIs(True, utils.is_s3_req(req))

    def test_mktime(self):
        date_headers = [
            'Thu, 01 Jan 1970 00:00:00 -0000',
            'Thu, 01 Jan 1970 00:00:00 GMT',
            'Thu, 01 Jan 1970 00:00:00 UTC',
            'Thu, 01 Jan 1970 08:00:00 +0800',
            'Wed, 31 Dec 1969 16:00:00 -0800',
            'Wed, 31 Dec 1969 16:00:00 PST',
        ]
        for header in date_headers:
            ts = utils.mktime(header)
            self.assertEqual(0, ts, 'Got %r for header %s' % (ts, header))

        # Last-Modified response style
        self.assertEqual(0, utils.mktime('1970-01-01T00:00:00'))

        # X-Amz-Date style
        self.assertEqual(0, utils.mktime('19700101T000000Z',
                                         s3request.SIGV4_X_AMZ_DATE_FORMAT))

    def test_mktime_weird_tz(self):
        orig_tz = os.environ.get('TZ', '')
        try:
            os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
            time.tzset()
            os.environ['TZ'] = '+0000'
            # No tzset! Simulating what Swift would do.
            self.assertNotEqual(0, time.timezone)
            self.test_mktime()
        finally:
            os.environ['TZ'] = orig_tz
            time.tzset()


class TestS3Timestamp(unittest.TestCase):
    def test_s3xmlformat(self):
        expected = '1970-01-01T00:00:01.000Z'
        # integer
        ts = utils.S3Timestamp(1)
        self.assertEqual(expected, ts.s3xmlformat)
        # microseconds digits are not included in Timestamp.normal so do not
        # cause the timestamp to be rounded up
        ts = utils.S3Timestamp(1.000001)
        self.assertEqual(expected, ts.s3xmlformat)

        # milliseconds unit should be rounded up
        expected = '1970-01-01T00:00:02.000Z'
        ts = utils.S3Timestamp(1.1)
        self.assertEqual(expected, ts.s3xmlformat)
        # float (deca-microseconds) should be rounded up too
        ts = utils.S3Timestamp(1.000010)
        self.assertEqual(expected, ts.s3xmlformat)
        # Bigger float (milliseconds) should be rounded up too
        ts = utils.S3Timestamp(1.9)
        self.assertEqual(expected, ts.s3xmlformat)

    def test_from_s3xmlformat(self):
        ts = utils.S3Timestamp.from_s3xmlformat('2014-06-10T22:47:32.000Z')
        self.assertIsInstance(ts, utils.S3Timestamp)
        self.assertEqual(1402440452, float(ts))
        self.assertEqual('2014-06-10T22:47:32.000000', ts.isoformat)

        ts = utils.S3Timestamp.from_s3xmlformat('1970-01-01T00:00:00.000Z')
        self.assertIsInstance(ts, utils.S3Timestamp)
        self.assertEqual(0.0, float(ts))
        self.assertEqual('1970-01-01T00:00:00.000000', ts.isoformat)

        ts = utils.S3Timestamp(1402440452.0)
        self.assertIsInstance(ts, utils.S3Timestamp)
        ts1 = utils.S3Timestamp.from_s3xmlformat(ts.s3xmlformat)
        self.assertIsInstance(ts1, utils.S3Timestamp)
        self.assertEqual(ts, ts1)

    def test_from_isoformat(self):
        ts = utils.S3Timestamp.from_isoformat('2014-06-10T22:47:32.054580')
        self.assertIsInstance(ts, utils.S3Timestamp)
        self.assertEqual(1402440452.05458, float(ts))
        self.assertEqual('2014-06-10T22:47:32.054580', ts.isoformat)
        self.assertEqual('2014-06-10T22:47:33.000Z', ts.s3xmlformat)


class TestConfig(unittest.TestCase):

    def _assert_defaults(self, conf):
        self.assertEqual([], conf.storage_domains)
        self.assertEqual('us-east-1', conf.location)
        self.assertFalse(conf.force_swift_request_proxy_log)
        self.assertTrue(conf.dns_compliant_bucket_names)
        self.assertTrue(conf.allow_multipart_uploads)
        self.assertFalse(conf.allow_no_owner)
        self.assertEqual(900, conf.allowable_clock_skew)
        self.assertFalse(conf.ratelimit_as_client_error)

    def test_defaults(self):
        # deliberately brittle so new defaults will need to be added to test
        conf = utils.Config()
        self._assert_defaults(conf)
        del conf.storage_domains
        del conf.location
        del conf.force_swift_request_proxy_log
        del conf.dns_compliant_bucket_names
        del conf.allow_multipart_uploads
        del conf.allow_no_owner
        del conf.allowable_clock_skew
        del conf.ratelimit_as_client_error
        del conf.max_upload_part_num
        self.assertEqual({}, conf)

    def test_update(self):
        conf = utils.Config()
        conf.update({'key1': 'val1', 'key2': 'val2'})
        self._assert_defaults(conf)
        self.assertEqual(conf.key1, 'val1')
        self.assertEqual(conf.key2, 'val2')

        conf.update({'allow_multipart_uploads': False})
        self.assertFalse(conf.allow_multipart_uploads)

    def test_set_get_delete(self):
        conf = utils.Config()
        self.assertRaises(AttributeError, lambda: conf.new_attr)
        conf.new_attr = 123
        self.assertEqual(123, conf.new_attr)
        del conf.new_attr
        self.assertRaises(AttributeError, lambda: conf.new_attr)


if __name__ == '__main__':
    unittest.main()
