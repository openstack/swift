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

from swift.common.middleware.s3api import utils, s3request

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

    def test_validate_bucket_name(self):
        # good cases
        self.assertTrue(utils.validate_bucket_name('bucket', True))
        self.assertTrue(utils.validate_bucket_name('bucket1', True))
        self.assertTrue(utils.validate_bucket_name('bucket-1', True))
        self.assertTrue(utils.validate_bucket_name('b.u.c.k.e.t', True))
        self.assertTrue(utils.validate_bucket_name('a' * 63, True))
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
        # milliseconds unit should be rounded up
        expected = '1970-01-01T00:00:02.000Z'
        ts = utils.S3Timestamp(1.1)
        self.assertEqual(expected, ts.s3xmlformat)
        # float (microseconds) should be floored too
        ts = utils.S3Timestamp(1.000001)
        self.assertEqual(expected, ts.s3xmlformat)
        # Bigger float (milliseconds) should be floored too
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
