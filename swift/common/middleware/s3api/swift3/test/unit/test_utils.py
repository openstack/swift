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
import mock

from swift3 import utils, request

strs = [
    ('Owner', 'owner'),
    ('DisplayName', 'display_name'),
    ('AccessControlPolicy', 'access_control_policy'),
]


class TestSwift3Utils(unittest.TestCase):
    def test_camel_to_snake(self):
        for s1, s2 in strs:
            self.assertEqual(utils.camel_to_snake(s1), s2)

    def test_snake_to_camel(self):
        for s1, s2 in strs:
            self.assertEqual(s1, utils.snake_to_camel(s2))

    def test_validate_bucket_name(self):
        # good cases
        self.assertTrue(utils.validate_bucket_name('bucket'))
        self.assertTrue(utils.validate_bucket_name('bucket1'))
        self.assertTrue(utils.validate_bucket_name('bucket-1'))
        self.assertTrue(utils.validate_bucket_name('b.u.c.k.e.t'))
        self.assertTrue(utils.validate_bucket_name('a' * 63))
        # bad cases
        self.assertFalse(utils.validate_bucket_name('a'))
        self.assertFalse(utils.validate_bucket_name('aa'))
        self.assertFalse(utils.validate_bucket_name('a+a'))
        self.assertFalse(utils.validate_bucket_name('a_a'))
        self.assertFalse(utils.validate_bucket_name('Bucket'))
        self.assertFalse(utils.validate_bucket_name('BUCKET'))
        self.assertFalse(utils.validate_bucket_name('bucket-'))
        self.assertFalse(utils.validate_bucket_name('bucket.'))
        self.assertFalse(utils.validate_bucket_name('bucket_'))
        self.assertFalse(utils.validate_bucket_name('bucket.-bucket'))
        self.assertFalse(utils.validate_bucket_name('bucket-.bucket'))
        self.assertFalse(utils.validate_bucket_name('bucket..bucket'))
        self.assertFalse(utils.validate_bucket_name('a' * 64))

    def test_validate_bucket_name_with_dns_compliant_bucket_names_false(self):

        with mock.patch('swift3.utils.CONF.dns_compliant_bucket_names', False):
            # good cases
            self.assertTrue(utils.validate_bucket_name('bucket'))
            self.assertTrue(utils.validate_bucket_name('bucket1'))
            self.assertTrue(utils.validate_bucket_name('bucket-1'))
            self.assertTrue(utils.validate_bucket_name('b.u.c.k.e.t'))
            self.assertTrue(utils.validate_bucket_name('a' * 63))
            self.assertTrue(utils.validate_bucket_name('a' * 255))
            self.assertTrue(utils.validate_bucket_name('a_a'))
            self.assertTrue(utils.validate_bucket_name('Bucket'))
            self.assertTrue(utils.validate_bucket_name('BUCKET'))
            self.assertTrue(utils.validate_bucket_name('bucket-'))
            self.assertTrue(utils.validate_bucket_name('bucket_'))
            self.assertTrue(utils.validate_bucket_name('bucket.-bucket'))
            self.assertTrue(utils.validate_bucket_name('bucket-.bucket'))
            self.assertTrue(utils.validate_bucket_name('bucket..bucket'))
            # bad cases
            self.assertFalse(utils.validate_bucket_name('a'))
            self.assertFalse(utils.validate_bucket_name('aa'))
            self.assertFalse(utils.validate_bucket_name('a+a'))
            # ending with dot seems invalid in US standard, too
            self.assertFalse(utils.validate_bucket_name('bucket.'))
            self.assertFalse(utils.validate_bucket_name('a' * 256))

    def test_s3timestamp(self):
        expected = '1970-01-01T00:00:01.000Z'
        # integer
        ts = utils.S3Timestamp(1)
        self.assertEqual(expected, ts.s3xmlformat)
        # milliseconds unit should be floored
        ts = utils.S3Timestamp(1.1)
        self.assertEqual(expected, ts.s3xmlformat)
        # float (microseconds) should be floored too
        ts = utils.S3Timestamp(1.000001)
        self.assertEqual(expected, ts.s3xmlformat)
        # Bigger float (milliseconds) should be floored too
        ts = utils.S3Timestamp(1.9)
        self.assertEqual(expected, ts.s3xmlformat)

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
                                         request.SIGV4_X_AMZ_DATE_FORMAT))

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

if __name__ == '__main__':
    unittest.main()
