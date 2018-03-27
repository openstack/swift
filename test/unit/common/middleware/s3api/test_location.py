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

import unittest

from swift.common.swob import Request

from test.unit.common.middleware.s3api import S3ApiTestCase
from swift.common.middleware.s3api.etree import fromstring


class TestS3ApiLocation(S3ApiTestCase):

    def test_object_location(self):
        req = Request.blank('/bucket?location',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'LocationConstraint')
        location = elem.text
        self.assertIsNone(location)

    def test_object_location_setting_as_us_west_1(self):
        self.s3api.conf.location = 'us-west-1'
        req = Request.blank('/bucket?location',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body, 'LocationConstraint')
        location = elem.text
        self.assertEqual(location, 'us-west-1')


if __name__ == '__main__':
    unittest.main()
