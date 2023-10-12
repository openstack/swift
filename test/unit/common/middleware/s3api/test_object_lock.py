# Copyright (c) 2010-2023 OpenStack Foundation
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


class TestS3ApiObjectLock(S3ApiTestCase):

    # The object-lock controller currently only returns a static response
    # as disabled. Things like ansible need this. So there isn't much to test.

    def test_get_object_lock(self):
        req = Request.blank('/bucket?object-lock',
                            environ={'REQUEST_METHOD': 'GET',
                                     'swift.trans_id': 'txt1234'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        elem = fromstring(body, 'Error')

        self.assertTrue(elem.getchildren())
        for child in elem.iterchildren():
            if child.tag == 'Code':
                self.assertEqual(child.text,
                                 'ObjectLockConfigurationNotFoundError')
            elif child.tag == 'Message':
                self.assertEqual(child.text,
                                 'Object Lock configuration does not exist '
                                 'for this bucket')
            elif child.tag == 'RequestId':
                self.assertEqual(child.text,
                                 'txt1234')
            elif child.tag == 'BucketName':
                self.assertEqual(child.text, 'bucket')
            else:
                self.fail('Found unknown sub entry')

    def test_put_object_lock(self):
        req = Request.blank('/bucket?object-lock',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        # This currently isn't implemented.
        self.assertEqual(status.split()[0], '501')


if __name__ == '__main__':
    unittest.main()
