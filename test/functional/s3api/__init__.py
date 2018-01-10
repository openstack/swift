# Copyright (c) 2011-2014 OpenStack Foundation.
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
import traceback
import test.functional as tf
from test.functional.s3api.s3_test_client import Connection


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class S3ApiBase(unittest2.TestCase):
    def __init__(self, method_name):
        super(S3ApiBase, self).__init__(method_name)
        self.method_name = method_name

    def setUp(self):
        if 's3api' not in tf.cluster_info:
            raise tf.SkipTest('s3api middleware is not enabled')
        try:
            self.conn = Connection()
            self.conn.reset()
        except Exception:
            message = '%s got an error during initialize process.\n\n%s' % \
                      (self.method_name, traceback.format_exc())
            # TODO: Find a way to make this go to FAIL instead of Error
            self.fail(message)

    def assertCommonResponseHeaders(self, headers, etag=None):
        """
        asserting common response headers with args
        :param headers: a dict of response headers
        :param etag: a string of md5(content).hexdigest() if not given,
                     this won't assert anything about etag. (e.g. DELETE obj)
        """
        self.assertTrue(headers['x-amz-id-2'] is not None)
        self.assertTrue(headers['x-amz-request-id'] is not None)
        self.assertTrue(headers['date'] is not None)
        # TODO; requires consideration
        # self.assertTrue(headers['server'] is not None)
        if etag is not None:
            self.assertTrue('etag' in headers)  # sanity
            self.assertEqual(etag, headers['etag'].strip('"'))
