#!/usr/bin/python

# Copyright (c) 2010-2012 OpenStack Foundation
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

from test.functional import check_response, retry, SkipTest
import test.functional as tf


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestHttpProtocol(unittest.TestCase):
    existing_metadata = None

    def test_invalid_path_info(self):
        if tf.skip:
            raise SkipTest

        def get(url, token, parsed, conn):
            path = "/info asdf"
            conn.request('GET', path, '', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get)
        resp.read()

        self.assertEqual(resp.status, 412)
        self.assertIsNotNone(resp.getheader('X-Trans-Id'))
        self.assertIsNotNone(resp.getheader('X-Openstack-Request-Id'))
        self.assertIn('tx', resp.getheader('X-Trans-Id'))
        self.assertIn('tx', resp.getheader('X-Openstack-Request-Id'))
        self.assertEqual(resp.getheader('X-Openstack-Request-Id'),
                         resp.getheader('X-Trans-Id'))
