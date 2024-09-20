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

import unittest
import traceback
from contextlib import contextmanager
import logging
from unittest import SkipTest

import os

import test.functional as tf
from test.functional.s3api.s3_test_client import (
    Connection, get_boto3_conn, tear_down_s3)
try:
    import boto
except ImportError:
    boto = None


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class S3ApiBase(unittest.TestCase):
    def __init__(self, method_name):
        super(S3ApiBase, self).__init__(method_name)
        self.method_name = method_name

    @contextmanager
    def quiet_boto_logging(self):
        original_level = logging.getLogger('boto').getEffectiveLevel()
        try:
            logging.getLogger('boto').setLevel(logging.INFO)
            yield
        finally:
            logging.getLogger('boto').setLevel(original_level)

    def setUp(self):
        if not tf.config.get('s3_access_key'):
            raise SkipTest('no s3api user configured')
        if 's3api' not in tf.cluster_info:
            raise SkipTest('s3api middleware is not enabled')
        if boto is None:
            raise SkipTest('boto 2.x library is not installed')
        if tf.config.get('account'):
            user_id = '%s:%s' % (tf.config['account'], tf.config['username'])
        else:
            user_id = tf.config['username']
        try:
            self.conn = Connection(
                tf.config['s3_access_key'], tf.config['s3_secret_key'],
                user_id=user_id)

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


class S3ApiBaseBoto3(S3ApiBase):
    def setUp(self):
        if not tf.config.get('s3_access_key'):
            raise SkipTest('no s3api user configured')
        if 's3api' not in tf.cluster_info:
            raise SkipTest('s3api middleware is not enabled')
        try:
            self.conn = get_boto3_conn(
                tf.config['s3_access_key'], tf.config['s3_secret_key'])
            self.endpoint_url = self.conn._endpoint.host
            self.access_key = self.conn._request_signer._credentials.access_key
            self.region = self.conn._client_config.region_name
            tear_down_s3(self.conn)
        except Exception:
            message = '%s got an error during initialize process.\n\n%s' % \
                      (self.method_name, traceback.format_exc())
            # TODO: Find a way to make this go to FAIL instead of Error
            self.fail(message)

    def tearDown(self):
        tear_down_s3(self.conn)


def skip_boto2_sort_header_bug(m):
    def wrapped(self, *args, **kwargs):
        if os.environ.get('S3_USE_SIGV4') == "True":
            # boto doesn't sort headers for v4 sigs properly; see
            # https://github.com/boto/boto/pull/3032
            # or https://github.com/boto/boto/pull/3176
            # or https://github.com/boto/boto/pull/3751
            # or https://github.com/boto/boto/pull/3824
            self.skipTest('This stuff got the issue of boto<=2.x')
        return m(self, *args, **kwargs)
    return wrapped


class SigV4Mixin(object):
    @classmethod
    def setUpClass(cls):
        os.environ['S3_USE_SIGV4'] = "True"

    @classmethod
    def tearDownClass(cls):
        del os.environ['S3_USE_SIGV4']

    def setUp(self):
        super(SigV4Mixin, self).setUp()
