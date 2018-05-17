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

from swift.common import swob
from swift.common.swob import Request
from swift.common.utils import json

from test.unit.common.middleware.s3api.test_s3_acl import s3acl
from test.unit.common.middleware.s3api import S3ApiTestCase
from swift.common.middleware.s3api.etree import fromstring
from swift.common.middleware.s3api.subresource import ACL, Owner, encode_acl


def create_bucket_list_json(buckets):
    """
    Create a json from bucket list
    :param buckets: a list of tuples (or lists) consist of elements orderd as
                    name, count, bytes
    """
    bucket_list = map(
        lambda item: {'name': item[0], 'count': item[1], 'bytes': item[2]},
        list(buckets))
    return json.dumps(bucket_list)


class TestS3ApiService(S3ApiTestCase):
    def setup_buckets(self):
        self.buckets = (('apple', 1, 200), ('orange', 3, 430))
        bucket_list = create_bucket_list_json(self.buckets)
        self.swift.register('GET', '/v1/AUTH_test', swob.HTTPOk, {},
                            bucket_list)

    def setUp(self):
        super(TestS3ApiService, self).setUp()

        self.setup_buckets()

    def test_service_GET_error(self):
        code = self._test_method_error('GET', '', swob.HTTPUnauthorized)
        self.assertEqual(code, 'SignatureDoesNotMatch')
        code = self._test_method_error('GET', '', swob.HTTPForbidden)
        self.assertEqual(code, 'AccessDenied')
        code = self._test_method_error('GET', '', swob.HTTPServerError)
        self.assertEqual(code, 'InternalError')

    @s3acl
    def test_service_GET(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListAllMyBucketsResult')

        all_buckets = elem.find('./Buckets')
        buckets = all_buckets.iterchildren('Bucket')
        listing = list(list(buckets)[0])
        self.assertEqual(len(listing), 2)

        names = []
        for b in all_buckets.iterchildren('Bucket'):
            names.append(b.find('./Name').text)

        self.assertEqual(len(names), len(self.buckets))
        for i in self.buckets:
            self.assertTrue(i[0] in names)

    @s3acl
    def test_service_GET_subresource(self):
        req = Request.blank('/?acl',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListAllMyBucketsResult')

        all_buckets = elem.find('./Buckets')
        buckets = all_buckets.iterchildren('Bucket')
        listing = list(list(buckets)[0])
        self.assertEqual(len(listing), 2)

        names = []
        for b in all_buckets.iterchildren('Bucket'):
            names.append(b.find('./Name').text)

        self.assertEqual(len(names), len(self.buckets))
        for i in self.buckets:
            self.assertTrue(i[0] in names)

    def test_service_GET_with_blind_resource(self):
        buckets = (('apple', 1, 200), ('orange', 3, 430),
                   ('apple+segment', 1, 200))
        expected = buckets[:-1]
        bucket_list = create_bucket_list_json(buckets)
        self.swift.register('GET', '/v1/AUTH_test', swob.HTTPOk, {},
                            bucket_list)

        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListAllMyBucketsResult')
        all_buckets = elem.find('./Buckets')
        buckets = all_buckets.iterchildren('Bucket')
        listing = list(list(buckets)[0])
        self.assertEqual(len(listing), 2)

        names = []
        for b in all_buckets.iterchildren('Bucket'):
            names.append(b.find('./Name').text)

        self.assertEqual(len(names), len(expected))
        for i in expected:
            self.assertTrue(i[0] in names)

    def _test_service_GET_for_check_bucket_owner(self, buckets):
        self.s3api.conf.check_bucket_owner = True
        bucket_list = create_bucket_list_json(buckets)
        self.swift.register('GET', '/v1/AUTH_test', swob.HTTPOk, {},
                            bucket_list)

        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

    @s3acl(s3acl_only=True)
    def test_service_GET_without_bucket(self):
        bucket_list = []
        for var in range(0, 10):
            bucket = 'bucket%s' % var
            self.swift.register('HEAD', '/v1/AUTH_test/%s' % bucket,
                                swob.HTTPNotFound, {}, None)
            bucket_list.append((bucket, var, 300 + var))

        status, headers, body = \
            self._test_service_GET_for_check_bucket_owner(bucket_list)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListAllMyBucketsResult')

        resp_buckets = elem.find('./Buckets')
        buckets = resp_buckets.iterchildren('Bucket')
        self.assertEqual(len(list(buckets)), 0)

    @s3acl(s3acl_only=True)
    def test_service_GET_without_owner_bucket(self):
        bucket_list = []
        for var in range(0, 10):
            user_id = 'test:other'
            bucket = 'bucket%s' % var
            owner = Owner(user_id, user_id)
            headers = encode_acl('container', ACL(owner, []))
            self.swift.register('HEAD', '/v1/AUTH_test/%s' % bucket,
                                swob.HTTPNoContent, headers, None)
            bucket_list.append((bucket, var, 300 + var))

        status, headers, body = \
            self._test_service_GET_for_check_bucket_owner(bucket_list)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListAllMyBucketsResult')

        resp_buckets = elem.find('./Buckets')
        buckets = resp_buckets.iterchildren('Bucket')
        self.assertEqual(len(list(buckets)), 0)

    @s3acl(s3acl_only=True)
    def test_service_GET_bucket_list(self):
        bucket_list = []
        for var in range(0, 10):
            if var % 3 == 0:
                user_id = 'test:tester'
            else:
                user_id = 'test:other'
            bucket = 'bucket%s' % var
            owner = Owner(user_id, user_id)
            headers = encode_acl('container', ACL(owner, []))
            # set register to get owner of buckets
            if var % 3 == 2:
                self.swift.register('HEAD', '/v1/AUTH_test/%s' % bucket,
                                    swob.HTTPNotFound, {}, None)
            else:
                self.swift.register('HEAD', '/v1/AUTH_test/%s' % bucket,
                                    swob.HTTPNoContent, headers, None)
            bucket_list.append((bucket, var, 300 + var))

        status, headers, body = \
            self._test_service_GET_for_check_bucket_owner(bucket_list)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body, 'ListAllMyBucketsResult')
        resp_buckets = elem.find('./Buckets')
        buckets = resp_buckets.iterchildren('Bucket')
        listing = list(list(buckets)[0])
        self.assertEqual(len(listing), 2)

        names = []
        for b in resp_buckets.iterchildren('Bucket'):
            names.append(b.find('./Name').text)

        # Check whether getting bucket only locate in multiples of 3 in
        # bucket_list which mean requested user is owner.
        expected_buckets = [b for i, b in enumerate(bucket_list)
                            if i % 3 == 0]
        self.assertEqual(len(names), len(expected_buckets))
        for i in expected_buckets:
            self.assertTrue(i[0] in names)
        self.assertEqual(len(self.swift.calls_with_headers), 11)

if __name__ == '__main__':
    unittest.main()
