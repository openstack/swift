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

import json
import unittest
import urllib.parse
from uuid import uuid4

from test.functional import check_response, cluster_info, retry, \
    requires_acls, load_constraint, requires_policies, SkipTest
import test.functional as tf


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestContainer(unittest.TestCase):

    def setUp(self):
        if tf.skip:
            raise SkipTest
        self.name = uuid4().hex
        # this container isn't created by default, but will be cleaned up
        self.container = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put)
        resp.read()
        # If the request was received and processed but the container-server
        # timed out getting the response back to the proxy, or the proxy timed
        # out getting the response back to the client, the next retry will 202
        self.assertIn(resp.status, (201, 202))

        self.max_meta_count = load_constraint('max_meta_count')
        self.max_meta_name_length = load_constraint('max_meta_name_length')
        self.max_meta_overall_size = load_constraint('max_meta_overall_size')
        self.max_meta_value_length = load_constraint('max_meta_value_length')

    def tearDown(self):
        if tf.skip:
            raise SkipTest

        def get(url, token, parsed, conn, container):
            conn.request(
                'GET', parsed.path + '/' + container + '?format=json', '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, container, obj):
            path = '/'.join([parsed.path, container, obj['name']])
            conn.request('DELETE', path, '', {'X-Auth-Token': token})
            return check_response(conn)

        for container in (self.name, self.container):
            while True:
                resp = retry(get, container)
                body = resp.read()
                if resp.status == 404:
                    break
                self.assertEqual(resp.status // 100, 2, resp.status)
                objs = json.loads(body)
                if not objs:
                    break
                for obj in objs:
                    resp = retry(delete, container, obj)
                    resp.read()
                    # Under load, container listing may not upate immediately,
                    # so we may attempt to delete the same object multiple
                    # times. Tolerate the object having already been deleted.
                    self.assertIn(resp.status, (204, 404))

        def delete(url, token, parsed, conn, container):
            conn.request('DELETE', parsed.path + '/' + container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        for container in (self.name, self.container):
            resp = retry(delete, container)
            resp.read()
            # self.container may not have been created at all, but even if it
            # has, for either container there may be a failure that trips the
            # retry despite the request having been successfully processed.
            self.assertIn(resp.status, (204, 404))

    def test_GET_HEAD_content_type(self):

        def send_req(url, token, parsed, conn, method, container, params):
            qs = '?%s' % urllib.parse.urlencode(params) if params else ''
            conn.request(method, parsed.path + '/' + container + qs, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(send_req, 'GET', self.name, {})
        # GET is still 204 if there's no objects!?
        self.assertEqual(resp.status, 204)
        # we respond text/plain by default
        self.assertEqual(resp.getheader('Content-Type'),
                         'text/plain; charset=utf-8')
        self.assertEqual(resp.getheader('Content-Length'), '0')

        resp = retry(send_req, 'HEAD', self.name, {})
        # HEAD will *always* 204
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('Content-Type'),
                         'text/plain; charset=utf-8')
        self.assertEqual(resp.getheader('Content-Length'), '0')

        def put_object(url, token, parsed, conn, container, obj_name):
            conn.request('PUT', '/'.join((parsed.path, container, obj_name)),
                         '', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put_object, self.name, 'obj1')
        self.assertEqual(resp.status, 201)

        resp = retry(send_req, 'GET', self.name, {})
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('Content-Type'),
                         'text/plain; charset=utf-8')

        resp = retry(send_req, 'HEAD', self.name, {})
        # HEAD will *always* 204
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('Content-Type'),
                         'text/plain; charset=utf-8')
        self.assertEqual(resp.getheader('Content-Length'), '0')

        # and we can ask for our preferred encoding format
        resp = retry(send_req, 'GET', self.name, {'format': 'json'})
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('Content-Type'),
                         'application/json; charset=utf-8')
        resp = retry(send_req, 'HEAD', self.name, {'format': 'json'})
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('Content-Type'),
                         'application/json; charset=utf-8')
        self.assertEqual(resp.getheader('Content-Length'), '0')

        resp = retry(send_req, 'GET', self.name, {'format': 'xml'})
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('Content-Type'),
                         'application/xml; charset=utf-8')
        resp = retry(send_req, 'HEAD', self.name, {'format': 'xml'})
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('Content-Type'),
                         'application/xml; charset=utf-8')
        self.assertEqual(resp.getheader('Content-Length'), '0')

    def test_multi_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, name, value):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token, name: value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(post, 'X-Container-Meta-One', '1')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertEqual(resp.getheader('x-container-meta-one'), '1')
        resp = retry(post, 'X-Container-Meta-Two', '2')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertEqual(resp.getheader('x-container-meta-one'), '1')
        self.assertEqual(resp.getheader('x-container-meta-two'), '2')

    def test_unicode_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, name, value):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token, name: value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        uni_value = u'uni\u0E12'
        # Note that py3 has issues with non-ascii header names; see
        # https://bugs.python.org/issue37093 -- so we won't test with unicode
        # header names
        resp = retry(post, 'X-Container-Meta-uni', uni_value)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertEqual(resp.getheader('X-Container-Meta-uni'),
                         uni_value)

    def test_PUT_metadata(self):
        if tf.skip:
            raise SkipTest

        def put(url, token, parsed, conn, name, value):
            conn.request('PUT', parsed.path + '/' + name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Meta-Test': value})
            return check_response(conn)

        def head(url, token, parsed, conn, name):
            conn.request('HEAD', parsed.path + '/' + name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def get(url, token, parsed, conn, name):
            conn.request('GET', parsed.path + '/' + name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', parsed.path + '/' + name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        name = uuid4().hex
        resp = retry(put, name, 'Value')
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(head, name)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')
        resp = retry(get, name)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')
        resp = retry(delete, name)
        resp.read()
        self.assertIn(resp.status, (204, 404))

        name = uuid4().hex
        resp = retry(put, name, '')
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(head, name)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertIsNone(resp.getheader('x-container-meta-test'))
        resp = retry(get, name)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertIsNone(resp.getheader('x-container-meta-test'))
        resp = retry(delete, name)
        resp.read()
        self.assertIn(resp.status, (204, 404))

    def test_POST_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, value):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Meta-Test': value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(head)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertIsNone(resp.getheader('x-container-meta-test'))
        resp = retry(get)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertIsNone(resp.getheader('x-container-meta-test'))
        resp = retry(post, 'Value')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')
        resp = retry(get)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')

    def test_PUT_bad_metadata(self):
        if tf.skip:
            raise SkipTest

        def put(url, token, parsed, conn, name, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('PUT', parsed.path + '/' + name, '', headers)
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', parsed.path + '/' + name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        name = uuid4().hex
        resp = retry(
            put, name,
            {'X-Container-Meta-' + ('k' * self.max_meta_name_length): 'v'})
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(delete, name)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        name = uuid4().hex
        resp = retry(
            put, name,
            {'X-Container-Meta-' + (
                'k' * (self.max_meta_name_length + 1)): 'v'})
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

        name = uuid4().hex
        resp = retry(
            put, name,
            {'X-Container-Meta-Too-Long': 'k' * self.max_meta_value_length})
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(delete, name)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        name = uuid4().hex
        resp = retry(
            put, name,
            {'X-Container-Meta-Too-Long': 'k' * (
                self.max_meta_value_length + 1)})
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

        name = uuid4().hex
        headers = {}
        for x in range(self.max_meta_count):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(put, name, headers)
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(delete, name)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        name = uuid4().hex
        headers = {}
        for x in range(self.max_meta_count + 1):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(put, name, headers)
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

        name = uuid4().hex
        headers = {}
        header_value = 'k' * self.max_meta_value_length
        size = 0
        x = 0
        while size < (self.max_meta_overall_size - 4
                      - self.max_meta_value_length):
            size += 4 + self.max_meta_value_length
            headers['X-Container-Meta-%04d' % x] = header_value
            x += 1
        if self.max_meta_overall_size - size > 1:
            headers['X-Container-Meta-k'] = \
                'v' * (self.max_meta_overall_size - size - 1)
        resp = retry(put, name, headers)
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(delete, name)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        name = uuid4().hex
        headers['X-Container-Meta-k'] = \
            'v' * (self.max_meta_overall_size - size)
        resp = retry(put, name, headers)
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

    def test_POST_bad_metadata(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path + '/' + self.name, '', headers)
            return check_response(conn)

        resp = retry(
            post,
            {'X-Container-Meta-' + ('k' * self.max_meta_name_length): 'v'})
        resp.read()
        self.assertEqual(resp.status, 204)
        # Clear it, so the value-length checking doesn't accidentally trip
        # the overall max
        resp = retry(
            post,
            {'X-Container-Meta-' + ('k' * self.max_meta_name_length): ''})
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(
            post,
            {'X-Container-Meta-' + (
                'k' * (self.max_meta_name_length + 1)): 'v'})
        resp.read()
        self.assertEqual(resp.status, 400)

        resp = retry(
            post,
            {'X-Container-Meta-Too-Long': 'k' * self.max_meta_value_length})
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(
            post,
            {'X-Container-Meta-Too-Long': 'k' * (
                self.max_meta_value_length + 1)})
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_POST_bad_metadata2(self):
        if tf.skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path + '/' + self.name, '', headers)
            return check_response(conn)

        headers = {}
        for x in range(self.max_meta_count):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 204)
        headers = {}
        for x in range(self.max_meta_count + 1):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_POST_bad_metadata3(self):
        if tf.skip:
            raise SkipTest

        if tf.in_process:
            tf.skip_if_no_xattrs()

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path + '/' + self.name, '', headers)
            return check_response(conn)

        headers = {}
        header_value = 'k' * self.max_meta_value_length
        size = 0
        x = 0
        while size < (self.max_meta_overall_size - 4
                      - self.max_meta_value_length):
            size += 4 + self.max_meta_value_length
            headers['X-Container-Meta-%04d' % x] = header_value
            x += 1
        if self.max_meta_overall_size - size > 1:
            headers['X-Container-Meta-k'] = \
                'v' * (self.max_meta_overall_size - size - 1)
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 204)
        # this POST includes metadata size that is over limit
        headers['X-Container-Meta-k'] = \
            'x' * (self.max_meta_overall_size - size)
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 400)
        # this POST would be ok and the aggregate backend metadata
        # size is on the border
        headers = {'X-Container-Meta-k':
                   'y' * (self.max_meta_overall_size - size - 1)}
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 204)
        # this last POST would be ok by itself but takes the aggregate
        # backend metadata size over limit
        headers = {'X-Container-Meta-k':
                   'z' * (self.max_meta_overall_size - size)}
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_public_container(self):
        if tf.skip:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path + '/' + self.name)
            return check_response(conn)

        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception as err:
            self.assertTrue(str(err).startswith('No result after '), err)

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': '.r:*,.rlistings'})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get)
        resp.read()
        self.assertEqual(resp.status, 204)

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token, 'X-Container-Read': ''})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception as err:
            self.assertTrue(str(err).startswith('No result after '), err)

    def test_cross_account_container(self):
        if tf.skip or tf.skip2:
            raise SkipTest
        # Obtain the first account's string
        first_account = ['unknown']

        def get1(url, token, parsed, conn):
            first_account[0] = parsed.path
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get1)
        resp.read()

        # Ensure we can't access the container with the second account
        def get2(url, token, parsed, conn):
            conn.request('GET', first_account[0] + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 403)

        # Make the container accessible by the second account
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': tf.swift_test_perm[1],
                          'X-Container-Write': tf.swift_test_perm[1]})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        # Ensure we can now use the container with the second account
        resp = retry(get2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 204)

        # Make the container private again
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token, 'X-Container-Read': '',
                          'X-Container-Write': ''})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        # Ensure we can't access the container with the second account again
        resp = retry(get2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 403)

    def test_cross_account_public_container(self):
        if tf.skip or tf.skip2:
            raise SkipTest

        if tf.in_process:
            tf.skip_if_no_xattrs()
        # Obtain the first account's string
        first_account = ['unknown']

        def get1(url, token, parsed, conn):
            first_account[0] = parsed.path
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get1)
        resp.read()

        # Ensure we can't access the container with the second account
        def get2(url, token, parsed, conn):
            conn.request('GET', first_account[0] + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 403)

        # Make the container completely public
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': '.r:*,.rlistings'})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        # Ensure we can now read the container with the second account
        resp = retry(get2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 204)

        # But we shouldn't be able to write with the second account
        def put2(url, token, parsed, conn):
            conn.request('PUT', first_account[0] + '/' + self.name + '/object',
                         'test object', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 403)

        # Now make the container also writable by the second account
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Write': tf.swift_test_perm[1]})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        # Ensure we can still read the container with the second account
        resp = retry(get2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 204)
        # And that we can now write with the second account
        resp = retry(put2, use_account=2)
        resp.read()
        self.assertEqual(resp.status, 201)

    def test_nonadmin_user(self):
        if tf.skip or tf.skip3:
            raise SkipTest

        if tf.in_process:
            tf.skip_if_no_xattrs()
        # Obtain the first account's string
        first_account = ['unknown']

        def get1(url, token, parsed, conn):
            first_account[0] = parsed.path
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get1)
        resp.read()

        # Ensure we can't access the container with the third account
        def get3(url, token, parsed, conn):
            conn.request('GET', first_account[0] + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get3, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # Make the container accessible by the third account
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': tf.swift_test_perm[2]})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        # Ensure we can now read the container with the third account
        resp = retry(get3, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)

        # But we shouldn't be able to write with the third account
        def put3(url, token, parsed, conn):
            conn.request('PUT', first_account[0] + '/' + self.name + '/object',
                         'test object', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put3, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # Now make the container also writable by the third account
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Write': tf.swift_test_perm[2]})
            return check_response(conn)

        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        # Ensure we can still read the container with the third account
        resp = retry(get3, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        # And that we can now write with the third account
        resp = retry(put3, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 201)

    @requires_acls
    def test_read_only_acl_listings(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list containers
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-only access
        acl_user = tf.swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-only can list containers
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(self.name, listing)

        # read-only can not create containers
        new_container_name = str(uuid4())
        resp = retry(put, new_container_name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # but it can see newly created ones
        resp = retry(put, new_container_name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(new_container_name, listing)

    @requires_acls
    def test_read_only_acl_metadata(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn, name):
            conn.request('GET', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def post(url, token, parsed, conn, name, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path + '/%s' % name, '', new_headers)
            return check_response(conn)

        # add some metadata
        value = str(uuid4())
        headers = {'x-container-meta-test': value}
        resp = retry(post, self.name, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # cannot see metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-only access
        acl_user = tf.swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-only can NOT write container metadata
        new_value = str(uuid4())
        headers = {'x-container-meta-test': new_value}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # read-only can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

    @requires_acls
    def test_read_write_acl_listings(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list containers
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-write access
        acl_user = tf.swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list containers
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(self.name, listing)

        # can create new containers
        new_container_name = str(uuid4())
        resp = retry(put, new_container_name, use_account=3)
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(new_container_name, listing)

        # can also delete them
        resp = retry(delete, new_container_name, use_account=3)
        resp.read()
        self.assertIn(resp.status, (204, 404))
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertNotIn(new_container_name, listing)

        # even if they didn't create them
        empty_container_name = str(uuid4())
        resp = retry(put, empty_container_name, use_account=1)
        resp.read()
        self.assertIn(resp.status, (201, 202))
        resp = retry(delete, empty_container_name, use_account=3)
        resp.read()
        self.assertIn(resp.status, (204, 404))

    @requires_acls
    def test_read_write_acl_metadata(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn, name):
            conn.request('GET', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def post(url, token, parsed, conn, name, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path + '/%s' % name, '', new_headers)
            return check_response(conn)

        # add some metadata
        value = str(uuid4())
        headers = {'x-container-meta-test': value}
        resp = retry(post, self.name, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # cannot see metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-write access
        acl_user = tf.swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-write can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # read-write can also write container metadata
        new_value = str(uuid4())
        headers = {'x-container-meta-test': new_value}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)

        # and remove it
        headers = {'x-remove-container-meta-test': 'true'}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertIsNone(resp.getheader('X-Container-Meta-Test'))

    @requires_acls
    def test_admin_acl_listing(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list containers
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant admin access
        acl_user = tf.swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list containers
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(self.name, listing)

        # can create new containers
        new_container_name = str(uuid4())
        resp = retry(put, new_container_name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertIn(new_container_name, listing)

        # can also delete them
        resp = retry(delete, new_container_name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, use_account=3)
        listing = resp.read().decode('utf8')
        self.assertEqual(resp.status, 200)
        self.assertNotIn(new_container_name, listing)

        # even if they didn't create them
        empty_container_name = str(uuid4())
        resp = retry(put, empty_container_name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(delete, empty_container_name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)

    @requires_acls
    def test_admin_acl_metadata(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn, name):
            conn.request('GET', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def post(url, token, parsed, conn, name, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path + '/%s' % name, '', new_headers)
            return check_response(conn)

        # add some metadata
        value = str(uuid4())
        headers = {'x-container-meta-test': value}
        resp = retry(post, self.name, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # cannot see metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant access
        acl_user = tf.swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # can also write container metadata
        new_value = str(uuid4())
        headers = {'x-container-meta-test': new_value}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)

        # and remove it
        headers = {'x-remove-container-meta-test': 'true'}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertIsNone(resp.getheader('X-Container-Meta-Test'))

    @requires_acls
    def test_protected_container_sync(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn, name):
            conn.request('GET', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def post(url, token, parsed, conn, name, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path + '/%s' % name, '', new_headers)
            return check_response(conn)

        # add some metadata
        value = str(uuid4())
        headers = {
            'x-container-sync-key': 'secret',
            'x-container-meta-test': value,
        }
        resp = retry(post, self.name, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), 'secret')
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # grant read-only access
        acl_user = tf.swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not sync-key
        self.assertIsNone(resp.getheader('X-Container-Sync-Key'))

        # and can not write
        headers = {'x-container-sync-key': str(uuid4())}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-write access
        acl_user = tf.swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not sync-key
        self.assertIsNone(resp.getheader('X-Container-Sync-Key'))

        # sanity check sync-key w/ account1
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), 'secret')

        # and can write
        new_value = str(uuid4())
        headers = {
            'x-container-sync-key': str(uuid4()),
            'x-container-meta-test': new_value,
        }
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=1)  # validate w/ account1
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)
        # but can not write sync-key
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), 'secret')

        # grant admin access
        acl_user = tf.swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # admin can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)
        # and ALSO sync-key
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), 'secret')

        # admin tester3 can even change sync-key
        new_secret = str(uuid4())
        headers = {'x-container-sync-key': new_secret}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), new_secret)

    @requires_acls
    def test_protected_container_acl(self):
        if tf.skip3:
            raise SkipTest

        def get(url, token, parsed, conn, name):
            conn.request('GET', parsed.path + '/%s' % name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def post(url, token, parsed, conn, name, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path + '/%s' % name, '', new_headers)
            return check_response(conn)

        # add some container acls
        value = str(uuid4())
        headers = {
            'x-container-read': 'jdoe',
            'x-container-write': 'jdoe',
            'x-container-meta-test': value,
        }
        resp = retry(post, self.name, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Read'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Write'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # grant read-only access
        acl_user = tf.swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not container acl
        self.assertIsNone(resp.getheader('X-Container-Read'))
        self.assertIsNone(resp.getheader('X-Container-Write'))

        # and can not write
        headers = {
            'x-container-read': 'frank',
            'x-container-write': 'frank',
        }
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-write access
        acl_user = tf.swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not container acl
        self.assertIsNone(resp.getheader('X-Container-Read'))
        self.assertIsNone(resp.getheader('X-Container-Write'))

        # sanity check container acls with account1
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Read'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Write'), 'jdoe')

        # and can write
        new_value = str(uuid4())
        headers = {
            'x-container-read': 'frank',
            'x-container-write': 'frank',
            'x-container-meta-test': new_value,
        }
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=1)  # validate w/ account1
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)
        # but can not write container acls
        self.assertEqual(resp.getheader('X-Container-Read'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Write'), 'jdoe')

        # grant admin access
        acl_user = tf.swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # admin can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)
        # and ALSO container acls
        self.assertEqual(resp.getheader('X-Container-Read'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Write'), 'jdoe')

        # admin tester3 can even change container acls
        new_value = str(uuid4())
        headers = {
            'x-container-read': '.r:*',
        }
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Read'), '.r:*')

    def test_long_name_content_type(self):
        if tf.skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            container_name = 'X' * 2048
            conn.request('PUT', '%s/%s' % (parsed.path, container_name),
                         'there', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 400)
        self.assertEqual(resp.getheader('Content-Type'),
                         'text/html; charset=UTF-8')

    def test_null_name(self):
        if tf.skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/abc%%00def' % parsed.path, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put)
        if (tf.web_front_end == 'apache2'):
            self.assertEqual(resp.status, 404)
        else:
            self.assertEqual(resp.read(), b'Invalid UTF8 or contains NULL')
            self.assertEqual(resp.status, 412)

    def test_create_container_gets_default_policy_by_default(self):
        try:
            default_policy = \
                tf.FunctionalStoragePolicyCollection.from_info().default
        except AssertionError:
            raise SkipTest()

        def put(url, token, parsed, conn):
            # using the empty storage policy header value here to ensure
            # that the default policy is chosen in case policy_specified is set
            # see __init__.py for details on policy_specified
            conn.request('PUT', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token, 'X-Storage-Policy': ''})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status // 100, 2)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(head)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('x-storage-policy'),
                         default_policy['name'])

    def test_error_invalid_storage_policy_name(self):
        def put(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('PUT', parsed.path + '/' + self.container, '',
                         new_headers)
            return check_response(conn)

        # create
        resp = retry(put, {'X-Storage-Policy': uuid4().hex})
        resp.read()
        self.assertEqual(resp.status, 400)

    @requires_policies
    def test_create_non_default_storage_policy_container(self):
        policy = self.policies.exclude(default=True).select()

        def put(url, token, parsed, conn, headers=None):
            base_headers = {'X-Auth-Token': token}
            if headers:
                base_headers.update(headers)
            conn.request('PUT', parsed.path + '/' + self.container, '',
                         base_headers)
            return check_response(conn)
        headers = {'X-Storage-Policy': policy['name']}
        resp = retry(put, headers=headers)
        resp.read()
        self.assertEqual(resp.status, 201)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(head)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('x-storage-policy'),
                         policy['name'])

        # and test recreate with-out specifying Storage Policy
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 202)
        # should still be original storage policy
        resp = retry(head)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('x-storage-policy'),
                         policy['name'])

        # delete it
        def delete(url, token, parsed, conn):
            conn.request('DELETE', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)

        # verify no policy header
        resp = retry(head)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertIsNone(headers.get('x-storage-policy'))

    @requires_policies
    def test_conflict_change_storage_policy_with_put(self):
        def put(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('PUT', parsed.path + '/' + self.container, '',
                         new_headers)
            return check_response(conn)

        # create
        policy = self.policies.select()
        resp = retry(put, {'X-Storage-Policy': policy['name']})
        resp.read()
        self.assertEqual(resp.status, 201)

        # can't change it
        other_policy = self.policies.exclude(name=policy['name']).select()
        resp = retry(put, {'X-Storage-Policy': other_policy['name']})
        resp.read()
        self.assertEqual(resp.status, 409)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        # still original policy
        resp = retry(head)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('x-storage-policy'),
                         policy['name'])

    @requires_policies
    def test_noop_change_storage_policy_with_post(self):
        def put(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('PUT', parsed.path + '/' + self.container, '',
                         new_headers)
            return check_response(conn)

        # create
        policy = self.policies.select()
        resp = retry(put, {'X-Storage-Policy': policy['name']})
        resp.read()
        self.assertEqual(resp.status, 201)

        def post(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path + '/' + self.container, '',
                         new_headers)
            return check_response(conn)
        # attempt update
        for header in ('X-Storage-Policy', 'X-Storage-Policy-Index'):
            other_policy = self.policies.exclude(name=policy['name']).select()
            resp = retry(post, {header: other_policy['name']})
            resp.read()
            self.assertEqual(resp.status, 204)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        # still original policy
        resp = retry(head)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEqual(headers.get('x-storage-policy'),
                         policy['name'])

    def test_container_quota_bytes(self):
        if 'container_quotas' not in cluster_info:
            raise SkipTest('Container quotas not enabled')

        if tf.in_process:
            tf.skip_if_no_xattrs()

        def post(url, token, parsed, conn, name, value):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token, name: value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        # set X-Container-Meta-Quota-Bytes is 10
        resp = retry(post, 'X-Container-Meta-Quota-Bytes', '10')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assertIn(resp.status, (200, 204))
        # confirm X-Container-Meta-Quota-Bytes
        self.assertEqual(resp.getheader('X-Container-Meta-Quota-Bytes'), '10')

        def put(url, token, parsed, conn, data):
            conn.request('PUT', parsed.path + '/' + self.name + '/object',
                         data, {'X-Auth-Token': token})
            return check_response(conn)

        # upload 11 bytes object
        resp = retry(put, b'01234567890')
        resp.read()
        self.assertEqual(resp.status, 413)

        # upload 10 bytes object
        resp = retry(put, b'0123456789')
        resp.read()
        self.assertEqual(resp.status, 201)

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path + '/' + self.name + '/object',
                         '', {'X-Auth-Token': token})
            return check_response(conn)

        # download 10 bytes object
        resp = retry(get)
        body = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(body, b'0123456789')


class BaseTestContainerACLs(unittest.TestCase):
    # subclasses can change the account in which container
    # is created/deleted by setUp/tearDown
    account = 1

    def _get_account(self, url, token, parsed, conn):
        return parsed.path

    def _get_tenant_id(self, url, token, parsed, conn):
        account = parsed.path
        return account.replace('/v1/AUTH_', '', 1)

    def setUp(self):
        if tf.skip or tf.skip2 or tf.skip_if_not_v3:
            raise SkipTest('AUTH VERSION 3 SPECIFIC TEST')
        self.name = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put, use_account=self.account)
        resp.read()
        self.assertEqual(resp.status, 201)

    def tearDown(self):
        if tf.skip or tf.skip2 or tf.skip_if_not_v3:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path + '/' + self.name + '?format=json',
                         '', {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, obj):
            conn.request('DELETE',
                         '/'.join([parsed.path, self.name, obj['name']]), '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        while True:
            resp = retry(get, use_account=self.account)
            body = resp.read()
            self.assertEqual(resp.status // 100, 2, resp.status)
            objs = json.loads(body)
            if not objs:
                break
            for obj in objs:
                resp = retry(delete, obj, use_account=self.account)
                resp.read()
                self.assertEqual(resp.status, 204)

        def delete(url, token, parsed, conn):
            conn.request('DELETE', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(delete, use_account=self.account)
        resp.read()
        self.assertEqual(resp.status, 204)

    def _assert_cross_account_acl_granted(self, granted, grantee_account, acl):
        '''
        Check whether a given container ACL is granted when a user specified
        by account_b attempts to access a container.
        '''
        # Obtain the first account's string
        first_account = retry(self._get_account, use_account=self.account)

        # Ensure we can't access the container with the grantee account
        def get2(url, token, parsed, conn):
            conn.request('GET', first_account + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get2, use_account=grantee_account)
        resp.read()
        self.assertEqual(resp.status, 403)

        def put2(url, token, parsed, conn):
            conn.request('PUT', first_account + '/' + self.name + '/object',
                         'test object', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put2, use_account=grantee_account)
        resp.read()
        self.assertEqual(resp.status, 403)

        # Post ACL to the container
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': acl,
                          'X-Container-Write': acl})
            return check_response(conn)

        resp = retry(post, use_account=self.account)
        resp.read()
        self.assertEqual(resp.status, 204)

        # Check access to container from grantee account with ACL in place
        resp = retry(get2, use_account=grantee_account)
        resp.read()
        expected = 204 if granted else 403
        self.assertEqual(resp.status, expected)

        resp = retry(put2, use_account=grantee_account)
        resp.read()
        expected = 201 if granted else 403
        self.assertEqual(resp.status, expected)

        # Make the container private again
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token, 'X-Container-Read': '',
                          'X-Container-Write': ''})
            return check_response(conn)

        resp = retry(post, use_account=self.account)
        resp.read()
        self.assertEqual(resp.status, 204)

        # Ensure we can't access the container with the grantee account again
        resp = retry(get2, use_account=grantee_account)
        resp.read()
        self.assertEqual(resp.status, 403)

        resp = retry(put2, use_account=grantee_account)
        resp.read()
        self.assertEqual(resp.status, 403)


class TestContainerACLsAccount1(BaseTestContainerACLs):
    def test_cross_account_acl_names_with_user_in_non_default_domain(self):
        # names in acls are disallowed when grantee is in a non-default domain
        acl = '%s:%s' % (tf.swift_test_tenant[3], tf.swift_test_user[3])
        self._assert_cross_account_acl_granted(False, 4, acl)

    def test_cross_account_acl_ids_with_user_in_non_default_domain(self):
        # ids are allowed in acls when grantee is in a non-default domain
        tenant_id = retry(self._get_tenant_id, use_account=4)
        acl = '%s:%s' % (tenant_id, '*')
        self._assert_cross_account_acl_granted(True, 4, acl)

    def test_cross_account_acl_names_in_default_domain(self):
        # names are allowed in acls when grantee and project are in
        # the default domain
        acl = '%s:%s' % (tf.swift_test_tenant[1], tf.swift_test_user[1])
        self._assert_cross_account_acl_granted(True, 2, acl)

    def test_cross_account_acl_ids_in_default_domain(self):
        # ids are allowed in acls when grantee and project are in
        # the default domain
        tenant_id = retry(self._get_tenant_id, use_account=2)
        acl = '%s:%s' % (tenant_id, '*')
        self._assert_cross_account_acl_granted(True, 2, acl)


class TestContainerACLsAccount4(BaseTestContainerACLs):
    account = 4

    def test_cross_account_acl_names_with_project_in_non_default_domain(self):
        # names in acls are disallowed when project is in a non-default domain
        acl = '%s:%s' % (tf.swift_test_tenant[0], tf.swift_test_user[0])
        self._assert_cross_account_acl_granted(False, 1, acl)

    def test_cross_account_acl_ids_with_project_in_non_default_domain(self):
        # ids are allowed in acls when project is in a non-default domain
        tenant_id = retry(self._get_tenant_id, use_account=1)
        acl = '%s:%s' % (tenant_id, '*')
        self._assert_cross_account_acl_granted(True, 1, acl)


if __name__ == '__main__':
    unittest.main()
