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
from nose import SkipTest
from uuid import uuid4

from swift.common.constraints import MAX_META_COUNT, MAX_META_NAME_LENGTH, \
    MAX_META_OVERALL_SIZE, MAX_META_VALUE_LENGTH

from swift_testing import check_response, retry, skip, skip2, skip3, \
    swift_test_perm, web_front_end, requires_acls, swift_test_user


class TestContainer(unittest.TestCase):

    def setUp(self):
        if skip:
            raise SkipTest
        self.name = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

    def tearDown(self):
        if skip:
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
            resp = retry(get)
            body = resp.read()
            self.assert_(resp.status // 100 == 2, resp.status)
            objs = json.loads(body)
            if not objs:
                break
            for obj in objs:
                resp = retry(delete, obj)
                resp.read()
                self.assertEqual(resp.status, 204)

        def delete(url, token, parsed, conn):
            conn.request('DELETE', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)

    def test_multi_metadata(self):
        if skip:
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
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-one'), '1')
        resp = retry(post, 'X-Container-Meta-Two', '2')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-one'), '1')
        self.assertEqual(resp.getheader('x-container-meta-two'), '2')

    def test_unicode_metadata(self):
        if skip:
            raise SkipTest

        def post(url, token, parsed, conn, name, value):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token, name: value})
            return check_response(conn)

        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        uni_key = u'X-Container-Meta-uni\u0E12'
        uni_value = u'uni\u0E12'
        if (web_front_end == 'integral'):
            resp = retry(post, uni_key, '1')
            resp.read()
            self.assertEqual(resp.status, 204)
            resp = retry(head)
            resp.read()
            self.assert_(resp.status in (200, 204), resp.status)
            self.assertEqual(resp.getheader(uni_key.encode('utf-8')), '1')
        resp = retry(post, 'X-Container-Meta-uni', uni_value)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('X-Container-Meta-uni'),
                         uni_value.encode('utf-8'))
        if (web_front_end == 'integral'):
            resp = retry(post, uni_key, uni_value)
            resp.read()
            self.assertEqual(resp.status, 204)
            resp = retry(head)
            resp.read()
            self.assert_(resp.status in (200, 204), resp.status)
            self.assertEqual(resp.getheader(uni_key.encode('utf-8')),
                             uni_value.encode('utf-8'))

    def test_PUT_metadata(self):
        if skip:
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
        self.assertEqual(resp.status, 201)
        resp = retry(head, name)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')
        resp = retry(get, name)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 204)

        name = uuid4().hex
        resp = retry(put, name, '')
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(head, name)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), None)
        resp = retry(get, name)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), None)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 204)

    def test_POST_metadata(self):
        if skip:
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
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), None)
        resp = retry(get)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), None)
        resp = retry(post, 'Value')
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')
        resp = retry(get)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEqual(resp.getheader('x-container-meta-test'), 'Value')

    def test_PUT_bad_metadata(self):
        if skip:
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
            {'X-Container-Meta-' + ('k' * MAX_META_NAME_LENGTH): 'v'})
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 204)
        name = uuid4().hex
        resp = retry(
            put, name,
            {'X-Container-Meta-' + ('k' * (MAX_META_NAME_LENGTH + 1)): 'v'})
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

        name = uuid4().hex
        resp = retry(
            put, name,
            {'X-Container-Meta-Too-Long': 'k' * MAX_META_VALUE_LENGTH})
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 204)
        name = uuid4().hex
        resp = retry(
            put, name,
            {'X-Container-Meta-Too-Long': 'k' * (MAX_META_VALUE_LENGTH + 1)})
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

        name = uuid4().hex
        headers = {}
        for x in xrange(MAX_META_COUNT):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(put, name, headers)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 204)
        name = uuid4().hex
        headers = {}
        for x in xrange(MAX_META_COUNT + 1):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(put, name, headers)
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

        name = uuid4().hex
        headers = {}
        header_value = 'k' * MAX_META_VALUE_LENGTH
        size = 0
        x = 0
        while size < MAX_META_OVERALL_SIZE - 4 - MAX_META_VALUE_LENGTH:
            size += 4 + MAX_META_VALUE_LENGTH
            headers['X-Container-Meta-%04d' % x] = header_value
            x += 1
        if MAX_META_OVERALL_SIZE - size > 1:
            headers['X-Container-Meta-k'] = \
                'v' * (MAX_META_OVERALL_SIZE - size - 1)
        resp = retry(put, name, headers)
        resp.read()
        self.assertEqual(resp.status, 201)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 204)
        name = uuid4().hex
        headers['X-Container-Meta-k'] = \
            'v' * (MAX_META_OVERALL_SIZE - size)
        resp = retry(put, name, headers)
        resp.read()
        self.assertEqual(resp.status, 400)
        resp = retry(delete, name)
        resp.read()
        self.assertEqual(resp.status, 404)

    def test_POST_bad_metadata(self):
        if skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path + '/' + self.name, '', headers)
            return check_response(conn)

        resp = retry(
            post,
            {'X-Container-Meta-' + ('k' * MAX_META_NAME_LENGTH): 'v'})
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(
            post,
            {'X-Container-Meta-' + ('k' * (MAX_META_NAME_LENGTH + 1)): 'v'})
        resp.read()
        self.assertEqual(resp.status, 400)

        resp = retry(
            post,
            {'X-Container-Meta-Too-Long': 'k' * MAX_META_VALUE_LENGTH})
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(
            post,
            {'X-Container-Meta-Too-Long': 'k' * (MAX_META_VALUE_LENGTH + 1)})
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_POST_bad_metadata2(self):
        if skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path + '/' + self.name, '', headers)
            return check_response(conn)

        headers = {}
        for x in xrange(MAX_META_COUNT):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 204)
        headers = {}
        for x in xrange(MAX_META_COUNT + 1):
            headers['X-Container-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_POST_bad_metadata3(self):
        if skip:
            raise SkipTest

        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path + '/' + self.name, '', headers)
            return check_response(conn)

        headers = {}
        header_value = 'k' * MAX_META_VALUE_LENGTH
        size = 0
        x = 0
        while size < MAX_META_OVERALL_SIZE - 4 - MAX_META_VALUE_LENGTH:
            size += 4 + MAX_META_VALUE_LENGTH
            headers['X-Container-Meta-%04d' % x] = header_value
            x += 1
        if MAX_META_OVERALL_SIZE - size > 1:
            headers['X-Container-Meta-k'] = \
                'v' * (MAX_META_OVERALL_SIZE - size - 1)
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 204)
        headers['X-Container-Meta-k'] = \
            'v' * (MAX_META_OVERALL_SIZE - size)
        resp = retry(post, headers)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_public_container(self):
        if skip:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path + '/' + self.name)
            return check_response(conn)

        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception as err:
            self.assert_(str(err).startswith('No result after '), err)

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
            self.assert_(str(err).startswith('No result after '), err)

    def test_cross_account_container(self):
        if skip or skip2:
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
                          'X-Container-Read': swift_test_perm[1],
                          'X-Container-Write': swift_test_perm[1]})
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
        if skip or skip2:
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

        # Now make the container also writeable by the second account
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Write': swift_test_perm[1]})
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
        if skip or skip3:
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
                          'X-Container-Read': swift_test_perm[2]})
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

        # Now make the container also writeable by the third account
        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.name, '',
                         {'X-Auth-Token': token,
                          'X-Container-Write': swift_test_perm[2]})
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
        if skip3:
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
        self.assertEquals(resp.status, 403)

        # grant read-only access
        acl_user = swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-only can list containers
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(self.name in listing)

        # read-only can not create containers
        new_container_name = str(uuid4())
        resp = retry(put, new_container_name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # but it can see newly created ones
        resp = retry(put, new_container_name, use_account=1)
        resp.read()
        self.assertEquals(resp.status, 201)
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(new_container_name in listing)

    @requires_acls
    def test_read_only_acl_metadata(self):
        if skip3:
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # cannot see metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant read-only access
        acl_user = swift_test_user[2]
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

    @requires_acls
    def test_read_write_acl_listings(self):
        if skip3:
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
        self.assertEquals(resp.status, 403)

        # grant read-write access
        acl_user = swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list containers
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(self.name in listing)

        # can create new containers
        new_container_name = str(uuid4())
        resp = retry(put, new_container_name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 201)
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(new_container_name in listing)

        # can also delete them
        resp = retry(delete, new_container_name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(new_container_name not in listing)

        # even if they didn't create them
        empty_container_name = str(uuid4())
        resp = retry(put, empty_container_name, use_account=1)
        resp.read()
        self.assertEquals(resp.status, 201)
        resp = retry(delete, empty_container_name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)

    @requires_acls
    def test_read_write_acl_metadata(self):
        if skip3:
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # cannot see metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant read-write access
        acl_user = swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # read-write can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # read-write can also write container metadata
        new_value = str(uuid4())
        headers = {'x-container-meta-test': new_value}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)

        # and remove it
        headers = {'x-remove-container-meta-test': 'true'}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), None)

    @requires_acls
    def test_admin_acl_listing(self):
        if skip3:
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
        self.assertEquals(resp.status, 403)

        # grant admin access
        acl_user = swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list containers
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(self.name in listing)

        # can create new containers
        new_container_name = str(uuid4())
        resp = retry(put, new_container_name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 201)
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(new_container_name in listing)

        # can also delete them
        resp = retry(delete, new_container_name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(get, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(new_container_name not in listing)

        # even if they didn't create them
        empty_container_name = str(uuid4())
        resp = retry(put, empty_container_name, use_account=1)
        resp.read()
        self.assertEquals(resp.status, 201)
        resp = retry(delete, empty_container_name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)

    @requires_acls
    def test_admin_acl_metadata(self):
        if skip3:
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # cannot see metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant access
        acl_user = swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # can also write container metadata
        new_value = str(uuid4())
        headers = {'x-container-meta-test': new_value}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)

        # and remove it
        headers = {'x-remove-container-meta-test': 'true'}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), None)

    @requires_acls
    def test_protected_container_sync(self):
        if skip3:
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), 'secret')
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # grant read-only access
        acl_user = swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not sync-key
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), None)

        # and can not write
        headers = {'x-container-sync-key': str(uuid4())}
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-write access
        acl_user = swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not sync-key
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), None)

        # sanity check sync-key w/ account1
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEquals(resp.status, 204)
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)
        # but can not write sync-key
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), 'secret')

        # grant admin access
        acl_user = swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # admin can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Sync-Key'), new_secret)

    @requires_acls
    def test_protected_container_acl(self):
        if skip3:
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Read'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Write'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)

        # grant read-only access
        acl_user = swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not container acl
        self.assertEqual(resp.getheader('X-Container-Read'), None)
        self.assertEqual(resp.getheader('X-Container-Write'), None)

        # and can not write
        headers = {
            'x-container-read': 'frank',
            'x-container-write': 'frank',
        }
        resp = retry(post, self.name, headers=headers, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # grant read-write access
        acl_user = swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), value)
        # but not container acl
        self.assertEqual(resp.getheader('X-Container-Read'), None)
        self.assertEqual(resp.getheader('X-Container-Write'), None)

        # sanity check container acls with account1
        resp = retry(get, self.name, use_account=1)
        resp.read()
        self.assertEquals(resp.status, 204)
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Meta-Test'), new_value)
        # but can not write container acls
        self.assertEqual(resp.getheader('X-Container-Read'), 'jdoe')
        self.assertEqual(resp.getheader('X-Container-Write'), 'jdoe')

        # grant admin access
        acl_user = swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # admin can read container metadata
        resp = retry(get, self.name, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 204)
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
        self.assertEquals(resp.status, 204)
        self.assertEqual(resp.getheader('X-Container-Read'), '.r:*')

    def test_long_name_content_type(self):
        if skip:
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
        if skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/abc%%00def' % parsed.path, '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(put)
        if (web_front_end == 'apache2'):
            self.assertEqual(resp.status, 404)
        else:
            self.assertEqual(resp.read(), 'Invalid UTF8 or contains NULL')
            self.assertEqual(resp.status, 412)


if __name__ == '__main__':
    unittest.main()
