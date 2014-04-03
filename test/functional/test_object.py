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
from nose import SkipTest
from uuid import uuid4

from swift.common.utils import json

from swift_testing import check_response, retry, skip, skip3, \
    swift_test_perm, web_front_end, requires_acls, swift_test_user


class TestObject(unittest.TestCase):

    def setUp(self):
        if skip:
            raise SkipTest
        self.container = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)
        self.obj = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, self.obj), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

    def tearDown(self):
        if skip:
            raise SkipTest

        def delete(url, token, parsed, conn, obj):
            conn.request('DELETE',
                         '%s/%s/%s' % (parsed.path, self.container, obj),
                         '', {'X-Auth-Token': token})
            return check_response(conn)

        # get list of objects in container
        def list(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, self.container),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(list)
        object_listing = resp.read()
        self.assertEqual(resp.status, 200)

        # iterate over object listing and delete all objects
        for obj in object_listing.splitlines():
            resp = retry(delete, obj)
            resp.read()
            self.assertEqual(resp.status, 204)

        # delete the container
        def delete(url, token, parsed, conn):
            conn.request('DELETE', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)

    def test_if_none_match(self):
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, 'if_none_match_test'), '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'If-None-Match': '*'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 412)

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, 'if_none_match_test'), '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'If-None-Match': 'somethingelse'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 400)

    def test_copy_object(self):
        if skip:
            raise SkipTest

        source = '%s/%s' % (self.container, self.obj)
        dest = '%s/%s' % (self.container, 'test_copy')

        # get contents of source
        def get_source(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, source),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get_source)
        source_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(source_contents, 'test')

        # copy source to dest with X-Copy-From
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token,
                          'Content-Length': '0',
                          'X-Copy-From': source})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # contents of dest should be the same as source
        def get_dest(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, dest),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get_dest)
        dest_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(dest_contents, source_contents)

        # delete the copy
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)
        # verify dest does not exist
        resp = retry(get_dest)
        resp.read()
        self.assertEqual(resp.status, 404)

        # copy source to dest with COPY
        def copy(url, token, parsed, conn):
            conn.request('COPY', '%s/%s' % (parsed.path, source), '',
                         {'X-Auth-Token': token,
                          'Destination': dest})
            return check_response(conn)
        resp = retry(copy)
        resp.read()
        self.assertEqual(resp.status, 201)

        # contents of dest should be the same as source
        resp = retry(get_dest)
        dest_contents = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(dest_contents, source_contents)

        # delete the copy
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)

    def test_public_object(self):
        if skip:
            raise SkipTest

        def get(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s/%s' % (parsed.path, self.container, self.obj))
            return check_response(conn)
        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception as err:
            self.assert_(str(err).startswith('No result after '))

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': '.r:*'})
            return check_response(conn)
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        resp = retry(get)
        resp.read()
        self.assertEqual(resp.status, 200)

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token, 'X-Container-Read': ''})
            return check_response(conn)
        resp = retry(post)
        resp.read()
        self.assertEqual(resp.status, 204)
        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception as err:
            self.assert_(str(err).startswith('No result after '))

    def test_private_object(self):
        if skip or skip3:
            raise SkipTest

        # Ensure we can't access the object with the third account
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, self.obj), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # create a shared container writable by account3
        shared_container = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (
                parsed.path, shared_container), '',
                {'X-Auth-Token': token,
                 'X-Container-Read': swift_test_perm[2],
                 'X-Container-Write': swift_test_perm[2]})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account can not copy from private container
        def copy(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, shared_container, 'private_object'), '',
                {'X-Auth-Token': token,
                 'Content-Length': '0',
                 'X-Copy-From': '%s/%s' % (self.container, self.obj)})
            return check_response(conn)
        resp = retry(copy, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # verify third account can write "obj1" to shared container
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account can copy "obj1" to shared container
        def copy2(url, token, parsed, conn):
            conn.request('COPY', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), '',
                {'X-Auth-Token': token,
                 'Destination': '%s/%s' % (shared_container, 'obj1')})
            return check_response(conn)
        resp = retry(copy2, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 201)

        # verify third account STILL can not copy from private container
        def copy3(url, token, parsed, conn):
            conn.request('COPY', '%s/%s/%s' % (
                parsed.path, self.container, self.obj), '',
                {'X-Auth-Token': token,
                 'Destination': '%s/%s' % (shared_container,
                                           'private_object')})
            return check_response(conn)
        resp = retry(copy3, use_account=3)
        resp.read()
        self.assertEqual(resp.status, 403)

        # clean up "obj1"
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, shared_container, 'obj1'), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)

        # clean up shared_container
        def delete(url, token, parsed, conn):
            conn.request('DELETE',
                         parsed.path + '/' + shared_container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)

    @requires_acls
    def test_read_only(self):
        if skip3:
            raise SkipTest

        def get_listing(url, token, parsed, conn):
            conn.request('GET', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def get(url, token, parsed, conn, name):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list objects
        resp = retry(get_listing, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # cannot get object
        resp = retry(get, self.obj, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant read-only access
        acl_user = swift_test_user[2]
        acl = {'read-only': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list objects
        resp = retry(get_listing, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(self.obj in listing)

        # can get object
        resp = retry(get, self.obj, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 200)
        self.assertEquals(body, 'test')

        # can not put an object
        obj_name = str(uuid4())
        resp = retry(put, obj_name, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 403)

        # can not delete an object
        resp = retry(delete, self.obj, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 403)

        # sanity with account1
        resp = retry(get_listing, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(obj_name not in listing)
        self.assert_(self.obj in listing)

    @requires_acls
    def test_read_write(self):
        if skip3:
            raise SkipTest

        def get_listing(url, token, parsed, conn):
            conn.request('GET', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def get(url, token, parsed, conn, name):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list objects
        resp = retry(get_listing, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # cannot get object
        resp = retry(get, self.obj, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant read-write access
        acl_user = swift_test_user[2]
        acl = {'read-write': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list objects
        resp = retry(get_listing, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(self.obj in listing)

        # can get object
        resp = retry(get, self.obj, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 200)
        self.assertEquals(body, 'test')

        # can put an object
        obj_name = str(uuid4())
        resp = retry(put, obj_name, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 201)

        # can delete an object
        resp = retry(delete, self.obj, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 204)

        # sanity with account1
        resp = retry(get_listing, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(obj_name in listing)
        self.assert_(self.obj not in listing)

    @requires_acls
    def test_admin(self):
        if skip3:
            raise SkipTest

        def get_listing(url, token, parsed, conn):
            conn.request('GET', '%s/%s' % (parsed.path, self.container), '',
                         {'X-Auth-Token': token})
            return check_response(conn)

        def post_account(url, token, parsed, conn, headers):
            new_headers = dict({'X-Auth-Token': token}, **headers)
            conn.request('POST', parsed.path, '', new_headers)
            return check_response(conn)

        def get(url, token, parsed, conn, name):
            conn.request('GET', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        def put(url, token, parsed, conn, name):
            conn.request('PUT', '%s/%s/%s' % (
                parsed.path, self.container, name), 'test',
                {'X-Auth-Token': token})
            return check_response(conn)

        def delete(url, token, parsed, conn, name):
            conn.request('DELETE', '%s/%s/%s' % (
                parsed.path, self.container, name), '',
                {'X-Auth-Token': token})
            return check_response(conn)

        # cannot list objects
        resp = retry(get_listing, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # cannot get object
        resp = retry(get, self.obj, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # grant admin access
        acl_user = swift_test_user[2]
        acl = {'admin': [acl_user]}
        headers = {'x-account-access-control': json.dumps(acl)}
        resp = retry(post_account, headers=headers, use_account=1)
        resp.read()
        self.assertEqual(resp.status, 204)

        # can list objects
        resp = retry(get_listing, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(self.obj in listing)

        # can get object
        resp = retry(get, self.obj, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 200)
        self.assertEquals(body, 'test')

        # can put an object
        obj_name = str(uuid4())
        resp = retry(put, obj_name, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 201)

        # can delete an object
        resp = retry(delete, self.obj, use_account=3)
        body = resp.read()
        self.assertEquals(resp.status, 204)

        # sanity with account1
        resp = retry(get_listing, use_account=3)
        listing = resp.read()
        self.assertEquals(resp.status, 200)
        self.assert_(obj_name in listing)
        self.assert_(self.obj not in listing)

    def test_manifest(self):
        if skip:
            raise SkipTest
        # Data for the object segments
        segments1 = ['one', 'two', 'three', 'four', 'five']
        segments2 = ['six', 'seven', 'eight']
        segments3 = ['nine', 'ten', 'eleven']

        # Upload the first set of segments
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments1/%s' % (
                parsed.path, self.container, str(objnum)), segments1[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments1)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEqual(resp.status, 201)

        # Upload the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token,
                    'X-Object-Manifest': '%s/segments1/' % self.container,
                    'Content-Type': 'text/jibberish', 'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Get the manifest (should get all the segments as the body)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), ''.join(segments1))
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader('content-type'), 'text/jibberish')

        # Get with a range at the start of the second segment
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token, 'Range': 'bytes=3-'})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), ''.join(segments1[1:]))
        self.assertEqual(resp.status, 206)

        # Get with a range in the middle of the second segment
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token, 'Range': 'bytes=5-'})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), ''.join(segments1)[5:])
        self.assertEqual(resp.status, 206)

        # Get with a full start and stop range
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token, 'Range': 'bytes=5-10'})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), ''.join(segments1)[5:11])
        self.assertEqual(resp.status, 206)

        # Upload the second set of segments
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments2/%s' % (
                parsed.path, self.container, str(objnum)), segments2[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments2)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEqual(resp.status, 201)

        # Get the manifest (should still be the first segments of course)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), ''.join(segments1))
        self.assertEqual(resp.status, 200)

        # Update the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (
                parsed.path, self.container), '', {
                    'X-Auth-Token': token,
                    'X-Object-Manifest': '%s/segments2/' % self.container,
                    'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Get the manifest (should be the second set of segments now)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), ''.join(segments2))
        self.assertEqual(resp.status, 200)

        if not skip3:

            # Ensure we can't access the manifest with the third account
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            resp.read()
            self.assertEqual(resp.status, 403)

            # Grant access to the third account
            def post(url, token, parsed, conn):
                conn.request('POST', '%s/%s' % (parsed.path, self.container),
                             '', {'X-Auth-Token': token,
                                  'X-Container-Read': swift_test_perm[2]})
                return check_response(conn)
            resp = retry(post)
            resp.read()
            self.assertEqual(resp.status, 204)

            # The third account should be able to get the manifest now
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            self.assertEqual(resp.read(), ''.join(segments2))
            self.assertEqual(resp.status, 200)

        # Create another container for the third set of segments
        acontainer = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', parsed.path + '/' + acontainer, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Upload the third set of segments in the other container
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments3/%s' % (
                parsed.path, acontainer, str(objnum)), segments3[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments3)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEqual(resp.status, 201)

        # Update the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (
                parsed.path, self.container), '',
                {'X-Auth-Token': token,
                 'X-Object-Manifest': '%s/segments3/' % acontainer,
                 'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        # Get the manifest to ensure it's the third set of segments
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (
                parsed.path, self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEqual(resp.read(), ''.join(segments3))
        self.assertEqual(resp.status, 200)

        if not skip3:

            # Ensure we can't access the manifest with the third account
            # (because the segments are in a protected container even if the
            # manifest itself is not).

            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            resp.read()
            self.assertEqual(resp.status, 403)

            # Grant access to the third account
            def post(url, token, parsed, conn):
                conn.request('POST', '%s/%s' % (parsed.path, acontainer),
                             '', {'X-Auth-Token': token,
                                  'X-Container-Read': swift_test_perm[2]})
                return check_response(conn)
            resp = retry(post)
            resp.read()
            self.assertEqual(resp.status, 204)

            # The third account should be able to get the manifest now
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (
                    parsed.path, self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            self.assertEqual(resp.read(), ''.join(segments3))
            self.assertEqual(resp.status, 200)

        # Delete the manifest
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/manifest' % (
                parsed.path,
                self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete, objnum)
        resp.read()
        self.assertEqual(resp.status, 204)

        # Delete the third set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments3/%s' % (
                parsed.path, acontainer, str(objnum)), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments3)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertEqual(resp.status, 204)

        # Delete the second set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments2/%s' % (
                parsed.path, self.container, str(objnum)), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments2)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertEqual(resp.status, 204)

        # Delete the first set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments1/%s' % (
                parsed.path, self.container, str(objnum)), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments1)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertEqual(resp.status, 204)

        # Delete the extra container
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s' % (parsed.path, acontainer), '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)

    def test_delete_content_type(self):
        if skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/hi' % (parsed.path, self.container),
                         'there', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/hi' % (parsed.path, self.container),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.getheader('Content-Type'),
                         'text/html; charset=UTF-8')

    def test_delete_if_delete_at_bad(self):
        if skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT',
                         '%s/%s/hi-delete-bad' % (parsed.path, self.container),
                         'there', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEqual(resp.status, 201)

        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/hi' % (parsed.path, self.container),
                         '', {'X-Auth-Token': token,
                              'X-If-Delete-At': 'bad'})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEqual(resp.status, 400)

    def test_null_name(self):
        if skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/abc%%00def' % (
                parsed.path,
                self.container), 'test', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        if (web_front_end == 'apache2'):
            self.assertEqual(resp.status, 404)
        else:
            self.assertEqual(resp.read(), 'Invalid UTF8 or contains NULL')
            self.assertEqual(resp.status, 412)

    def test_cors(self):
        if skip:
            raise SkipTest

        def is_strict_mode(url, token, parsed, conn):
            conn.request('GET', '/info')
            resp = conn.getresponse()
            if resp.status // 100 == 2:
                info = json.loads(resp.read())
                return info.get('swift', {}).get('strict_cors_mode', False)
            return False

        def put_cors_cont(url, token, parsed, conn, orig):
            conn.request(
                'PUT', '%s/%s' % (parsed.path, self.container),
                '', {'X-Auth-Token': token,
                     'X-Container-Meta-Access-Control-Allow-Origin': orig})
            return check_response(conn)

        def put_obj(url, token, parsed, conn, obj):
            conn.request(
                'PUT', '%s/%s/%s' % (parsed.path, self.container, obj),
                'test', {'X-Auth-Token': token})
            return check_response(conn)

        def check_cors(url, token, parsed, conn,
                       method, obj, headers):
            if method != 'OPTIONS':
                headers['X-Auth-Token'] = token
            conn.request(
                method, '%s/%s/%s' % (parsed.path, self.container, obj),
                '', headers)
            return conn.getresponse()

        strict_cors = retry(is_strict_mode)

        resp = retry(put_cors_cont, '*')
        resp.read()
        self.assertEquals(resp.status // 100, 2)

        resp = retry(put_obj, 'cat')
        resp.read()
        self.assertEquals(resp.status // 100, 2)

        resp = retry(check_cors,
                     'OPTIONS', 'cat', {'Origin': 'http://m.com'})
        self.assertEquals(resp.status, 401)

        resp = retry(check_cors,
                     'OPTIONS', 'cat',
                     {'Origin': 'http://m.com',
                      'Access-Control-Request-Method': 'GET'})

        self.assertEquals(resp.status, 200)
        resp.read()
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEquals(headers.get('access-control-allow-origin'),
                          '*')

        resp = retry(check_cors,
                     'GET', 'cat', {'Origin': 'http://m.com'})
        self.assertEquals(resp.status, 200)
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEquals(headers.get('access-control-allow-origin'),
                          '*')

        resp = retry(check_cors,
                     'GET', 'cat', {'Origin': 'http://m.com',
                                    'X-Web-Mode': 'True'})
        self.assertEquals(resp.status, 200)
        headers = dict((k.lower(), v) for k, v in resp.getheaders())
        self.assertEquals(headers.get('access-control-allow-origin'),
                          '*')

        ####################

        resp = retry(put_cors_cont, 'http://secret.com')
        resp.read()
        self.assertEquals(resp.status // 100, 2)

        resp = retry(check_cors,
                     'OPTIONS', 'cat',
                     {'Origin': 'http://m.com',
                      'Access-Control-Request-Method': 'GET'})
        resp.read()
        self.assertEquals(resp.status, 401)

        if strict_cors:
            resp = retry(check_cors,
                         'GET', 'cat', {'Origin': 'http://m.com'})
            resp.read()
            self.assertEquals(resp.status, 200)
            headers = dict((k.lower(), v) for k, v in resp.getheaders())
            self.assertTrue('access-control-allow-origin' not in headers)

            resp = retry(check_cors,
                         'GET', 'cat', {'Origin': 'http://secret.com'})
            resp.read()
            self.assertEquals(resp.status, 200)
            headers = dict((k.lower(), v) for k, v in resp.getheaders())
            self.assertEquals(headers.get('access-control-allow-origin'),
                              'http://secret.com')
        else:
            resp = retry(check_cors,
                         'GET', 'cat', {'Origin': 'http://m.com'})
            resp.read()
            self.assertEquals(resp.status, 200)
            headers = dict((k.lower(), v) for k, v in resp.getheaders())
            self.assertEquals(headers.get('access-control-allow-origin'),
                              'http://m.com')


if __name__ == '__main__':
    unittest.main()
