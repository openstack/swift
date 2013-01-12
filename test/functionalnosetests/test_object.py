#!/usr/bin/python

# Copyright (c) 2010-2012 OpenStack, LLC.
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

from swift.common.constraints import MAX_META_COUNT, MAX_META_NAME_LENGTH, \
    MAX_META_OVERALL_SIZE, MAX_META_VALUE_LENGTH

from swift_testing import check_response, retry, skip, skip3, swift_test_user


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
        self.assertEquals(resp.status, 201)
        self.obj = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, self.container,
                self.obj), 'test', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)

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
        self.assertEquals(resp.status, 200)

        # iterate over object listing and delete all objects
        for obj in object_listing.splitlines():
            resp = retry(delete, obj)
            resp.read()
            self.assertEquals(resp.status, 204)

        # delete the container
        def delete(url, token, parsed, conn):
            conn.request('DELETE', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)

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
        self.assertEquals(resp.status, 200)
        self.assertEquals(source_contents, 'test')

        # copy source to dest with X-Copy-From
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token,
                          'Content-Length': '0',
                          'X-Copy-From': source})
            return check_response(conn)
        resp = retry(put)
        contents = resp.read()
        self.assertEquals(resp.status, 201)

        # contents of dest should be the same as source
        def get_dest(url, token, parsed, conn):
            conn.request('GET',
                         '%s/%s' % (parsed.path, dest),
                         '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get_dest)
        dest_contents = resp.read()
        self.assertEquals(resp.status, 200)
        self.assertEquals(dest_contents, source_contents)

        # delete the copy
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s' % (parsed.path, dest), '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)
        # verify dest does not exist
        resp = retry(get_dest)
        resp.read()
        self.assertEquals(resp.status, 404)

        # copy source to dest with COPY
        def copy(url, token, parsed, conn):
            conn.request('COPY', '%s/%s' % (parsed.path, source), '',
                         {'X-Auth-Token': token,
                          'Destination': dest})
            return check_response(conn)
        resp = retry(copy)
        contents = resp.read()
        self.assertEquals(resp.status, 201)

        # contents of dest should be the same as source
        resp = retry(get_dest)
        dest_contents = resp.read()
        self.assertEquals(resp.status, 200)
        self.assertEquals(dest_contents, source_contents)

        # delete the copy
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)

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
        except Exception, err:
            self.assert_(str(err).startswith('No result after '))

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token,
                          'X-Container-Read': '.r:*'})
            return check_response(conn)
        resp = retry(post)
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(get)
        resp.read()
        self.assertEquals(resp.status, 200)

        def post(url, token, parsed, conn):
            conn.request('POST', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token, 'X-Container-Read': ''})
            return check_response(conn)
        resp = retry(post)
        resp.read()
        self.assertEquals(resp.status, 204)
        try:
            resp = retry(get)
            raise Exception('Should not have been able to GET')
        except Exception, err:
            self.assert_(str(err).startswith('No result after '))

    def test_private_object(self):
        if skip or skip3:
            raise SkipTest

        # Ensure we can't access the object with the third account
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/%s' % (parsed.path, self.container,
                                              self.obj), '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # create a shared container writable by account3
        shared_container = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s' % (parsed.path,
                                           shared_container), '',
                         {'X-Auth-Token': token,
                         'X-Container-Read': swift_test_user[2],
                         'X-Container-Write': swift_test_user[2]})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)

        # verify third account can not copy from private container
        def copy(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path,
                                              shared_container,
                                              'private_object'),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Copy-From': '%s/%s' % (self.container,
                                                        self.obj)})
            return check_response(conn)
        resp = retry(copy, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # verify third account can write "obj1" to shared container
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/%s' % (parsed.path, shared_container,
                'obj1'), 'test', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 201)

        # verify third account can copy "obj1" to shared container
        def copy2(url, token, parsed, conn):
            conn.request('COPY', '%s/%s/%s' % (parsed.path,
                                               shared_container,
                                               'obj1'),
                         '', {'X-Auth-Token': token,
                              'Destination': '%s/%s' % (shared_container,
                                              'obj1')})
            return check_response(conn)
        resp = retry(copy2, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 201)

        # verify third account STILL can not copy from private container
        def copy3(url, token, parsed, conn):
            conn.request('COPY', '%s/%s/%s' % (parsed.path,
                                               self.container,
                                               self.obj),
                         '', {'X-Auth-Token': token,
                              'Destination': '%s/%s' % (shared_container,
                                              'private_object')})
            return check_response(conn)
        resp = retry(copy3, use_account=3)
        resp.read()
        self.assertEquals(resp.status, 403)

        # clean up "obj1"
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/%s' % (parsed.path, shared_container,
                                         'obj1'), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)

        # clean up shared_container
        def delete(url, token, parsed, conn):
            conn.request('DELETE',
                         parsed.path + '/' + shared_container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)

    def test_manifest(self):
        if skip:
            raise SkipTest
        # Data for the object segments
        segments1 = ['one', 'two', 'three', 'four', 'five']
        segments2 = ['six', 'seven', 'eight']
        segments3 = ['nine', 'ten', 'eleven']

        # Upload the first set of segments
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments1/%s' % (parsed.path,
                self.container, str(objnum)), segments1[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments1)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEquals(resp.status, 201)

        # Upload the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token,
                'X-Object-Manifest': '%s/segments1/' % self.container,
                'Content-Type': 'text/jibberish', 'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)

        # Get the manifest (should get all the segments as the body)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEquals(resp.read(), ''.join(segments1))
        self.assertEquals(resp.status, 200)
        self.assertEquals(resp.getheader('content-type'), 'text/jibberish')

        # Get with a range at the start of the second segment
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token, 'Range':
                'bytes=3-'})
            return check_response(conn)
        resp = retry(get)
        self.assertEquals(resp.read(), ''.join(segments1[1:]))
        self.assertEquals(resp.status, 206)

        # Get with a range in the middle of the second segment
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token, 'Range':
                'bytes=5-'})
            return check_response(conn)
        resp = retry(get)
        self.assertEquals(resp.read(), ''.join(segments1)[5:])
        self.assertEquals(resp.status, 206)

        # Get with a full start and stop range
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token, 'Range':
                'bytes=5-10'})
            return check_response(conn)
        resp = retry(get)
        self.assertEquals(resp.read(), ''.join(segments1)[5:11])
        self.assertEquals(resp.status, 206)

        # Upload the second set of segments
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments2/%s' % (parsed.path,
                self.container, str(objnum)), segments2[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments2)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEquals(resp.status, 201)

        # Get the manifest (should still be the first segments of course)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEquals(resp.read(), ''.join(segments1))
        self.assertEquals(resp.status, 200)

        # Update the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token,
                'X-Object-Manifest': '%s/segments2/' % self.container,
                'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)

        # Get the manifest (should be the second set of segments now)
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEquals(resp.read(), ''.join(segments2))
        self.assertEquals(resp.status, 200)

        if not skip3:

            # Ensure we can't access the manifest with the third account
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (parsed.path,
                    self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            resp.read()
            self.assertEquals(resp.status, 403)

            # Grant access to the third account
            def post(url, token, parsed, conn):
                conn.request('POST', '%s/%s' % (parsed.path, self.container),
                    '', {'X-Auth-Token': token, 'X-Container-Read':
                    swift_test_user[2]})
                return check_response(conn)
            resp = retry(post)
            resp.read()
            self.assertEquals(resp.status, 204)

            # The third account should be able to get the manifest now
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (parsed.path,
                    self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            self.assertEquals(resp.read(), ''.join(segments2))
            self.assertEquals(resp.status, 200)

        # Create another container for the third set of segments
        acontainer = uuid4().hex

        def put(url, token, parsed, conn):
            conn.request('PUT', parsed.path + '/' + acontainer, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)

        # Upload the third set of segments in the other container
        def put(url, token, parsed, conn, objnum):
            conn.request('PUT', '%s/%s/segments3/%s' % (parsed.path,
                acontainer, str(objnum)), segments3[objnum],
                {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments3)):
            resp = retry(put, objnum)
            resp.read()
            self.assertEquals(resp.status, 201)

        # Update the manifest
        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token,
                'X-Object-Manifest': '%s/segments3/' % acontainer,
                'Content-Length': '0'})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)

        # Get the manifest to ensure it's the third set of segments
        def get(url, token, parsed, conn):
            conn.request('GET', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(get)
        self.assertEquals(resp.read(), ''.join(segments3))
        self.assertEquals(resp.status, 200)

        if not skip3:

            # Ensure we can't access the manifest with the third account
            # (because the segments are in a protected container even if the
            # manifest itself is not).

            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (parsed.path,
                    self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            resp.read()
            self.assertEquals(resp.status, 403)

            # Grant access to the third account
            def post(url, token, parsed, conn):
                conn.request('POST', '%s/%s' % (parsed.path, acontainer),
                    '', {'X-Auth-Token': token, 'X-Container-Read':
                    swift_test_user[2]})
                return check_response(conn)
            resp = retry(post)
            resp.read()
            self.assertEquals(resp.status, 204)

            # The third account should be able to get the manifest now
            def get(url, token, parsed, conn):
                conn.request('GET', '%s/%s/manifest' % (parsed.path,
                    self.container), '', {'X-Auth-Token': token})
                return check_response(conn)
            resp = retry(get, use_account=3)
            self.assertEquals(resp.read(), ''.join(segments3))
            self.assertEquals(resp.status, 200)

        # Delete the manifest
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/manifest' % (parsed.path,
                self.container), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete, objnum)
        resp.read()
        self.assertEquals(resp.status, 204)

        # Delete the third set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments3/%s' % (parsed.path,
                acontainer, str(objnum)), '', {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments3)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertEquals(resp.status, 204)

        # Delete the second set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments2/%s' % (parsed.path,
                self.container, str(objnum)), '', {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments2)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertEquals(resp.status, 204)

        # Delete the first set of segments
        def delete(url, token, parsed, conn, objnum):
            conn.request('DELETE', '%s/%s/segments1/%s' % (parsed.path,
                self.container, str(objnum)), '', {'X-Auth-Token': token})
            return check_response(conn)
        for objnum in xrange(len(segments1)):
            resp = retry(delete, objnum)
            resp.read()
            self.assertEquals(resp.status, 204)

        # Delete the extra container
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s' % (parsed.path, acontainer), '',
                {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)

    def test_delete_content_type(self):
        if skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/hi' % (parsed.path,
                self.container), 'there', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        resp.read()
        self.assertEquals(resp.status, 201)

        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/hi' % (parsed.path, self.container),
                '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)
        self.assertEquals(resp.getheader('Content-Type'),
                          'text/html; charset=UTF-8')

    def test_null_name(self):
        if skip:
            raise SkipTest

        def put(url, token, parsed, conn):
            conn.request('PUT', '%s/%s/abc%%00def' % (parsed.path,
                self.container), 'test', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(put)
        self.assertEquals(resp.read(), 'Invalid UTF8 or contains NULL')
        self.assertEquals(resp.status, 412)


if __name__ == '__main__':
    unittest.main()
