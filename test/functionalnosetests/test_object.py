#!/usr/bin/python

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
        def delete(url, token, parsed, conn):
            conn.request('DELETE', '%s/%s/%s' % (parsed.path, self.container,
                self.obj), '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(delete)
        resp.read()
        self.assertEquals(resp.status, 204)
        def delete(url, token, parsed, conn):
            conn.request('DELETE', parsed.path + '/' + self.container, '',
                         {'X-Auth-Token': token})
            return check_response(conn)
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


if __name__ == '__main__':
    unittest.main()
