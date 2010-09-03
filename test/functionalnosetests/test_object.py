#!/usr/bin/python

import unittest
from nose import SkipTest
from uuid import uuid4

from swift.common.constraints import MAX_META_COUNT, MAX_META_NAME_LENGTH, \
    MAX_META_OVERALL_SIZE, MAX_META_VALUE_LENGTH

from swift_testing import check_response, retry, skip


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
                          'X-Container-Read': '.ref:any'})
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


if __name__ == '__main__':
    unittest.main()
