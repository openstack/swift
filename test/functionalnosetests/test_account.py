#!/usr/bin/python

import unittest
from nose import SkipTest

from swift.common.constraints import MAX_META_COUNT, MAX_META_NAME_LENGTH, \
    MAX_META_OVERALL_SIZE, MAX_META_VALUE_LENGTH

from swift_testing import check_response, retry, skip


class TestAccount(unittest.TestCase):

    def test_metadata(self):
        if skip:
            raise SkipTest
        def post(url, token, parsed, conn, value):
            conn.request('POST', parsed.path, '',
                {'X-Auth-Token': token, 'X-Account-Meta-Test': value})
            return check_response(conn)
        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)
        def get(url, token, parsed, conn):
            conn.request('GET', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(post, '')
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEquals(resp.getheader('x-account-meta-test'), None)
        resp = retry(get)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEquals(resp.getheader('x-account-meta-test'), None)
        resp = retry(post, 'Value')
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEquals(resp.getheader('x-account-meta-test'), 'Value')
        resp = retry(get)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEquals(resp.getheader('x-account-meta-test'), 'Value')

    def test_multi_metadata(self):
        if skip:
            raise SkipTest
        def post(url, token, parsed, conn, name, value):
            conn.request('POST', parsed.path, '',
                         {'X-Auth-Token': token, name: value})
            return check_response(conn)
        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)
        resp = retry(post, 'X-Account-Meta-One', '1')
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEquals(resp.getheader('x-account-meta-one'), '1')
        resp = retry(post, 'X-Account-Meta-Two', '2')
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(head)
        resp.read()
        self.assert_(resp.status in (200, 204), resp.status)
        self.assertEquals(resp.getheader('x-account-meta-one'), '1')
        self.assertEquals(resp.getheader('x-account-meta-two'), '2')

    def test_bad_metadata(self):
        if skip:
            raise SkipTest
        def post(url, token, parsed, conn, extra_headers):
            headers = {'X-Auth-Token': token}
            headers.update(extra_headers)
            conn.request('POST', parsed.path, '', headers)
            return check_response(conn)
        resp = retry(post,
                {'X-Account-Meta-' + ('k' * MAX_META_NAME_LENGTH): 'v'})
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(post,
                {'X-Account-Meta-' + ('k' * (MAX_META_NAME_LENGTH + 1)): 'v'})
        resp.read()
        self.assertEquals(resp.status, 400)

        resp = retry(post,
                {'X-Account-Meta-Too-Long': 'k' * MAX_META_VALUE_LENGTH})
        resp.read()
        self.assertEquals(resp.status, 204)
        resp = retry(post,
                {'X-Account-Meta-Too-Long': 'k' * (MAX_META_VALUE_LENGTH + 1)})
        resp.read()
        self.assertEquals(resp.status, 400)

        headers = {}
        for x in xrange(MAX_META_COUNT):
            headers['X-Account-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEquals(resp.status, 204)
        headers = {}
        for x in xrange(MAX_META_COUNT + 1):
            headers['X-Account-Meta-%d' % x] = 'v'
        resp = retry(post, headers)
        resp.read()
        self.assertEquals(resp.status, 400)

        headers = {}
        header_value = 'k' * MAX_META_VALUE_LENGTH
        size = 0
        x = 0
        while size < MAX_META_OVERALL_SIZE - 4 - MAX_META_VALUE_LENGTH:
            size += 4 + MAX_META_VALUE_LENGTH
            headers['X-Account-Meta-%04d' % x] = header_value
            x += 1
        if MAX_META_OVERALL_SIZE - size > 1:
            headers['X-Account-Meta-k'] = \
                'v' * (MAX_META_OVERALL_SIZE - size - 1)
        resp = retry(post, headers)
        resp.read()
        self.assertEquals(resp.status, 204)
        headers['X-Account-Meta-k'] = \
            'v' * (MAX_META_OVERALL_SIZE - size)
        resp = retry(post, headers)
        resp.read()
        self.assertEquals(resp.status, 400)


if __name__ == '__main__':
    unittest.main()
