# Copyright (c) 2010 OpenStack, LLC.
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

from __future__ import with_statement
import logging
import os
import sys
import unittest
from contextlib import contextmanager

import eventlet
from webob import Request

from swift.common.middleware import auth

# mocks
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


class FakeMemcache(object):
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, timeout=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except:
            pass
        return True


def mock_http_connect(response, headers=None, with_exc=False):
    class FakeConn(object):
        def __init__(self, status, headers, with_exc):
            self.status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = '1234'
            self.with_exc = with_exc
            self.headers = headers
            if self.headers is None:
                self.headers = {}
        def getresponse(self):
            if self.with_exc:
                raise Exception('test')
            return self
        def getheader(self, header):
            return self.headers[header]
        def read(self, amt=None):
            return ''
        def close(self):
            return
    return lambda *args, **kwargs: FakeConn(response, headers, with_exc)


class Logger(object):
    def __init__(self):
        self.error_value = None
        self.exception_value = None
    def error(self, msg, *args, **kwargs):
        self.error_value = (msg, args, kwargs)
    def exception(self, msg, *args, **kwargs):
        _, exc, _ = sys.exc_info()
        self.exception_value = (msg,
            '%s %s' % (exc.__class__.__name__, str(exc)), args, kwargs)
# tests

class FakeApp(object):
    def __call__(self, env, start_response):
        return ['204 No Content']

def start_response(*args):
    pass

class TestAuth(unittest.TestCase):

    def setUp(self):
        self.test_auth = auth.filter_factory({})(FakeApp())

    def test_auth_fail(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(404)
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': FakeMemcache()},
                lambda x, y: None))
            self.assert_(result.startswith('401'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_success(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
                {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,cfa'})
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': FakeMemcache()},
                lambda x, y: None))
            self.assert_(result.startswith('204'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_memcache(self):
        old_http_connect = auth.http_connect
        try:
            fake_memcache = FakeMemcache()
            auth.http_connect = mock_http_connect(204,
                {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,cfa'})
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': fake_memcache},
                lambda x, y: None))
            self.assert_(result.startswith('204'), result)
            auth.http_connect = mock_http_connect(404)
            # Should still be in memcache
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': fake_memcache},
                lambda x, y: None))
            self.assert_(result.startswith('204'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_just_expired(self):
        old_http_connect = auth.http_connect
        try:
            fake_memcache = FakeMemcache()
            auth.http_connect = mock_http_connect(204,
                {'x-auth-ttl': '0', 'x-auth-groups': 'act:usr,act,cfa'})
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': fake_memcache},
                lambda x, y: None))
            self.assert_(result.startswith('204'), result)
            auth.http_connect = mock_http_connect(404)
            # Should still be in memcache, but expired
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': fake_memcache},
                lambda x, y: None))
            self.assert_(result.startswith('401'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_success(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
                {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,cfa'})
            req = Request.blank('/v/a/c/o', headers={'x-auth-token': 't'})
            req.environ['swift.cache'] = FakeMemcache()
            result = ''.join(self.test_auth(req.environ, start_response))
            self.assert_(result.startswith('204'), result)
            self.assertEquals(req.remote_user, 'act:usr,act,cfa')
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_no_header(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
                {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,cfa'})
            req = Request.blank('/v/a/c/o')
            req.environ['swift.cache'] = FakeMemcache()
            result = ''.join(self.test_auth(req.environ, start_response))
            self.assert_(result.startswith('204'), result)
            self.assert_(not req.remote_user, req.remote_user)
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_storage_token(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
                {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,cfa'})
            req = Request.blank('/v/a/c/o', headers={'x-storage-token': 't'})
            req.environ['swift.cache'] = FakeMemcache()
            result = ''.join(self.test_auth(req.environ, start_response))
            self.assert_(result.startswith('204'), result)
            self.assertEquals(req.remote_user, 'act:usr,act,cfa')
        finally:
            auth.http_connect = old_http_connect

    def test_authorize_bad_path(self):
        req = Request.blank('/badpath')
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('401'), resp)
        req = Request.blank('/badpath')
        req.remote_user = 'act:usr,act,cfa'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

    def test_authorize_account_access(self):
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act,cfa'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

    def test_authorize_acl_group_access(self):
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act2'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr2'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

    def test_authorize_acl_referrer_access(self):
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:.example.com'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
        req = Request.blank('/v1/cfa')
        req.remote_user = 'act:usr,act'
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/cfa')
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('401'), resp)
        req = Request.blank('/v1/cfa')
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/cfa')
        req.acl = '.r:.example.com'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('401'), resp)
        req = Request.blank('/v1/cfa')
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)


if __name__ == '__main__':
    unittest.main()
