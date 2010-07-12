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

from swift.common import auth

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
        return "OK"

def start_response(*args):
    pass

class TestAuth(unittest.TestCase):

    def setUp(self):
        self.test_auth = auth.DevAuthMiddleware(
            FakeApp(), {}, FakeMemcache(), Logger())

    def test_auth_fail(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(404)
            self.assertFalse(self.test_auth.auth('a','t'))
        finally:
            auth.http_connect = old_http_connect

    def test_auth_success(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204, {'x-auth-ttl':'1234'})
            self.assertTrue(self.test_auth.auth('a','t'))
        finally:
            auth.http_connect = old_http_connect

    def test_auth_memcache(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204, {'x-auth-ttl':'1234'})
            self.assertTrue(self.test_auth.auth('a','t'))
            auth.http_connect = mock_http_connect(404)
            # Should still be in memcache
            self.assertTrue(self.test_auth.auth('a','t'))
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_success(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204, {'x-auth-ttl':'1234'})
            req = Request.blank('/v/a/c/o', headers={'x-auth-token':'t'})
            resp = self.test_auth(req.environ, start_response)
            self.assertEquals(resp, 'OK')
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_no_header(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204, {'x-auth-ttl':'1234'})
            req = Request.blank('/v/a/c/o')
            resp = self.test_auth(req.environ, start_response)
            self.assertEquals(resp, ['Missing Auth Token'])
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_storage_token(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204, {'x-auth-ttl':'1234'})
            req = Request.blank('/v/a/c/o', headers={'x-storage-token':'t'})
            resp = self.test_auth(req.environ, start_response)
            self.assertEquals(resp, 'OK')
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_only_version(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204, {'x-auth-ttl':'1234'})
            req = Request.blank('/v', headers={'x-auth-token':'t'})
            resp = self.test_auth(req.environ, start_response)
            self.assertEquals(resp, ['Bad URL'])
        finally:
            auth.http_connect = old_http_connect


if __name__ == '__main__':
    unittest.main()
