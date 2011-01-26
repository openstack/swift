# Copyright (c) 2010-2011 OpenStack, LLC.
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
        except Exception:
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
        _junk, exc, _junk = sys.exc_info()
        self.exception_value = (msg,
            '%s %s' % (exc.__class__.__name__, str(exc)), args, kwargs)


class FakeApp(object):

    def __init__(self):
        self.i_was_called = False

    def __call__(self, env, start_response):
        self.i_was_called = True
        req = Request.blank('', environ=env)
        if 'swift.authorize' in env:
            resp = env['swift.authorize'](req)
            if resp:
                return resp(env, start_response)
        return ['204 No Content']


def start_response(*args):
    pass


class TestAuth(unittest.TestCase):

    def setUp(self):
        self.test_auth = auth.filter_factory({})(FakeApp())

    def test_auth_deny_non_reseller_prefix(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            reqenv = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/BLAH_account',
                'HTTP_X_AUTH_TOKEN': 'BLAH_t', 'swift.cache': FakeMemcache()}
            result = ''.join(self.test_auth(reqenv, lambda x, y: None))
            self.assert_(result.startswith('401'), result)
            self.assertEquals(reqenv['swift.authorize'],
                              self.test_auth.denied_response)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_deny_non_reseller_prefix_no_override(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            fake_authorize = lambda x: lambda x, y: ['500 Fake']
            reqenv = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/BLAH_account',
                'HTTP_X_AUTH_TOKEN': 'BLAH_t', 'swift.cache': FakeMemcache(),
                'swift.authorize': fake_authorize}
            result = ''.join(self.test_auth(reqenv, lambda x, y: None))
            self.assert_(result.startswith('500 Fake'), result)
            self.assertEquals(reqenv['swift.authorize'], fake_authorize)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_no_reseller_prefix_deny(self):
        # Ensures that when we have no reseller prefix, we don't deny a request
        # outright but set up a denial swift.authorize and pass the request on
        # down the chain.
        old_http_connect = auth.http_connect
        try:
            local_app = FakeApp()
            local_auth = \
                auth.filter_factory({'reseller_prefix': ''})(local_app)
            auth.http_connect = mock_http_connect(404)
            reqenv = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/account',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': FakeMemcache()}
            result = ''.join(local_auth(reqenv, lambda x, y: None))
            self.assert_(result.startswith('401'), result)
            self.assert_(local_app.i_was_called)
            self.assertEquals(reqenv['swift.authorize'],
                              local_auth.denied_response)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_no_reseller_prefix_allow(self):
        # Ensures that when we have no reseller prefix, we can still allow
        # access if our auth server accepts requests
        old_http_connect = auth.http_connect
        try:
            local_app = FakeApp()
            local_auth = \
                auth.filter_factory({'reseller_prefix': ''})(local_app)
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            reqenv = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/act',
                'HTTP_X_AUTH_TOKEN': 't', 'swift.cache': None}
            result = ''.join(local_auth(reqenv, lambda x, y: None))
            self.assert_(result.startswith('204'), result)
            self.assert_(local_app.i_was_called)
            self.assertEquals(reqenv['swift.authorize'],
                              local_auth.authorize)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_no_reseller_prefix_no_token(self):
        # Check that normally we set up a call back to our authorize.
        local_auth = \
            auth.filter_factory({'reseller_prefix': ''})(FakeApp())
        reqenv = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/account',
                  'swift.cache': FakeMemcache()}
        result = ''.join(local_auth(reqenv, lambda x, y: None))
        self.assert_(result.startswith('401'), result)
        self.assertEquals(reqenv['swift.authorize'], local_auth.authorize)
        # Now make sure we don't override an existing swift.authorize when we
        # have no reseller prefix.
        local_authorize = lambda req: None
        reqenv['swift.authorize'] = local_authorize
        result = ''.join(local_auth(reqenv, lambda x, y: None))
        self.assert_(result.startswith('204'), result)
        self.assertEquals(reqenv['swift.authorize'], local_authorize)

    def test_auth_fail(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(404)
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 'AUTH_t', 'swift.cache': FakeMemcache()},
                lambda x, y: None))
            self.assert_(result.startswith('401'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_success(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'PATH_INFO': '/v/AUTH_cfa', 'HTTP_X_AUTH_TOKEN': 'AUTH_t',
                'swift.cache': FakeMemcache()}, lambda x, y: None))
            self.assert_(result.startswith('204'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_memcache(self):
        old_http_connect = auth.http_connect
        try:
            fake_memcache = FakeMemcache()
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'PATH_INFO': '/v/AUTH_cfa', 'HTTP_X_AUTH_TOKEN': 'AUTH_t',
                'swift.cache': fake_memcache}, lambda x, y: None))
            self.assert_(result.startswith('204'), result)
            auth.http_connect = mock_http_connect(404)
            # Should still be in memcache
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'PATH_INFO': '/v/AUTH_cfa', 'HTTP_X_AUTH_TOKEN': 'AUTH_t',
                'swift.cache': fake_memcache}, lambda x, y: None))
            self.assert_(result.startswith('204'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_auth_just_expired(self):
        old_http_connect = auth.http_connect
        try:
            fake_memcache = FakeMemcache()
            auth.http_connect = mock_http_connect(204,
                {'x-auth-ttl': '0', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'PATH_INFO': '/v/AUTH_cfa', 'HTTP_X_AUTH_TOKEN': 'AUTH_t',
                'swift.cache': fake_memcache}, lambda x, y: None))
            self.assert_(result.startswith('204'), result)
            auth.http_connect = mock_http_connect(404)
            # Should still be in memcache, but expired
            result = ''.join(self.test_auth({'REQUEST_METHOD': 'GET',
                'HTTP_X_AUTH_TOKEN': 'AUTH_t', 'swift.cache': fake_memcache},
                lambda x, y: None))
            self.assert_(result.startswith('401'), result)
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_success(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            req = Request.blank('/v/AUTH_cfa/c/o',
                                headers={'x-auth-token': 'AUTH_t'})
            req.environ['swift.cache'] = FakeMemcache()
            result = ''.join(self.test_auth(req.environ, start_response))
            self.assert_(result.startswith('204'), result)
            self.assertEquals(req.remote_user, 'act:usr,act,AUTH_cfa')
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_no_header(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            req = Request.blank('/v/AUTH_cfa/c/o')
            req.environ['swift.cache'] = FakeMemcache()
            result = ''.join(self.test_auth(req.environ, start_response))
            self.assert_(result.startswith('401'), result)
            self.assert_(not req.remote_user, req.remote_user)
        finally:
            auth.http_connect = old_http_connect

    def test_middleware_storage_token(self):
        old_http_connect = auth.http_connect
        try:
            auth.http_connect = mock_http_connect(204,
               {'x-auth-ttl': '1234', 'x-auth-groups': 'act:usr,act,AUTH_cfa'})
            req = Request.blank('/v/AUTH_cfa/c/o',
                                headers={'x-storage-token': 'AUTH_t'})
            req.environ['swift.cache'] = FakeMemcache()
            result = ''.join(self.test_auth(req.environ, start_response))
            self.assert_(result.startswith('204'), result)
            self.assertEquals(req.remote_user, 'act:usr,act,AUTH_cfa')
        finally:
            auth.http_connect = old_http_connect

    def test_authorize_bad_path(self):
        req = Request.blank('/badpath')
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 401)
        req = Request.blank('/badpath')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)
        req = Request.blank('')
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 404)
        req = Request.blank('')
        req.environ['swift.cache'] = FakeMemcache()
        result = ''.join(self.test_auth(req.environ, lambda x, y: None))
        self.assert_(result.startswith('404'), result)

    def test_authorize_account_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

    def test_authorize_acl_group_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act2'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr2'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

    def test_deny_cross_reseller(self):
        # Tests that cross-reseller is denied, even if ACLs/group names match
        req = Request.blank('/v1/OTHER_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        req.acl = 'act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

    def test_authorize_acl_referrer_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:.example.com'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 401)
        req = Request.blank('/v1/AUTH_cfa')
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.acl = '.r:.example.com'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 401)
        req = Request.blank('/v1/AUTH_cfa')
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)

    def test_account_put_permissions(self):
        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

        # Even PUTs to your own account as account admin should fail
        req = Request.blank('/v1/AUTH_old', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

    def test_account_delete_permissions(self):
        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

        # Even DELETEs to your own account as account admin should fail
        req = Request.blank('/v1/AUTH_old',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp and resp.status_int, 403)


if __name__ == '__main__':
    unittest.main()
