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

from swift.common.swob import Request

from swift.common.middleware import account_quotas

from swift.proxy.controllers.base import _get_cache_key, \
    headers_to_account_info


class FakeCache(object):
    def __init__(self, val):
        self.val = val

    def get(self, *args):
        return self.val

    def set(self, *args, **kwargs):
        pass


class FakeBadApp(object):
    def __init__(self, headers=[]):
        self.headers = headers

    def __call__(self, env, start_response):
        start_response('404 NotFound', self.headers)
        return []


class FakeApp(object):
    def __init__(self, headers=[]):
        self.headers = headers

    def __call__(self, env, start_response):
        # Cache the account_info (same as a real application)
        cache_key, env_key = _get_cache_key('a', None)
        env[env_key] = headers_to_account_info(self.headers, 200)
        start_response('200 OK', self.headers)
        return []


def start_response(*args):
    pass


class TestAccountQuota(unittest.TestCase):

    def test_unauthorized(self):
        headers = [('x-account-bytes-used', '1000'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        #Response code of 200 because authentication itself is not done here
        self.assertEquals(res.status_int, 200)

    def test_no_quotas(self):
        headers = [('x-account-bytes-used', '1000'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 200)

    def test_exceed_bytes_quota(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', '0')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 413)

    def test_exceed_bytes_quota_reseller(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', '0')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache,
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 200)

    def test_bad_application_quota(self):
        headers = []
        app = account_quotas.AccountQuotaMiddleware(FakeBadApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 404)

    def test_no_info_quota(self):
        headers = []
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 200)

    def test_not_exceed_bytes_quota(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', 2000)]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 200)

    def test_invalid_quotas(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': 'abc',
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 400)

    def test_valid_quotas_admin(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': '100'})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 403)

    def test_valid_quotas_reseller(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': '100',
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 200)

    def test_delete_quotas(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': ''})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 403)

    def test_delete_quotas_reseller(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        req = Request.blank('/v1/a/c',
                            environ={'REQUEST_METHOD': 'POST',
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': '',
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEquals(res.status_int, 200)

    def test_invalid_request_exception(self):
        headers = [('x-account-bytes-used', '1000'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        #Response code of 200 because authentication itself is not done here
        self.assertEquals(res.status_int, 200)


if __name__ == '__main__':
    unittest.main()
