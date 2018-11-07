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

from swift.common.swob import Request, wsgify, HTTPForbidden

from swift.common.middleware import account_quotas, copy

from swift.proxy.controllers.base import get_cache_key, \
    headers_to_account_info, headers_to_object_info


class FakeCache(object):
    def __init__(self, val):
        self.val = val

    def get(self, *args):
        return self.val

    def set(self, *args, **kwargs):
        pass


class FakeBadApp(object):
    def __init__(self, headers=None):
        if headers is None:
            headers = []
        self.headers = headers

    def __call__(self, env, start_response):
        start_response('404 NotFound', self.headers)
        return []


class FakeApp(object):
    def __init__(self, headers=None):
        if headers is None:
            headers = []
        self.headers = headers

    def __call__(self, env, start_response):
        if 'swift.authorize' in env:
            aresp = env['swift.authorize'](Request(env))
            if aresp:
                return aresp(env, start_response)
        if env['REQUEST_METHOD'] == "HEAD" and \
                env['PATH_INFO'] == '/v1/a/c2/o2':
            cache_key = get_cache_key('a', 'c2', 'o2')
            env.setdefault('swift.infocache', {})[cache_key] = \
                headers_to_object_info(self.headers, 200)
            start_response('200 OK', self.headers)
        elif env['REQUEST_METHOD'] == "HEAD" and \
                env['PATH_INFO'] == '/v1/a/c2/o3':
            start_response('404 Not Found', [])
        else:
            # Cache the account_info (same as a real application)
            cache_key = get_cache_key('a')
            env.setdefault('swift.infocache', {})[cache_key] = \
                headers_to_account_info(self.headers, 200)
            start_response('200 OK', self.headers)
        return []


class FakeAuthFilter(object):

    def __init__(self, app):
        self.app = app

    @wsgify
    def __call__(self, req):
        def authorize(req):
            if req.headers['x-auth-token'] == 'secret':
                return
            return HTTPForbidden(request=req)
        req.environ['swift.authorize'] = authorize
        return req.get_response(self.app)


class TestAccountQuota(unittest.TestCase):

    def test_unauthorized(self):
        headers = [('x-account-bytes-used', '1000'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        # Response code of 200 because authentication itself is not done here
        self.assertEqual(res.status_int, 200)

    def test_no_quotas(self):
        headers = [('x-account-bytes-used', '1000'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_obj_request_ignores_attempt_to_set_quotas(self):
        # If you try to set X-Account-Meta-* on an object, it's ignored, so
        # the quota middleware shouldn't complain about it even if we're not a
        # reseller admin.
        headers = [('x-account-bytes-used', '1000')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Account-Meta-Quota-Bytes': '99999'},
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_container_request_ignores_attempt_to_set_quotas(self):
        # As with an object, if you try to set X-Account-Meta-* on a
        # container, it's ignored.
        headers = [('x-account-bytes-used', '1000')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c',
                            headers={'X-Account-Meta-Quota-Bytes': '99999'},
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_bogus_quota_is_ignored(self):
        # This can happen if the metadata was set by a user prior to the
        # activation of the account-quota middleware
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', 'pasty-plastogene')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_exceed_bytes_quota(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', '0')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_exceed_quota_not_authorized(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', '0')]
        app = FakeAuthFilter(
            account_quotas.AccountQuotaMiddleware(FakeApp(headers)))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'x-auth-token': 'bad-secret'},
                            environ={'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 403)

    def test_exceed_quota_authorized(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', '0')]
        app = FakeAuthFilter(
            account_quotas.AccountQuotaMiddleware(FakeApp(headers)))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'x-auth-token': 'secret'},
                            environ={'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 413)

    def test_under_quota_not_authorized(self):
        headers = [('x-account-bytes-used', '0'),
                   ('x-account-meta-quota-bytes', '1000')]
        app = FakeAuthFilter(
            account_quotas.AccountQuotaMiddleware(FakeApp(headers)))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'x-auth-token': 'bad-secret'},
                            environ={'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 403)

    def test_under_quota_authorized(self):
        headers = [('x-account-bytes-used', '0'),
                   ('x-account-meta-quota-bytes', '1000')]
        app = FakeAuthFilter(
            account_quotas.AccountQuotaMiddleware(FakeApp(headers)))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'x-auth-token': 'secret'},
                            environ={'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_over_quota_container_create_still_works(self):
        headers = [('x-account-bytes-used', '1001'),
                   ('x-account-meta-quota-bytes', '1000')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/new_container',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'HTTP_X_CONTAINER_META_BERT': 'ernie',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_over_quota_container_post_still_works(self):
        headers = [('x-account-bytes-used', '1001'),
                   ('x-account-meta-quota-bytes', '1000')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/new_container',
                            environ={'REQUEST_METHOD': 'POST',
                                     'HTTP_X_CONTAINER_META_BERT': 'ernie',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_over_quota_obj_post_still_works(self):
        headers = [('x-account-bytes-used', '1001'),
                   ('x-account-meta-quota-bytes', '1000')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'POST',
                                     'HTTP_X_OBJECT_META_BERT': 'ernie',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_exceed_bytes_quota_reseller(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', '0')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache,
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_exceed_bytes_quota_reseller_copy_from(self):
        headers = [('x-account-bytes-used', '500'),
                   ('x-account-meta-quota-bytes', '1000'),
                   ('content-length', '1000')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache,
                                     'reseller_request': True},
                            headers={'x-copy-from': 'c2/o2'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_exceed_bytes_quota_reseller_copy_verb(self):
        headers = [('x-account-bytes-used', '500'),
                   ('x-account-meta-quota-bytes', '1000'),
                   ('content-length', '1000')]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.cache': cache,
                                     'reseller_request': True},
                            headers={'Destination': 'c/o'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_bad_application_quota(self):
        headers = []
        app = account_quotas.AccountQuotaMiddleware(FakeBadApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 404)

    def test_no_info_quota(self):
        headers = []
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_not_exceed_bytes_quota(self):
        headers = [('x-account-bytes-used', '1000'),
                   ('x-account-meta-quota-bytes', 2000)]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_invalid_quotas(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': 'abc',
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 400)

    def test_valid_quotas_admin(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': '100'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 403)

    def test_valid_quotas_reseller(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': '100',
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_delete_quotas(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a',
                            environ={'REQUEST_METHOD': 'POST',
                                     'swift.cache': cache,
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': ''})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 403)

    def test_delete_quotas_with_remove_header(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a', environ={
            'REQUEST_METHOD': 'POST',
            'swift.cache': cache,
            'HTTP_X_REMOVE_ACCOUNT_META_QUOTA_BYTES': 'True'})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 403)

    def test_delete_quotas_reseller(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        req = Request.blank('/v1/a',
                            environ={'REQUEST_METHOD': 'POST',
                                     'HTTP_X_ACCOUNT_META_QUOTA_BYTES': '',
                                     'reseller_request': True})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_delete_quotas_with_remove_header_reseller(self):
        headers = [('x-account-bytes-used', '0'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1/a', environ={
            'REQUEST_METHOD': 'POST',
            'swift.cache': cache,
            'HTTP_X_REMOVE_ACCOUNT_META_QUOTA_BYTES': 'True',
            'reseller_request': True})
        res = req.get_response(app)
        self.assertEqual(res.status_int, 200)

    def test_invalid_request_exception(self):
        headers = [('x-account-bytes-used', '1000'), ]
        app = account_quotas.AccountQuotaMiddleware(FakeApp(headers))
        cache = FakeCache(None)
        req = Request.blank('/v1',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache})
        res = req.get_response(app)
        # Response code of 200 because authentication itself is not done here
        self.assertEqual(res.status_int, 200)


class AccountQuotaCopyingTestCases(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.aq_filter = account_quotas.filter_factory({})(self.app)
        self.copy_filter = copy.filter_factory({})(self.aq_filter)

    def test_exceed_bytes_quota_copy_from(self):
        headers = [('x-account-bytes-used', '500'),
                   ('x-account-meta-quota-bytes', '1000'),
                   ('content-length', '1000')]
        self.app.headers = headers
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o2'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_exceed_bytes_quota_copy_verb(self):
        headers = [('x-account-bytes-used', '500'),
                   ('x-account-meta-quota-bytes', '1000'),
                   ('content-length', '1000')]
        self.app.headers = headers
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.cache': cache},
                            headers={'Destination': '/c/o'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 413)
        self.assertEqual(res.body, b'Upload exceeds quota.')

    def test_not_exceed_bytes_quota_copy_from(self):
        headers = [('x-account-bytes-used', '0'),
                   ('x-account-meta-quota-bytes', '1000'),
                   ('content-length', '1000')]
        self.app.headers = headers
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o2'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

    def test_not_exceed_bytes_quota_copy_verb(self):
        headers = [('x-account-bytes-used', '0'),
                   ('x-account-meta-quota-bytes', '1000'),
                   ('content-length', '1000')]
        self.app.headers = headers
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c2/o2',
                            environ={'REQUEST_METHOD': 'COPY',
                                     'swift.cache': cache},
                            headers={'Destination': '/c/o'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

    def test_quota_copy_from_no_src(self):
        headers = [('x-account-bytes-used', '0'),
                   ('x-account-meta-quota-bytes', '1000')]
        self.app.headers = headers
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': '/c2/o3'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 200)

    def test_quota_copy_from_bad_src(self):
        headers = [('x-account-bytes-used', '0'),
                   ('x-account-meta-quota-bytes', '1000')]
        self.app.headers = headers
        cache = FakeCache(None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'swift.cache': cache},
                            headers={'x-copy-from': 'bad_path'})
        res = req.get_response(self.copy_filter)
        self.assertEqual(res.status_int, 412)

if __name__ == '__main__':
    unittest.main()
