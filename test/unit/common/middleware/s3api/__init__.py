# Copyright (c) 2011-2014 OpenStack Foundation.
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
import json
import unittest
from datetime import datetime
import email
from unittest import mock
import time
from contextlib import contextmanager

from swift.common import swob, utils
from swift.common.http import is_success

from swift.common.middleware.s3api.s3api import filter_factory
from swift.common.middleware.s3api.etree import fromstring
from swift.common.middleware.s3api.subresource import Owner, encode_acl, \
    Grant, User, ACL, PERMISSIONS, AllUsers, AuthenticatedUsers

from test.unit.common.middleware.helpers import FakeSwift


class FakeAuthApp(object):
    container_existence_skip_cache = 0.0
    account_existence_skip_cache = 0.0

    def __init__(self, app):
        self.remote_user = 'authorized'
        self.app = app

    def _update_s3_path_info(self, env):
        """
        For S3 requests, Swift auth middleware replaces a user name in
        env['PATH_INFO'] with a valid tenant id.
        E.g. '/v1/test:tester/bucket/object' will become
        '/v1/AUTH_test/bucket/object'. This method emulates the behavior.
        """
        tenant_user = swob.str_to_wsgi(env['s3api.auth_details']['access_key'])
        tenant, user = tenant_user.rsplit(':', 1)

        path = env['PATH_INFO']
        # Make sure it's valid WSGI
        swob.wsgi_to_str(path)
        env['PATH_INFO'] = path.replace(tenant_user, 'AUTH_' + tenant)

    @staticmethod
    def authorize_cb(req):
        # Assume swift owner, if not yet set
        req.environ.setdefault('swift_owner', True)
        # But then default to blocking authz, to ensure we've replaced
        # the default auth system
        return swob.HTTPForbidden(request=req)

    def handle(self, env):
        if 's3api.auth_details' in env:
            self._update_s3_path_info(env)
        else:
            return

        if self.remote_user:
            env['REMOTE_USER'] = self.remote_user

        if env['REQUEST_METHOD'] == 'TEST':
            env['swift.authorize'] = self.authorize_cb
        else:
            env['swift.authorize'] = lambda req: None

        if 'swift.authorize_override' in env:
            return

    def __call__(self, env, start_response):
        self.handle(env)
        return self.app(env, start_response)


class S3ApiTestCase(unittest.TestCase):

    def __init__(self, name):
        unittest.TestCase.__init__(self, name)

    def _wrap_app(self, app):
        return FakeAuthApp(app)

    def setUp(self):
        # setup default config dict
        self.conf = {
            'allow_no_owner': False,
            'location': 'us-east-1',
            'dns_compliant_bucket_names': True,
            'max_bucket_listing': 1000,
            'max_parts_listing': 1000,
            'max_multi_delete_objects': 1000,
            's3_acl': False,
            'storage_domain': 'localhost',
            'auth_pipeline_check': True,
            'max_upload_part_num': 10000,
            'check_bucket_owner': False,
            'force_swift_request_proxy_log': False,
            'allow_multipart_uploads': True,
            'min_segment_size': 5242880,
            'log_level': 'debug'
        }

        # note: self.conf has no __file__ key so check_pipeline will be skipped
        # when constructing self.s3api
        self.swift = FakeSwift()
        self.app = self._wrap_app(self.swift)
        self.app._pipeline_final_app = self.swift
        self.s3api = filter_factory({}, **self.conf)(self.app)
        self.logger = self.s3api.logger = self.swift.logger

        # if you change the registered acl response for /bucket or
        # /bucket/object tearDown will complain at you; you can set this to
        # True in order to indicate you know what you're doing
        self.s3acl_response_modified = False

        self.swift.register('HEAD', '/v1/AUTH_test',
                            swob.HTTPOk, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('PUT', '/v1/AUTH_test/bucket',
                            swob.HTTPCreated, {}, None)
        self.swift.register('POST', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('GET', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, {'etag': 'object etag'}, "")
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPCreated, {'etag': 'object etag'}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNoContent, {}, None)

        self.mock_get_swift_info_result = {'object_versioning': {}}
        for s3api_path in (
            'controllers.obj',
            'controllers.bucket',
            'controllers.multi_delete',
            'controllers.versioning',
        ):
            patcher = mock.patch(
                'swift.common.middleware.s3api.%s.get_swift_info' % s3api_path,
                return_value=self.mock_get_swift_info_result)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _register_bucket_policy_index_head(self, bucket, bucket_policy_index):
        # register bucket HEAD response with given policy index header
        headers = {'X-Backend-Storage-Policy-Index': str(bucket_policy_index)}
        self.swift.register('HEAD', '/v1/AUTH_test/' + bucket,
                            swob.HTTPNoContent, headers)

    def _assert_policy_index(self, req_headers, resp_headers, policy_index):
        self.assertNotIn('X-Backend-Storage-Policy-Index', req_headers)
        self.assertEqual(resp_headers.get('X-Backend-Storage-Policy-Index'),
                         str(policy_index))

    def _get_error_code(self, body):
        elem = fromstring(body, 'Error')
        return elem.find('./Code').text

    def _get_error_message(self, body):
        elem = fromstring(body, 'Error')
        return elem.find('./Message').text

    def _test_method_error(self, method, path, response_class, headers={},
                           env={}, expected_xml_tags=None,
                           expected_status=None):
        if not path.startswith('/'):
            path = '/' + path  # add a missing slash before the path

        uri = '/v1/AUTH_test'
        if path != '/':
            uri += path

        self.swift.register(method, uri, response_class, headers, None)
        headers.update({'Authorization': 'AWS test:tester:hmac',
                        'Date': self.get_date_header()})
        env.update({'REQUEST_METHOD': method})
        req = swob.Request.blank(path, environ=env, headers=headers)
        status, headers, body = self.call_s3api(req)
        if expected_status is not None:
            self.assertEqual(status, expected_status)
        if expected_xml_tags is not None:
            elem = fromstring(body, 'Error')
            self.assertEqual(set(expected_xml_tags),
                             {x.tag for x in elem})
        return self._get_error_code(body)

    def get_date_header(self, skew=0):
        # email.utils.formatdate returns utc timestamp in default
        return email.utils.formatdate(time.time() + skew)

    def get_v4_amz_date_header(self, offset=None):
        when = datetime.now(utils.UTC)
        if offset is not None:
            when += offset
        return when.strftime('%Y%m%dT%H%M%SZ')

    def call_app(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.app

        req.headers.setdefault("User-Agent", "Mozzarella Foxfire")

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = swob.HeaderKeyDict(h)

        body_iter = app(req.environ, start_response)
        body = b''
        caught_exc = None
        try:
            for chunk in body_iter:
                body += chunk
        except Exception as exc:
            if expect_exception:
                caught_exc = exc
            else:
                raise

        if expect_exception:
            return status[0], headers[0], body, caught_exc
        else:
            return status[0], headers[0], body

    @contextmanager
    def stubbed_container_info(self, versioning_enabled=False):
        """
        some tests might want to opt-out of container_info HEAD requests; e.g.

        with self.stubbed_container_info():
            status, headers, body = self.call_s3api(req)
        """
        fake_info = {'status': 204}
        if versioning_enabled:
            fake_info['sysmeta'] = {
                'versions-container': '\x00versions\x00bucket',
            }

        with mock.patch('swift.common.middleware.s3api.s3request.'
                        'get_container_info', return_value=fake_info):
            yield

    def call_s3api(self, req, **kwargs):
        return self.call_app(req, app=self.s3api, **kwargs)


def _gen_test_headers(owner, grants=[], resource='container'):
    if not grants:
        grants = [Grant(User('test:tester'), 'FULL_CONTROL')]
    return encode_acl(resource, ACL(owner, grants))


def _gen_grant(permission):
    # generate Grant with a grantee named by "permission"
    account_name = '%s:%s' % ('test', permission.lower())
    return Grant(User(account_name), permission)


class S3ApiTestCaseAcl(S3ApiTestCase):

    def setUp(self):
        super(S3ApiTestCaseAcl, self).setUp()
        self.s3api.conf.s3_acl = True

        # some extra buckets for s3acl tests
        buckets = ['bucket', 'public', 'authenticated']
        for bucket in buckets:
            path = '/v1/AUTH_test/' + bucket
            self.swift.register('HEAD', path, swob.HTTPNoContent, {}, None),
            self.swift.register('GET', path, swob.HTTPOk, {}, json.dumps([])),

        # setup sticky ACL headers...
        self.grants = [_gen_grant(perm) for perm in PERMISSIONS]
        self.default_owner = Owner('test:tester', 'test:tester')
        container_headers = _gen_test_headers(self.default_owner, self.grants)
        object_headers = _gen_test_headers(
            self.default_owner, self.grants, 'object')
        public_headers = _gen_test_headers(
            self.default_owner, [Grant(AllUsers(), 'READ')])
        authenticated_headers = _gen_test_headers(
            self.default_owner, [Grant(AuthenticatedUsers(), 'READ')],
            'bucket')

        sticky_s3acl_headers = {
            '/v1/AUTH_test/bucket': container_headers,
            '/v1/AUTH_test/bucket+segments': container_headers,
            '/v1/AUTH_test/bucket/object': object_headers,
            '/v1/AUTH_test/public': public_headers,
            '/v1/AUTH_test/authenticated': authenticated_headers,
        }
        for path, headers in sticky_s3acl_headers.items():
            self.swift.update_sticky_response_headers(path, headers)

    def tearDown(self):
        # sanity the test didn't break the the ACLs
        swift_path_acl_resp_checks = {
            '/v1/AUTH_test/bucket': (
                'X-Container-Sysmeta-S3api-Acl', '/bucket',
                swob.HTTPNoContent),
            '/v1/AUTH_test/bucket/object': (
                'X-Object-Sysmeta-S3api-Acl', '/bucket/object', swob.HTTPOk),
        }
        check_paths = []
        for swift_path, (acl, check, resp_class) in \
                swift_path_acl_resp_checks.items():
            if self.s3acl_response_modified:
                # this is expected to reset back to the original sticky headers
                self.swift.register('HEAD', swift_path, resp_class, {}, None)
            req = swob.Request.blank(swift_path, method='HEAD')
            status, headers, body = self.call_app(req)
            if is_success(int(status.split()[0])):
                self.assertIn(acl, headers,
                              'In tearDown it seems the test (accidently?) '
                              'removed the ACL on %s' % swift_path)
                check_paths.append(check)
            else:
                self.fail('test changed resp for %s' % swift_path)
        account_expected = {
            'test:tester': 200,
            'test:other': 403,
        }
        for account, expected in account_expected.items():
            for path in check_paths:
                req = swob.Request.blank(path, method='HEAD', headers={
                    'Authorization': 'AWS %s:hmac' % account,
                    'Date': self.get_date_header()})
                status, headers, body = self.call_s3api(req)
                self.assertEqual(int(status.split()[0]), expected,
                                 'In tearDown it seems the test (accidently?) '
                                 'broke ACL access for %s to %s' % (
                                     account, path))
