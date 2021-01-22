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

import unittest
from datetime import datetime
import email
import mock
import time

from swift.common import swob

from swift.common.middleware.s3api.s3api import filter_factory
from swift.common.middleware.s3api.etree import fromstring

from test.debug_logger import debug_logger
from test.unit.common.middleware.s3api.helpers import FakeSwift


class FakeApp(object):
    def __init__(self):
        self.swift = FakeSwift()

    def _update_s3_path_info(self, env):
        """
        For S3 requests, Swift auth middleware replaces a user name in
        env['PATH_INFO'] with a valid tenant id.
        E.g. '/v1/test:tester/bucket/object' will become
        '/v1/AUTH_test/bucket/object'. This method emulates the behavior.
        """
        tenant_user = env['s3api.auth_details']['access_key']
        tenant, user = tenant_user.rsplit(':', 1)

        path = env['PATH_INFO']
        env['PATH_INFO'] = path.replace(tenant_user, 'AUTH_' + tenant)

    def __call__(self, env, start_response):
        if 's3api.auth_details' in env:
            self._update_s3_path_info(env)

        if env['REQUEST_METHOD'] == 'TEST':

            def authorize_cb(req):
                # Assume swift owner, if not yet set
                req.environ.setdefault('REMOTE_USER', 'authorized')
                req.environ.setdefault('swift_owner', True)
                # But then default to blocking authz, to ensure we've replaced
                # the default auth system
                return swob.HTTPForbidden(request=req)

            env['swift.authorize'] = authorize_cb

        return self.swift(env, start_response)


class S3ApiTestCase(unittest.TestCase):
    def __init__(self, name):
        unittest.TestCase.__init__(self, name)

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
            'max_upload_part_num': 1000,
            'check_bucket_owner': False,
            'force_swift_request_proxy_log': False,
            'allow_multipart_uploads': True,
            'min_segment_size': 5242880,
            'log_level': 'debug'
        }

        self.app = FakeApp()
        self.swift = self.app.swift
        # note: self.conf has no __file__ key so check_pipeline will be skipped
        # when constructing self.s3api
        self.s3api = filter_factory({}, **self.conf)(self.app)
        self.logger = self.s3api.logger = self.swift.logger = debug_logger()

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

    def get_date_header(self):
        # email.utils.formatdate returns utc timestamp in default
        return email.utils.formatdate(time.time())

    def get_v4_amz_date_header(self, when=None):
        if when is None:
            when = datetime.utcnow()
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

    def call_s3api(self, req, **kwargs):
        return self.call_app(req, app=self.s3api, **kwargs)
