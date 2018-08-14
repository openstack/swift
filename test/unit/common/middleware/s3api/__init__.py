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
import time

from swift.common import swob

from swift.common.middleware.s3api.s3api import filter_factory
from swift.common.middleware.s3api.etree import fromstring
from swift.common.middleware.s3api.utils import Config

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

        return self.swift(env, start_response)


class S3ApiTestCase(unittest.TestCase):
    def __init__(self, name):
        unittest.TestCase.__init__(self, name)

    def setUp(self):
        # setup default config
        self.conf = Config({
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
        })
        # those 2 settings has existed the original test setup
        self.conf.log_level = 'debug'

        self.app = FakeApp()
        self.swift = self.app.swift
        self.s3api = filter_factory({}, **self.conf)(self.app)

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

    def _get_error_code(self, body):
        elem = fromstring(body, 'Error')
        return elem.find('./Code').text

    def _get_error_message(self, body):
        elem = fromstring(body, 'Error')
        return elem.find('./Message').text

    def _test_method_error(self, method, path, response_class, headers={},
                           env={}, expected_xml_tags=None):
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
        if expected_xml_tags is not None:
            elem = fromstring(body, 'Error')
            self.assertEqual(set(expected_xml_tags),
                             {x.tag for x in elem})
        return self._get_error_code(body)

    def get_date_header(self):
        # email.utils.formatdate returns utc timestamp in default
        return email.utils.formatdate(time.time())

    def get_v4_amz_date_header(self):
        return datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

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
