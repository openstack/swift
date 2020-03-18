# Copyright (c) 2010-2012 OpenStack Foundation
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

from swift.common.swob import Request, Response
from swift.common.middleware import gatekeeper


class FakeApp(object):
    def __init__(self, headers=None):
        if headers is None:
            headers = {}

        self.headers = headers
        self.req = None

    def __call__(self, env, start_response):
        self.req = Request(env)
        return Response(request=self.req, body=b'FAKE APP',
                        headers=self.headers)(env, start_response)


class FakeMiddleware(object):

    def __init__(self, app, conf, header_list=None):
        self.app = app
        self.conf = conf
        self.header_list = header_list

    def __call__(self, env, start_response):

        def fake_resp(status, response_headers, exc_info=None):
            for i in self.header_list:
                response_headers.append(i)
            return start_response(status, response_headers, exc_info)

        return self.app(env, fake_resp)


class TestGatekeeper(unittest.TestCase):
    methods = ['PUT', 'POST', 'GET', 'DELETE', 'HEAD', 'COPY', 'OPTIONS']

    allowed_headers = {'xx-account-sysmeta-foo': 'value',
                       'xx-container-sysmeta-foo': 'value',
                       'xx-object-sysmeta-foo': 'value',
                       'x-account-meta-foo': 'value',
                       'x-container-meta-foo': 'value',
                       'x-object-meta-foo': 'value',
                       'x-timestamp-foo': 'value'}

    sysmeta_headers = {'x-account-sysmeta-': 'value',
                       'x-container-sysmeta-': 'value',
                       'x-object-sysmeta-': 'value',
                       'x-account-sysmeta-foo': 'value',
                       'x-container-sysmeta-foo': 'value',
                       'x-object-sysmeta-foo': 'value',
                       'X-Account-Sysmeta-BAR': 'value',
                       'X-Container-Sysmeta-BAR': 'value',
                       'X-Object-Sysmeta-BAR': 'value'}

    x_backend_headers = {'X-Backend-Replication': 'true',
                         'X-Backend-Replication-Headers': 'stuff'}

    object_transient_sysmeta_headers = {
        'x-object-transient-sysmeta-': 'value',
        'x-object-transient-sysmeta-foo': 'value'}
    x_timestamp_headers = {'X-Timestamp': '1455952805.719739'}

    forbidden_headers_out = dict(sysmeta_headers)
    forbidden_headers_out.update(x_backend_headers)
    forbidden_headers_out.update(object_transient_sysmeta_headers)
    forbidden_headers_in = dict(forbidden_headers_out)
    shunted_headers_in = dict(x_timestamp_headers)

    def _assertHeadersEqual(self, expected, actual):
        for key in expected:
            self.assertIn(key.lower(), actual)

    def _assertHeadersAbsent(self, unexpected, actual):
        for key in unexpected:
            self.assertNotIn(key.lower(), actual)

    def get_app(self, app, global_conf, **local_conf):
        factory = gatekeeper.filter_factory(global_conf, **local_conf)
        return factory(app)

    def test_ok_header(self):
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': 'PUT'},
                            headers=self.allowed_headers)
        fake_app = FakeApp()
        app = self.get_app(fake_app, {})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(resp.body, b'FAKE APP')
        self._assertHeadersEqual(self.allowed_headers, fake_app.req.headers)

    def _test_reserved_header_removed_inbound(self, method):
        headers = dict(self.forbidden_headers_in)
        headers.update(self.allowed_headers)
        headers.update(self.shunted_headers_in)
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': method},
                            headers=headers)
        fake_app = FakeApp()
        app = self.get_app(fake_app, {})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        expected_headers = dict(self.allowed_headers)
        # shunt_inbound_x_timestamp should be enabled by default
        expected_headers.update({'X-Backend-Inbound-' + k: v
                                 for k, v in self.shunted_headers_in.items()})
        self._assertHeadersEqual(expected_headers, fake_app.req.headers)
        unexpected_headers = dict(self.forbidden_headers_in)
        unexpected_headers.update(self.shunted_headers_in)
        self._assertHeadersAbsent(unexpected_headers, fake_app.req.headers)

    def test_reserved_header_removed_inbound(self):
        for method in self.methods:
            self._test_reserved_header_removed_inbound(method)

    def _test_reserved_header_shunted_inbound(self, method):
        headers = dict(self.shunted_headers_in)
        headers.update(self.allowed_headers)
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': method},
                            headers=headers)
        fake_app = FakeApp()
        app = self.get_app(fake_app, {}, shunt_inbound_x_timestamp='true')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        expected_headers = dict(self.allowed_headers)
        expected_headers.update({'X-Backend-Inbound-' + k: v
                                 for k, v in self.shunted_headers_in.items()})
        self._assertHeadersEqual(expected_headers, fake_app.req.headers)
        self._assertHeadersAbsent(self.shunted_headers_in,
                                  fake_app.req.headers)

    def test_reserved_header_shunted_inbound(self):
        for method in self.methods:
            self._test_reserved_header_shunted_inbound(method)

    def _test_reserved_header_shunt_bypassed_inbound(self, method):
        headers = dict(self.shunted_headers_in)
        headers.update(self.allowed_headers)
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': method},
                            headers=headers)
        fake_app = FakeApp()
        app = self.get_app(fake_app, {}, shunt_inbound_x_timestamp='false')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        expected_headers = dict(self.allowed_headers)
        expected_headers.update(self.shunted_headers_in)
        self._assertHeadersEqual(expected_headers, fake_app.req.headers)

    def test_reserved_header_shunt_bypassed_inbound(self):
        for method in self.methods:
            self._test_reserved_header_shunt_bypassed_inbound(method)

    def _test_reserved_header_removed_outbound(self, method):
        headers = dict(self.forbidden_headers_out)
        headers.update(self.allowed_headers)
        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': method})
        fake_app = FakeApp(headers=headers)
        app = self.get_app(fake_app, {})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self._assertHeadersEqual(self.allowed_headers, resp.headers)
        self._assertHeadersAbsent(self.forbidden_headers_out, resp.headers)

    def test_reserved_header_removed_outbound(self):
        for method in self.methods:
            self._test_reserved_header_removed_outbound(method)

    def _test_duplicate_headers_not_removed(self, method, app_hdrs):

        def fake_factory(global_conf, **local_conf):
            conf = global_conf.copy()
            conf.update(local_conf)
            headers = [('X-Header', 'xxx'),
                       ('X-Header', 'yyy')]

            def fake_filter(app):
                return FakeMiddleware(app, conf, headers)
            return fake_filter

        def fake_start_response(status, response_headers, exc_info=None):
            hdr_list = []
            for k, v in response_headers:
                if k == 'X-Header':
                    hdr_list.append(v)
            self.assertTrue('xxx' in hdr_list)
            self.assertTrue('yyy' in hdr_list)
            self.assertEqual(len(hdr_list), 2)

        req = Request.blank('/v/a/c', environ={'REQUEST_METHOD': method})
        fake_app = FakeApp(headers=app_hdrs)
        factory = gatekeeper.filter_factory({})
        factory_wrap = fake_factory({})
        app = factory(factory_wrap(fake_app))
        app(req.environ, fake_start_response)

    def test_duplicate_headers_not_removed(self):
        for method in self.methods:
            for app_hdrs in ({}, self.forbidden_headers_out):
                self._test_duplicate_headers_not_removed(method, app_hdrs)

    def _test_location_header(self, location_path):
        headers = {'Location': location_path}
        req = Request.blank(
            '/v/a/c', environ={'REQUEST_METHOD': 'GET',
                               'swift.leave_relative_location': True})

        class SelfishApp(FakeApp):
            def __call__(self, env, start_response):
                self.req = Request(env)
                resp = Response(request=self.req, body=b'FAKE APP',
                                headers=self.headers)
                # like webob, middlewares in the pipeline may rewrite
                # location header from relative to absolute
                resp.location = resp.absolute_location()
                return resp(env, start_response)

        selfish_app = SelfishApp(headers=headers)

        app = self.get_app(selfish_app, {})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertIn('Location', resp.headers)
        self.assertEqual(resp.headers['Location'], location_path)

    def test_location_header_fixed(self):
        self._test_location_header('/v/a/c/o2')
        self._test_location_header('/v/a/c/o2?query=path&query2=doit')
        self._test_location_header('/v/a/c/o2?query=path#test')
        self._test_location_header('/v/a/c/o2;whatisparam?query=path#test')

    def test_allow_reserved_names(self):
        fake_app = FakeApp()
        app = self.get_app(fake_app, {})
        headers = {
            'X-Allow-Reserved-Names': 'some-value'
        }

        req = Request.blank('/v/a/c/o', method='GET', headers=headers)
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertNotIn('X-Backend-Allow-Reserved-Names',
                         fake_app.req.headers)
        self.assertIn('X-Allow-Reserved-Names',
                      fake_app.req.headers)
        self.assertEqual(
            'some-value',
            fake_app.req.headers['X-Allow-Reserved-Names'])

        app.allow_reserved_names_header = True
        req = Request.blank('/v/a/c/o', method='GET', headers=headers)
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertIn('X-Backend-Allow-Reserved-Names',
                      fake_app.req.headers)
        self.assertEqual(
            'some-value',
            fake_app.req.headers['X-Backend-Allow-Reserved-Names'])
        self.assertEqual(
            'some-value',
            req.headers['X-Backend-Allow-Reserved-Names'])
        self.assertNotIn('X-Allow-Reserved-Names', fake_app.req.headers)
        self.assertNotIn('X-Allow-Reserved-Names', req.headers)


if __name__ == '__main__':
    unittest.main()
