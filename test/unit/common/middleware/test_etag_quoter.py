# Copyright (c) 2010-2020 OpenStack Foundation
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

from unittest import mock
import unittest

from swift.common import swob
from swift.common.middleware import etag_quoter
from swift.proxy.controllers.base import get_cache_key

from test.unit.common.middleware.helpers import FakeSwift


def set_info_cache(req, cache_data, account, container=None):
    req.environ.setdefault('swift.infocache', {})[
        get_cache_key(account, container)] = cache_data


class TestEtagQuoter(unittest.TestCase):
    def get_mw(self, conf, etag='unquoted-etag', path=None):
        if path is None:
            path = '/v1/AUTH_acc/con/some/path/to/obj'
        app = FakeSwift()
        hdrs = {} if etag is None else {'ETag': etag}
        app.register('GET', path, swob.HTTPOk, hdrs)
        return etag_quoter.filter_factory({}, **conf)(app)

    @mock.patch('swift.common.middleware.etag_quoter.register_swift_info')
    def test_swift_info(self, mock_register):
        self.get_mw({})
        self.assertEqual(mock_register.mock_calls, [
            mock.call('etag_quoter', enable_by_default=False)])
        mock_register.reset_mock()

        self.get_mw({'enable_by_default': '1'})
        self.assertEqual(mock_register.mock_calls, [
            mock.call('etag_quoter', enable_by_default=True)])
        mock_register.reset_mock()

        self.get_mw({'enable_by_default': 'no'})
        self.assertEqual(mock_register.mock_calls, [
            mock.call('etag_quoter', enable_by_default=False)])

    def test_account_on_overrides_cluster_off(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': '1'},
        }, 'AUTH_acc')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {},
        }, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 'false'}))
        self.assertEqual(resp.headers['ETag'], '"unquoted-etag"')

    def test_account_off_overrides_cluster_on(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': 'no'},
        }, 'AUTH_acc')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {},
        }, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 'yes'}))
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')

    def test_container_on_overrides_cluster_off(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {},
        }, 'AUTH_acc')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': 't'},
        }, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 'false'}))
        self.assertEqual(resp.headers['ETag'], '"unquoted-etag"')

    def test_container_off_overrides_cluster_on(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {},
        }, 'AUTH_acc')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': '0'},
        }, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 'yes'}))
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')

    def test_container_on_overrides_account_off(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': 'no'},
        }, 'AUTH_acc')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': 't'},
        }, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({}))
        self.assertEqual(resp.headers['ETag'], '"unquoted-etag"')

    def test_container_off_overrides_account_on(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': 'yes'},
        }, 'AUTH_acc')
        set_info_cache(req, {
            'status': 200,
            'sysmeta': {'rfc-compliant-etags': 'false'},
        }, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({}))
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')

    def test_cluster_wide(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 't'}))
        self.assertEqual(resp.headers['ETag'], '"unquoted-etag"')

    def test_already_valid(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 't'},
                                            '"quoted-etag"'))
        self.assertEqual(resp.headers['ETag'], '"quoted-etag"')

    def test_already_weak_but_valid(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 't'},
                                            'W/"weak-etag"'))
        self.assertEqual(resp.headers['ETag'], 'W/"weak-etag"')

    def test_only_half_valid(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 't'},
                                            '"weird-etag'))
        self.assertEqual(resp.headers['ETag'], '""weird-etag"')

    def test_no_etag(self):
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(self.get_mw({'enable_by_default': 't'},
                                            etag=None))
        self.assertNotIn('ETag', resp.headers)

    def test_non_swift_path(self):
        path = '/some/other/location/entirely'
        req = swob.Request.blank(path)
        resp = req.get_response(self.get_mw({'enable_by_default': 't'},
                                            path=path))
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')

    def test_non_object_request(self):
        path = '/v1/AUTH_acc/con'
        req = swob.Request.blank(path)
        resp = req.get_response(self.get_mw({'enable_by_default': 't'},
                                            path=path))
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')

    def test_no_container_info(self):
        mw = self.get_mw({'enable_by_default': 't'})
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc')
        mw.app.register('HEAD', '/v1/AUTH_acc/con',
                        swob.HTTPServiceUnavailable, {})
        resp = req.get_response(mw)
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')
        set_info_cache(req, {'status': 404, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(mw)
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(mw)
        self.assertEqual(resp.headers['ETag'], '"unquoted-etag"')

    def test_no_account_info(self):
        mw = self.get_mw({'enable_by_default': 't'})
        req = swob.Request.blank('/v1/AUTH_acc/con/some/path/to/obj')
        mw.app.register('HEAD', '/v1/AUTH_acc',
                        swob.HTTPServiceUnavailable, {})
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc', 'con')
        resp = req.get_response(mw)
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')
        set_info_cache(req, {'status': 404, 'sysmeta': {}}, 'AUTH_acc')
        resp = req.get_response(mw)
        self.assertEqual(resp.headers['ETag'], 'unquoted-etag')
        set_info_cache(req, {'status': 200, 'sysmeta': {}}, 'AUTH_acc')
        resp = req.get_response(mw)
        self.assertEqual(resp.headers['ETag'], '"unquoted-etag"')
