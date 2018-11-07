# Copyright (c) 2013 OpenStack Foundation
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
import os
import shutil
import tempfile
import unittest
import uuid
import mock

from swift.common import swob
from swift.common.middleware import container_sync
from swift.proxy.controllers.base import get_cache_key
from swift.proxy.controllers.info import InfoController

from test.unit import FakeLogger


class FakeApp(object):

    def __call__(self, env, start_response):
        if env.get('PATH_INFO') == '/info':
            controller = InfoController(
                app=None, version=None, expose_info=True,
                disallowed_sections=[], admin_key=None)
            handler = getattr(controller, env.get('REQUEST_METHOD'))
            return handler(swob.Request(env))(env, start_response)
        if env.get('swift.authorize_override'):
            body = b'Response to Authorized Request'
        else:
            body = b'Pass-Through Response'
        headers = [('Content-Length', str(len(body)))]
        if 'HTTP_X_TIMESTAMP' in env:
            headers.append(('X-Timestamp', env['HTTP_X_TIMESTAMP']))
        start_response('200 OK', headers)
        return [body]


class TestContainerSync(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        with open(
                os.path.join(self.tempdir, 'container-sync-realms.conf'),
                'w') as fp:
            fp.write('''
[US]
key = 9ff3b71c849749dbaec4ccdd3cbab62b
key2 = 1a0a5a0cbd66448084089304442d6776
cluster_dfw1 = http://dfw1.host/v1/
            ''')
        self.app = FakeApp()
        self.conf = {'swift_dir': self.tempdir}
        self.sync = container_sync.ContainerSync(self.app, self.conf)

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=1)

    def test_current_not_set(self):
        # no 'current' option set by default
        self.assertIsNone(self.sync.realm)
        self.assertIsNone(self.sync.cluster)
        info = {}

        def capture_swift_info(key, **options):
            info[key] = options

        with mock.patch(
                'swift.common.middleware.container_sync.register_swift_info',
                new=capture_swift_info):
            self.sync.register_info()

        for realm, realm_info in info['container_sync']['realms'].items():
            for cluster, options in realm_info['clusters'].items():
                self.assertEqual(options.get('current', False), False)

    def test_current_invalid(self):
        self.conf = {'swift_dir': self.tempdir, 'current': 'foo'}
        self.sync = container_sync.ContainerSync(self.app, self.conf,
                                                 logger=FakeLogger())
        self.assertIsNone(self.sync.realm)
        self.assertIsNone(self.sync.cluster)
        info = {}

        def capture_swift_info(key, **options):
            info[key] = options

        with mock.patch(
                'swift.common.middleware.container_sync.register_swift_info',
                new=capture_swift_info):
            self.sync.register_info()

        for realm, realm_info in info['container_sync']['realms'].items():
            for cluster, options in realm_info['clusters'].items():
                self.assertEqual(options.get('current', False), False)

        error_lines = self.sync.logger.get_lines_for_level('error')
        self.assertEqual(error_lines, ['Invalid current '
                                       '//REALM/CLUSTER (foo)'])

    def test_current_in_realms_conf(self):
        self.conf = {'swift_dir': self.tempdir, 'current': '//us/dfw1'}
        self.sync = container_sync.ContainerSync(self.app, self.conf)
        self.assertEqual('US', self.sync.realm)
        self.assertEqual('DFW1', self.sync.cluster)
        info = {}

        def capture_swift_info(key, **options):
            info[key] = options

        with mock.patch(
                'swift.common.middleware.container_sync.register_swift_info',
                new=capture_swift_info):
            self.sync.register_info()

        for realm, realm_info in info['container_sync']['realms'].items():
            for cluster, options in realm_info['clusters'].items():
                if options.get('current'):
                    break
        self.assertEqual(realm, self.sync.realm)
        self.assertEqual(cluster, self.sync.cluster)

    def test_missing_from_realms_conf(self):
        self.conf = {'swift_dir': self.tempdir, 'current': 'foo/bar'}
        self.sync = container_sync.ContainerSync(self.app, self.conf,
                                                 logger=FakeLogger())
        self.assertEqual('FOO', self.sync.realm)
        self.assertEqual('BAR', self.sync.cluster)
        info = {}

        def capture_swift_info(key, **options):
            info[key] = options

        with mock.patch(
                'swift.common.middleware.container_sync.register_swift_info',
                new=capture_swift_info):
            self.sync.register_info()

        for realm, realm_info in info['container_sync']['realms'].items():
            for cluster, options in realm_info['clusters'].items():
                self.assertEqual(options.get('current', False), False)

        for line in self.sync.logger.get_lines_for_level('error'):
            self.assertEqual(line, 'Unknown current '
                             '//REALM/CLUSTER (//FOO/BAR)')

    def test_pass_through(self):
        req = swob.Request.blank('/v1/a/c')
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body, b'Pass-Through Response')

    def test_not_enough_args(self):
        req = swob.Request.blank(
            '/v1/a/c', headers={'x-container-sync-auth': 'a'})
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '401 Unauthorized')
        self.assertEqual(
            resp.body,
            b'X-Container-Sync-Auth header not valid; '
            b'contact cluster operator for support.')
        self.assertTrue(
            'cs:not-3-args' in req.environ.get('swift.log_info'),
            req.environ.get('swift.log_info'))

    def test_realm_miss(self):
        req = swob.Request.blank(
            '/v1/a/c', headers={'x-container-sync-auth': 'invalid nonce sig'})
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '401 Unauthorized')
        self.assertEqual(
            resp.body,
            b'X-Container-Sync-Auth header not valid; '
            b'contact cluster operator for support.')
        self.assertTrue(
            'cs:no-local-realm-key' in req.environ.get('swift.log_info'),
            req.environ.get('swift.log_info'))

    def test_user_key_miss(self):
        req = swob.Request.blank(
            '/v1/a/c', headers={'x-container-sync-auth': 'US nonce sig'})
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '401 Unauthorized')
        self.assertEqual(
            resp.body,
            b'X-Container-Sync-Auth header not valid; '
            b'contact cluster operator for support.')
        self.assertTrue(
            'cs:no-local-user-key' in req.environ.get('swift.log_info'),
            req.environ.get('swift.log_info'))

    def test_invalid_sig(self):
        req = swob.Request.blank(
            '/v1/a/c', headers={'x-container-sync-auth': 'US nonce sig'})
        infocache = req.environ.setdefault('swift.infocache', {})
        infocache[get_cache_key('a', 'c')] = {'sync_key': 'abc'}
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '401 Unauthorized')
        self.assertEqual(
            resp.body,
            b'X-Container-Sync-Auth header not valid; '
            b'contact cluster operator for support.')
        self.assertIn('cs:invalid-sig', req.environ.get('swift.log_info'))
        self.assertNotIn('swift.authorize_override', req.environ)
        self.assertNotIn('swift.slo_override', req.environ)

    def test_valid_sig(self):
        ts = '1455221706.726999_0123456789abcdef'
        sig = self.sync.realms_conf.get_sig(
            'GET', '/v1/a/c', ts, 'nonce',
            self.sync.realms_conf.key('US'), 'abc')
        req = swob.Request.blank('/v1/a/c', headers={
            'x-container-sync-auth': 'US nonce ' + sig,
            'x-backend-inbound-x-timestamp': ts})
        infocache = req.environ.setdefault('swift.infocache', {})
        infocache[get_cache_key('a', 'c')] = {'sync_key': 'abc'}
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body, b'Response to Authorized Request')
        self.assertIn('cs:valid', req.environ.get('swift.log_info'))
        self.assertIn('X-Timestamp', resp.headers)
        self.assertEqual(ts, resp.headers['X-Timestamp'])
        self.assertIn('swift.authorize_override', req.environ)
        self.assertIn('swift.slo_override', req.environ)

    def test_valid_sig2(self):
        sig = self.sync.realms_conf.get_sig(
            'GET', '/v1/a/c', '0', 'nonce',
            self.sync.realms_conf.key2('US'), 'abc')
        req = swob.Request.blank(
            '/v1/a/c', headers={'x-container-sync-auth': 'US nonce ' + sig})
        infocache = req.environ.setdefault('swift.infocache', {})
        infocache[get_cache_key('a', 'c')] = {'sync_key': 'abc'}
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '200 OK')
        self.assertEqual(resp.body, b'Response to Authorized Request')
        self.assertIn('cs:valid', req.environ.get('swift.log_info'))
        self.assertIn('swift.authorize_override', req.environ)
        self.assertIn('swift.slo_override', req.environ)

    def test_info(self):
        req = swob.Request.blank('/info')
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '200 OK')
        result = json.loads(resp.body)
        self.assertEqual(
            result.get('container_sync'),
            {'realms': {'US': {'clusters': {'DFW1': {}}}}})

    def test_info_always_fresh(self):
        req = swob.Request.blank('/info')
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '200 OK')
        result = json.loads(resp.body)
        self.assertEqual(
            result.get('container_sync'),
            {'realms': {'US': {'clusters': {'DFW1': {}}}}})
        with open(
                os.path.join(self.tempdir, 'container-sync-realms.conf'),
                'w') as fp:
            fp.write('''
[US]
key = 9ff3b71c849749dbaec4ccdd3cbab62b
key2 = 1a0a5a0cbd66448084089304442d6776
cluster_dfw1 = http://dfw1.host/v1/

[UK]
key = 400b3b357a80413f9d956badff1d9dfe
cluster_lon3 = http://lon3.host/v1/
            ''')
        self.sync.realms_conf.reload()
        req = swob.Request.blank('/info')
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '200 OK')
        result = json.loads(resp.body)
        self.assertEqual(
            result.get('container_sync'),
            {'realms': {
                'US': {'clusters': {'DFW1': {}}},
                'UK': {'clusters': {'LON3': {}}}}})

    def test_allow_full_urls_setting(self):
        req = swob.Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-container-sync-to': 'http://host/v1/a/c'})
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '200 OK')
        self.conf = {'swift_dir': self.tempdir, 'allow_full_urls': 'false'}
        self.sync = container_sync.ContainerSync(self.app, self.conf)
        req = swob.Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'x-container-sync-to': 'http://host/v1/a/c'})
        resp = req.get_response(self.sync)
        self.assertEqual(resp.status, '400 Bad Request')
        self.assertEqual(
            resp.body,
            b'Full URLs are not allowed for X-Container-Sync-To values. Only '
            b'realm values of the format //realm/cluster/account/container '
            b'are allowed.\n')

    def test_filter(self):
        app = FakeApp()
        unique = uuid.uuid4().hex
        sync = container_sync.filter_factory(
            {'global': 'global_value', 'swift_dir': unique},
            **{'local': 'local_value'})(app)
        self.assertEqual(sync.app, app)
        self.assertEqual(sync.conf, {
            'global': 'global_value', 'swift_dir': unique,
            'local': 'local_value'})
        req = swob.Request.blank('/info')
        resp = req.get_response(sync)
        self.assertEqual(resp.status, '200 OK')
        result = json.loads(resp.body)
        self.assertEqual(result.get('container_sync'), {'realms': {}})


if __name__ == '__main__':
    unittest.main()
