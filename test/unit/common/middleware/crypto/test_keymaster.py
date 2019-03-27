#  Copyright (c) 2015 OpenStack Foundation
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
import base64
import copy
import hashlib
import hmac

import os

import mock
import unittest

from getpass import getuser
from swift.common import swob
from swift.common.middleware.crypto import keymaster
from swift.common.middleware.crypto.crypto_utils import CRYPTO_KEY_CALLBACK
from swift.common.swob import Request
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts
from test.unit.common.middleware.crypto.crypto_helpers import (
    TEST_KEYMASTER_CONF)
from test.unit import tmpfile


def capture_start_response():
    calls = []

    def start_response(*args):
        calls.append(args)
    return start_response, calls


class TestKeymaster(unittest.TestCase):

    def setUp(self):
        super(TestKeymaster, self).setUp()
        self.swift = FakeSwift()
        self.app = keymaster.KeyMaster(self.swift, TEST_KEYMASTER_CONF)

    def test_object_path(self):
        self.verify_keys_for_path(
            '/a/c/o', expected_keys=('object', 'container'))
        self.verify_keys_for_path(
            '/a/c//o', expected_keys=('object', 'container'))

    def test_container_path(self):
        self.verify_keys_for_path(
            '/a/c', expected_keys=('container',))

    def verify_keys_for_path(self, path, expected_keys, key_id=None):
        put_keys = None
        for method, resp_class, status in (
                ('PUT', swob.HTTPCreated, '201'),
                ('POST', swob.HTTPAccepted, '202'),
                ('GET', swob.HTTPOk, '200'),
                ('HEAD', swob.HTTPNoContent, '204')):
            resp_headers = {}
            self.swift.register(
                method, '/v1' + path, resp_class, resp_headers, b'')
            req = Request.blank(
                '/v1' + path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            self.app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertTrue(calls[0][0].startswith(status))
            self.assertNotIn('swift.crypto.override', req.environ)
            self.assertIn(CRYPTO_KEY_CALLBACK, req.environ,
                          '%s not set in env' % CRYPTO_KEY_CALLBACK)
            keys = req.environ.get(CRYPTO_KEY_CALLBACK)(key_id=key_id)
            self.assertIn('id', keys)
            id = keys.pop('id')
            self.assertEqual(path, id['path'])
            self.assertEqual('2', id['v'])
            keys.pop('all_ids')
            self.assertListEqual(sorted(expected_keys), sorted(keys.keys()),
                                 '%s %s got keys %r, but expected %r'
                                 % (method, path, keys.keys(), expected_keys))
            if put_keys is not None:
                # check all key sets were consistent for this path
                self.assertDictEqual(put_keys, keys)
            else:
                put_keys = keys
        return put_keys

    def test_key_uniqueness(self):
        # a rudimentary check that different keys are made for different paths
        ref_path_parts = ('a1', 'c1', 'o1')
        path = '/' + '/'.join(ref_path_parts)
        ref_keys = self.verify_keys_for_path(
            path, expected_keys=('object', 'container'))

        # for same path and for each differing path check that keys are unique
        # when path to object or container is unique and vice-versa
        for path_parts in [(a, c, o) for a in ('a1', 'a2')
                           for c in ('c1', 'c2')
                           for o in ('o1', 'o2')]:
            path = '/' + '/'.join(path_parts)
            keys = self.verify_keys_for_path(
                path, expected_keys=('object', 'container'))
            # object keys should only be equal when complete paths are equal
            self.assertEqual(path_parts == ref_path_parts,
                             keys['object'] == ref_keys['object'],
                             'Path %s keys:\n%s\npath %s keys\n%s' %
                             (ref_path_parts, ref_keys, path_parts, keys))
            # container keys should only be equal when paths to container are
            # equal
            self.assertEqual(path_parts[:2] == ref_path_parts[:2],
                             keys['container'] == ref_keys['container'],
                             'Path %s keys:\n%s\npath %s keys\n%s' %
                             (ref_path_parts, ref_keys, path_parts, keys))

    def test_filter(self):
        factory = keymaster.filter_factory(TEST_KEYMASTER_CONF)
        self.assertTrue(callable(factory))
        self.assertTrue(callable(factory(self.swift)))

    def test_app_exception(self):
        app = keymaster.KeyMaster(
            FakeAppThatExcepts(), TEST_KEYMASTER_CONF)
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        start_response, _ = capture_start_response()
        self.assertRaises(Exception, app, req.environ, start_response)

    def test_missing_conf_section(self):
        sample_conf = "[default]\nuser = %s\n" % getuser()
        with tmpfile(sample_conf) as conf_file:
            self.assertRaisesRegexp(
                ValueError, 'Unable to find keymaster config section in.*',
                keymaster.KeyMaster, self.swift, {
                    'keymaster_config_path': conf_file})

    def test_root_secret(self):
        def do_test(dflt_id):
            for secret in (os.urandom(32), os.urandom(33), os.urandom(50)):
                encoded_secret = base64.b64encode(secret)
                self.assertIsInstance(encoded_secret, bytes)
                for conf_val in (
                        encoded_secret,
                        encoded_secret.decode('ascii'),
                        encoded_secret[:30] + b'\n' + encoded_secret[30:],
                        (encoded_secret[:30] + b'\n' +
                         encoded_secret[30:]).decode('ascii')):
                    try:
                        app = keymaster.KeyMaster(
                            self.swift, {'encryption_root_secret': conf_val,
                                         'active_root_secret_id': dflt_id,
                                         'keymaster_config_path': ''})
                        self.assertEqual(secret, app.root_secret)
                    except AssertionError as err:
                        self.fail(str(err) + ' for secret %r' % conf_val)
        do_test(None)
        do_test('')

    def test_no_root_secret(self):
        with self.assertRaises(ValueError) as cm:
            keymaster.KeyMaster(self.swift, {})
        self.assertEqual('No secret loaded for active_root_secret_id None',
                         str(cm.exception))

    def test_multiple_root_secrets(self):
        secrets = {None: os.urandom(32),
                   '22': os.urandom(33),
                   'my_secret_id': os.urandom(50)}

        conf = {}
        for secret_id, secret in secrets.items():
            opt = ('encryption_root_secret%s' %
                   (('_%s' % secret_id) if secret_id else ''))
            conf[opt] = base64.b64encode(secret)
        app = keymaster.KeyMaster(self.swift, conf)
        self.assertEqual(secrets, app._root_secrets)
        self.assertEqual([None, '22', 'my_secret_id'], app.root_secret_ids)

    def test_chained_keymasters(self):
        conf_inner = {'active_root_secret_id': '22'}
        conf_inner.update(
            ('encryption_root_secret_%s' % secret_id, base64.b64encode(secret))
            for secret_id, secret in [('22', os.urandom(33)),
                                      ('my_secret_id', os.urandom(50))])
        conf_outer = {'encryption_root_secret': base64.b64encode(
            os.urandom(32))}
        app = keymaster.KeyMaster(
            keymaster.KeyMaster(self.swift, conf_inner),
            conf_outer)

        self.swift.register('GET', '/v1/a/c', swob.HTTPOk, {}, b'')
        req = Request.blank('/v1/a/c')
        start_response, calls = capture_start_response()
        app(req.environ, start_response)
        self.assertEqual(1, len(calls))
        self.assertNotIn('swift.crypto.override', req.environ)
        self.assertIn(CRYPTO_KEY_CALLBACK, req.environ,
                      '%s not set in env' % CRYPTO_KEY_CALLBACK)
        keys = copy.deepcopy(req.environ[CRYPTO_KEY_CALLBACK](key_id=None))
        self.assertIn('id', keys)
        self.assertEqual(keys.pop('id'), {
            'v': '2',
            'path': '/a/c',
            'secret_id': '22',
        })
        # Inner-most active root secret wins
        root_key = base64.b64decode(conf_inner['encryption_root_secret_22'])
        self.assertIn('container', keys)
        self.assertEqual(keys.pop('container'),
                         hmac.new(root_key, b'/a/c',
                                  digestmod=hashlib.sha256).digest())
        self.assertIn('all_ids', keys)
        all_keys = set()
        at_least_one_old_style_id = False
        for key_id in keys.pop('all_ids'):
            # Can get key material for each key_id
            all_keys.add(req.environ[CRYPTO_KEY_CALLBACK](
                key_id=key_id)['container'])

            if 'secret_id' in key_id:
                self.assertIn(key_id.pop('secret_id'), {'22', 'my_secret_id'})
            else:
                at_least_one_old_style_id = True
            self.assertEqual(key_id, {
                'path': '/a/c',
                'v': '2',
            })
        self.assertTrue(at_least_one_old_style_id)
        self.assertEqual(len(all_keys), 3)
        self.assertFalse(keys)

        # Also all works for objects
        self.swift.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, b'')
        req = Request.blank('/v1/a/c/o')
        start_response, calls = capture_start_response()
        app(req.environ, start_response)
        self.assertEqual(1, len(calls))
        self.assertNotIn('swift.crypto.override', req.environ)
        self.assertIn(CRYPTO_KEY_CALLBACK, req.environ,
                      '%s not set in env' % CRYPTO_KEY_CALLBACK)
        keys = req.environ.get(CRYPTO_KEY_CALLBACK)(key_id=None)
        self.assertIn('id', keys)
        self.assertEqual(keys.pop('id'), {
            'v': '2',
            'path': '/a/c/o',
            'secret_id': '22',
        })
        root_key = base64.b64decode(conf_inner['encryption_root_secret_22'])
        self.assertIn('container', keys)
        self.assertEqual(keys.pop('container'),
                         hmac.new(root_key, b'/a/c',
                                  digestmod=hashlib.sha256).digest())
        self.assertIn('object', keys)
        self.assertEqual(keys.pop('object'),
                         hmac.new(root_key, b'/a/c/o',
                                  digestmod=hashlib.sha256).digest())
        self.assertIn('all_ids', keys)
        at_least_one_old_style_id = False
        for key_id in keys.pop('all_ids'):
            if 'secret_id' not in key_id:
                at_least_one_old_style_id = True
            else:
                self.assertIn(key_id.pop('secret_id'), {'22', 'my_secret_id'})
            self.assertEqual(key_id, {
                'path': '/a/c/o',
                'v': '2',
            })
        self.assertTrue(at_least_one_old_style_id)
        self.assertEqual(len(all_keys), 3)
        self.assertFalse(keys)

    def test_multiple_root_secrets_with_invalid_secret(self):
        conf = {'encryption_root_secret': base64.b64encode(os.urandom(32)),
                # too short...
                'encryption_root_secret_22': base64.b64encode(os.urandom(31))}
        with self.assertRaises(ValueError) as err:
            keymaster.KeyMaster(self.swift, conf)
        self.assertEqual(
            'encryption_root_secret_22 option in proxy-server.conf '
            'must be a base64 encoding of at least 32 raw bytes',
            str(err.exception))

    def test_multiple_root_secrets_with_invalid_id(self):
        def do_test(bad_option):
            conf = {'encryption_root_secret': base64.b64encode(os.urandom(32)),
                    bad_option: base64.b64encode(os.urandom(32))}
            with self.assertRaises(ValueError) as err:
                keymaster.KeyMaster(self.swift, conf)
            self.assertEqual(
                'Malformed root secret option name %s' % bad_option,
                str(err.exception))
        do_test('encryption_root_secret1')
        do_test('encryption_root_secret123')
        do_test('encryption_root_secret_')

    def test_multiple_root_secrets_missing_active_root_secret_id(self):
        conf = {'encryption_root_secret_22': base64.b64encode(os.urandom(32))}
        with self.assertRaises(ValueError) as err:
            keymaster.KeyMaster(self.swift, conf)
        self.assertEqual(
            'No secret loaded for active_root_secret_id None',
            str(err.exception))

        conf = {'encryption_root_secret_22': base64.b64encode(os.urandom(32)),
                'active_root_secret_id': 'missing'}
        with self.assertRaises(ValueError) as err:
            keymaster.KeyMaster(self.swift, conf)
        self.assertEqual(
            'No secret loaded for active_root_secret_id missing',
            str(err.exception))

    def test_correct_root_secret_used(self):
        secrets = {None: os.urandom(32),
                   '22': os.urandom(33),
                   'my_secret_id': os.urandom(50)}

        # no active_root_secret_id configured
        conf = {}
        for secret_id, secret in secrets.items():
            opt = ('encryption_root_secret%s' %
                   (('_%s' % secret_id) if secret_id else ''))
            conf[opt] = base64.b64encode(secret)
        self.app = keymaster.KeyMaster(self.swift, conf)
        keys = self.verify_keys_for_path('/a/c/o', ('container', 'object'))
        expected_keys = {
            'container': hmac.new(secrets[None], b'/a/c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets[None], b'/a/c/o',
                               digestmod=hashlib.sha256).digest()}
        self.assertEqual(expected_keys, keys)

        # active_root_secret_id configured
        conf['active_root_secret_id'] = '22'
        self.app = keymaster.KeyMaster(self.swift, conf)
        keys = self.verify_keys_for_path('/a/c/o', ('container', 'object'))
        expected_keys = {
            'container': hmac.new(secrets['22'], b'/a/c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets['22'], b'/a/c/o',
                               digestmod=hashlib.sha256).digest()}
        self.assertEqual(expected_keys, keys)

        # secret_id passed to fetch_crypto_keys callback
        for secret_id in ('my_secret_id', None):
            keys = self.verify_keys_for_path(
                '/a/c/o', ('container', 'object'),
                key_id={'secret_id': secret_id, 'v': '2', 'path': '/a/c/o'})
            expected_keys = {
                'container': hmac.new(secrets[secret_id], b'/a/c',
                                      digestmod=hashlib.sha256).digest(),
                'object': hmac.new(secrets[secret_id], b'/a/c/o',
                                   digestmod=hashlib.sha256).digest()}
            self.assertEqual(expected_keys, keys)

    def test_keys_cached(self):
        secrets = {None: os.urandom(32),
                   '22': os.urandom(33),
                   'my_secret_id': os.urandom(50)}
        conf = {}
        for secret_id, secret in secrets.items():
            opt = ('encryption_root_secret%s' %
                   (('_%s' % secret_id) if secret_id else ''))
            conf[opt] = base64.b64encode(secret)
        conf['active_root_secret_id'] = '22'
        self.app = keymaster.KeyMaster(self.swift, conf)
        orig_create_key = self.app.create_key
        calls = []

        def mock_create_key(path, secret_id=None):
            calls.append((path, secret_id))
            return orig_create_key(path, secret_id)

        context = keymaster.KeyMasterContext(self.app, 'a', 'c', 'o')
        with mock.patch.object(self.app, 'create_key', mock_create_key):
            keys = context.fetch_crypto_keys()
        expected_keys = {
            'container': hmac.new(secrets['22'], b'/a/c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets['22'], b'/a/c/o',
                               digestmod=hashlib.sha256).digest(),
            'id': {'path': '/a/c/o', 'secret_id': '22', 'v': '2'},
            'all_ids': [
                {'path': '/a/c/o', 'v': '2'},
                {'path': '/a/c/o', 'secret_id': '22', 'v': '2'},
                {'path': '/a/c/o', 'secret_id': 'my_secret_id', 'v': '2'}]}
        self.assertEqual(expected_keys, keys)
        self.assertEqual([('/a/c', '22'), ('/a/c/o', '22')], calls)
        with mock.patch.object(self.app, 'create_key', mock_create_key):
            keys = context.fetch_crypto_keys()
        # no more calls to create_key
        self.assertEqual([('/a/c', '22'), ('/a/c/o', '22')], calls)
        self.assertEqual(expected_keys, keys)
        with mock.patch.object(self.app, 'create_key', mock_create_key):
            keys = context.fetch_crypto_keys(key_id={
                'secret_id': None, 'v': '2', 'path': '/a/c/o'})
        expected_keys = {
            'container': hmac.new(secrets[None], b'/a/c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets[None], b'/a/c/o',
                               digestmod=hashlib.sha256).digest(),
            'id': {'path': '/a/c/o', 'v': '2'},
            'all_ids': [
                {'path': '/a/c/o', 'v': '2'},
                {'path': '/a/c/o', 'secret_id': '22', 'v': '2'},
                {'path': '/a/c/o', 'secret_id': 'my_secret_id', 'v': '2'}]}
        self.assertEqual(expected_keys, keys)
        self.assertEqual([('/a/c', '22'), ('/a/c/o', '22'),
                          ('/a/c', None), ('/a/c/o', None)],
                         calls)

    def test_v1_keys(self):
        secrets = {None: os.urandom(32),
                   '22': os.urandom(33)}
        conf = {}
        for secret_id, secret in secrets.items():
            opt = ('encryption_root_secret%s' %
                   (('_%s' % secret_id) if secret_id else ''))
            conf[opt] = base64.b64encode(secret)
        conf['active_root_secret_id'] = '22'
        self.app = keymaster.KeyMaster(self.swift, conf)
        orig_create_key = self.app.create_key
        calls = []

        def mock_create_key(path, secret_id=None):
            calls.append((path, secret_id))
            return orig_create_key(path, secret_id)

        context = keymaster.KeyMasterContext(self.app, 'a', 'c', 'o')
        for version in ('1', '2'):
            with mock.patch.object(self.app, 'create_key', mock_create_key):
                keys = context.fetch_crypto_keys(key_id={
                    'v': version, 'path': '/a/c/o'})
            expected_keys = {
                'container': hmac.new(secrets[None], b'/a/c',
                                      digestmod=hashlib.sha256).digest(),
                'object': hmac.new(secrets[None], b'/a/c/o',
                                   digestmod=hashlib.sha256).digest(),
                'id': {'path': '/a/c/o', 'v': version},
                'all_ids': [
                    {'path': '/a/c/o', 'v': version},
                    {'path': '/a/c/o', 'secret_id': '22', 'v': version}]}
            self.assertEqual(expected_keys, keys)
            self.assertEqual([('/a/c', None), ('/a/c/o', None)], calls)
            del calls[:]

        context = keymaster.KeyMasterContext(self.app, 'a', 'c', '/o')
        with mock.patch.object(self.app, 'create_key', mock_create_key):
            keys = context.fetch_crypto_keys(key_id={
                'v': '1', 'path': '/o'})
        expected_keys = {
            'container': hmac.new(secrets[None], b'/a/c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets[None], b'/o',
                               digestmod=hashlib.sha256).digest(),
            'id': {'path': '/o', 'v': '1'},
            'all_ids': [
                {'path': '/o', 'v': '1'},
                {'path': '/o', 'secret_id': '22', 'v': '1'}]}
        self.assertEqual(expected_keys, keys)
        self.assertEqual([('/a/c', None), ('/o', None)], calls)
        del calls[:]

        context = keymaster.KeyMasterContext(self.app, 'a', 'c', '/o')
        with mock.patch.object(self.app, 'create_key', mock_create_key):
            keys = context.fetch_crypto_keys(key_id={
                'v': '2', 'path': '/a/c//o'})
        expected_keys = {
            'container': hmac.new(secrets[None], b'/a/c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets[None], b'/a/c//o',
                               digestmod=hashlib.sha256).digest(),
            'id': {'path': '/a/c//o', 'v': '2'},
            'all_ids': [
                {'path': '/a/c//o', 'v': '2'},
                {'path': '/a/c//o', 'secret_id': '22', 'v': '2'}]}
        self.assertEqual(expected_keys, keys)
        self.assertEqual([('/a/c', None), ('/a/c//o', None)], calls)

    def test_v1_keys_with_weird_paths(self):
        secrets = {None: os.urandom(32),
                   '22': os.urandom(33)}
        conf = {}
        for secret_id, secret in secrets.items():
            opt = ('encryption_root_secret%s' %
                   (('_%s' % secret_id) if secret_id else ''))
            conf[opt] = base64.b64encode(secret)
        conf['active_root_secret_id'] = '22'
        self.app = keymaster.KeyMaster(self.swift, conf)
        orig_create_key = self.app.create_key
        calls = []

        def mock_create_key(path, secret_id=None):
            calls.append((path, secret_id))
            return orig_create_key(path, secret_id)

        # request path doesn't match stored path -- this could happen if you
        # misconfigured your proxy to have copy right of encryption
        context = keymaster.KeyMasterContext(self.app, 'a', 'not-c', 'not-o')
        for version in ('1', '2'):
            with mock.patch.object(self.app, 'create_key', mock_create_key):
                keys = context.fetch_crypto_keys(key_id={
                    'v': version, 'path': '/a/c/o'})
            expected_keys = {
                'container': hmac.new(secrets[None], b'/a/c',
                                      digestmod=hashlib.sha256).digest(),
                'object': hmac.new(secrets[None], b'/a/c/o',
                                   digestmod=hashlib.sha256).digest(),
                'id': {'path': '/a/c/o', 'v': version},
                'all_ids': [
                    {'path': '/a/c/o', 'v': version},
                    {'path': '/a/c/o', 'secret_id': '22', 'v': version}]}
            self.assertEqual(expected_keys, keys)
            self.assertEqual([('/a/c', None), ('/a/c/o', None)], calls)
            del calls[:]

        context = keymaster.KeyMasterContext(
            self.app, 'not-a', 'not-c', '/not-o')
        with mock.patch.object(self.app, 'create_key', mock_create_key):
            keys = context.fetch_crypto_keys(key_id={
                'v': '1', 'path': '/o'})
        expected_keys = {
            'container': hmac.new(secrets[None], b'/not-a/not-c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets[None], b'/o',
                               digestmod=hashlib.sha256).digest(),
            'id': {'path': '/o', 'v': '1'},
            'all_ids': [
                {'path': '/o', 'v': '1'},
                {'path': '/o', 'secret_id': '22', 'v': '1'}]}
        self.assertEqual(expected_keys, keys)
        self.assertEqual([('/not-a/not-c', None), ('/o', None)], calls)
        del calls[:]

        context = keymaster.KeyMasterContext(
            self.app, 'not-a', 'not-c', '/not-o')
        with mock.patch.object(self.app, 'create_key', mock_create_key):
            keys = context.fetch_crypto_keys(key_id={
                'v': '2', 'path': '/a/c//o'})
        expected_keys = {
            'container': hmac.new(secrets[None], b'/a/c',
                                  digestmod=hashlib.sha256).digest(),
            'object': hmac.new(secrets[None], b'/a/c//o',
                               digestmod=hashlib.sha256).digest(),
            'id': {'path': '/a/c//o', 'v': '2'},
            'all_ids': [
                {'path': '/a/c//o', 'v': '2'},
                {'path': '/a/c//o', 'secret_id': '22', 'v': '2'}]}
        self.assertEqual(expected_keys, keys)
        self.assertEqual([('/a/c', None), ('/a/c//o', None)], calls)

    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    def test_keymaster_config_path(self, mock_readconf):
        for secret in (os.urandom(32), os.urandom(33), os.urandom(50)):
            enc_secret = base64.b64encode(secret)
            self.assertIsInstance(enc_secret, bytes)
            for conf_val in (enc_secret, enc_secret.decode('ascii'),
                             enc_secret[:30] + b'\n' + enc_secret[30:],
                             enc_secret[:30] + b'\r\n' + enc_secret[30:],
                             (enc_secret[:30] + b'\n' +
                              enc_secret[30:]).decode('ascii'),
                             (enc_secret[:30] + b'\r\n' +
                              enc_secret[30:]).decode('ascii')):
                mock_readconf.reset_mock()
                mock_readconf.return_value = {
                    'encryption_root_secret': conf_val}

                app = keymaster.KeyMaster(self.swift, {
                    'keymaster_config_path': '/some/path'})
                try:
                    self.assertEqual(secret, app.root_secret)
                    self.assertEqual(mock_readconf.mock_calls, [
                        mock.call('/some/path', 'keymaster')])
                except AssertionError as err:
                    self.fail(str(err) + ' for secret %r' % secret)

    def test_invalid_root_secret(self):
        for secret in (base64.b64encode(os.urandom(31)),  # too short
                       base64.b64encode(os.urandom(31)).decode('ascii'),
                       u'a' * 44 + u'????', b'a' * 44 + b'????',  # not base64
                       u'a' * 45, b'a' * 45,  # bad padding
                       99, None):
            conf = {'encryption_root_secret': secret}
            try:
                with self.assertRaises(ValueError) as err:
                    keymaster.KeyMaster(self.swift, conf)
                self.assertEqual(
                    'encryption_root_secret option in proxy-server.conf '
                    'must be a base64 encoding of at least 32 raw bytes',
                    str(err.exception))
            except AssertionError as err:
                self.fail(str(err) + ' for conf %s' % str(conf))

    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    def test_root_secret_path_invalid_secret(self, mock_readconf):
        for secret in (base64.b64encode(os.urandom(31)),  # too short
                       base64.b64encode(os.urandom(31)).decode('ascii'),
                       u'a' * 44 + u'????', b'a' * 44 + b'????',  # not base64
                       u'a' * 45, b'a' * 45,  # bad padding
                       99, None):
            mock_readconf.reset_mock()
            mock_readconf.return_value = {'encryption_root_secret': secret}

            try:
                with self.assertRaises(ValueError) as err:
                    keymaster.KeyMaster(self.swift, {
                        'keymaster_config_path': '/some/other/path'})
                self.assertEqual(
                    'encryption_root_secret option in /some/other/path '
                    'must be a base64 encoding of at least 32 raw bytes',
                    str(err.exception))
                self.assertEqual(mock_readconf.mock_calls, [
                    mock.call('/some/other/path', 'keymaster')])
            except AssertionError as err:
                self.fail(str(err) + ' for secret %r' % secret)

    def test_can_only_configure_secret_in_one_place(self):
        def do_test(conf):
            with self.assertRaises(ValueError) as err:
                keymaster.KeyMaster(self.swift, conf)
            expected_message = ('keymaster_config_path is set, but there are '
                                'other config options specified:')
            self.assertTrue(str(err.exception).startswith(expected_message),
                            "Error message does not start with '%s'" %
                            expected_message)

        conf = {'encryption_root_secret': 'a' * 44,
                'keymaster_config_path': '/etc/swift/keymaster.conf'}
        do_test(conf)
        conf = {'encryption_root_secret_1': 'a' * 44,
                'keymaster_config_path': '/etc/swift/keymaster.conf'}
        do_test(conf)
        conf = {'encryption_root_secret_': 'a' * 44,
                'keymaster_config_path': '/etc/swift/keymaster.conf'}
        do_test(conf)
        conf = {'active_root_secret_id': '1',
                'keymaster_config_path': '/etc/swift/keymaster.conf'}
        do_test(conf)


if __name__ == '__main__':
    unittest.main()
