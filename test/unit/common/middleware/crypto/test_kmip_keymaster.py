# -*- coding: utf-8 -*-
#  Copyright (c) 2018 OpenStack Foundation
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

import logging
import mock
import os
import unittest
from tempfile import mkdtemp
from textwrap import dedent
from shutil import rmtree
import sys
sys.modules['kmip'] = mock.Mock()
sys.modules['kmip.pie'] = mock.Mock()
sys.modules['kmip.pie.client'] = mock.Mock()

from swift.common.middleware.crypto.kmip_keymaster import KmipKeyMaster


KMIP_CLIENT_CLASS = \
    'swift.common.middleware.crypto.kmip_keymaster.ProxyKmipClient'


class MockProxyKmipClient(object):
    def __init__(self, secrets, calls, kwargs):
        calls.append(('__init__', kwargs))
        self.secrets = secrets
        self.calls = calls

    def get(self, uid):
        self.calls.append(('get', uid))
        return self.secrets[uid]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


def create_secret(algorithm_name, length, value):
    algorithm = mock.MagicMock()
    algorithm.name = algorithm_name
    secret = mock.MagicMock(cryptographic_algorithm=algorithm,
                            cryptographic_length=length,
                            value=value)
    return secret


def create_mock_client(secrets, calls):
    def mock_client(*args, **kwargs):
        if args:
            raise Exception('unexpected args provided: %r' % (args,))
        return MockProxyKmipClient(secrets, calls, kwargs)
    return mock_client


class InMemoryHandler(logging.Handler):
    def __init__(self):
        self.messages = []
        super(InMemoryHandler, self).__init__()

    def handle(self, record):
        self.messages.append(record.msg)


class TestKmipKeymaster(unittest.TestCase):

    def setUp(self):
        self.tempdir = mkdtemp()

    def tearDown(self):
        rmtree(self.tempdir)

    def test_config_in_filter_section(self):
        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'kmip_keymaster',
                'key_id': '1234'}
        secrets = {'1234': create_secret('AES', 256, b'x' * 32)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS, create_mock_client(secrets, calls)):
            km = KmipKeyMaster(None, conf)

        self.assertEqual({None: b'x' * 32}, km._root_secrets)
        self.assertEqual(None, km.active_secret_id)
        self.assertIsNone(km.keymaster_config_path)
        self.assertEqual(calls, [
            ('__init__', {'config_file': '/etc/swift/proxy-server.conf',
                          'config': 'filter:kmip_keymaster'}),
            ('get', '1234'),
        ])

    def test_multikey_config_in_filter_section(self):
        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'kmip-keymaster',
                'key_id': '1234',
                'key_id_xyzzy': 'foobar',
                'key_id_alt_secret_id': 'foobar',
                'active_root_secret_id': 'xyzzy'}
        secrets = {'1234': create_secret('AES', 256, b'x' * 32),
                   'foobar': create_secret('AES', 256, b'y' * 32)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS, create_mock_client(secrets, calls)):
            km = KmipKeyMaster(None, conf)

        self.assertEqual({None: b'x' * 32, 'xyzzy': b'y' * 32,
                          'alt_secret_id': b'y' * 32},
                         km._root_secrets)
        self.assertEqual('xyzzy', km.active_secret_id)
        self.assertIsNone(km.keymaster_config_path)
        self.assertEqual(calls, [
            ('__init__', {'config_file': '/etc/swift/proxy-server.conf',
                          'config': 'filter:kmip-keymaster'}),
            ('get', '1234'),
            ('get', 'foobar'),
        ])

    def test_bad_active_key(self):
        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'kmip_keymaster',
                'key_id': '1234',
                'key_id_xyzzy': 'foobar',
                'active_root_secret_id': 'unknown'}
        secrets = {'1234': create_secret('AES', 256, b'x' * 32),
                   'foobar': create_secret('AES', 256, b'y' * 32)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS,
                        create_mock_client(secrets, calls)), \
                self.assertRaises(ValueError) as raised:
            KmipKeyMaster(None, conf)
        self.assertEqual('No secret loaded for active_root_secret_id unknown',
                         str(raised.exception))

    def test_config_in_separate_file(self):
        km_conf = """
        [kmip_keymaster]
        key_id = 4321
        """
        km_config_file = os.path.join(self.tempdir, 'km.conf')
        with open(km_config_file, 'wt') as fd:
            fd.write(dedent(km_conf))

        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'keymaster-kmip',
                'keymaster_config_path': km_config_file}
        secrets = {'4321': create_secret('AES', 256, b'x' * 32)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS, create_mock_client(secrets, calls)):
            km = KmipKeyMaster(None, conf)
        self.assertEqual({None: b'x' * 32}, km._root_secrets)
        self.assertEqual(None, km.active_secret_id)
        self.assertEqual(km_config_file, km.keymaster_config_path)
        self.assertEqual(calls, [
            ('__init__', {'config_file': km_config_file,
                          'config': 'kmip_keymaster'}),
            ('get', '4321')])

    def test_multikey_config_in_separate_file(self):
        km_conf = """
        [kmip_keymaster]
        key_id = 4321
        key_id_secret_id = another id
        active_root_secret_id = secret_id
        """
        km_config_file = os.path.join(self.tempdir, 'km.conf')
        with open(km_config_file, 'wt') as fd:
            fd.write(dedent(km_conf))

        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'kmip_keymaster',
                'keymaster_config_path': km_config_file}
        secrets = {'4321': create_secret('AES', 256, b'x' * 32),
                   'another id': create_secret('AES', 256, b'y' * 32)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS, create_mock_client(secrets, calls)):
            km = KmipKeyMaster(None, conf)
        self.assertEqual({None: b'x' * 32, 'secret_id': b'y' * 32},
                         km._root_secrets)
        self.assertEqual('secret_id', km.active_secret_id)
        self.assertEqual(km_config_file, km.keymaster_config_path)
        self.assertEqual(calls, [
            ('__init__', {'config_file': km_config_file,
                          'config': 'kmip_keymaster'}),
            ('get', '4321'),
            ('get', 'another id')])

    def test_proxy_server_conf_dir(self):
        proxy_server_conf_dir = os.path.join(self.tempdir, 'proxy_server.d')
        os.mkdir(proxy_server_conf_dir)

        # KmipClient can't read conf from a dir, so check that is caught early
        conf = {'__file__': proxy_server_conf_dir,
                '__name__': 'kmip_keymaster',
                'key_id': '789'}
        with self.assertRaises(ValueError) as cm:
            KmipKeyMaster(None, conf)
        self.assertIn('config cannot be read from conf dir', str(cm.exception))

        # ...but a conf file in a conf dir could point back to itself for the
        # KmipClient config
        km_config_file = os.path.join(proxy_server_conf_dir, '40.conf')
        km_conf = """
        [filter:kmip_keymaster]
        keymaster_config_file = %s

        [kmip_keymaster]
        key_id = 789
        """ % km_config_file

        with open(km_config_file, 'wt') as fd:
            fd.write(dedent(km_conf))

        conf = {'__file__': proxy_server_conf_dir,
                '__name__': 'kmip_keymaster',
                'keymaster_config_path': km_config_file}
        secrets = {'789': create_secret('AES', 256, b'x' * 32)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS, create_mock_client(secrets, calls)):
            km = KmipKeyMaster(None, conf)
        self.assertEqual({None: b'x' * 32}, km._root_secrets)
        self.assertEqual(None, km.active_secret_id)
        self.assertEqual(km_config_file, km.keymaster_config_path)
        self.assertEqual(calls, [
            ('__init__', {'config_file': km_config_file,
                          # NB: no "filter:"
                          'config': 'kmip_keymaster'}),
            ('get', '789')])

    def test_bad_key_length(self):
        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'kmip_keymaster',
                'key_id': '1234'}
        secrets = {'1234': create_secret('AES', 128, b'x' * 16)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS,
                        create_mock_client(secrets, calls)), \
                self.assertRaises(ValueError) as cm:
            KmipKeyMaster(None, conf)
        self.assertIn('Expected key 1234 to be an AES-256 key',
                      str(cm.exception))
        self.assertEqual(calls, [
            ('__init__', {'config_file': '/etc/swift/proxy-server.conf',
                          'config': 'filter:kmip_keymaster'}),
            ('get', '1234')])

    def test_bad_key_algorithm(self):
        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'kmip_keymaster',
                'key_id': '1234'}
        secrets = {'1234': create_secret('notAES', 256, b'x' * 32)}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS,
                        create_mock_client(secrets, calls)), \
                self.assertRaises(ValueError) as cm:
            KmipKeyMaster(None, conf)
        self.assertIn('Expected key 1234 to be an AES-256 key',
                      str(cm.exception))
        self.assertEqual(calls, [
            ('__init__', {'config_file': '/etc/swift/proxy-server.conf',
                          'config': 'filter:kmip_keymaster'}),
            ('get', '1234')])

    def test_missing_key_id(self):
        conf = {'__file__': '/etc/swift/proxy-server.conf',
                '__name__': 'kmip_keymaster'}
        secrets = {}
        calls = []
        with mock.patch(KMIP_CLIENT_CLASS,
                        create_mock_client(secrets, calls)), \
                self.assertRaises(ValueError) as cm:
            KmipKeyMaster(None, conf)
        self.assertEqual('No secret loaded for active_root_secret_id None',
                         str(cm.exception))
        # We make the client, but never use it
        self.assertEqual(calls, [
            ('__init__', {'config_file': '/etc/swift/proxy-server.conf',
                          'config': 'filter:kmip_keymaster'})])

    def test_logger_manipulations(self):
        root_logger = logging.getLogger()
        old_level = root_logger.getEffectiveLevel()
        handler = InMemoryHandler()
        try:
            root_logger.setLevel(logging.DEBUG)
            root_logger.addHandler(handler)

            conf = {'__file__': '/etc/swift/proxy-server.conf',
                    '__name__': 'kmip_keymaster'}
            secrets = {}
            calls = []
            with mock.patch(KMIP_CLIENT_CLASS,
                            create_mock_client(secrets, calls)), \
                    self.assertRaises(ValueError):
                # missing key_id, as above, but that's not the interesting bit
                KmipKeyMaster(None, conf)

            self.assertEqual(handler.messages, [])

            logger = logging.getLogger('kmip.services.server.kmip_protocol')
            logger.debug('Something secret!')
            logger.info('Something useful')
            self.assertNotIn('Something secret!', handler.messages)
            self.assertIn('Something useful', handler.messages)

            logger = logging.getLogger('kmip.core.config_helper')
            logger.debug('Also secret')
            logger.warning('Also useful')
            self.assertNotIn('Also secret', handler.messages)
            self.assertIn('Also useful', handler.messages)

            logger = logging.getLogger('kmip')
            logger.debug('Boring, but not secret')
            self.assertIn('Boring, but not secret', handler.messages)
        finally:
            root_logger.setLevel(old_level)
            root_logger.removeHandler(handler)
