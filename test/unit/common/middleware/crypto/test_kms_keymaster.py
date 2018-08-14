# -*- coding: utf-8 -*-
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
import mock
import unittest
import sys
sys.modules['castellan'] = mock.Mock()
sys.modules['castellan.common'] = mock.Mock()
sys.modules['castellan.common.credentials'] = mock.Mock()

from keystoneauth1.exceptions.connection import ConnectFailure
from keystoneauth1.exceptions.http import Unauthorized
from keystoneclient.exceptions import DiscoveryFailure
from swift.common.middleware.crypto import kms_keymaster
from swift.common.swob import Request
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts

TEST_KMS_INVALID_KEY_ID = 'invalid-kms-key-id'
TEST_KMS_NONEXISTENT_KEY_ID = '11111111-1111-1111-1111-ffffffffffff'
TEST_KMS_OPAQUE_KEY_ID = '22222222-2222-2222-2222-aaaaaaaaaaaa'
TEST_KMS_SHORT_KEY_ID = '22222222-2222-2222-2222-bbbbbbbbbbbb'
TEST_KMS_DES_KEY_ID = '22222222-2222-2222-2222-cccccccccccc'
TEST_KMS_NONE_KEY_ID = '22222222-2222-2222-2222-dddddddddddd'
TEST_KMS_INVALID_API_VERSION = 'vBadVersion'
TEST_KMS_INVALID_USER_DOMAIN_NAME = "baduserdomainname"
TEST_KMS_CONNECT_FAILURE_URL = 'http://endpoint_url_connect_error:45621'
TEST_KMS_NON_BARBICAN_URL = 'http://endpoint_url_nonbarbican:45621'
TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF = {
    'keymaster_config_path': 'PATH_TO_KEYMASTER_CONFIG_FILE',
}
TEST_KMS_KEYMASTER_CONF = {
    'auth_endpoint': 'kmsauthurlv3',
    'password': 'kmspass',
    'username': 'kmsuser',
    'user_domain_id': None,
    'user_domain_name': 'default',
    'project_id': None,
    'project_name': 'kmsproject',
    'project_domain_id': None,
    'project_domain_name': 'default',
    'key_id': 'valid_kms_key_id-abcdefg-123456'
}


def capture_start_response():
    calls = []

    def start_response(*args):
        calls.append(args)
    return start_response, calls


def mock_castellan_api_side_effect(*args, **kwargs):
    return MockBarbicanKeyManager(args[0])


def mock_options_set_defaults_side_effect(*args, **kwargs):
    '''
    Add options from kwargs into args dict.
    '''
    args[0].update(kwargs)


def mock_config_opts_side_effect(*args, **kwargs):
    return dict()


def mock_keystone_password_side_effect(auth_url, username, password,
                                       project_name, user_domain_name,
                                       project_domain_name, user_id,
                                       user_domain_id, trust_id,
                                       domain_id, domain_name, project_id,
                                       project_domain_id, reauthenticate):
    return MockPassword(auth_url, username, password, project_name,
                        user_domain_name, project_domain_name, user_id,
                        user_domain_id, trust_id, domain_id, domain_name,
                        project_id, project_domain_id, reauthenticate)

ERR_MESSAGE_SECRET_INCORRECTLY_SPECIFIED = 'Secret incorrectly specified.'
ERR_MESSAGE_KEY_UUID_NOT_FOUND = 'Key not found, uuid: '


class MockBarbicanKeyManager(object):
    def __init__(self, conf):
        self.conf = conf

    def get(self, ctxt, key_id):
        # If authentication fails, raise an exception here.
        if (TEST_KMS_KEYMASTER_CONF['username'] !=
                ctxt.username
                or TEST_KMS_KEYMASTER_CONF['password'] !=
                ctxt.password or
                TEST_KMS_KEYMASTER_CONF['user_domain_name'] !=
                ctxt.user_domain_name):
            raise Unauthorized(
                message='The request you have made requires authentication.',
                http_status=401)
        elif self.conf['auth_endpoint'] == TEST_KMS_CONNECT_FAILURE_URL:
            raise ConnectFailure('Unable to establish connection')
        elif self.conf['auth_endpoint'] == TEST_KMS_NON_BARBICAN_URL:
            raise DiscoveryFailure(
                'Could not determine a suitable URL for the plugin')
        elif (self.conf['auth_endpoint'] !=
              TEST_KMS_KEYMASTER_CONF['auth_endpoint']):
            raise Unauthorized(
                message='Cannot authorize API client.')
        elif (key_id == TEST_KMS_NONEXISTENT_KEY_ID):
            message = ERR_MESSAGE_KEY_UUID_NOT_FOUND + key_id
            '''
            Raising a ManagedObjectNotFoundError would require importing it
            from castellan.common.exception. To avoid this import, raising a
            general Exception.
            '''
            raise Exception(message)
        elif key_id == TEST_KMS_INVALID_KEY_ID:
            raise ValueError(ERR_MESSAGE_SECRET_INCORRECTLY_SPECIFIED)
        elif key_id == TEST_KMS_NONE_KEY_ID:
            return None
        key_str = (str(key_id[0]) * 32).encode('utf8')
        return MockBarbicanKey(key_str, key_id)


class MockBarbicanKey(object):
    def __init__(self, key_material, key_id):
        self.key_material = key_material
        self.bit_length = len(key_material) * 8
        if key_id == TEST_KMS_OPAQUE_KEY_ID:
            self.format = 'Opaque'
        else:
            self.format = 'RAW'
            self.algorithm = "aes"
        if key_id == TEST_KMS_DES_KEY_ID:
            self.format = 'des'
        if key_id == TEST_KMS_SHORT_KEY_ID:
            self.bit_length = 128
            self.key_material[:128]

    def get_encoded(self):
        return self.key_material

    def format(self):
        return self.format


class MockPassword(object):
    def __init__(self, auth_url, username, password, project_name,
                 user_domain_name, project_domain_name, user_id,
                 user_domain_id, trust_id, domain_id, domain_name, project_id,
                 project_domain_id, reauthenticate):
        self.auth_url = auth_url
        self.password = password
        self.username = username
        self.user_domain_name = user_domain_name
        self.project_name = project_name
        self.project_domain_name = project_domain_name
        self.user_id = user_id,
        self.user_domain_id = user_domain_id,
        self.trust_id = trust_id,
        self.domain_id = domain_id,
        self.domain_name = domain_name,
        self.project_id = project_id,
        self.project_domain_id = project_domain_id,
        self.reauthenticate = reauthenticate


class TestKmsKeymaster(unittest.TestCase):
    """
    Unit tests for storing the encryption root secret in a Barbican external
    key management system accessed using Castellan.
    """

    def setUp(self):
        super(TestKmsKeymaster, self).setUp()
        self.swift = FakeSwift()

    """
    Tests using the v3 Identity API, where all calls to Barbican are mocked.
    """

    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch.object(kms_keymaster.KmsKeyMaster,
                       '_get_root_secret')
    def test_filter_v3(self, mock_get_root_secret_from_kms,
                       mock_readconf):
        mock_get_root_secret_from_kms.return_value = (
            base64.b64encode(b'x' * 32))
        mock_readconf.return_value = TEST_KMS_KEYMASTER_CONF
        factory = kms_keymaster.filter_factory(TEST_KMS_KEYMASTER_CONF)
        self.assertTrue(callable(factory))
        self.assertTrue(callable(factory(self.swift)))

    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch.object(kms_keymaster.KmsKeyMaster,
                       '_get_root_secret')
    def test_app_exception_v3(self, mock_get_root_secret_from_kms,
                              mock_readconf):
        mock_get_root_secret_from_kms.return_value = (
            base64.b64encode(b'x' * 32))
        mock_readconf.return_value = TEST_KMS_KEYMASTER_CONF
        app = kms_keymaster.KmsKeyMaster(
            FakeAppThatExcepts(), TEST_KMS_KEYMASTER_CONF)
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        start_response, _ = capture_start_response()
        self.assertRaises(Exception, app, req.environ, start_response)

    @mock.patch.object(kms_keymaster.KmsKeyMaster, '_get_root_secret')
    def test_get_root_secret(
            self, mock_get_root_secret_from_kms):
        # Successful call with coarse _get_root_secret_from_kms() mock.
        mock_get_root_secret_from_kms.return_value = (
            base64.b64encode(b'x' * 32))
        # Provide valid Barbican configuration parameters in proxy-server
        # config.
        self.app = kms_keymaster.KmsKeyMaster(self.swift,
                                              TEST_KMS_KEYMASTER_CONF)
        # Verify that _get_root_secret_from_kms() was called with the
        # correct parameters.
        mock_get_root_secret_from_kms.assert_called_with(
            TEST_KMS_KEYMASTER_CONF
        )

    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch.object(kms_keymaster.KmsKeyMaster, '_get_root_secret')
    def test_get_root_secret_from_external_file(
            self, mock_get_root_secret_from_kms, mock_readconf):
        # Return valid Barbican configuration parameters.
        mock_readconf.return_value = TEST_KMS_KEYMASTER_CONF
        # Successful call with coarse _get_root_secret_from_kms() mock.
        mock_get_root_secret_from_kms.return_value = (
            base64.b64encode(b'x' * 32))
        # Point to external config in proxy-server config.
        self.app = kms_keymaster.KmsKeyMaster(
            self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
        # Verify that _get_root_secret_from_kms() was called with the
        # correct parameters.
        mock_get_root_secret_from_kms.assert_called_with(
            TEST_KMS_KEYMASTER_CONF
        )
        self.assertEqual(mock_readconf.mock_calls, [
            mock.call('PATH_TO_KEYMASTER_CONFIG_FILE', 'kms_keymaster')])

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config, mock_keystone_password):
        # Successful call with finer grained mocks.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return valid Barbican configuration parameters.
        '''
        mock_readconf.return_value = TEST_KMS_KEYMASTER_CONF

        '''
        Verify that no exceptions are raised by the mocked functions.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(self.swift,
                                                  TEST_KMS_KEYMASTER_CONF)
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_invalid_key_id(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Invalid key ID.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['key_id'] = TEST_KMS_INVALID_KEY_ID
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though key id invalid')
        except ValueError as e:
            self.assertEqual(e.args[0],
                             ERR_MESSAGE_SECRET_INCORRECTLY_SPECIFIED)
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_nonexistent_key_id(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Nonexistent key.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['key_id'] = TEST_KMS_NONEXISTENT_KEY_ID
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though key id invalid')
        except Exception as e:
            expected_message = ('Key not found, uuid: ' +
                                TEST_KMS_NONEXISTENT_KEY_ID)
            self.assertEqual(e.args[0], expected_message)

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_invalid_key_format(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Nonexistent key.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['key_id'] = TEST_KMS_OPAQUE_KEY_ID
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though key format invalid')
        except ValueError:
            pass
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_config_file_and_params(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Both external config file and config parameters specified.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['keymaster_config_path'] = (
            'PATH_TO_KEYMASTER_CONFIG_FILE'
        )
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(self.swift, kms_conf)
            raise Exception('Success even though config invalid')
        except Exception as e:
            expected_message = ('keymaster_config_path is set, but there are '
                                'other config options specified:')
            self.assertTrue(e.args[0].startswith(expected_message),
                            "Error message does not start with '%s'" %
                            expected_message)

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_invalid_username(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Invalid username.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['username'] = 'invaliduser'
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though username invalid')
        except Unauthorized as e:
            self.assertEqual(e.http_status, 401)
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_invalid_password(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Invalid password.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['password'] = 'invalidpassword'
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though password invalid')
        except Unauthorized as e:
            self.assertEqual(e.http_status, 401)
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_connect_failure_auth_url(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config, mock_keystone_password):
        # Connect failure kms auth_url.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['auth_endpoint'] = TEST_KMS_CONNECT_FAILURE_URL
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though auth_url invalid')
        except ConnectFailure:
            pass
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_bad_auth_url(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Bad kms auth_url.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['auth_endpoint'] = TEST_KMS_NON_BARBICAN_URL
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though auth_url invalid')
        except DiscoveryFailure:
            pass
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_bad_user_domain_name(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config, mock_keystone_password):
        # Bad user domain name with mocks.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['user_domain_name'] = (
            TEST_KMS_INVALID_USER_DOMAIN_NAME)
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though api_version invalid')
        except Unauthorized as e:
            self.assertEqual(e.http_status, 401)
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_invalid_key_algorithm(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Nonexistent key.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['key_id'] = TEST_KMS_DES_KEY_ID
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though key format invalid')
        except ValueError:
            pass
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_invalid_key_length(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Nonexistent key.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['key_id'] = TEST_KMS_SHORT_KEY_ID
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though key format invalid')
        except ValueError:
            pass
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_mocked_castellan_keymanager_none_key(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,
            mock_keystone_password):
        # Nonexistent key.
        mock_keystone_password.side_effect = (
            mock_keystone_password_side_effect)
        '''
        Set side_effect functions.
        '''
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)
        '''
        Return invalid Barbican configuration parameters.
        '''
        kms_conf = dict(TEST_KMS_KEYMASTER_CONF)
        kms_conf['key_id'] = TEST_KMS_NONE_KEY_ID
        mock_readconf.return_value = kms_conf

        '''
        Verify that an exception is raised by the mocked function.
        '''
        try:
            self.app = kms_keymaster.KmsKeyMaster(
                self.swift, TEST_PROXYSERVER_CONF_EXTERNAL_KEYMASTER_CONF)
            raise Exception('Success even though None key returned')
        except ValueError:
            pass
        except Exception:
            print("Unexpected error: %s" % sys.exc_info()[0])
            raise

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword', MockPassword)
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_get_root_secret_multiple_keys(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config,):
        config = dict(TEST_KMS_KEYMASTER_CONF)
        config.update({
            'key_id_foo': 'foo-valid_kms_key_id-123456',
            'key_id_bar': 'bar-valid_kms_key_id-123456',
            'active_root_secret_id': 'foo'})

        # Set side_effect functions.
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)

        # Return valid Barbican configuration parameters.
        mock_readconf.return_value = config

        self.app = kms_keymaster.KmsKeyMaster(self.swift,
                                              config)

        expected_secrets = {
            None: b'vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv',
            'foo': b'ffffffffffffffffffffffffffffffff',
            'bar': b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'}
        self.assertDictEqual(self.app._root_secrets, expected_secrets)
        self.assertEqual(self.app.active_secret_id, 'foo')

    @mock.patch('swift.common.middleware.crypto.kms_keymaster.'
                'keystone_password.KeystonePassword', MockPassword)
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.cfg')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.options')
    @mock.patch('swift.common.middleware.crypto.keymaster.readconf')
    @mock.patch('swift.common.middleware.crypto.kms_keymaster.key_manager')
    def test_get_root_secret_legacy_key_id(
            self, mock_castellan_key_manager, mock_readconf,
            mock_castellan_options, mock_oslo_config):

        # Set side_effect functions.
        mock_castellan_key_manager.API.side_effect = (
            mock_castellan_api_side_effect)
        mock_castellan_options.set_defaults.side_effect = (
            mock_options_set_defaults_side_effect)
        mock_oslo_config.ConfigOpts.side_effect = (
            mock_config_opts_side_effect)

        # Return valid Barbican configuration parameters.
        mock_readconf.return_value = TEST_KMS_KEYMASTER_CONF

        self.app = kms_keymaster.KmsKeyMaster(self.swift,
                                              TEST_KMS_KEYMASTER_CONF)

        expected_secrets = {None: b'vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'}
        self.assertDictEqual(self.app._root_secrets, expected_secrets)
        self.assertIsNone(self.app.active_secret_id)


if __name__ == '__main__':
    unittest.main()
