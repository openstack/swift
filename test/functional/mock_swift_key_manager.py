# Copyright (c) 2017 OpenStack Foundation
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

from castellan.tests.unit.key_manager.mock_key_manager import MockKeyManager


class MockSwiftKeyManager(MockKeyManager):
    """Mocking key manager for Swift functional tests.

    This mock key manager implementation extends the Castellan mock key
    manager with support for a pre-existing key that the Swift proxy server
    can use as the root encryption secret. The actual key material bytes
    for the root encryption secret changes each time this mock key manager is
    instantiated, meaning that data written earlier is no longer accessible
    once the proxy server is restarted.

    To use this mock key manager instead of the default Barbican key manager,
    set the following property in the [kms_keymaster] section in the
    keymaster.conf configuration file pointed to using the
    keymaster_config_path property in the [filter:kms_keymaster] section in the
    proxy-server.conf file:

        api_class = test.functional.mock_swift_key_manager.MockSwiftKeyManager

    In case of a Python import error, make sure that the swift directory under
    which this mock key manager resides is early in the sys.path, e.g., by
    setting it in the PYTHONPATH environment variable before starting the
    proxy server.

    This key manager is not suitable for use in production deployments.
    """

    def __init__(self, configuration=None):
        super(MockSwiftKeyManager, self).__init__(configuration)
        '''
        Create a new, random symmetric key for use as the encryption root
        secret.
        '''
        existing_key = self._generate_key(algorithm='AES', length=256)
        '''
        Store the key under the UUID 'mock_key_manager_existing_key', from
        where it can be retrieved by the proxy server. In the kms_keymaster
        configuration, set the following property to use this key:

            key_id = mock_key_manager_existing_key
        '''
        self.keys['mock_key_manager_existing_key'] = existing_key
