# Copyright (c) 2016 OpenStack Foundation
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
from castellan import key_manager, options
from castellan.common.credentials import keystone_password
from oslo_config import cfg
from swift.common.middleware.crypto.keymaster import KeyMaster
from swift.common.utils import readconf


class KmsKeyMaster(KeyMaster):
    """Middleware for retrieving a encryption root secret from an external KMS.

    The middleware accesses the encryption root secret from an external key
    management system (KMS), e.g., a Barbican service, using Castellan. To be
    able to do so, the appropriate configuration options shall be set in the
    proxy-server.conf file, or in the configuration pointed to using the
    keymaster_config_path configuration value in the proxy-server.conf file.
    """

    def __init__(self, app, conf):
        # Call the superclass __init__() method, which calls the overridden
        # self._get_root_secret() below.
        super(KmsKeyMaster, self).__init__(app, conf)

    def _get_root_secret(self, conf):
        """
        Retrieve the root encryption secret from an external key management
        system using Castellan.

        :param conf: the keymaster config section from proxy-server.conf
        :type conf: dict

        :return: the encryption root secret binary bytes
        :rtype: bytearray
        """
        if self.keymaster_config_path is not None:
            keymaster_opts = ['username', 'password', 'project_name',
                              'user_domain_name', 'project_domain_name',
                              'user_id', 'user_domain_id', 'trust_id',
                              'domain_id', 'domain_name', 'project_id',
                              'project_domain_id', 'reauthenticate',
                              'auth_endpoint', 'api_class', 'key_id']
            if any(opt in conf for opt in keymaster_opts):
                raise ValueError('keymaster_config_path is set, but there '
                                 'are other config options specified: %s' %
                                 ", ".join(list(
                                     set(keymaster_opts).intersection(conf))))
            conf = readconf(self.keymaster_config_path, 'kms_keymaster')
        ctxt = keystone_password.KeystonePassword(
            auth_url=conf.get('auth_endpoint'),
            username=conf.get('username'),
            password=conf.get('password'),
            project_name=conf.get('project_name'),
            user_domain_name=conf.get('user_domain_name'),
            project_domain_name=conf.get(
                'project_domain_name'),
            user_id=conf.get('user_id'),
            user_domain_id=conf.get('user_domain_id'),
            trust_id=conf.get('trust_id'),
            domain_id=conf.get('domain_id'),
            domain_name=conf.get('domain_name'),
            project_id=conf.get('project_id'),
            project_domain_id=conf.get('project_domain_id'),
            reauthenticate=conf.get('reauthenticate'))
        oslo_conf = cfg.ConfigOpts()
        options.set_defaults(
            oslo_conf, auth_endpoint=conf.get('auth_endpoint'),
            api_class=conf.get('api_class')
        )
        options.enable_logging()
        manager = key_manager.API(oslo_conf)
        key = manager.get(ctxt, conf.get('key_id'))
        if key is None:
            raise ValueError("Retrieval of encryption root secret with key_id "
                             "'%s' returned None." % conf.get('key_id'))
        try:
            if (key.bit_length < 256) or (key.algorithm.lower() != "aes"):
                raise ValueError('encryption root secret stored in the '
                                 'external KMS must be an AES key of at least '
                                 '256 bits (provided key length: %d, provided '
                                 'key algorithm: %s)'
                                 % (key.bit_length, key.algorithm))
            if (key.format != 'RAW'):
                raise ValueError('encryption root secret stored in the '
                                 'external KMS must be in RAW format and not '
                                 'e.g., as a base64 encoded string (format of '
                                 'key with uuid %s: %s)' %
                                 (conf.get('key_id'), key.format))
        except Exception:
            raise ValueError("Secret with key_id '%s' is not a symmetric key "
                             "(type: %s)" % (conf.get('key_id'),
                                             str(type(key))))
        return key.get_encoded()


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def kms_keymaster_filter(app):
        return KmsKeyMaster(app, conf)

    return kms_keymaster_filter
