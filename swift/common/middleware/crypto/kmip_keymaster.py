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
import os

from swift.common.middleware.crypto import keymaster

from kmip.pie.client import ProxyKmipClient

"""
This middleware enables Swift to fetch a root secret from a KMIP service.
The root secret is expected to have been previously created in the KMIP service
and is referenced by its unique identifier. The secret should be an AES-256
symmetric key.

To use this middleware, edit the swift proxy-server.conf to insert the
middleware in the wsgi pipeline, replacing any other keymaster middleware::

    [pipeline:main]
    pipeline = catch_errors gatekeeper healthcheck proxy-logging \
        <other middleware> kmip_keymaster encryption proxy-logging proxy-server

and add a new filter section::

    [filter:kmip_keymaster]
    use = egg:swift#kmip_keymaster
    key_id = <unique id of secret to be fetched from the KMIP service>
    host = <KMIP server host>
    port = <KMIP server port>
    certfile = /path/to/client/cert.pem
    keyfile = /path/to/client/key.pem
    ca_certs = /path/to/server/cert.pem
    username = <KMIP username>
    password = <KMIP password>

Apart from ``use`` and ``key_id`` the options are as defined for a PyKMIP
client. The authoritative definition of these options can be found at
`https://pykmip.readthedocs.io/en/latest/client.html`_

The value of the ``key_id`` option should be the unique identifier for a secret
that will be retrieved from the KMIP service.

The keymaster configuration can alternatively be defined in a separate config
file by using the ``keymaster_config_path`` option::

    [filter:kmip_keymaster]
    use = egg:swift#kmip_keymaster
    keymaster_config_path=/etc/swift/kmip_keymaster.conf

In this case, the ``filter:kmip_keymaster`` section should contain no other
options than ``use`` and ``keymaster_config_path``. All other options should be
defined in the separate config file in a section named ``kmip_keymaster``. For
example::

    [kmip_keymaster]
    key_id = 1234567890
    host = 127.0.0.1
    port = 5696
    certfile = /etc/swift/kmip_client.crt
    keyfile = /etc/swift/kmip_client.key
    ca_certs = /etc/swift/kmip_server.crt
    username = swift
    password = swift_password
"""


class KmipKeyMaster(keymaster.KeyMaster):
    log_route = 'kmip_keymaster'
    keymaster_opts = ('host', 'port', 'certfile', 'keyfile',
                      'ca_certs', 'username', 'password',
                      'active_root_secret_id', 'key_id')
    keymaster_conf_section = 'kmip_keymaster'

    def _get_root_secret(self, conf):
        if self.keymaster_config_path:
            section = self.keymaster_conf_section
        else:
            section = conf['__name__']

        if os.path.isdir(conf['__file__']):
            raise ValueError(
                'KmipKeyMaster config cannot be read from conf dir %s. Use '
                'keymaster_config_path option in the proxy server config to '
                'specify a config file.')

        key_id = conf.get('key_id')
        if not key_id:
            raise ValueError('key_id option is required')

        kmip_logger = logging.getLogger('kmip')
        for handler in self.logger.logger.handlers:
            kmip_logger.addHandler(handler)

        with ProxyKmipClient(
            config=section,
            config_file=conf['__file__']
        ) as client:
            secret = client.get(key_id)
        if (secret.cryptographic_algorithm.name,
                secret.cryptographic_length) != ('AES', 256):
            raise ValueError('Expected an AES-256 key, not %s-%d' % (
                secret.cryptographic_algorithm.name,
                secret.cryptographic_length))
        return secret.value


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def keymaster_filter(app):
        return KmipKeyMaster(app, conf)

    return keymaster_filter
