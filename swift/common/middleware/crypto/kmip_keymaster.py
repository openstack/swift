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
from swift.common.utils import LogLevelFilter

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
    key_id_<secret_id> = <unique id of additional secret to be fetched>
    active_root_secret_id = <secret_id to be used for new encryptions>
    host = <KMIP server host>
    port = <KMIP server port>
    certfile = /path/to/client/cert.pem
    keyfile = /path/to/client/key.pem
    ca_certs = /path/to/server/cert.pem
    username = <KMIP username>
    password = <KMIP password>

Apart from ``use``, ``key_id*``, ``active_root_secret_id`` the options are
as defined for a PyKMIP client. The authoritative definition of these options
can be found at `https://pykmip.readthedocs.io/en/latest/client.html`_

The value of each ``key_id*`` option should be a unique identifier for a secret
to be retrieved from the KMIP service. Any of these secrets may be used for
*decryption*.

The value of the ``active_root_secret_id`` option should be the ``secret_id``
for the secret that should be used for all new *encryption*. If not specified,
the ``key_id`` secret will be used.

.. note::

    To ensure there is no loss of data availability, deploying a new key to
    your cluster requires a two-stage config change. First, add the new key
    to the ``key_id_<secret_id>`` option and restart the proxy-server. Do this
    for all proxies. Next, set the ``active_root_secret_id`` option to the
    new secret id and restart the proxy. Again, do this for all proxies. This
    process ensures that all proxies will have the new key available for
    *decryption* before any proxy uses it for *encryption*.

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
    key_id_foo = 2468024680
    key_id_bar = 1357913579
    active_root_secret_id = foo
    host = 127.0.0.1
    port = 5696
    certfile = /etc/swift/kmip_client.crt
    keyfile = /etc/swift/kmip_client.key
    ca_certs = /etc/swift/kmip_server.crt
    username = swift
    password = swift_password
"""


class KmipKeyMaster(keymaster.BaseKeyMaster):
    log_route = 'kmip_keymaster'
    keymaster_opts = ('host', 'port', 'certfile', 'keyfile',
                      'ca_certs', 'username', 'password',
                      'active_root_secret_id', 'key_id*')
    keymaster_conf_section = 'kmip_keymaster'

    def _load_keymaster_config_file(self, conf):
        conf = super(KmipKeyMaster, self)._load_keymaster_config_file(conf)
        if self.keymaster_config_path:
            section = self.keymaster_conf_section
        else:
            # __name__ is just the filter name, not the whole section name.
            # Luckily, PasteDeploy only uses the one prefix for filters.
            section = 'filter:' + conf['__name__']

        if os.path.isdir(conf['__file__']):
            raise ValueError(
                'KmipKeyMaster config cannot be read from conf dir %s. Use '
                'keymaster_config_path option in the proxy server config to '
                'specify a config file.')

        # Make sure we've got the kmip log handler set up before
        # we instantiate a client
        kmip_logger = logging.getLogger('kmip')
        for handler in self.logger.logger.handlers:
            kmip_logger.addHandler(handler)

        debug_filter = LogLevelFilter(logging.DEBUG)
        for name in (
                # The kmip_protocol logger includes hex-encoded data off the
                # wire, which may include key material!! We *NEVER* want that
                # enabled.
                'kmip.services.server.kmip_protocol',
                # The config_helper logger includes any password that may be
                # provided, which doesn't seem great either.
                'kmip.core.config_helper',
        ):
            logging.getLogger(name).addFilter(debug_filter)

        self.proxy_kmip_client = ProxyKmipClient(
            config=section,
            config_file=conf['__file__']
        )
        return conf

    def _get_root_secret(self, conf):
        multikey_opts = self._load_multikey_opts(conf, 'key_id')
        kmip_to_secret = {}
        root_secrets = {}
        with self.proxy_kmip_client as client:
            for opt, secret_id, kmip_id in multikey_opts:
                if kmip_id in kmip_to_secret:
                    # Save some round trips if there are multiple
                    # secret_ids for a single kmip_id
                    root_secrets[secret_id] = root_secrets[
                        kmip_to_secret[kmip_id]]
                    continue
                secret = client.get(kmip_id)
                algo = secret.cryptographic_algorithm.name
                length = secret.cryptographic_length
                if (algo, length) != ('AES', 256):
                    raise ValueError(
                        'Expected key %s to be an AES-256 key, not %s-%d' % (
                            kmip_id, algo, length))
                root_secrets[secret_id] = secret.value
                kmip_to_secret.setdefault(kmip_id, secret_id)
        return root_secrets


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def keymaster_filter(app):
        return KmipKeyMaster(app, conf)

    return keymaster_filter
