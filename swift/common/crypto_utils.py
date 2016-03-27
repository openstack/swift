# Copyright (c) 2015 OpenStack Foundation
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

from swift import gettext_ as _
from swift.common.swob import HTTPInternalServerError
from swift.common.wsgi import WSGIContext
from swift.common.request_helpers import strip_sys_meta_prefix, \
    strip_object_transient_sysmeta_prefix


class CryptoWSGIContext(WSGIContext):
    """
    Base class for contexts used by crypto middlewares.
    """
    def __init__(self, filter, logger):
        super(CryptoWSGIContext, self).__init__(filter.app)
        self.crypto = filter.crypto
        self.logger = logger

    def get_keys(self, env):
        # Get the key(s) from the keymaster
        try:
            fetch_crypto_keys = env['swift.crypto.fetch_crypto_keys']
        except KeyError:
            self.logger.exception(_(
                'ERROR get_keys() swift.crypto.fetch_crypto_keys not in env'))
            raise HTTPInternalServerError(
                'swift.crypto.fetch_crypto_keys not in env')

        try:
            return fetch_crypto_keys()
        except Exception as err:  # noqa
            # TODO - change this case to use KeyMasterException when it exists
            self.logger.exception(_(
                'ERROR get_keys(): from swift.crypto.fetch_crypto_keys: %s')
                % err)
            raise HTTPInternalServerError(
                "swift.crypto.fetch_crypto_keys had exception: %s"
                % err.message)


def is_crypto_meta(header, server_type):
    return (strip_sys_meta_prefix(
        server_type, header.lower()).startswith('crypto-meta') or
        strip_object_transient_sysmeta_prefix(
        header.lower()).startswith('crypto-meta'))
