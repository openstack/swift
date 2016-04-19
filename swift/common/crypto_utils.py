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

import base64
from swift import gettext_ as _
from swift.common.swob import HTTPInternalServerError
from swift.common.wsgi import WSGIContext
from swift.common.request_helpers import strip_sys_meta_prefix, \
    strip_object_transient_sysmeta_prefix
from swift.common.swob import HTTPBadRequest

CRYPTO_KEY_CALLBACK = 'swift.callback.fetch_crypto_keys'


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
            fetch_crypto_keys = env[CRYPTO_KEY_CALLBACK]
        except KeyError:
            self.logger.exception(_(
                'ERROR get_keys() %s not in env') % CRYPTO_KEY_CALLBACK)
            raise HTTPInternalServerError(
                '%s not in env' % CRYPTO_KEY_CALLBACK)

        try:
            return fetch_crypto_keys()
        except Exception as err:
            self.logger.exception(_(
                'ERROR get_keys(): from %(callback)s: %(err)s'),
                {'callback': CRYPTO_KEY_CALLBACK, 'err': str(err)})
            raise HTTPInternalServerError(
                "%(callback)s had exception: %(err)s" %
                {'callback': CRYPTO_KEY_CALLBACK, 'err': err.message})


def is_crypto_meta(header, server_type):
    return (strip_sys_meta_prefix(
        server_type, header.lower()).startswith('crypto-meta') or
        strip_object_transient_sysmeta_prefix(
        header.lower()).startswith('crypto-meta'))


def parse_header_keys(req):
    """
    Utility function to parse headers for BYOK

    :param req: request object
    :return: a dictionary with any parsed keys, empty if no keys are found in
             headers
    :raises: HTTPBadRequest if one of the keys in BYOK headers is missing
             or has wrong length
    """
    keys = {}
    if ('X-Crypto-Object-Key' in req.headers or
            'X-Crypto-Container-Key' in req.headers):

        def _validate_key(header_name):
            try:
                key = base64.b64decode(req.headers[header_name])
            except KeyError:
                raise HTTPBadRequest("%s is missing" % header_name)
            except TypeError:
                raise HTTPBadRequest("%s is an invalid format" % header_name)
            if len(key) != 32:
                raise HTTPBadRequest("%s length should be 32" % header_name)
            return key

        keys['object'] = _validate_key('X-Crypto-Object-Key')
        keys['container'] = _validate_key('X-Crypto-Container-Key')

    return keys
