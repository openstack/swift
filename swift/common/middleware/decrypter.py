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
import json
from swift.common.utils import get_logger, config_true_value, \
    parse_content_range, closing_if_possible
from swift.common.http import is_success
from swift.common.swob import Request, HTTPException, HTTPInternalServerError
from swift.common.middleware.crypto import Crypto
from swift.common.crypto_utils import CryptoWSGIContext
from swift.common.exceptions import EncryptionException, StopDecryption


class DecrypterObjContext(CryptoWSGIContext):
    def __init__(self, decrypter, logger):
        super(DecrypterObjContext, self).__init__(decrypter, logger)
        self.body_crypto_ctxt = None

    def get_sysmeta_crypto_meta(self, hkey, error_if_none):
        crypto_meta_json = self._response_header_value(hkey)
        if crypto_meta_json is None:
            if error_if_none:
                # TODO - if this case is hit, then keymaster did not
                # do its job in setting the override flag, correct?
                self.logger.error("Error: No sysmeta-crypto-meta for %s." %
                                  hkey)
                raise EncryptionException("No sysmeta-crypto-meta for %s" %
                                          hkey)
            else:
                self.logger.warn("Warning: No sysmeta-crypto-meta for %s." %
                                 hkey)
                raise StopDecryption("No sysmeta-crypto-meta for %s" %
                                     hkey)

        # Build the crypto_meta from the json object. Note that json.loads
        # always produces unicode strings, to ensure the resultant crypto_meta
        # matches the original object cast all key and value data (other then
        # the iv) to a str. This will work in py3 as well where all strings are
        # unicode implying the cast is effectively a no-op.
        crypto_meta = \
            {str(key): (base64.b64decode(value) if key == 'iv' else str(value))
             for key, value in json.loads(crypto_meta_json).items()}

        # no need to check cipher, since this is only called for sysmeta
        # and the cipher is checked with the body crypto-meta
        return crypto_meta

    def process_resp_headers(self, req):
        # Get 'X-Object-Sysmeta-Crypto-Meta'
        # TODO - this really should pass "true" for "error_if_none".
        # but there are cases in the functests that will hit this line
        # with no crypto-meta, and it just means its not encrypted.
        # the keymaster should be setting the override flag in this case.
        body_crypto_meta = self.get_sysmeta_crypto_meta(
            'X-Object-Sysmeta-Crypto-Meta', False)

        body_iv = body_crypto_meta.get('iv')
        body_cipher = body_crypto_meta.get('cipher')

        if body_cipher != self.crypto.get_cipher():
            raise EncryptionException(
                "Encrypted with cipher %s, but can only decrypt with cipher %s"
                % (body_cipher, self.crypto.get_cipher()))

        keys = self.get_keys(req.environ)

        c_range = self._response_header_value('Content-Range')
        offset = 0
        if c_range:
            # Determine the offset within the whole object if ranged GET
            offset, end, total = parse_content_range(c_range)
            self.logger.debug("Range is: %s - %s, %s" % (offset, end, total))

        self.body_crypto_ctxt = self.crypto.create_decryption_ctxt(
            keys['object'], body_iv, offset)

        # Strip out encrypted headers.  Replace with the decrypted versions.
        # TODO: This is overkill but is anticipating more headers to be
        # processed in future versions.
        mod_hdr_pairs = []

        # TODO: check this header exists at same time as checking crypto-meta
        etag = self._response_header_value('X-Object-Sysmeta-Crypto-Etag')
        mod_hdr_pairs.append(('etag', etag))

        mod_hdr_keys = map(lambda h: h[0].lower(), mod_hdr_pairs)
        mod_resp_headers = filter(lambda h: h[0].lower() not in mod_hdr_keys,
                                  self._response_headers)

        for pair in mod_hdr_pairs:
            mod_resp_headers.append(pair)

        return mod_resp_headers

    def process_resp(self, req):
        # Only proceed processing if an error has not occurred
        if not is_success(self._get_status_int()):
            return None

        if config_true_value(req.environ.get('swift.crypto.override')):
            self.logger.debug('No decryption is necessary because of override')
            return None

        try:
            return self.process_resp_headers(req)
        except StopDecryption:
            return None
        except EncryptionException as err:
            self.logger.error('Could not process headers for obj get: %s' %
                              str(err))
            raise HTTPInternalServerError(body=str(err),
                                          content_type='text/plain')

    def GET(self, req, start_response):
        app_resp = self._app_call(req.environ)

        mod_resp_headers = self.process_resp(req)

        if mod_resp_headers is None:
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return app_resp

        if not self.body_crypto_ctxt:
            raise HTTPInternalServerError("Crypto context was not set")

        start_response(self._response_status,
                       mod_resp_headers,
                       self._response_exc_info)

        def iter_response(iterable, crypto_ctxt):
            with closing_if_possible(iterable):
                for chunk in iterable:
                    yield crypto_ctxt.update(chunk)

        return iter_response(app_resp, self.body_crypto_ctxt)

    def HEAD(self, req, start_response):
        app_resp = self._app_call(req.environ)

        mod_resp_headers = self.process_resp(req)

        if mod_resp_headers is None:
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
        else:
            start_response(self._response_status, mod_resp_headers,
                           self._response_exc_info)

        return app_resp


class Decrypter(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="decrypter")
        self.conf = conf

    def __call__(self, env, start_response):
        self.crypto = get_crypto(self.conf)

        req = Request(env)
        try:
            req.split_path(4, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if hasattr(DecrypterObjContext, req.method):
            # handle only those request methods that may require keys
            km_context = DecrypterObjContext(self, self.logger)
            try:
                return getattr(km_context, req.method)(req, start_response)
            except HTTPException as err_resp:
                return err_resp(env, start_response)

        # anything else
        return self.app(env, start_response)


def get_crypto(conf):
    return Crypto(conf)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def decrypter_filter(app):
        return Decrypter(app, conf)
    return decrypter_filter
