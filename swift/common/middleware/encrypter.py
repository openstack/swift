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

from hashlib import md5
import base64
import json
from swift.common.crypto_utils import CryptoWSGIContext
from swift.common.utils import get_logger, config_true_value
from swift.common.swob import Request, HTTPException, HTTPUnprocessableEntity
from swift.common.middleware.crypto import Crypto


class EncInputWrapper(object):
    """File-like object to be swapped in for wsgi.input."""
    def __init__(self, crypto, keys, env, client_etag,
                 crypto_meta, footers_to_add, logger):
        self.wsgi_input = env['wsgi.input']
        self.crypto = crypto
        self.crypto_ctxt = crypto.create_encryption_ctxt(keys['object'],
                                                         crypto_meta.get('iv'))
        self.keys = keys
        self.iv = crypto_meta.get('iv')
        self.env = env
        self.client_etag = client_etag
        self.plaintext_etag = md5()
        self.ciphertext_etag = md5()
        self.crypto_meta = crypto_meta
        self.footers_to_add = footers_to_add
        self.logger = logger

    def read(self, *args, **kwargs):
        return self.readChunk(self.wsgi_input.read, *args, **kwargs)

    def readline(self, *args, **kwargs):
        return self.readChunk(self.wsgi_input.readline, *args, **kwargs)

    def readChunk(self, read_method, *args, **kwargs):
        chunk = read_method(*args, **kwargs)

        if chunk:
            self.plaintext_etag.update(chunk)
            # Encrypt one chunk at a time
            ciphertext = self.crypto_ctxt.update(chunk)
            self.ciphertext_etag.update(ciphertext)
            return ciphertext

        self.plaintext_etag = self.plaintext_etag.hexdigest()
        # If client supplied etag, then validate while we have plaintext
        if self.client_etag:
            if self.plaintext_etag != self.client_etag:
                raise HTTPUnprocessableEntity(request=Request(self.env))

            self.footers_to_add['Etag'] = self.ciphertext_etag.hexdigest()

        # TODO: This crypto-etag will be encrypted in a later version
        self.footers_to_add['X-Object-Sysmeta-Crypto-Etag'] = \
            self.plaintext_etag

        # TODO: This etag will be encrypted in a later version
        self.footers_to_add['X-Backend-Container-Update-Override-Etag'] = \
            self.plaintext_etag

        for key in self.footers_to_add:
            self.logger.debug("encrypter added footer %s: %s" %
                              (key, self.footers_to_add[key]))

        return chunk


class EncrypterObjContext(CryptoWSGIContext):
    def __init__(self, encrypter, logger):
        super(EncrypterObjContext, self).__init__(encrypter, logger)
        self.footers_to_add = {}

    def footers_callback(self, footers):
        for key in self.footers_to_add:
            footers.update(
                {key: self.footers_to_add[key]})

    def PUT(self, req, start_response):
        keys = self.get_keys(req.environ)
        body_iv = self.crypto.create_iv()
        body_crypto_meta = get_crypto_meta(
            body_iv, self.crypto.get_cipher())

        # Set the crypto-meta associated to the obj body. The IV value is
        # random bytes and as a result needs to be encoded before sending over
        # the wire. Do this by wrapping the crypto meta in a json object
        # and encode the iv value. Base64 encoding returns a bytes object
        # in py3, to future proof the code, decode this data to produce a
        # string, which is what the json.dumps function expects.
        req.headers['X-Object-Sysmeta-Crypto-Meta'] = json.dumps({
            key: (base64.b64encode(value).decode() if key == 'iv' else value)
            for key, value in body_crypto_meta.items()})

        client_etag = None
        if 'etag' in req.headers:
            # save-off in order to validate client etag when possible
            client_etag = req.headers['etag']

            # remove client etag that will not match encrypted data
            self.logger.debug("client supplied ETAG is being removed.")
            req.headers.pop('etag', None)

        # TODO - remove the use of this callback when we stop using FF
        req.environ['swift.update.footers'] = self.footers_callback
        enc_input_proxy = EncInputWrapper(self.crypto, keys,
                                          req.environ, client_etag,
                                          body_crypto_meta,
                                          self.footers_to_add,
                                          self.logger)
        req.environ['wsgi.input'] = enc_input_proxy

        resp = self._app_call(req.environ)

        # If an etag is in the response headers, then set
        # over the value with the plaintext version
        mod_resp_headers = self._response_headers
        if enc_input_proxy.plaintext_etag:
            mod_resp_headers = filter(lambda h: h[0].lower() != 'etag',
                                      self._response_headers)
            mod_resp_headers.append(('etag', enc_input_proxy.plaintext_etag))
        else:
            # TODO: can this ever be true? If not, remove the if clause
            pass

        start_response(self._response_status, mod_resp_headers,
                       self._response_exc_info)
        return resp


def get_crypto_meta(iv, cipher):
    return {'iv': iv, 'cipher': cipher}


class Encrypter(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="encrypter")
        self.conf = conf

    def __call__(self, env, start_response):
        # If override is set in env, then just pass along
        req = Request(env)
        if config_true_value(env.get('swift.crypto.override')):
            return self.app(env, start_response)

        self.crypto = get_crypto(self.conf)

        try:
            req.split_path(4, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if hasattr(EncrypterObjContext, req.method):
            # handle only those request methods that may require keys
            km_context = EncrypterObjContext(self, self.logger)
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

    def encrypter_filter(app):
        return Encrypter(app, conf)
    return encrypter_filter
