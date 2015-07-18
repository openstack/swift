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
from swift.common.request_helpers import get_obj_persisted_sysmeta_prefix, \
    strip_user_meta_prefix, is_user_meta
from swift.common.swob import Request, HTTPException, HTTPUnprocessableEntity
from swift.common.middleware.crypto import Crypto
from swift.common.constraints import check_metadata


def _dump_crypto_meta(crypto_meta):
    """
    Set the crypto-meta associated to the obj body.

    The IV value is random bytes and as a result needs to be encoded before
    sending over the wire. Do this by wrapping the crypto meta in a json object
    and encode the iv value. Base64 encoding returns a bytes object in py3, to
    future proof the code, decode this data to produce a string, which is what
    the json.dumps function expects.

    :param crypto_meta: a dict containing crypto meta items
    :returns: a string serialization of a crypto meta dict
    """
    return json.dumps({
        name: (base64.b64encode(value).decode() if name == 'iv' else value)
        for name, value in crypto_meta.items()})


def encrypt_header_val(crypto, value, key, append_crypto_meta=False):
    """
    Encrypt a value using the supplied key.

    :param value: value to encrypt
    :param key: crypto key to use
    :param crypto_meta: a crypto-meta dict of form returned by
        :py:func:`~swift.common.middleware.crypto.Crypto.get_crypto_meta`
    :returns: a tuple of (encrypted value, crypto_meta) where crypto_meta is a
        dict of form returned by
        :py:func:`~swift.common.middleware.crypto.Crypto.get_crypto_meta`
    """
    if not value:
        return '', None

    crypto_meta = crypto.get_crypto_meta()
    crypto_ctxt = crypto.create_encryption_ctxt(key, crypto_meta['iv'])

    enc_val = base64.b64encode(crypto_ctxt.update(value))
    crypto_meta = _dump_crypto_meta(crypto_meta)
    if append_crypto_meta:
        # Store the crypto-meta with the value itself
        enc_val = '%s; meta=%s' % (enc_val, crypto_meta)
    return enc_val, crypto_meta


class EncInputWrapper(object):
    """File-like object to be swapped in for wsgi.input."""
    def __init__(self, crypto, keys, req, logger):
        self.env = req.environ
        self.wsgi_input = req.environ['wsgi.input']
        self.crypto = crypto
        crypto_meta = self.crypto.get_crypto_meta()
        self.crypto_ctxt = crypto.create_encryption_ctxt(keys['object'],
                                                         crypto_meta.get('iv'))
        self.keys = keys
        self.client_etag = req.headers.pop('etag', None)
        self.plaintext_etag = md5()
        self.ciphertext_etag = md5()
        self.footers_to_add = {}
        self.logger = logger

        req.headers['X-Object-Sysmeta-Crypto-Meta'] = \
            _dump_crypto_meta(crypto_meta)
        # TODO - remove the use of this callback when we stop using FF
        req.environ['swift.update.footers'] = self.footers_callback

    def footers_callback(self, footers):
        for key in self.footers_to_add:
            footers.update(
                {key: self.footers_to_add[key]})

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

        # Encrypt the plaintext etag using the object key and persist as
        # sysmeta along with the crypto parameters that were used.
        val, crypto_meta = encrypt_header_val(
            self.crypto, self.plaintext_etag, self.keys['object'])
        self.footers_to_add['X-Object-Sysmeta-Crypto-Etag'] = val
        self.footers_to_add['X-Object-Sysmeta-Crypto-Meta-Etag'] = crypto_meta

        # TODO: Encrypt the plaintext etag using the container key and use it
        # to override the container update value, with the crypto parameters
        # appended.
        # val, _ = encrypt_header_val(
        #     self.crypto, self.plaintext_etag, self.keys['container'],
        #     append_crypto_meta=True)
        val = self.plaintext_etag
        self.footers_to_add['X-Backend-Container-Update-Override-Etag'] = val

        for key in self.footers_to_add:
            self.logger.debug("encrypter added footer %s: %s" %
                              (key, self.footers_to_add[key]))

        return chunk


class EncrypterObjContext(CryptoWSGIContext):
    def __init__(self, encrypter, logger):
        super(EncrypterObjContext, self).__init__(encrypter, logger)
        self.server_type = 'object'

    def encrypt_user_metadata(self, req, keys):
        """
        Encrypt user-metadata header values. For each user metadata header, add
        a corresponding sysmeta header with the crypto metadata required to
        decrypt later.
        """

        # Check the user-metadata length before encrypting and encoding
        error_response = check_metadata(req, self.server_type.lower())
        if error_response:
                return error_response

        prefix = "%scrypto-meta-" % get_obj_persisted_sysmeta_prefix()

        for name, val in req.headers.items():
            # TODO - when encrypting account/container meta,
            # there will need to be an exception list.
            # if ('Meta-Temp-Url' in hdr) or \
            #         ('Meta-Access-Control-Allow-Origin' in hdr):
            #     continue
            if is_user_meta(self.server_type, name) and val:
                req.headers[name], full_crypto_meta = encrypt_header_val(
                    self.crypto, val, keys[self.server_type])
                # short_name is extracted in order to use it for
                # naming the corresponding crypto-meta
                # TODO we could get a sysmeta name clash if user meta has
                # X-Object-Meta-Etag
                short_name = strip_user_meta_prefix(self.server_type, name)
                req.headers[prefix + short_name] = full_crypto_meta
                self.logger.debug("encrypted user meta %s: %s"
                                  % (name, req.headers[name]))

    def encrypt_req_headers(self, req, keys):
        content_type = req.headers.get('Content-Type')
        if content_type:
            if 'container' not in keys:
                # TODO fail somewhere else, earlier, or not at all
                self.logger.error('Error: no container key to encrypt')
                raise HTTPUnprocessableEntity(request=req)

            # Encrypt the plaintext content-type using the object key and
            # persist as sysmeta along with the crypto parameters that were
            # used. Do this for PUT and POST because object_post_as_copy mode
            # allows content-type to be updated on a POST.
            req.headers['Content-Type'], crypto_meta = encrypt_header_val(
                self.crypto, content_type, keys[self.server_type],
                append_crypto_meta=True)
            self.logger.debug("encrypted for object Content-Type: %s using %s"
                              % (req.headers['Content-Type'], crypto_meta))

            # TODO: Encrypt the plaintext content-type using the container
            # key and use it to override the container update value, with the
            # crypto parameters appended.
            # val, _ = encrypt_header_val(
            #     self.crypto, ct, keys['container'], append_crypto_meta=True)
            val = content_type
            name = 'X-Backend-Container-Update-Override-Content-Type'
            req.headers[name] = val
            self.logger.debug("encrypted for container %s: %s" %
                              (name, val))

        error_resp = self.encrypt_user_metadata(req, keys)
        if error_resp:
            return error_resp

    def PUT(self, req, start_response):
        keys = self.get_keys(req.environ)

        error_resp = self.encrypt_req_headers(req, keys)
        if error_resp:
            return error_resp(req.environ, start_response)

        enc_input_proxy = EncInputWrapper(self.crypto, keys, req, self.logger)
        req.environ['wsgi.input'] = enc_input_proxy

        resp = self._app_call(req.environ)

        # If an etag is in the response headers, then replace its value with
        # the plaintext version
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

    def POST(self, req, start_response):
        """
        Encrypt the new object headers with a new iv and the current crypto.
        Note that an object may have encrypted headers while the body may
        remain unencrypted.
        """
        keys = self.get_keys(req.environ)
        error_resp = self.encrypt_req_headers(req, keys)
        if error_resp:
            return error_resp(req.environ, start_response)

        resp = self._app_call(req.environ)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp


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
