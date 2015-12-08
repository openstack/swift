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
from contextlib import contextmanager
from hashlib import md5
import base64
import json
import urllib
from swift.common.crypto_utils import CryptoWSGIContext
from swift.common.utils import get_logger, config_true_value
from swift.common.request_helpers import get_object_transient_sysmeta_prefix, \
    strip_user_meta_prefix, is_user_meta
from swift.common.swob import Request, Match, HTTPException, \
    HTTPUnprocessableEntity
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
    return urllib.quote_plus(json.dumps({
        name: (base64.b64encode(value).decode() if name == 'iv' else value)
        for name, value in crypto_meta.items()}))


def encrypt_header_val(crypto, value, key, append_crypto_meta=False,
                       iv_base=None):
    """
    Encrypt a value using the supplied key.

    :param value: value to encrypt
    :param key: crypto key to use
    :param crypto_meta: a crypto-meta dict of form returned by
        :py:func:`~swift.common.middleware.crypto.Crypto.get_crypto_meta`
    :param iv_base: an optional string from which an iv will be derived
    :returns: a tuple of (encrypted value, crypto_meta) where crypto_meta is a
        dict of form returned by
        :py:func:`~swift.common.middleware.crypto.Crypto.get_crypto_meta`
    """
    if not value:
        return '', None

    crypto_meta = crypto.get_crypto_meta(iv_base=iv_base)
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
        self.path = req.path
        self.crypto = crypto
        crypto_meta = self.crypto.get_crypto_meta()
        self.crypto_ctxt = crypto.create_encryption_ctxt(keys['object'],
                                                         crypto_meta.get('iv'))
        self.keys = keys
        # remove any Etag from headers, it won't be valid for ciphertext and
        # we'll send the ciphertext Etag later in footer metadata
        self.client_etag = req.headers.pop('etag', None)
        self.plaintext_md5 = md5()
        self.ciphertext_md5 = md5()
        self.footers_to_add = {}
        self.logger = logger

        req.headers['X-Object-Sysmeta-Crypto-Meta'] = \
            _dump_crypto_meta(crypto_meta)
        self.install_footers_callback(req)

    def install_footers_callback(self, req):
        # the proxy controller will call back for footer metadata after
        # body has been sent
        inner_callback = req.environ.get('swift.callback.update_footers')

        def footers_callback(footers):
            # Encrypt the plaintext etag using the object key and persist as
            # sysmeta along with the crypto parameters that were used.
            val, crypto_meta = encrypt_header_val(
                self.crypto, self.plaintext_md5.hexdigest(),
                self.keys['object'], iv_base=self.path)
            footers['X-Object-Sysmeta-Crypto-Etag'] = val
            footers['X-Object-Sysmeta-Crypto-Meta-Etag'] = crypto_meta

            # TODO: Encrypt the plaintext etag using the container key and use
            # it to override the container update value, with the crypto
            # parameters appended.
            val = self.plaintext_md5.hexdigest()
            footers['X-Backend-Container-Update-Override-Etag'] = val

            if inner_callback:
                # pass on footers dict to any other callback that was
                # registered before this one. It may override any
                # x-backend-container-update-overide- headers that were set.
                inner_callback(footers)

            # we override any previous notion of etag with the ciphertext etag
            footers['Etag'] = self.ciphertext_md5.hexdigest()

        req.environ['swift.callback.update_footers'] = footers_callback

    def read(self, *args, **kwargs):
        return self.readChunk(self.wsgi_input.read, *args, **kwargs)

    def readline(self, *args, **kwargs):
        return self.readChunk(self.wsgi_input.readline, *args, **kwargs)

    def readChunk(self, read_method, *args, **kwargs):
        chunk = read_method(*args, **kwargs)

        if chunk:
            self.plaintext_md5.update(chunk)
            # Encrypt one chunk at a time
            ciphertext = self.crypto_ctxt.update(chunk)
            self.ciphertext_md5.update(ciphertext)
            return ciphertext

        # If client supplied etag, then validate against plaintext etag
        if self.client_etag:
            if self.plaintext_md5.hexdigest() != self.client_etag:
                raise HTTPUnprocessableEntity(request=Request(self.env))

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

        prefix = "%scrypto-meta-" % get_object_transient_sysmeta_prefix()

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
        if 'container' not in keys:
            # TODO fail somewhere else, earlier, or not at all
            self.logger.error('Error: no container key to encrypt')
            raise HTTPUnprocessableEntity(request=req)

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
        mod_resp_headers = filter(lambda h: h[0].lower() != 'etag',
                                  self._response_headers)
        if len(mod_resp_headers) < len(self._response_headers):
            mod_resp_headers.append(
                ('etag', enc_input_proxy.plaintext_md5.hexdigest()))

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

    @contextmanager
    def _encrypt_conditional_etags(self, req, header_name):
        old_etags = req.headers.get(header_name)
        if old_etags:
            keys = self.get_keys(req.environ)
            new_etags = []
            for etag in Match(old_etags).tags:
                if etag == '*':
                    new_etags.append(etag)
                    continue
                crypto_etag, meta = encrypt_header_val(self.crypto, etag,
                                                       keys[self.server_type],
                                                       iv_base=req.path)
                new_etags.extend(('"%s"' % etag, '"%s"' % crypto_etag))

            req.headers[header_name] = ', '.join(new_etags)
            req.headers.setdefault(
                'X-Backend-Etag-Is-At', 'X-Object-Sysmeta-Crypto-Etag')

        try:
            yield
        finally:
            if old_etags:
                req.headers[header_name] = old_etags

    def handle_get_or_head(self, req, start_response):
        with self._encrypt_conditional_etags(req, 'If-Match'):
            with self._encrypt_conditional_etags(req, 'If-None-Match'):
                resp = self._app_call(req.environ)
                start_response(self._response_status, self._response_headers,
                               self._response_exc_info)
        return resp

    def HEAD(self, req, start_response):
        return self.handle_get_or_head(req, start_response)

    def GET(self, req, start_response):
        return self.handle_get_or_head(req, start_response)


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

        self.crypto = Crypto(self.conf)

        try:
            req.split_path(4, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if hasattr(EncrypterObjContext, req.method):
            # handle only those request methods that may require keys
            enc_context = EncrypterObjContext(self, self.logger)
            try:
                return getattr(enc_context, req.method)(req, start_response)
            except HTTPException as err_resp:
                return err_resp(env, start_response)

        # anything else
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def encrypter_filter(app):
        return Encrypter(app, conf)
    return encrypter_filter
