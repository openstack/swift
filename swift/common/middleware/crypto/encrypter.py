# Copyright (c) 2015-2016 OpenStack Foundation
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
import hashlib
import hmac
from contextlib import contextmanager

from swift.common.constraints import check_metadata
from swift.common.http import is_success
from swift.common.middleware.crypto.crypto_utils import CryptoWSGIContext, \
    dump_crypto_meta, append_crypto_meta, Crypto
from swift.common.request_helpers import get_object_transient_sysmeta, \
    strip_user_meta_prefix, is_user_meta, update_etag_is_at_header, \
    get_container_update_override_key
from swift.common.swob import Request, Match, HTTPException, \
    HTTPUnprocessableEntity, wsgi_to_bytes, bytes_to_wsgi, normalize_etag
from swift.common.utils import get_logger, config_true_value, \
    MD5_OF_EMPTY_STRING, md5, InputProxy


def encrypt_header_val(crypto, value, key):
    """
    Encrypt a header value using the supplied key.

    :param crypto: a Crypto instance
    :param value: value to encrypt
    :param key: crypto key to use
    :returns: a tuple of (encrypted value, crypto_meta) where crypto_meta is a
        dict of form returned by
        :py:func:`~swift.common.middleware.crypto.Crypto.get_crypto_meta`
    :raises ValueError: if value is empty
    """
    if not value:
        raise ValueError('empty value is not acceptable')

    crypto_meta = crypto.create_crypto_meta()
    crypto_ctxt = crypto.create_encryption_ctxt(key, crypto_meta['iv'])
    enc_val = bytes_to_wsgi(base64.b64encode(
        crypto_ctxt.update(wsgi_to_bytes(value))))
    return enc_val, crypto_meta


def _hmac_etag(key, etag):
    """
    Compute an HMAC-SHA256 using given key and etag.

    :param key: The starting key for the hash.
    :param etag: The etag to hash.
    :returns: a Base64-encoded representation of the HMAC
    """
    if not isinstance(etag, bytes):
        etag = wsgi_to_bytes(etag)
    result = hmac.new(key, etag, digestmod=hashlib.sha256).digest()
    return base64.b64encode(result).decode()


class EncInputWrapper(InputProxy):
    """File-like object to be swapped in for wsgi.input."""
    def __init__(self, crypto, keys, req, logger):
        super().__init__(req.environ['wsgi.input'])
        self.env = req.environ
        self.path = req.path
        self.crypto = crypto
        self.body_crypto_ctxt = None
        self.keys = keys
        self.plaintext_md5 = None
        self.ciphertext_md5 = None
        self.logger = logger
        self.install_footers_callback(req)

    def _init_encryption_context(self):
        # do this once when body is first read
        if self.body_crypto_ctxt is None:
            self.body_crypto_meta = self.crypto.create_crypto_meta()
            body_key = self.crypto.create_random_key()
            # wrap the body key with object key
            self.body_crypto_meta['body_key'] = self.crypto.wrap_key(
                self.keys['object'], body_key)
            self.body_crypto_meta['key_id'] = self.keys['id']
            self.body_crypto_ctxt = self.crypto.create_encryption_ctxt(
                body_key, self.body_crypto_meta.get('iv'))
            self.plaintext_md5 = md5(usedforsecurity=False)
            self.ciphertext_md5 = md5(usedforsecurity=False)

    def install_footers_callback(self, req):
        # the proxy controller will call back for footer metadata after
        # body has been sent
        inner_callback = req.environ.get('swift.callback.update_footers')
        # remove any Etag from headers, it won't be valid for ciphertext and
        # we'll send the ciphertext Etag later in footer metadata
        client_etag = req.headers.pop('etag', None)
        override_header = get_container_update_override_key('etag')
        container_listing_etag_header = req.headers.get(override_header)

        def footers_callback(footers):
            if inner_callback:
                # pass on footers dict to any other callback that was
                # registered before this one. It may override any footers that
                # were set.
                inner_callback(footers)

            plaintext_etag = None
            if self.body_crypto_ctxt:
                plaintext_etag = self.plaintext_md5.hexdigest()
                # If client (or other middleware) supplied etag, then validate
                # against plaintext etag
                etag_to_check = footers.get('Etag') or client_etag
                if (etag_to_check is not None and
                        plaintext_etag != etag_to_check):
                    raise HTTPUnprocessableEntity(request=Request(self.env))

                # override any previous notion of etag with the ciphertext etag
                footers['Etag'] = self.ciphertext_md5.hexdigest()

                # Encrypt the plaintext etag using the object key and persist
                # as sysmeta along with the crypto parameters that were used.
                encrypted_etag, etag_crypto_meta = encrypt_header_val(
                    self.crypto, plaintext_etag, self.keys['object'])
                footers['X-Object-Sysmeta-Crypto-Etag'] = \
                    append_crypto_meta(encrypted_etag, etag_crypto_meta)
                footers['X-Object-Sysmeta-Crypto-Body-Meta'] = \
                    dump_crypto_meta(self.body_crypto_meta)

                # Also add an HMAC of the etag for use when evaluating
                # conditional requests
                footers['X-Object-Sysmeta-Crypto-Etag-Mac'] = _hmac_etag(
                    self.keys['object'], plaintext_etag)
            else:
                # No data was read from body, nothing was encrypted, so don't
                # set any crypto sysmeta for the body, but do re-instate any
                # etag provided in inbound request if other middleware has not
                # already set a value.
                if client_etag is not None:
                    footers.setdefault('Etag', client_etag)

            # When deciding on the etag that should appear in container
            # listings, look for:
            #   * override in the footer, otherwise
            #   * override in the header, and finally
            #   * MD5 of the plaintext received
            # This may be None if no override was set and no data was read. An
            # override value of '' will be passed on.
            container_listing_etag = footers.get(
                override_header, container_listing_etag_header)

            if container_listing_etag is None:
                container_listing_etag = plaintext_etag

            if (container_listing_etag and
                    (container_listing_etag != MD5_OF_EMPTY_STRING or
                     plaintext_etag)):
                # Encrypt the container-listing etag using the container key
                # and a random IV, and use it to override the container update
                # value, with the crypto parameters appended. We use the
                # container key here so that only that key is required to
                # decrypt all etag values in a container listing when handling
                # a container GET request. Don't encrypt an MD5_OF_EMPTY_STRING
                # unless there actually was some body content, in which case
                # the container-listing etag is possibly conveying some
                # non-obvious information.
                val, crypto_meta = encrypt_header_val(
                    self.crypto, container_listing_etag,
                    self.keys['container'])
                crypto_meta['key_id'] = self.keys['id']
                footers[override_header] = \
                    append_crypto_meta(val, crypto_meta)
            # else: no override was set and no data was read

        req.environ['swift.callback.update_footers'] = footers_callback

    def chunk_update(self, chunk, eof, *args, **kwargs):
        if chunk:
            self._init_encryption_context()
            self.plaintext_md5.update(chunk)
            # Encrypt one chunk at a time
            ciphertext = self.body_crypto_ctxt.update(chunk)
            self.ciphertext_md5.update(ciphertext)
            return ciphertext

        return chunk


class EncrypterObjContext(CryptoWSGIContext):
    def __init__(self, encrypter, logger):
        super(EncrypterObjContext, self).__init__(
            encrypter, 'object', logger)

    def _check_headers(self, req):
        # Check the user-metadata length before encrypting and encoding
        error_response = check_metadata(req, self.server_type)
        if error_response:
            raise error_response

    def encrypt_user_metadata(self, req, keys):
        """
        Encrypt user-metadata header values. Replace each x-object-meta-<key>
        user metadata header with a corresponding
        x-object-transient-sysmeta-crypto-meta-<key> header which has the
        crypto metadata required to decrypt appended to the encrypted value.

        :param req: a swob Request
        :param keys: a dict of encryption keys
        """
        prefix = get_object_transient_sysmeta('crypto-meta-')
        user_meta_headers = [h for h in req.headers.items() if
                             is_user_meta(self.server_type, h[0]) and h[1]]
        crypto_meta = None
        for name, val in user_meta_headers:
            short_name = strip_user_meta_prefix(self.server_type, name)
            new_name = prefix + short_name
            enc_val, crypto_meta = encrypt_header_val(
                self.crypto, val, keys[self.server_type])
            req.headers[new_name] = append_crypto_meta(enc_val, crypto_meta)
            req.headers.pop(name)
        # store a single copy of the crypto meta items that are common to all
        # encrypted user metadata independently of any such meta that is stored
        # with the object body because it might change on a POST. This is done
        # for future-proofing - the meta stored here is not currently used
        # during decryption.
        if crypto_meta:
            meta = dump_crypto_meta({'cipher': crypto_meta['cipher'],
                                     'key_id': keys['id']})
            req.headers[get_object_transient_sysmeta('crypto-meta')] = meta

    def handle_put(self, req, start_response):
        self._check_headers(req)
        keys = self.get_keys(req.environ, required=['object', 'container'])
        self.encrypt_user_metadata(req, keys)

        enc_input_proxy = EncInputWrapper(self.crypto, keys, req, self.logger)
        req.environ['wsgi.input'] = enc_input_proxy

        resp = self._app_call(req.environ)

        # If an etag is in the response headers and a plaintext etag was
        # calculated, then overwrite the response value with the plaintext etag
        # provided it matches the ciphertext etag. If it does not match then do
        # not overwrite and allow the response value to return to client.
        mod_resp_headers = self._response_headers
        if (is_success(self._get_status_int()) and
                enc_input_proxy.plaintext_md5):
            plaintext_etag = enc_input_proxy.plaintext_md5.hexdigest()
            ciphertext_etag = enc_input_proxy.ciphertext_md5.hexdigest()
            mod_resp_headers = [
                (h, v if (h.lower() != 'etag' or
                          normalize_etag(v) != ciphertext_etag)
                    else plaintext_etag)
                for h, v in mod_resp_headers]

        start_response(self._response_status, mod_resp_headers,
                       self._response_exc_info)
        return resp

    def handle_post(self, req, start_response):
        """
        Encrypt the new object headers with a new iv and the current crypto.
        Note that an object may have encrypted headers while the body may
        remain unencrypted.
        """
        self._check_headers(req)
        keys = self.get_keys(req.environ)
        self.encrypt_user_metadata(req, keys)

        resp = self._app_call(req.environ)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp

    @contextmanager
    def _mask_conditional_etags(self, req, header_name):
        """
        Calculate HMACs of etags in header value and append to existing list.
        The HMACs are calculated in the same way as was done for the object
        plaintext etag to generate the value of
        X-Object-Sysmeta-Crypto-Etag-Mac when the object was PUT. The object
        server can therefore use these HMACs to evaluate conditional requests.
        HMACs of the etags are appended for the current root secrets and
        historic root secrets because it is not known which of them may have
        been used to generate the on-disk etag HMAC.

        The existing etag values are left in the list of values to match in
        case the object was not encrypted when it was PUT. It is unlikely that
        a masked etag value would collide with an unmasked value.

        :param req: an instance of swob.Request
        :param header_name: name of header that has etags to mask
        :return: True if any etags were masked, False otherwise
        """
        masked = False
        old_etags = req.headers.get(header_name)
        if old_etags:
            all_keys = self.get_multiple_keys(req.environ)
            new_etags = []
            for etag in Match(old_etags).tags:
                if etag == '*':
                    new_etags.append(etag)
                    continue
                new_etags.append('"%s"' % etag)
                for keys in all_keys:
                    masked_etag = _hmac_etag(keys['object'], etag)
                    new_etags.append('"%s"' % masked_etag)
                masked = True

            req.headers[header_name] = ', '.join(new_etags)

        try:
            yield masked
        finally:
            if old_etags:
                req.headers[header_name] = old_etags

    def handle_get_or_head(self, req, start_response):
        with self._mask_conditional_etags(req, 'If-Match') as masked1:
            with self._mask_conditional_etags(req, 'If-None-Match') as masked2:
                if masked1 or masked2:
                    update_etag_is_at_header(
                        req, 'X-Object-Sysmeta-Crypto-Etag-Mac')
                resp = self._app_call(req.environ)
                start_response(self._response_status, self._response_headers,
                               self._response_exc_info)
        return resp


class Encrypter(object):
    """Middleware for encrypting data and user metadata.

    By default all PUT or POST'ed object data and/or metadata will be
    encrypted. Encryption of new data and/or metadata may be disabled by
    setting the ``disable_encryption`` option to True. However, this middleware
    should remain in the pipeline in order for existing encrypted data to be
    read.
    """

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="encrypter")
        self.crypto = Crypto(conf)
        self.disable_encryption = config_true_value(
            conf.get('disable_encryption', 'false'))

    def __call__(self, env, start_response):
        # If override is set in env, then just pass along
        if config_true_value(env.get('swift.crypto.override')):
            return self.app(env, start_response)

        req = Request(env)

        if self.disable_encryption and req.method in ('PUT', 'POST'):
            return self.app(env, start_response)
        try:
            req.split_path(4, 4, True)
            is_object_request = True
        except ValueError:
            is_object_request = False
        if not is_object_request:
            return self.app(env, start_response)

        if req.method in ('GET', 'HEAD'):
            handler = EncrypterObjContext(self, self.logger).handle_get_or_head
        elif req.method == 'PUT':
            handler = EncrypterObjContext(self, self.logger).handle_put
        elif req.method == 'POST':
            handler = EncrypterObjContext(self, self.logger).handle_post
        else:
            # anything else
            return self.app(env, start_response)

        try:
            return handler(req, start_response)
        except HTTPException as err_resp:
            return err_resp(env, start_response)
