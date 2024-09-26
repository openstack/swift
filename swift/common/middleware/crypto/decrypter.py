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
import json

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import is_success
from swift.common.middleware.crypto.crypto_utils import CryptoWSGIContext, \
    load_crypto_meta, extract_crypto_meta, Crypto
from swift.common.exceptions import EncryptionException, UnknownSecretIdError
from swift.common.request_helpers import get_object_transient_sysmeta, \
    get_sys_meta_prefix, get_user_meta_prefix, \
    get_container_update_override_key
from swift.common.swob import Request, HTTPException, \
    HTTPInternalServerError, wsgi_to_bytes, bytes_to_wsgi
from swift.common.utils import get_logger, config_true_value, \
    parse_content_range, closing_if_possible, parse_content_type, \
    FileLikeIter, multipart_byteranges_to_document_iters

DECRYPT_CHUNK_SIZE = 65536


def purge_crypto_sysmeta_headers(headers):
    return [h for h in headers if not
            h[0].lower().startswith(
                (get_object_transient_sysmeta('crypto-'),
                 get_sys_meta_prefix('object') + 'crypto-'))]


class BaseDecrypterContext(CryptoWSGIContext):
    def get_crypto_meta(self, header_name, check=True):
        """
        Extract a crypto_meta dict from a header.

        :param header_name: name of header that may have crypto_meta
        :param check: if True validate the crypto meta
        :return: A dict containing crypto_meta items
        :raises EncryptionException: if an error occurs while parsing the
                                     crypto meta
        """
        crypto_meta_json = self._response_header_value(header_name)

        if crypto_meta_json is None:
            return None
        crypto_meta = load_crypto_meta(crypto_meta_json)
        if check:
            self.crypto.check_crypto_meta(crypto_meta)
        return crypto_meta

    def get_unwrapped_key(self, crypto_meta, wrapping_key):
        """
        Get a wrapped key from crypto-meta and unwrap it using the provided
        wrapping key.

        :param crypto_meta: a dict of crypto-meta
        :param wrapping_key: key to be used to decrypt the wrapped key
        :return: an unwrapped key
        :raises HTTPInternalServerError: if the crypto-meta has no wrapped key
                                         or the unwrapped key is invalid
        """
        try:
            return self.crypto.unwrap_key(wrapping_key,
                                          crypto_meta['body_key'])
        except KeyError as err:
            self.logger.error(
                'Error decrypting %(resp_type)s: Missing %(key)s',
                {'resp_type': self.server_type, 'key': err})
        except ValueError as err:
            self.logger.error('Error decrypting %(resp_type)s: %(reason)s',
                              {'resp_type': self.server_type, 'reason': err})
        raise HTTPInternalServerError(
            body='Error decrypting %s' % self.server_type,
            content_type='text/plain')

    def decrypt_value_with_meta(self, value, key, required, decoder):
        """
        Base64-decode and decrypt a value if crypto meta can be extracted from
        the value itself, otherwise return the value unmodified.

        A value should either be a string that does not contain the ';'
        character or should be of the form::

            <base64-encoded ciphertext>;swift_meta=<crypto meta>

        :param value: value to decrypt
        :param key: crypto key to use
        :param required: if True then the value is required to be decrypted
                         and an EncryptionException will be raised if the
                         header cannot be decrypted due to missing crypto meta.
        :param decoder: function to turn the decrypted bytes into useful data
        :returns: decrypted value if crypto meta is found, otherwise the
                  unmodified value
        :raises EncryptionException: if an error occurs while parsing crypto
                                     meta or if the header value was required
                                     to be decrypted but crypto meta was not
                                     found.
        """
        extracted_value, crypto_meta = extract_crypto_meta(value)
        if crypto_meta:
            self.crypto.check_crypto_meta(crypto_meta)
            value = self.decrypt_value(
                extracted_value, key, crypto_meta, decoder)
        elif required:
            raise EncryptionException(
                "Missing crypto meta in value %s" % value)

        return value

    def decrypt_value(self, value, key, crypto_meta, decoder):
        """
        Base64-decode and decrypt a value using the crypto_meta provided.

        :param value: a base64-encoded value to decrypt
        :param key: crypto key to use
        :param crypto_meta: a crypto-meta dict of form returned by
            :py:func:`~swift.common.middleware.crypto.Crypto.get_crypto_meta`
        :param decoder: function to turn the decrypted bytes into useful data
        :returns: decrypted value
        """
        if not value:
            return decoder(b'')
        crypto_ctxt = self.crypto.create_decryption_ctxt(
            key, crypto_meta['iv'], 0)
        return decoder(crypto_ctxt.update(base64.b64decode(value)))

    def get_decryption_keys(self, req, crypto_meta=None):
        """
        Determine if a response should be decrypted, and if so then fetch keys.

        :param req: a Request object
        :param crypto_meta: a dict of crypto metadata
        :returns: a dict of decryption keys
        """
        if config_true_value(req.environ.get('swift.crypto.override')):
            self.logger.debug('No decryption is necessary because of override')
            return None

        key_id = crypto_meta.get('key_id') if crypto_meta else None
        return self.get_keys(req.environ, key_id=key_id)


class DecrypterObjContext(BaseDecrypterContext):
    def __init__(self, decrypter, logger):
        super(DecrypterObjContext, self).__init__(decrypter, 'object', logger)

    def _decrypt_header(self, header, value, key, required=False):
        """
        Attempt to decrypt a header value that may be encrypted.

        :param header: the header name
        :param value: the header value
        :param key: decryption key
        :param required: if True then the header is required to be decrypted
                         and an HTTPInternalServerError will be raised if the
                         header cannot be decrypted due to missing crypto meta.
        :return: decrypted value or the original value if it was not encrypted.
        :raises HTTPInternalServerError: if an error occurred during decryption
                                         or if the header value was required to
                                         be decrypted but crypto meta was not
                                         found.
        """
        try:
            return self.decrypt_value_with_meta(
                value, key, required, bytes_to_wsgi)
        except EncryptionException as err:
            self.logger.error(
                "Error decrypting header %(header)s: %(error)s",
                {'header': header, 'error': err})
            raise HTTPInternalServerError(
                body='Error decrypting header',
                content_type='text/plain')

    def decrypt_user_metadata(self, keys):
        prefix = get_object_transient_sysmeta('crypto-meta-')
        prefix_len = len(prefix)
        new_prefix = get_user_meta_prefix(self.server_type).title()
        result = []
        for name, val in self._response_headers:
            if name.lower().startswith(prefix) and val:
                short_name = name[prefix_len:]
                decrypted_value = self._decrypt_header(
                    name, val, keys[self.server_type], required=True)
                result.append((new_prefix + short_name, decrypted_value))
        return result

    def decrypt_resp_headers(self, put_keys, post_keys, update_cors_exposed):
        """
        Find encrypted headers and replace with the decrypted versions.

        :param put_keys: a dict of decryption keys used for object PUT.
        :param post_keys: a dict of decryption keys used for object POST.
        :return: A list of headers with any encrypted headers replaced by their
                 decrypted values.
        :raises HTTPInternalServerError: if any error occurs while decrypting
                                         headers
        """
        mod_hdr_pairs = []

        if put_keys:
            # Decrypt plaintext etag and place in Etag header for client
            # response
            etag_header = 'X-Object-Sysmeta-Crypto-Etag'
            encrypted_etag = self._response_header_value(etag_header)
            if encrypted_etag:
                decrypted_etag = self._decrypt_header(
                    etag_header, encrypted_etag, put_keys['object'],
                    required=True)
                mod_hdr_pairs.append(('Etag', decrypted_etag))

            etag_header = get_container_update_override_key('etag')
            encrypted_etag = self._response_header_value(etag_header)
            if encrypted_etag:
                decrypted_etag = self._decrypt_header(
                    etag_header, encrypted_etag, put_keys['container'])
                mod_hdr_pairs.append((etag_header, decrypted_etag))

        # Decrypt all user metadata. Encrypted user metadata values are stored
        # in the x-object-transient-sysmeta-crypto-meta- namespace. Those are
        # decrypted and moved back to the x-object-meta- namespace. Prior to
        # decryption, the response should have no x-object-meta- headers, but
        # if it does then they will be overwritten by any decrypted headers
        # that map to the same x-object-meta- header names i.e. decrypted
        # headers win over unexpected, unencrypted headers.
        if post_keys:
            decrypted_meta = self.decrypt_user_metadata(post_keys)
            mod_hdr_pairs.extend(decrypted_meta)
        else:
            decrypted_meta = []

        mod_hdr_names = {h.lower() for h, v in mod_hdr_pairs}

        found_aceh = False
        for header, value in self._response_headers:
            lheader = header.lower()
            if lheader in mod_hdr_names:
                continue
            if lheader == 'access-control-expose-headers':
                found_aceh = True
                mod_hdr_pairs.append((header, value + ', ' + ', '.join(
                    meta.lower() for meta, _data in decrypted_meta)))
            else:
                mod_hdr_pairs.append((header, value))
        if update_cors_exposed and not found_aceh:
            mod_hdr_pairs.append(('Access-Control-Expose-Headers', ', '.join(
                meta.lower() for meta, _data in decrypted_meta)))
        return mod_hdr_pairs

    def multipart_response_iter(self, resp, boundary, body_key, crypto_meta):
        """
        Decrypts a multipart mime doc response body.

        :param resp: application response
        :param boundary: multipart boundary string
        :param body_key: decryption key for the response body
        :param crypto_meta: crypto_meta for the response body
        :return: generator for decrypted response body
        """
        with closing_if_possible(resp):
            parts_iter = multipart_byteranges_to_document_iters(
                FileLikeIter(resp), boundary)
            for first_byte, last_byte, length, headers, body in parts_iter:
                yield b"--" + boundary + b"\r\n"

                for header, value in headers:
                    yield b"%s: %s\r\n" % (wsgi_to_bytes(header),
                                           wsgi_to_bytes(value))

                yield b"\r\n"

                decrypt_ctxt = self.crypto.create_decryption_ctxt(
                    body_key, crypto_meta['iv'], first_byte)
                for chunk in iter(lambda: body.read(DECRYPT_CHUNK_SIZE), b''):
                    yield decrypt_ctxt.update(chunk)

                yield b"\r\n"

            yield b"--" + boundary + b"--"

    def response_iter(self, resp, body_key, crypto_meta, offset):
        """
        Decrypts a response body.

        :param resp: application response
        :param body_key: decryption key for the response body
        :param crypto_meta: crypto_meta for the response body
        :param offset: offset into object content at which response body starts
        :return: generator for decrypted response body
        """
        decrypt_ctxt = self.crypto.create_decryption_ctxt(
            body_key, crypto_meta['iv'], offset)
        with closing_if_possible(resp):
            for chunk in resp:
                yield decrypt_ctxt.update(chunk)

    def _read_crypto_meta(self, header, check):
        crypto_meta = None
        if (is_success(self._get_status_int()) or
                self._get_status_int() in (304, 412)):
            try:
                crypto_meta = self.get_crypto_meta(header, check)
            except EncryptionException as err:
                self.logger.error('Error decrypting object: %s', err)
                raise HTTPInternalServerError(
                    body='Error decrypting object', content_type='text/plain')
        return crypto_meta

    def handle(self, req, start_response):
        app_resp = self._app_call(req.environ)

        try:
            put_crypto_meta = self._read_crypto_meta(
                'X-Object-Sysmeta-Crypto-Body-Meta', True)
            put_keys = self.get_decryption_keys(req, put_crypto_meta)
            post_crypto_meta = self._read_crypto_meta(
                'X-Object-Transient-Sysmeta-Crypto-Meta', False)
            post_keys = self.get_decryption_keys(req, post_crypto_meta)
        except EncryptionException as err:
            self.logger.error(
                "Error decrypting object: %s",
                err)
            raise HTTPInternalServerError(
                body='Error decrypting object',
                content_type='text/plain')

        if put_keys is None and post_keys is None:
            # skip decryption
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return app_resp

        mod_resp_headers = self.decrypt_resp_headers(
            put_keys, post_keys,
            update_cors_exposed=bool(req.headers.get('origin')))

        if put_crypto_meta and req.method == 'GET' and \
                is_success(self._get_status_int()):
            # 2xx response and encrypted body
            body_key = self.get_unwrapped_key(
                put_crypto_meta, put_keys['object'])
            content_type, content_type_attrs = parse_content_type(
                self._response_header_value('Content-Type'))

            if (self._get_status_int() == 206 and
                    content_type == 'multipart/byteranges'):
                boundary = wsgi_to_bytes(dict(content_type_attrs)["boundary"])
                resp_iter = self.multipart_response_iter(
                    app_resp, boundary, body_key, put_crypto_meta)
            else:
                offset = 0
                content_range = self._response_header_value('Content-Range')
                if content_range:
                    # Determine offset within the whole object if ranged GET
                    offset, end, total = parse_content_range(content_range)
                resp_iter = self.response_iter(
                    app_resp, body_key, put_crypto_meta, offset)
        else:
            # don't decrypt body of unencrypted or non-2xx responses
            resp_iter = app_resp

        mod_resp_headers = purge_crypto_sysmeta_headers(mod_resp_headers)
        start_response(self._response_status, mod_resp_headers,
                       self._response_exc_info)

        return resp_iter


class DecrypterContContext(BaseDecrypterContext):
    def __init__(self, decrypter, logger):
        super(DecrypterContContext, self).__init__(
            decrypter, 'container', logger)

    def handle(self, req, start_response):
        app_resp = self._app_call(req.environ)

        if is_success(self._get_status_int()):
            # only decrypt body of 2xx responses
            headers = HeaderKeyDict(self._response_headers)
            content_type = headers.get('content-type', '').split(';', 1)[0]
            if content_type == 'application/json':
                app_resp = self.process_json_resp(req, app_resp)

        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)

        return app_resp

    def process_json_resp(self, req, resp_iter):
        """
        Parses json body listing and decrypt encrypted entries. Updates
        Content-Length header with new body length and return a body iter.
        """
        with closing_if_possible(resp_iter):
            resp_body = b''.join(resp_iter)
        body_json = json.loads(resp_body)
        new_body = json.dumps([self.decrypt_obj_dict(req, obj_dict)
                               for obj_dict in body_json]).encode('ascii')
        self.update_content_length(len(new_body))
        return [new_body]

    def decrypt_obj_dict(self, req, obj_dict):
        if 'hash' in obj_dict:
            # each object's etag may have been encrypted with a different key
            # so fetch keys based on its crypto meta
            ciphertext, crypto_meta = extract_crypto_meta(obj_dict['hash'])
            bad_keys = set()
            if crypto_meta:
                try:
                    self.crypto.check_crypto_meta(crypto_meta)
                    keys = self.get_decryption_keys(req, crypto_meta)
                    # Note that symlinks (for example) may put swift paths in
                    # the listing ETag, so we can't just use ASCII.
                    obj_dict['hash'] = self.decrypt_value(
                        ciphertext, keys['container'], crypto_meta,
                        decoder=lambda x: x.decode('utf-8'))
                except EncryptionException as err:
                    if not isinstance(err, UnknownSecretIdError) or \
                            err.args[0] not in bad_keys:
                        # Only warn about an unknown key once per listing
                        self.logger.error(
                            "Error decrypting container listing: %s",
                            err)
                    if isinstance(err, UnknownSecretIdError):
                        bad_keys.add(err.args[0])
                    obj_dict['hash'] = '<unknown>'
        return obj_dict


class Decrypter(object):
    """Middleware for decrypting data and user metadata."""

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="decrypter")
        self.crypto = Crypto(conf)

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            parts = req.split_path(3, 4, True)
            is_cont_or_obj_req = True
        except ValueError:
            is_cont_or_obj_req = False
        if not is_cont_or_obj_req:
            return self.app(env, start_response)

        if parts[3] and req.method in ('GET', 'HEAD'):
            handler = DecrypterObjContext(self, self.logger).handle
        elif parts[2] and req.method == 'GET':
            handler = DecrypterContContext(self, self.logger).handle
        else:
            # url and/or request verb is not handled by decrypter
            return self.app(env, start_response)

        try:
            return handler(req, start_response)
        except HTTPException as err_resp:
            return err_resp(env, start_response)
