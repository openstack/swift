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

from swift import gettext_ as _
from swift.common.http import is_success
from swift.common.middleware.crypto.crypto_utils import CryptoWSGIContext, \
    load_crypto_meta, extract_crypto_meta, Crypto
from swift.common.exceptions import EncryptionException
from swift.common.request_helpers import get_object_transient_sysmeta, \
    get_sys_meta_prefix, get_user_meta_prefix
from swift.common.swob import Request, HTTPException, HTTPInternalServerError
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
    def get_crypto_meta(self, header_name):
        """
        Extract a crypto_meta dict from a header.

        :param header_name: name of header that may have crypto_meta
        :return: A dict containing crypto_meta items
        :raises EncryptionException: if an error occurs while parsing the
                                     crypto meta
        """
        crypto_meta_json = self._response_header_value(header_name)

        if crypto_meta_json is None:
            return None
        crypto_meta = load_crypto_meta(crypto_meta_json)
        self.crypto.check_crypto_meta(crypto_meta)
        return crypto_meta

    def get_unwrapped_key(self, crypto_meta, wrapping_key):
        """
        Get a wrapped key from crypto-meta and unwrap it using the provided
        wrapping key.

        :param crypto_meta: a dict of crypto-meta
        :param wrapping_key: key to be used to decrypt the wrapped key
        :return: an unwrapped key
        :raises EncryptionException: if the crypto-meta has no wrapped key or
                                     the unwrapped key is invalid
        """
        try:
            return self.crypto.unwrap_key(wrapping_key,
                                          crypto_meta['body_key'])
        except KeyError as err:
            self.logger.error(
                _('Error decrypting %(resp_type)s: Missing %(key)s'),
                {'resp_type': self.server_type, 'key': err})
        except ValueError as err:
            self.logger.error(_('Error decrypting %(resp_type)s: %(reason)s'),
                              {'resp_type': self.server_type, 'reason': err})
        raise HTTPInternalServerError(
            body='Error decrypting %s' % self.server_type,
            content_type='text/plain')

    def decrypt_value_with_meta(self, value, key, required=False):
        """
        Base64-decode and decrypt a value if crypto meta can be extracted from
        the value itself, otherwise return the value unmodified.

        A value should either be a string that does not contain the ';'
        character or should be of the form:

            <base64-encoded ciphertext>;swift_meta=<crypto meta>

        :param value: value to decrypt
        :param key: crypto key to use
        :param required: if True then the value is required to be decrypted
                         and an EncryptionException will be raised if the
                         header cannot be decrypted due to missing crypto meta.
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
            value = self.decrypt_value(extracted_value, key, crypto_meta)
        elif required:
            raise EncryptionException(
                "Missing crypto meta in value %s" % value)

        return value

    def decrypt_value(self, value, key, crypto_meta):
        """
        Base64-decode and decrypt a value using the crypto_meta provided.

        :param value: a base64-encoded value to decrypt
        :param key: crypto key to use
        :param crypto_meta: a crypto-meta dict of form returned by
            :py:func:`~swift.common.middleware.crypto.Crypto.get_crypto_meta`
        :returns: decrypted value
        """
        if not value:
            return ''
        crypto_ctxt = self.crypto.create_decryption_ctxt(
            key, crypto_meta['iv'], 0)
        return crypto_ctxt.update(base64.b64decode(value))

    def get_decryption_keys(self, req):
        """
        Determine if a response should be decrypted, and if so then fetch keys.

        :param req: a Request object
        :returns: a dict of decryption keys
        """
        if config_true_value(req.environ.get('swift.crypto.override')):
            self.logger.debug('No decryption is necessary because of override')
            return None

        return self.get_keys(req.environ)


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
            return self.decrypt_value_with_meta(value, key, required)
        except EncryptionException as err:
            self.logger.error(
                _("Error decrypting header %(header)s: %(error)s"),
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

    def decrypt_resp_headers(self, keys):
        """
        Find encrypted headers and replace with the decrypted versions.

        :param keys: a dict of decryption keys.
        :return: A list of headers with any encrypted headers replaced by their
                 decrypted values.
        :raises HTTPInternalServerError: if any error occurs while decrypting
                                         headers
        """
        mod_hdr_pairs = []

        # Decrypt plaintext etag and place in Etag header for client response
        etag_header = 'X-Object-Sysmeta-Crypto-Etag'
        encrypted_etag = self._response_header_value(etag_header)
        if encrypted_etag:
            decrypted_etag = self._decrypt_header(
                etag_header, encrypted_etag, keys['object'], required=True)
            mod_hdr_pairs.append(('Etag', decrypted_etag))

        etag_header = 'X-Object-Sysmeta-Container-Update-Override-Etag'
        encrypted_etag = self._response_header_value(etag_header)
        if encrypted_etag:
            decrypted_etag = self._decrypt_header(
                etag_header, encrypted_etag, keys['container'])
            mod_hdr_pairs.append((etag_header, decrypted_etag))

        # Decrypt all user metadata. Encrypted user metadata values are stored
        # in the x-object-transient-sysmeta-crypto-meta- namespace. Those are
        # decrypted and moved back to the x-object-meta- namespace. Prior to
        # decryption, the response should have no x-object-meta- headers, but
        # if it does then they will be overwritten by any decrypted headers
        # that map to the same x-object-meta- header names i.e. decrypted
        # headers win over unexpected, unencrypted headers.
        mod_hdr_pairs.extend(self.decrypt_user_metadata(keys))

        mod_hdr_names = {h.lower() for h, v in mod_hdr_pairs}
        mod_hdr_pairs.extend([(h, v) for h, v in self._response_headers
                              if h.lower() not in mod_hdr_names])
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
                yield "--" + boundary + "\r\n"

                for header_pair in headers:
                    yield "%s: %s\r\n" % header_pair

                yield "\r\n"

                decrypt_ctxt = self.crypto.create_decryption_ctxt(
                    body_key, crypto_meta['iv'], first_byte)
                for chunk in iter(lambda: body.read(DECRYPT_CHUNK_SIZE), ''):
                    yield decrypt_ctxt.update(chunk)

                yield "\r\n"

            yield "--" + boundary + "--"

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

    def handle_get(self, req, start_response):
        app_resp = self._app_call(req.environ)

        keys = self.get_decryption_keys(req)
        if keys is None:
            # skip decryption
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return app_resp

        mod_resp_headers = self.decrypt_resp_headers(keys)

        crypto_meta = None
        if is_success(self._get_status_int()):
            try:
                crypto_meta = self.get_crypto_meta(
                    'X-Object-Sysmeta-Crypto-Body-Meta')
            except EncryptionException as err:
                self.logger.error(_('Error decrypting object: %s'), err)
                raise HTTPInternalServerError(
                    body='Error decrypting object', content_type='text/plain')

        if crypto_meta:
            # 2xx response and encrypted body
            body_key = self.get_unwrapped_key(crypto_meta, keys['object'])
            content_type, content_type_attrs = parse_content_type(
                self._response_header_value('Content-Type'))

            if (self._get_status_int() == 206 and
                    content_type == 'multipart/byteranges'):
                boundary = dict(content_type_attrs)["boundary"]
                resp_iter = self.multipart_response_iter(
                    app_resp, boundary, body_key, crypto_meta)
            else:
                offset = 0
                content_range = self._response_header_value('Content-Range')
                if content_range:
                    # Determine offset within the whole object if ranged GET
                    offset, end, total = parse_content_range(content_range)
                resp_iter = self.response_iter(
                    app_resp, body_key, crypto_meta, offset)
        else:
            # don't decrypt body of unencrypted or non-2xx responses
            resp_iter = app_resp

        mod_resp_headers = purge_crypto_sysmeta_headers(mod_resp_headers)
        start_response(self._response_status, mod_resp_headers,
                       self._response_exc_info)

        return resp_iter

    def handle_head(self, req, start_response):
        app_resp = self._app_call(req.environ)

        keys = self.get_decryption_keys(req)

        if keys is None:
            # skip decryption
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
        else:
            mod_resp_headers = self.decrypt_resp_headers(keys)
            mod_resp_headers = purge_crypto_sysmeta_headers(mod_resp_headers)
            start_response(self._response_status, mod_resp_headers,
                           self._response_exc_info)

        return app_resp


class DecrypterContContext(BaseDecrypterContext):
    def __init__(self, decrypter, logger):
        super(DecrypterContContext, self).__init__(
            decrypter, 'container', logger)

    def handle_get(self, req, start_response):
        app_resp = self._app_call(req.environ)

        if is_success(self._get_status_int()):
            # only decrypt body of 2xx responses
            handler = keys = None
            for header, value in self._response_headers:
                if header.lower() == 'content-type' and \
                        value.split(';', 1)[0] == 'application/json':
                    handler = self.process_json_resp
                    keys = self.get_decryption_keys(req)

            if handler and keys:
                try:
                    app_resp = handler(keys['container'], app_resp)
                except EncryptionException as err:
                    self.logger.error(
                        _("Error decrypting container listing: %s"),
                        err)
                    raise HTTPInternalServerError(
                        body='Error decrypting container listing',
                        content_type='text/plain')

        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)

        return app_resp

    def process_json_resp(self, key, resp_iter):
        """
        Parses json body listing and decrypt encrypted entries. Updates
        Content-Length header with new body length and return a body iter.
        """
        with closing_if_possible(resp_iter):
            resp_body = ''.join(resp_iter)
        body_json = json.loads(resp_body)
        new_body = json.dumps([self.decrypt_obj_dict(obj_dict, key)
                               for obj_dict in body_json])
        self.update_content_length(len(new_body))
        return [new_body]

    def decrypt_obj_dict(self, obj_dict, key):
        if 'hash' in obj_dict:
            ciphertext = obj_dict['hash']
            obj_dict['hash'] = self.decrypt_value_with_meta(ciphertext, key)
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
        except ValueError:
            return self.app(env, start_response)

        if parts[3] and req.method == 'GET':
            handler = DecrypterObjContext(self, self.logger).handle_get
        elif parts[3] and req.method == 'HEAD':
            handler = DecrypterObjContext(self, self.logger).handle_head
        elif parts[2] and req.method == 'GET':
            handler = DecrypterContContext(self, self.logger).handle_get
        else:
            # url and/or request verb is not handled by decrypter
            return self.app(env, start_response)

        try:
            return handler(req, start_response)
        except HTTPException as err_resp:
            return err_resp(env, start_response)
