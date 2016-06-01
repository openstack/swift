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
try:
    import xml.etree.cElementTree as ElementTree
except ImportError:
    import xml.etree.ElementTree as ElementTree

from swift.common.http import is_success
from swift.common.middleware.crypto_utils import CryptoWSGIContext, \
    load_crypto_meta, extract_crypto_meta, Crypto
from swift.common.exceptions import EncryptionException
from swift.common.request_helpers import strip_user_meta_prefix, is_user_meta,\
    get_object_transient_sysmeta, get_listing_content_type
from swift.common.swob import Request, HTTPException, HTTPInternalServerError
from swift.common.utils import get_logger, config_true_value, \
    parse_content_range, closing_if_possible, parse_content_type, \
    FileLikeIter, multipart_byteranges_to_document_iters

DECRYPT_CHUNK_SIZE = 65536


class BaseDecrypterContext(CryptoWSGIContext):
    def __init__(self, decrypter, server_type, logger):
        super(BaseDecrypterContext, self).__init__(
            decrypter, server_type, logger)

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
            err = 'Missing %s' % err
        except ValueError as err:
            pass
        msg = 'Error decrypting %s' % self.server_type
        self.logger.error('%s: %s', msg, err)
        raise HTTPInternalServerError(body=msg, content_type='text/plain')

    def decrypt_value_with_meta(self, value, key):
        """
        Decrypt a value if suitable crypto_meta can be extracted from the
        value itself.

        :param value: value to decrypt
        :param key: crypto key to use
        :returns: decrypted value if valid crypto_meta is found, otherwise the
            unmodified value
        :raises EncryptionException: if an error occurs while parsing
                                     crypto meta
        """
        value, crypto_meta = extract_crypto_meta(value)
        if crypto_meta:
            self.crypto.check_crypto_meta(crypto_meta)
            value = self.decrypt_value(value, key, crypto_meta)
        # it's not an error to have been passed an unencrypted value
        return value

    def decrypt_value(self, value, key, crypto_meta):
        """
        Decrypt a value using the crypto_meta provided.

        :param value: value to decrypt
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

    def process_resp(self, req):
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

    def _decrypt_header(self, header, value, key, crypto_meta_header,
                        required=False):
        """
        Attempt to decrypt a header that may be encrypted.

        :param header: the header name
        :param value: the header value
        :param key: decryption key
        :param crypto_meta_header: name of header that may have crypto meta
        :param required: if True then the header is required to be decrypted
                         and an HTTPInternalServerError will be raised if the
                         header cannot be decrypted due to missing crypto meta.
        :return: a decrypted header value or None if the header value was not
                 decrypted and was not required to be decrypted.
        :raises HTTPInternalServerError: if the header value was required to be
                                         decrypted but crypto meta was not
                                         found.
        """
        try:
            crypto_meta = self.get_crypto_meta(crypto_meta_header)
            if crypto_meta:
                # the corresponding value must have been encrypted/encoded
                return self.decrypt_value(value, key, crypto_meta)
            elif required:
                raise EncryptionException(
                    "Missing crypto meta from %s" % crypto_meta_header)
            else:
                # This is not an error - some user meta headers may not
                # be encrypted
                return None
        except EncryptionException as e:
            msg = "Error decrypting header"
            self.logger.error("%s %s: %s", msg, header, e)
            raise HTTPInternalServerError(body=msg, content_type='text/plain')

    def decrypt_user_metadata(self, keys):
        prefix = get_object_transient_sysmeta('crypto-meta-')
        result = []
        for name, val in self._response_headers:
            if is_user_meta(self.server_type, name) and val:
                short_name = strip_user_meta_prefix(self.server_type, name)
                decrypted_value = self._decrypt_header(
                    name, val, keys[self.server_type], prefix + short_name)
                if decrypted_value:
                    result.append((name, decrypted_value))
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
                etag_header, encrypted_etag, keys['container'],
                'X-Object-Sysmeta-Crypto-Meta-Etag', required=True)
            mod_hdr_pairs.append(('Etag', decrypted_etag))

        etag_header = 'X-Object-Sysmeta-Container-Update-Override-Etag'
        encrypted_etag = self._response_header_value(etag_header)
        if encrypted_etag:
            decrypted_etag = self.decrypt_value_with_meta(
                encrypted_etag, keys['container'])
            mod_hdr_pairs.append((etag_header, decrypted_etag))

        # Decrypt all user metadata
        mod_hdr_pairs.extend(self.decrypt_user_metadata(keys))

        mod_hdr_names = {h.lower() for h, v in mod_hdr_pairs}
        mod_resp_headers = [(h, v) for h, v in self._response_headers
                            if h.lower() not in mod_hdr_names]

        mod_resp_headers.extend(mod_hdr_pairs)

        return mod_resp_headers

    def multipart_response_iter(self, resp, boundary, body_key, crypto_meta):
        """
        Decrypts a multipart mime doc response body.

        :param resp: application response
        :param boundary: multipart boundary string
        :param keys: a dict of decryption keys.
        :param crypto_meta: crypto_meta for the response body
        :return: generator for decrypted response body
        """
        with closing_if_possible(resp):
            parts_iter = multipart_byteranges_to_document_iters(
                FileLikeIter(resp), boundary)
            for first_byte, last_byte, length, headers, body in parts_iter:
                yield "--" + boundary + "\r\n"

                for header in headers:
                    yield "%s: %s\r\n" % header

                yield "\r\n"

                decrypt_ctxt = self.crypto.create_decryption_ctxt(
                    body_key, crypto_meta['iv'], first_byte)
                for chunk in iter(lambda: body.read(DECRYPT_CHUNK_SIZE), ''):
                    chunk = decrypt_ctxt.update(chunk)
                    yield chunk

                yield "\r\n"

            yield "--" + boundary + "--"

    def response_iter(self, resp, body_key, crypto_meta, offset):
        """
        Decrypts a response body.

        :param resp: application response
        :param keys: a dict of decryption keys.
        :param crypto_meta: crypto_meta for the response body
        :param offset: offset into object content at which response body starts
        :return: generator for decrypted response body
        """
        decrypt_ctxt = self.crypto.create_decryption_ctxt(
            body_key, crypto_meta['iv'], offset)
        with closing_if_possible(resp):
            for chunk in resp:
                yield decrypt_ctxt.update(chunk)

    def GET(self, req, start_response):
        app_resp = self._app_call(req.environ)

        keys = self.process_resp(req)
        if keys is None:
            # skip decryption
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return app_resp

        # retrieve content-type before headers are modified
        ct = self._response_header_value('Content-Type')
        content_type, content_type_attrs = parse_content_type(ct)

        mod_resp_headers = self.decrypt_resp_headers(keys)

        if is_success(self._get_status_int()):
            try:
                crypto_meta = self.get_crypto_meta(
                    'X-Object-Sysmeta-Crypto-Meta')
            except EncryptionException as err:
                msg = 'Error decrypting object'
                self.logger.error('%s: %s', msg, err)
                raise HTTPInternalServerError(
                    body=msg, content_type='text/plain')

            if not crypto_meta:
                self.logger.debug("No sysmeta-crypto-meta for body.")
                resp_iter = app_resp
            elif (self._get_status_int() == 206 and
                  content_type == 'multipart/byteranges'):
                body_key = self.get_unwrapped_key(crypto_meta, keys['object'])
                boundary = dict(content_type_attrs)["boundary"]
                resp_iter = self.multipart_response_iter(
                    app_resp, boundary, body_key, crypto_meta)
            else:
                body_key = self.get_unwrapped_key(crypto_meta, keys['object'])
                offset = 0
                content_range = self._response_header_value('Content-Range')
                if content_range:
                    # Determine offset within the whole object if ranged GET
                    offset, end, total = parse_content_range(content_range)
                resp_iter = self.response_iter(
                    app_resp, body_key, crypto_meta, offset)
        else:
            # don't decrypt body of non-2xx responses
            resp_iter = app_resp

        start_response(self._response_status, mod_resp_headers,
                       self._response_exc_info)

        return resp_iter

    def HEAD(self, req, start_response):
        app_resp = self._app_call(req.environ)

        keys = self.process_resp(req)

        if keys is None:
            # skip decryption
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
        else:
            mod_resp_headers = self.decrypt_resp_headers(keys)
            start_response(self._response_status, mod_resp_headers,
                           self._response_exc_info)

        return app_resp


class DecrypterContContext(BaseDecrypterContext):
    def __init__(self, decrypter, logger):
        super(DecrypterContContext, self).__init__(
            decrypter, 'container', logger)

    def GET(self, req, start_response):
        app_resp = self._app_call(req.environ)

        if is_success(self._get_status_int()):
            # only decrypt body of 2xx responses
            keys = self.process_resp(req)
            if keys:
                key = keys['container']
                out_content_type = get_listing_content_type(req)
                try:
                    if out_content_type == 'application/json':
                        app_resp = self.process_json_resp(key, app_resp)
                    elif out_content_type.endswith('/xml'):
                        app_resp = self.process_xml_resp(key, app_resp)
                except EncryptionException as e:
                    msg = "Error decrypting container listing"
                    self.logger.error("%s: %s", msg, e)
                    raise HTTPInternalServerError(
                        body=msg, content_type='text/plain')

        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)

        return app_resp

    def update_content_length(self, new_total_len):
        self._response_headers = [
            (h, v) for h, v in self._response_headers
            if h.lower() != 'content-length']
        self._response_headers.append(('Content-Length', str(new_total_len)))

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
        ciphertext = obj_dict['hash']
        obj_dict['hash'] = self.decrypt_value_with_meta(ciphertext, key)

        # TODO - did we need to use the length to decide to decrypt?
        # if etag and (len(etag) > constraints.ETAG_LENGTH):
        return obj_dict

    def process_xml_resp(self, key, resp_iter):
        """
        Parses xml body listing and decrypt encrypted entries. Updates
        Content-Length header with new body length and return a body iter.
        """
        with closing_if_possible(resp_iter):
            resp_body = ''.join(resp_iter)
        tree = ElementTree.fromstring(resp_body)
        for elem in tree.iter('hash'):
            ciphertext = elem.text.encode('utf8')
            plain = self.decrypt_value_with_meta(ciphertext, key)
            elem.text = plain.decode('utf8')
        new_body = ElementTree.tostring(tree, encoding='UTF-8').replace(
            "<?xml version='1.0' encoding='UTF-8'?>",
            '<?xml version="1.0" encoding="UTF-8"?>', 1)
        self.update_content_length(len(new_body))
        return [new_body]


class Decrypter(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="decrypter")
        self.conf = conf
        self.crypto = Crypto(self.conf)

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            parts = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if parts[3] and hasattr(DecrypterObjContext, req.method):
            dec_context = DecrypterObjContext(self, self.logger)
        elif parts[2] and hasattr(DecrypterContContext, req.method):
            dec_context = DecrypterContContext(self, self.logger)
        else:
            # url and/or request verb is not handled by decrypter
            dec_context = None

        if dec_context:
            try:
                return getattr(dec_context, req.method)(req, start_response)
            except HTTPException as err_resp:
                return err_resp(env, start_response)

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def decrypter_filter(app):
        return Decrypter(app, conf)
    return decrypter_filter
