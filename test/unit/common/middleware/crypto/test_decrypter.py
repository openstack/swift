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
import os
import unittest

from unittest import mock

from swift.common.request_helpers import is_object_transient_sysmeta
from swift.common.utils import MD5_OF_EMPTY_STRING
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware.crypto import decrypter
from swift.common.middleware.crypto.crypto_utils import CRYPTO_KEY_CALLBACK, \
    dump_crypto_meta, Crypto, load_crypto_meta
from swift.common.swob import Request, HTTPException, HTTPOk, \
    HTTPPreconditionFailed, HTTPNotFound, HTTPPartialContent, bytes_to_wsgi

from test.debug_logger import debug_logger
from test.unit.common.middleware.crypto.crypto_helpers import md5hex, \
    fetch_crypto_keys, FAKE_IV, encrypt, fake_get_crypto_meta
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts


def get_crypto_meta_header(crypto_meta=None):
    if crypto_meta is None:
        crypto_meta = fake_get_crypto_meta()
    return dump_crypto_meta(crypto_meta)


def encrypt_and_append_meta(value, key, crypto_meta=None):
    if not isinstance(value, bytes):
        value = value.encode('ascii')
    return '%s; swift_meta=%s' % (
        base64.b64encode(encrypt(value, key, FAKE_IV)).decode('ascii'),
        get_crypto_meta_header(crypto_meta))


class TestDecrypterObjectRequests(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.decrypter = decrypter.Decrypter(self.app, {})
        self.decrypter.logger = debug_logger()

    def _make_response_headers(self, content_length, plaintext_etag, keys,
                               body_key, key_id=None):
        # helper method to make a typical set of response headers for a GET or
        # HEAD request
        cont_key = keys['container']
        object_key = keys['object']
        body_key_meta = {'key': encrypt(body_key, object_key, FAKE_IV),
                         'iv': FAKE_IV}
        body_crypto_meta = fake_get_crypto_meta(body_key=body_key_meta)
        other_crypto_meta = fake_get_crypto_meta()
        if key_id:
            body_crypto_meta['key_id'] = key_id
            other_crypto_meta['key_id'] = key_id
        return HeaderKeyDict({
            'Etag': 'hashOfCiphertext',
            'content-type': 'text/plain',
            'content-length': content_length,
            'X-Object-Sysmeta-Crypto-Etag': '%s; swift_meta=%s' % (
                bytes_to_wsgi(base64.b64encode(encrypt(
                    plaintext_etag.encode('ascii'), object_key, FAKE_IV))),
                get_crypto_meta_header(other_crypto_meta)),
            'X-Object-Sysmeta-Crypto-Body-Meta':
                get_crypto_meta_header(body_crypto_meta),
            'X-Object-Transient-Sysmeta-Crypto-Meta':
                get_crypto_meta_header(other_crypto_meta),
            'x-object-transient-sysmeta-crypto-meta-test':
                bytes_to_wsgi(base64.b64encode(encrypt(
                    b'encrypt me', object_key, FAKE_IV))) +
                ';swift_meta=' + get_crypto_meta_header(other_crypto_meta),
            'x-object-sysmeta-container-update-override-etag':
                encrypt_and_append_meta('encrypt me, too', cont_key),
            'x-object-sysmeta-test': 'do not encrypt me',
        })

    def _test_request_success(self, method, body, key_id=None):
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        plaintext_etag = md5hex(body)
        body_key = os.urandom(32)
        enc_body = encrypt(body, body_key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(enc_body), plaintext_etag, fetch_crypto_keys(key_id=key_id),
            body_key, key_id=key_id)
        if key_id:
            crypto_meta = load_crypto_meta(
                hdrs['X-Object-Sysmeta-Crypto-Body-Meta'])
            # sanity check that the test setup used provided key_id
            self.assertEqual(key_id, crypto_meta['key_id'])
        # there shouldn't be any x-object-meta- headers, but if there are
        # then the decrypted header will win where there is a name clash...
        hdrs.update({
            'x-object-meta-test': 'unexpected, overwritten by decrypted value',
            'x-object-meta-distinct': 'unexpected but distinct from encrypted'
        })
        self.app.register(
            method, '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        self.assertEqual('unexpected but distinct from encrypted',
                         resp.headers['x-object-meta-distinct'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])
        self.assertEqual(
            'encrypt me, too',
            resp.headers['X-Object-Sysmeta-Container-Update-Override-Etag'])
        self.assertNotIn('X-Object-Sysmeta-Crypto-Body-Meta', resp.headers)
        self.assertNotIn('X-Object-Sysmeta-Crypto-Etag', resp.headers)
        self.assertNotIn('Access-Control-Expose-Headers', resp.headers)
        return resp

    def test_GET_success(self):
        body = b'FAKE APP'
        resp = self._test_request_success('GET', body)
        self.assertEqual(body, resp.body)

        key_id_val = {'secret_id': 'myid'}
        resp = self._test_request_success('GET', body, key_id=key_id_val)
        self.assertEqual(body, resp.body)

        key_id_val = {'secret_id': ''}
        resp = self._test_request_success('GET', body, key_id=key_id_val)
        self.assertEqual(body, resp.body)

    def test_HEAD_success(self):
        body = b'FAKE APP'
        resp = self._test_request_success('HEAD', body)
        self.assertEqual(b'', resp.body)

        key_id_val = {'secret_id': 'myid'}
        resp = self._test_request_success('HEAD', body, key_id=key_id_val)
        self.assertEqual(b'', resp.body)

        key_id_val = {'secret_id': ''}
        resp = self._test_request_success('HEAD', body, key_id=key_id_val)
        self.assertEqual(b'', resp.body)

    def _check_different_keys_for_data_and_metadata(self, method):
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        data_key_id = {}
        metadata_key_id = {'secret_id': 'myid'}
        body = b'object data'
        plaintext_etag = md5hex(body)
        body_key = os.urandom(32)
        enc_body = encrypt(body, body_key, FAKE_IV)
        data_key = fetch_crypto_keys(data_key_id)
        metadata_key = fetch_crypto_keys(metadata_key_id)
        # synthesise response headers to mimic different key used for data PUT
        # vs metadata POST
        hdrs = self._make_response_headers(
            len(enc_body), plaintext_etag, data_key, body_key,
            key_id=data_key_id)
        metadata_hdrs = self._make_response_headers(
            len(enc_body), plaintext_etag, metadata_key, body_key,
            key_id=metadata_key_id)
        for k, v in metadata_hdrs.items():
            if is_object_transient_sysmeta(k):
                self.assertNotEqual(hdrs[k], v)  # sanity check
                hdrs[k] = v

        self.app.register(
            method, '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        self.assertEqual(
            'encrypt me, too',
            resp.headers['X-Object-Sysmeta-Container-Update-Override-Etag'])
        return resp

    def test_GET_different_keys_for_data_and_metadata(self):
        resp = self._check_different_keys_for_data_and_metadata('GET')
        self.assertEqual(b'object data', resp.body)

    def test_HEAD_different_keys_for_data_and_metadata(self):
        resp = self._check_different_keys_for_data_and_metadata('HEAD')
        self.assertEqual(b'', resp.body)

    def _check_unencrypted_data_and_encrypted_metadata(self, method):
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'object data'
        plaintext_etag = md5hex(body)
        metadata_key = fetch_crypto_keys()
        # synthesise headers for unencrypted PUT + headers for encrypted POST
        hdrs = HeaderKeyDict({
            'Etag': plaintext_etag,
            'content-type': 'text/plain',
            'content-length': len(body)})
        # we don't the data related headers but need a body key to keep the
        # helper function happy
        body_key = os.urandom(32)
        metadata_hdrs = self._make_response_headers(
            len(body), plaintext_etag, metadata_key, body_key)
        for k, v in metadata_hdrs.items():
            if is_object_transient_sysmeta(k):
                hdrs[k] = v

        self.app.register(
            method, '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        self.assertNotIn('Access-Control-Expose-Headers', resp.headers)
        return resp

    def test_GET_unencrypted_data_and_encrypted_metadata(self):
        resp = self._check_unencrypted_data_and_encrypted_metadata('GET')
        self.assertEqual(b'object data', resp.body)

    def test_HEAD_unencrypted_data_and_encrypted_metadata(self):
        resp = self._check_unencrypted_data_and_encrypted_metadata('HEAD')
        self.assertEqual(b'', resp.body)

    def _check_encrypted_data_and_unencrypted_metadata(self, method):
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'object data'
        plaintext_etag = md5hex(body)
        body_key = os.urandom(32)
        enc_body = encrypt(body, body_key, FAKE_IV)
        data_key = fetch_crypto_keys()
        hdrs = self._make_response_headers(
            len(enc_body), plaintext_etag, data_key, body_key)
        to_remove = [k for k in hdrs if is_object_transient_sysmeta(k)]
        for k in to_remove:
            hdrs.pop(k)
        hdrs['x-object-meta-test'] = 'unencrypted'

        self.app.register(
            method, '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('unencrypted', resp.headers['x-object-meta-test'])
        self.assertNotIn('Access-Control-Expose-Headers', resp.headers)
        return resp

    def test_GET_encrypted_data_and_unencrypted_metadata(self):
        resp = self._check_encrypted_data_and_unencrypted_metadata('GET')
        self.assertEqual(b'object data', resp.body)

    def test_HEAD_encrypted_data_and_unencrypted_metadata(self):
        resp = self._check_encrypted_data_and_unencrypted_metadata('HEAD')
        self.assertEqual(b'', resp.body)

    def test_headers_case(self):
        body = b'fAkE ApP'
        req = Request.blank('/v1/a/c/o', body='FaKe', headers={
            'Origin': 'http://example.com'})
        req.environ[CRYPTO_KEY_CALLBACK] = fetch_crypto_keys
        plaintext_etag = md5hex(body)
        body_key = os.urandom(32)
        enc_body = encrypt(body, body_key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(enc_body), plaintext_etag, fetch_crypto_keys(), body_key)

        hdrs.update({
            'x-Object-mEta-ignoRes-caSe': 'thIs pArt WilL bE cOol',
            'access-control-Expose-Headers': 'x-object-meta-ignores-case',
            'access-control-allow-origin': '*',
        })
        self.assertNotIn('x-object-meta-test', [k.lower() for k in hdrs])
        self.app.register(
            'GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)

        status, headers, app_iter = req.call_application(self.decrypter)
        self.assertEqual(status, '200 OK')
        expected = {
            'Etag': '7f7837924188f7b511a9e3881a9f77a8',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
            'encrypt me, too',
            'X-Object-Meta-Test': 'encrypt me',
            'Content-Length': '8',
            'X-Object-Meta-Ignores-Case': 'thIs pArt WilL bE cOol',
            'X-Object-Sysmeta-Test': 'do not encrypt me',
            'Content-Type': 'text/plain',
            'Access-Control-Expose-Headers': ', '.join([
                'x-object-meta-ignores-case',
                'x-object-meta-test',
            ]),
            'Access-Control-Allow-Origin': '*',
        }
        self.assertEqual(dict(headers), expected)
        self.assertEqual(b'fAkE ApP', b''.join(app_iter))

    def _test_412_response(self, method):
        # simulate a 412 response to a conditional GET which has an Etag header
        data = b'the object content'
        env = {CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env, method=method)
        resp_body = b'I am sorry, you have failed to meet a precondition'
        hdrs = self._make_response_headers(
            len(resp_body), md5hex(data), fetch_crypto_keys(), b'not used')
        self.app.register(method, '/v1/a/c/o', HTTPPreconditionFailed,
                          body=resp_body, headers=hdrs)
        resp = req.get_response(self.decrypter)

        self.assertEqual('412 Precondition Failed', resp.status)
        # the response body should not be decrypted, it is already plaintext
        self.assertEqual(resp_body if method == 'GET' else b'', resp.body)
        # whereas the Etag and other headers should be decrypted
        self.assertEqual(md5hex(data), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])

    def test_GET_412_response(self):
        self._test_412_response('GET')

    def test_HEAD_412_response(self):
        self._test_412_response('HEAD')

    def _test_404_response(self, method):
        # simulate a 404 response, sanity check response headers
        env = {CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env, method=method)
        resp_body = b'You still have not found what you are looking for'
        hdrs = {'content-type': 'text/plain',
                'content-length': len(resp_body)}
        self.app.register(method, '/v1/a/c/o', HTTPNotFound,
                          body=resp_body, headers=hdrs)
        resp = req.get_response(self.decrypter)

        self.assertEqual('404 Not Found', resp.status)
        # the response body should not be decrypted, it is already plaintext
        self.assertEqual(resp_body if method == 'GET' else b'', resp.body)
        # there should be no etag header inserted by decrypter
        self.assertNotIn('Etag', resp.headers)
        self.assertEqual('text/plain', resp.headers['Content-Type'])

    def test_GET_404_response(self):
        self._test_404_response('GET')

    def test_HEAD_404_response(self):
        self._test_404_response('HEAD')

    def test_GET_missing_etag_crypto_meta(self):
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), md5hex(body), fetch_crypto_keys(), b'not used')
        # simulate missing crypto meta from encrypted etag
        hdrs['X-Object-Sysmeta-Crypto-Etag'] = bytes_to_wsgi(base64.b64encode(
            encrypt(md5hex(body).encode('ascii'), key, FAKE_IV)))
        self.app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body,
                          headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn(b'Error decrypting header', resp.body)
        self.assertIn('Error decrypting header X-Object-Sysmeta-Crypto-Etag',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def _test_override_etag_bad_meta(self, method, bad_crypto_meta):
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), md5hex(body), fetch_crypto_keys(), b'not used')
        # simulate missing crypto meta from encrypted override etag
        hdrs['X-Object-Sysmeta-Container-Update-Override-Etag'] = \
            encrypt_and_append_meta(
                md5hex(body), key, crypto_meta=bad_crypto_meta)
        self.app.register(method, '/v1/a/c/o', HTTPOk, body=enc_body,
                          headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn('Error decrypting header '
                      'X-Object-Sysmeta-Container-Update-Override-Etag',
                      self.decrypter.logger.get_lines_for_level('error')[0])
        return resp

    def test_GET_override_etag_bad_iv(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['iv'] = b'bad_iv'
        resp = self._test_override_etag_bad_meta('GET', bad_crypto_meta)
        self.assertIn(b'Error decrypting header', resp.body)

    def test_HEAD_override_etag_bad_iv(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['iv'] = b'bad_iv'
        resp = self._test_override_etag_bad_meta('HEAD', bad_crypto_meta)
        self.assertEqual(b'', resp.body)

    def test_GET_override_etag_bad_cipher(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['cipher'] = 'unknown cipher'
        resp = self._test_override_etag_bad_meta('GET', bad_crypto_meta)
        self.assertIn(b'Error decrypting header', resp.body)

    def test_HEAD_override_etag_bad_cipher(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['cipher'] = 'unknown cipher'
        resp = self._test_override_etag_bad_meta('HEAD', bad_crypto_meta)
        self.assertEqual(b'', resp.body)

    def _test_bad_key(self, method):
        # use bad key
        def bad_fetch_crypto_keys(**kwargs):
            keys = fetch_crypto_keys()
            keys['object'] = b'bad key'
            return keys

        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: bad_fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), md5hex(body), fetch_crypto_keys(), b'not used')
        self.app.register(method, '/v1/a/c/o', HTTPOk, body=enc_body,
                          headers=hdrs)
        return req.get_response(self.decrypter)

    def test_HEAD_with_bad_key(self):
        resp = self._test_bad_key('HEAD')
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn("Bad key for 'object'",
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_bad_key(self):
        resp = self._test_bad_key('GET')
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual(b'Unable to retrieve encryption keys.',
                         resp.body)
        self.assertIn("Bad key for 'object'",
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def _test_bad_crypto_meta_for_user_metadata(self, method, bad_crypto_meta):
        # use bad iv for metadata headers
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), md5hex(body),
            fetch_crypto_keys(), b'not used')
        enc_val = base64.b64encode(encrypt(
            b'encrypt me', key, FAKE_IV)).decode('ascii')
        if bad_crypto_meta:
            enc_val += ';swift_meta=' + get_crypto_meta_header(
                crypto_meta=bad_crypto_meta)
        hdrs['x-object-transient-sysmeta-crypto-meta-test'] = enc_val
        self.app.register(method, '/v1/a/c/o', HTTPOk, body=enc_body,
                          headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn(
            'Error decrypting header X-Object-Transient-Sysmeta-Crypto-Meta-'
            'Test', self.decrypter.logger.get_lines_for_level('error')[0])
        return resp

    def test_HEAD_with_missing_crypto_meta_for_user_metadata(self):
        self._test_bad_crypto_meta_for_user_metadata('HEAD', None)
        self.assertIn('Missing crypto meta in value',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_missing_crypto_meta_for_user_metadata(self):
        self._test_bad_crypto_meta_for_user_metadata('GET', None)
        self.assertIn('Missing crypto meta in value',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_HEAD_with_bad_iv_for_user_metadata(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['iv'] = b'bad_iv'
        self._test_bad_crypto_meta_for_user_metadata('HEAD', bad_crypto_meta)
        self.assertIn('IV must be length 16',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_HEAD_with_missing_iv_for_user_metadata(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta.pop('iv')
        self._test_bad_crypto_meta_for_user_metadata('HEAD', bad_crypto_meta)
        self.assertIn(
            'iv', self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_bad_iv_for_user_metadata(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['iv'] = b'bad_iv'
        resp = self._test_bad_crypto_meta_for_user_metadata(
            'GET', bad_crypto_meta)
        self.assertEqual(b'Error decrypting header', resp.body)
        self.assertIn('IV must be length 16',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_missing_iv_for_user_metadata(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta.pop('iv')
        resp = self._test_bad_crypto_meta_for_user_metadata(
            'GET', bad_crypto_meta)
        self.assertEqual(b'Error decrypting header', resp.body)
        self.assertIn(
            'iv', self.decrypter.logger.get_lines_for_level('error')[0])

    def _test_GET_with_bad_crypto_meta_for_object_body(self, bad_crypto_meta):
        # use bad iv for object body
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), md5hex(body), fetch_crypto_keys(), b'not used')
        hdrs['X-Object-Sysmeta-Crypto-Body-Meta'] = \
            get_crypto_meta_header(crypto_meta=bad_crypto_meta)
        self.app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body,
                          headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual(b'Error decrypting object', resp.body)
        self.assertIn('Error decrypting object',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_bad_iv_for_object_body(self):
        bad_crypto_meta = fake_get_crypto_meta(key=os.urandom(32))
        bad_crypto_meta['iv'] = b'bad_iv'
        self._test_GET_with_bad_crypto_meta_for_object_body(bad_crypto_meta)
        self.assertIn('IV must be length 16',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_missing_iv_for_object_body(self):
        bad_crypto_meta = fake_get_crypto_meta(key=os.urandom(32))
        bad_crypto_meta.pop('iv')
        self._test_GET_with_bad_crypto_meta_for_object_body(bad_crypto_meta)
        self.assertIn("Missing 'iv'",
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_bad_body_key_for_object_body(self):
        body_key_meta = {'key': b'wrapped too short key', 'iv': FAKE_IV}
        bad_crypto_meta = fake_get_crypto_meta(body_key=body_key_meta)
        self._test_GET_with_bad_crypto_meta_for_object_body(bad_crypto_meta)
        self.assertIn('Key must be length 32',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_with_missing_body_key_for_object_body(self):
        bad_crypto_meta = fake_get_crypto_meta()  # no key by default
        self._test_GET_with_bad_crypto_meta_for_object_body(bad_crypto_meta)
        self.assertIn("Missing 'body_key'",
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def _test_req_metadata_not_encrypted(self, method):
        # check that metadata is not decrypted if it does not have crypto meta;
        # testing for case of an unencrypted POST to an object.
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        plaintext_etag = md5hex(body)
        body_key = os.urandom(32)
        enc_body = encrypt(body, body_key, FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), plaintext_etag, fetch_crypto_keys(), body_key)
        hdrs.pop('x-object-transient-sysmeta-crypto-meta-test')
        hdrs['x-object-meta-test'] = 'plaintext not encrypted'
        self.app.register(
            method, '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('plaintext not encrypted',
                         resp.headers['x-object-meta-test'])

    def test_HEAD_metadata_not_encrypted(self):
        self._test_req_metadata_not_encrypted('HEAD')

    def test_GET_metadata_not_encrypted(self):
        self._test_req_metadata_not_encrypted('GET')

    def test_GET_unencrypted_data(self):
        # testing case of an unencrypted object with encrypted metadata from
        # a later POST
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        obj_key = fetch_crypto_keys()['object']
        hdrs = {'Etag': md5hex(body),
                'content-type': 'text/plain',
                'content-length': len(body),
                'x-object-transient-sysmeta-crypto-meta-test':
                    bytes_to_wsgi(base64.b64encode(encrypt(
                        b'encrypt me', obj_key, FAKE_IV))) +
                    ';swift_meta=' + get_crypto_meta_header(),
                'x-object-sysmeta-test': 'do not encrypt me'}
        self.app.register('GET', '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual(body, resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        # POSTed user meta was encrypted
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        # PUT sysmeta was not encrypted
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])

    def test_GET_multiseg(self):
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        chunks = [b'some', b'chunks', b'of data']
        body = b''.join(chunks)
        plaintext_etag = md5hex(body)
        body_key = os.urandom(32)
        ctxt = Crypto().create_encryption_ctxt(body_key, FAKE_IV)
        enc_body = [encrypt(chunk, ctxt=ctxt) for chunk in chunks]
        hdrs = self._make_response_headers(
            sum(map(len, enc_body)), plaintext_etag, fetch_crypto_keys(),
            body_key)
        self.app.register(
            'GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual(body, resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])

    def test_GET_multiseg_with_range(self):
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        req.headers['Content-Range'] = 'bytes 3-10/17'
        chunks = [b'0123', b'45678', b'9abcdef']
        body = b''.join(chunks)
        plaintext_etag = md5hex(body)
        body_key = os.urandom(32)
        ctxt = Crypto().create_encryption_ctxt(body_key, FAKE_IV)
        enc_body = [encrypt(chunk, ctxt=ctxt) for chunk in chunks]
        enc_body = [enc_body[0][3:], enc_body[1], enc_body[2][:2]]
        hdrs = self._make_response_headers(
            sum(map(len, enc_body)), plaintext_etag, fetch_crypto_keys(),
            body_key)
        hdrs['content-range'] = req.headers['Content-Range']
        self.app.register(
            'GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual(b'3456789a', resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])

    # Force the decrypter context updates to be less than one of our range
    # sizes to check that the decrypt context offset is setup correctly with
    # offset to first byte of range for first update and then re-used.
    # Do mocking here to have the mocked value have effect in the generator
    # function.
    @mock.patch.object(decrypter, 'DECRYPT_CHUNK_SIZE', 4)
    def test_GET_multipart_ciphertext(self):
        # build fake multipart response body
        body_key = os.urandom(32)
        plaintext = b'Cwm fjord veg balks nth pyx quiz'
        plaintext_etag = md5hex(plaintext)
        ciphertext = encrypt(plaintext, body_key, FAKE_IV)
        parts = ((0, 3, 'text/plain'),
                 (4, 9, 'text/plain; charset=us-ascii'),
                 (24, 32, 'text/plain'))
        length = len(ciphertext)
        body = b''
        for start, end, ctype in parts:
            body += b'--multipartboundary\r\n'
            body += b'Content-Type: %s\r\n' % ctype.encode('utf-8')
            body += b'Content-Range: bytes %d-%d/%d' % (start, end - 1, length)
            body += b'\r\n\r\n' + ciphertext[start:end] + b'\r\n'
        body += b'--multipartboundary--'

        # register request with fake swift
        hdrs = self._make_response_headers(
            len(body), plaintext_etag, fetch_crypto_keys(), body_key)
        hdrs['content-type'] = \
            'multipart/byteranges;boundary=multipartboundary'
        self.app.register('GET', '/v1/a/c/o', HTTPPartialContent, body=body,
                          headers=hdrs)

        # issue request
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        resp = req.get_response(self.decrypter)

        self.assertEqual('206 Partial Content', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        self.assertEqual('multipart/byteranges;boundary=multipartboundary',
                         resp.headers['Content-Type'])

        # the multipart headers could be re-ordered, so parse response body to
        # verify expected content
        resp_lines = resp.body.split(b'\r\n')
        resp_lines.reverse()
        for start, end, ctype in parts:
            self.assertEqual(b'--multipartboundary', resp_lines.pop())
            expected_header_lines = {
                b'Content-Type: %s' % ctype.encode('utf8'),
                b'Content-Range: bytes %d-%d/%d' % (start, end - 1, length)}
            resp_header_lines = {resp_lines.pop(), resp_lines.pop()}
            self.assertEqual(expected_header_lines, resp_header_lines)
            self.assertEqual(b'', resp_lines.pop())
            self.assertEqual(plaintext[start:end], resp_lines.pop())
        self.assertEqual(b'--multipartboundary--', resp_lines.pop())

        # we should have consumed the whole response body
        self.assertFalse(resp_lines)

    def test_GET_multipart_content_type(self):
        # *just* having multipart content type shouldn't trigger the mime doc
        # code path
        body_key = os.urandom(32)
        plaintext = b'Cwm fjord veg balks nth pyx quiz'
        plaintext_etag = md5hex(plaintext)
        ciphertext = encrypt(plaintext, body_key, FAKE_IV)

        # register request with fake swift
        hdrs = self._make_response_headers(
            len(ciphertext), plaintext_etag, fetch_crypto_keys(), body_key)
        hdrs['content-type'] = \
            'multipart/byteranges;boundary=multipartboundary'
        self.app.register('GET', '/v1/a/c/o', HTTPOk, body=ciphertext,
                          headers=hdrs)

        # issue request
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        resp = req.get_response(self.decrypter)

        self.assertEqual('200 OK', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual(len(plaintext), int(resp.headers['Content-Length']))
        self.assertEqual('multipart/byteranges;boundary=multipartboundary',
                         resp.headers['Content-Type'])
        self.assertEqual(plaintext, resp.body)

    def test_GET_multipart_no_body_crypto_meta(self):
        # build fake multipart response body
        plaintext = b'Cwm fjord veg balks nth pyx quiz'
        plaintext_etag = md5hex(plaintext)
        parts = ((0, 3, 'text/plain'),
                 (4, 9, 'text/plain; charset=us-ascii'),
                 (24, 32, 'text/plain'))
        length = len(plaintext)
        body = b''
        for start, end, ctype in parts:
            body += b'--multipartboundary\r\n'
            body += b'Content-Type: %s\r\n' % ctype.encode('utf-8')
            body += b'Content-Range: bytes %d-%d/%d' % (start, end - 1, length)
            body += b'\r\n\r\n' + plaintext[start:end] + b'\r\n'
        body += b'--multipartboundary--'

        # register request with fake swift
        hdrs = {
            'Etag': plaintext_etag,
            'content-type': 'multipart/byteranges;boundary=multipartboundary',
            'content-length': len(body)}
        self.app.register('GET', '/v1/a/c/o', HTTPPartialContent, body=body,
                          headers=hdrs)

        # issue request
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        resp = req.get_response(self.decrypter)

        self.assertEqual('206 Partial Content', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        self.assertEqual('multipart/byteranges;boundary=multipartboundary',
                         resp.headers['Content-Type'])

        # the multipart response body should be unchanged
        self.assertEqual(body, resp.body)

    def _test_GET_multipart_bad_body_crypto_meta(self, bad_crypto_meta):
        # build fake multipart response body
        key = fetch_crypto_keys()['object']
        ctxt = Crypto().create_encryption_ctxt(key, FAKE_IV)
        plaintext = b'Cwm fjord veg balks nth pyx quiz'
        plaintext_etag = md5hex(plaintext)
        ciphertext = encrypt(plaintext, ctxt=ctxt)
        parts = ((0, 3, 'text/plain'),
                 (4, 9, 'text/plain; charset=us-ascii'),
                 (24, 32, 'text/plain'))
        length = len(ciphertext)
        body = b''
        for start, end, ctype in parts:
            body += b'--multipartboundary\r\n'
            body += b'Content-Type: %s\r\n' % ctype.encode('utf-8')
            body += b'Content-Range: bytes %d-%d/%d' % (start, end - 1, length)
            body += b'\r\n\r\n' + ciphertext[start:end] + b'\r\n'
        body += b'--multipartboundary--'

        # register request with fake swift
        hdrs = self._make_response_headers(
            len(body), plaintext_etag, fetch_crypto_keys(), b'not used')
        hdrs['content-type'] = \
            'multipart/byteranges;boundary=multipartboundary'
        hdrs['X-Object-Sysmeta-Crypto-Body-Meta'] = \
            get_crypto_meta_header(bad_crypto_meta)
        self.app.register('GET', '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)

        # issue request
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        resp = req.get_response(self.decrypter)

        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual(b'Error decrypting object', resp.body)
        self.assertIn('Error decrypting object',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_multipart_bad_body_cipher(self):
        self._test_GET_multipart_bad_body_crypto_meta(
            {'cipher': 'Mystery cipher', 'iv': b'1234567887654321'})
        self.assertIn('Cipher must be AES_CTR_256',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_multipart_missing_body_cipher(self):
        self._test_GET_multipart_bad_body_crypto_meta(
            {'iv': b'1234567887654321'})
        self.assertIn('cipher',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_multipart_too_short_body_iv(self):
        self._test_GET_multipart_bad_body_crypto_meta(
            {'cipher': 'AES_CTR_256', 'iv': b'too short'})
        self.assertIn('IV must be length 16',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_multipart_too_long_body_iv(self):
        self._test_GET_multipart_bad_body_crypto_meta(
            {'cipher': 'AES_CTR_256', 'iv': b'a little too long'})
        self.assertIn('IV must be length 16',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_multipart_missing_body_iv(self):
        self._test_GET_multipart_bad_body_crypto_meta(
            {'cipher': 'AES_CTR_256'})
        self.assertIn('iv',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_missing_key_callback(self):
        # Do not provide keys, and do not set override flag
        env = {'REQUEST_METHOD': 'GET'}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        enc_body = encrypt(body, fetch_crypto_keys()['object'], FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), md5hex(b'not the body'),
            fetch_crypto_keys(), b'not used')
        self.app.register(
            'GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual(b'Unable to retrieve encryption keys.',
                         resp.body)
        self.assertIn('missing callback',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_error_in_key_callback(self):
        def raise_exc(**kwargs):
            raise Exception('Testing')

        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: raise_exc}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        enc_body = encrypt(body, fetch_crypto_keys()['object'], FAKE_IV)
        hdrs = self._make_response_headers(
            len(body), md5hex(body), fetch_crypto_keys(), b'not used')
        self.app.register(
            'GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual(b'Unable to retrieve encryption keys.',
                         resp.body)
        self.assertIn('from callback: Testing',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_cipher_mismatch_for_body(self):
        # Cipher does not match
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        enc_body = encrypt(body, fetch_crypto_keys()['object'], FAKE_IV)
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['cipher'] = 'unknown_cipher'
        hdrs = self._make_response_headers(
            len(enc_body), md5hex(body), fetch_crypto_keys(), b'not used')
        hdrs['X-Object-Sysmeta-Crypto-Body-Meta'] = \
            get_crypto_meta_header(crypto_meta=bad_crypto_meta)
        self.app.register(
            'GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual(b'Error decrypting object', resp.body)
        self.assertIn('Error decrypting object',
                      self.decrypter.logger.get_lines_for_level('error')[0])
        self.assertIn('Bad crypto meta: Cipher',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_cipher_mismatch_for_metadata(self):
        # Cipher does not match
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, FAKE_IV)
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['cipher'] = 'unknown_cipher'
        hdrs = self._make_response_headers(
            len(enc_body), md5hex(body), fetch_crypto_keys(), b'not used')
        enc_val = bytes_to_wsgi(base64.b64encode(
            encrypt(b'encrypt me', key, FAKE_IV)))

        hdrs.update({'x-object-transient-sysmeta-crypto-meta-test':
                     enc_val + ';swift_meta=' +
                     get_crypto_meta_header(crypto_meta=bad_crypto_meta)})
        self.app.register(
            'GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual(b'Error decrypting header', resp.body)
        self.assertIn(
            'Error decrypting header X-Object-Transient-Sysmeta-Crypto-Meta-'
            'Test', self.decrypter.logger.get_lines_for_level('error')[0])

    def test_GET_decryption_override(self):
        # This covers the case of an old un-encrypted object
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'swift.crypto.override': True}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = b'FAKE APP'
        hdrs = {'Etag': md5hex(body),
                'content-type': 'text/plain',
                'content-length': len(body),
                'x-object-meta-test': 'do not encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        self.app.register('GET', '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)
        resp = req.get_response(self.decrypter)
        self.assertEqual(body, resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-meta-test'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])


class TestDecrypterContainerRequests(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.decrypter = decrypter.Decrypter(self.app, {})
        self.decrypter.logger = debug_logger()

    def _make_cont_get_req(self, resp_body, format, override=False,
                           callback=fetch_crypto_keys):
        path = '/v1/a/c'
        content_type = 'text/plain'
        if format:
            path = '%s/?format=%s' % (path, format)
            content_type = 'application/' + format
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: callback}
        if override:
            env['swift.crypto.override'] = True
        req = Request.blank(path, environ=env)
        hdrs = {'content-type': content_type}
        self.app.register('GET', path, HTTPOk, body=resp_body, headers=hdrs)
        return req.get_response(self.decrypter)

    def test_GET_container_success(self):
        # no format requested, listing has names only
        fake_body = b'testfile1\ntestfile2\n'
        calls = [0]

        def wrapped_fetch_crypto_keys():
            calls[0] += 1
            return fetch_crypto_keys()

        resp = self._make_cont_get_req(fake_body, None,
                                       callback=wrapped_fetch_crypto_keys)

        self.assertEqual('200 OK', resp.status)
        self.assertEqual(resp.body.split(b'\n'), [
            b'testfile1',
            b'testfile2',
            b'',
        ])
        self.assertEqual(0, calls[0])

    def test_GET_container_json(self):
        content_type_1 = u'\uF10F\uD20D\uB30B\u9409'
        content_type_2 = 'text/plain; param=foo'
        pt_etag1 = 'c6e8196d7f0fff6444b90861fe8d609d'
        pt_etag2 = 'ac0374ed4d43635f803c82469d0b5a10'
        key = fetch_crypto_keys()['container']

        subdir = {"subdir": "pseudo-dir/"}

        obj_dict_1 = {"bytes": 16,
                      "last_modified": "2015-04-14T23:33:06.439040",
                      "hash": encrypt_and_append_meta(
                          pt_etag1.encode('utf-8'), key),
                      "name": "testfile",
                      "content_type": content_type_1}

        obj_dict_2 = {"bytes": 24,
                      "last_modified": "2015-04-14T23:33:06.519020",
                      "hash": encrypt_and_append_meta(
                          pt_etag2.encode('utf-8'), key),
                      "name": "testfile2",
                      "content_type": content_type_2}

        listing = [subdir, obj_dict_1, obj_dict_2]
        fake_body = json.dumps(listing).encode('ascii')

        resp = self._make_cont_get_req(fake_body, 'json')

        self.assertEqual('200 OK', resp.status)
        body = resp.body
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        body_json = json.loads(body)
        self.assertEqual(3, len(body_json))
        self.assertDictEqual(subdir, body_json[0])
        obj_dict_1['hash'] = pt_etag1
        self.assertDictEqual(obj_dict_1, body_json[1])
        obj_dict_2['hash'] = pt_etag2
        self.assertDictEqual(obj_dict_2, body_json[2])

    def test_GET_container_json_with_crypto_override(self):
        content_type_1 = 'image/jpeg'
        content_type_2 = 'text/plain; param=foo'
        pt_etag1 = 'c6e8196d7f0fff6444b90861fe8d609d'
        pt_etag2 = 'ac0374ed4d43635f803c82469d0b5a10'

        obj_dict_1 = {"bytes": 16,
                      "last_modified": "2015-04-14T23:33:06.439040",
                      "hash": pt_etag1,
                      "name": "testfile",
                      "content_type": content_type_1}

        obj_dict_2 = {"bytes": 24,
                      "last_modified": "2015-04-14T23:33:06.519020",
                      "hash": pt_etag2,
                      "name": "testfile2",
                      "content_type": content_type_2}

        listing = [obj_dict_1, obj_dict_2]
        fake_body = json.dumps(listing).encode('ascii')

        resp = self._make_cont_get_req(fake_body, 'json', override=True)

        self.assertEqual('200 OK', resp.status)
        body = resp.body
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        body_json = json.loads(body)
        self.assertEqual(2, len(body_json))
        self.assertDictEqual(obj_dict_1, body_json[0])
        self.assertDictEqual(obj_dict_2, body_json[1])

    def test_cont_get_json_req_with_cipher_mismatch(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['cipher'] = 'unknown_cipher'
        key = fetch_crypto_keys()['container']
        pt_etag = 'c6e8196d7f0fff6444b90861fe8d609d'
        ct_etag = encrypt_and_append_meta(pt_etag, key,
                                          crypto_meta=bad_crypto_meta)

        obj_dict_1 = {"bytes": 16,
                      "last_modified": "2015-04-14T23:33:06.439040",
                      "hash": ct_etag,
                      "name": "testfile",
                      "content_type": "image/jpeg"}

        listing = [obj_dict_1]
        fake_body = json.dumps(listing).encode('ascii')

        resp = self._make_cont_get_req(fake_body, 'json')

        self.assertEqual('200 OK', resp.status)
        self.assertEqual(
            ['<unknown>'],
            [x['hash'] for x in json.loads(resp.body)])
        self.assertIn("Cipher must be AES_CTR_256",
                      self.decrypter.logger.get_lines_for_level('error')[0])
        self.assertIn('Error decrypting container listing',
                      self.decrypter.logger.get_lines_for_level('error')[0])

    def test_cont_get_json_req_with_unknown_secret_id(self):
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['key_id'] = {'secret_id': 'unknown_key'}
        key = fetch_crypto_keys()['container']
        pt_etag = 'c6e8196d7f0fff6444b90861fe8d609d'
        ct_etag = encrypt_and_append_meta(pt_etag, key,
                                          crypto_meta=bad_crypto_meta)

        obj_dict_1 = {"bytes": 16,
                      "last_modified": "2015-04-14T23:33:06.439040",
                      "hash": ct_etag,
                      "name": "testfile",
                      "content_type": "image/jpeg"}

        listing = [obj_dict_1]
        fake_body = json.dumps(listing).encode('ascii')

        resp = self._make_cont_get_req(fake_body, 'json')

        self.assertEqual('200 OK', resp.status)
        self.assertEqual(
            ['<unknown>'],
            [x['hash'] for x in json.loads(resp.body)])
        self.assertEqual(self.decrypter.logger.get_lines_for_level('error'), [
            'get_keys(): unknown key id: unknown_key',
            'Error decrypting container listing: unknown_key',
        ])

    def test_GET_container_json_not_encrypted_obj(self):
        pt_etag = '%s; symlink_path=/a/c/o' % MD5_OF_EMPTY_STRING

        obj_dict = {"bytes": 0,
                    "last_modified": "2015-04-14T23:33:06.439040",
                    "hash": pt_etag,
                    "name": "symlink",
                    "content_type": 'application/symlink'}

        listing = [obj_dict]
        fake_body = json.dumps(listing).encode('ascii')

        resp = self._make_cont_get_req(fake_body, 'json')

        self.assertEqual('200 OK', resp.status)
        body = resp.body
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        body_json = json.loads(body)
        self.assertEqual(1, len(body_json))
        self.assertEqual(pt_etag, body_json[0]['hash'])


class TestModuleMethods(unittest.TestCase):
    def test_purge_crypto_sysmeta_headers(self):
        retained_headers = {'x-object-sysmeta-test1': 'keep',
                            'x-object-meta-test2': 'retain',
                            'x-object-transient-sysmeta-test3': 'leave intact',
                            'etag': 'hold onto',
                            'x-other': 'cherish',
                            'x-object-not-meta': 'do not remove'}
        purged_headers = {'x-object-sysmeta-crypto-test1': 'remove',
                          'x-object-transient-sysmeta-crypto-test2': 'purge'}
        test_headers = retained_headers.copy()
        test_headers.update(purged_headers)
        actual = decrypter.purge_crypto_sysmeta_headers(test_headers.items())

        for k, v in actual:
            k = k.lower()
            self.assertNotIn(k, purged_headers)
            self.assertEqual(retained_headers[k], v)
            retained_headers.pop(k)
        self.assertFalse(retained_headers)


class TestDecrypter(unittest.TestCase):
    def test_app_exception(self):
        app = decrypter.Decrypter(FakeAppThatExcepts(HTTPException), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        with self.assertRaises(HTTPException) as catcher:
            req.get_response(app)
        self.assertEqual(FakeAppThatExcepts.MESSAGE, catcher.exception.body)


if __name__ == '__main__':
    unittest.main()
