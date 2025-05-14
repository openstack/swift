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
import json
import os
import unittest

from unittest import mock

import urllib.parse
from swift.common.middleware.crypto import encrypter
from swift.common.middleware.crypto.crypto_utils import (
    CRYPTO_KEY_CALLBACK, Crypto)
from swift.common.swob import (
    Request, HTTPException, HTTPCreated, HTTPAccepted, HTTPOk, HTTPBadRequest,
    wsgi_to_bytes, bytes_to_wsgi)
from swift.common.utils import FileLikeIter, MD5_OF_EMPTY_STRING

from test.debug_logger import debug_logger
from test.unit.common.middleware.crypto.crypto_helpers import (
    fetch_crypto_keys, md5hex, FAKE_IV, encrypt)
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts


@mock.patch('swift.common.middleware.crypto.crypto_utils.Crypto.create_iv',
            lambda *args: FAKE_IV)
class TestEncrypter(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.encrypter = encrypter.Encrypter(self.app, {})
        self.encrypter.logger = debug_logger()

    def _verify_user_metadata(self, req_hdrs, name, value, key):
        # verify encrypted version of user metadata
        self.assertNotIn('X-Object-Meta-' + name, req_hdrs)
        expected_hdr = 'X-Object-Transient-Sysmeta-Crypto-Meta-' + name
        self.assertIn(expected_hdr, req_hdrs)
        enc_val, param = req_hdrs[expected_hdr].split(';')
        param = param.strip()
        self.assertTrue(param.startswith('swift_meta='))
        actual_meta = json.loads(
            urllib.parse.unquote_plus(param[len('swift_meta='):]))
        self.assertEqual(Crypto.cipher, actual_meta['cipher'])
        meta_iv = base64.b64decode(actual_meta['iv'])
        self.assertEqual(FAKE_IV, meta_iv)
        self.assertEqual(
            base64.b64encode(encrypt(wsgi_to_bytes(value), key, meta_iv)),
            wsgi_to_bytes(enc_val))
        # if there is any encrypted user metadata then this header should exist
        self.assertIn('X-Object-Transient-Sysmeta-Crypto-Meta', req_hdrs)
        common_meta = json.loads(urllib.parse.unquote_plus(
            req_hdrs['X-Object-Transient-Sysmeta-Crypto-Meta']))
        self.assertDictEqual({'cipher': Crypto.cipher,
                              'key_id': {'v': 'fake', 'path': '/a/c/fake'}},
                             common_meta)

    def test_PUT_req(self):
        body_key = os.urandom(32)
        object_key = fetch_crypto_keys()['object']
        plaintext = b'FAKE APP'
        plaintext_etag = md5hex(plaintext)
        ciphertext = encrypt(plaintext, body_key, FAKE_IV)
        ciphertext_etag = md5hex(ciphertext)

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'etag': plaintext_etag,
                'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'x-object-meta-etag': 'not to be confused with the Etag!',
                'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        with mock.patch(
            'swift.common.middleware.crypto.crypto_utils.'
            'Crypto.create_random_key',
                return_value=body_key):
            resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])

        # verify metadata items
        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual(('PUT', '/v1/a/c/o'), (call.method, call.path))

        # verify body crypto meta
        actual = call.footers['X-Object-Sysmeta-Crypto-Body-Meta']
        actual = json.loads(urllib.parse.unquote_plus(actual))
        self.assertEqual(Crypto().cipher, actual['cipher'])
        self.assertEqual(FAKE_IV, base64.b64decode(actual['iv']))

        # verify wrapped body key
        expected_wrapped_key = encrypt(body_key, object_key, FAKE_IV)
        self.assertEqual(expected_wrapped_key,
                         base64.b64decode(actual['body_key']['key']))
        self.assertEqual(FAKE_IV,
                         base64.b64decode(actual['body_key']['iv']))
        self.assertEqual(fetch_crypto_keys()['id'], actual['key_id'])

        # verify etag
        self.assertEqual(ciphertext_etag, call.footers['Etag'])

        encrypted_etag, _junk, etag_meta = \
            call.footers['X-Object-Sysmeta-Crypto-Etag'].partition(
                '; swift_meta=')
        # verify crypto_meta was appended to this etag
        self.assertTrue(etag_meta)
        actual_meta = json.loads(urllib.parse.unquote_plus(etag_meta))
        self.assertEqual(Crypto().cipher, actual_meta['cipher'])

        # verify encrypted version of plaintext etag
        actual = base64.b64decode(encrypted_etag)
        etag_iv = base64.b64decode(actual_meta['iv'])
        enc_etag = encrypt(plaintext_etag.encode('ascii'), object_key, etag_iv)
        self.assertEqual(enc_etag, actual)

        # verify etag MAC for conditional requests
        actual_hmac = base64.b64decode(
            call.footers['X-Object-Sysmeta-Crypto-Etag-Mac'])
        exp_hmac = hmac.new(
            object_key,
            plaintext_etag.encode('ascii'),
            hashlib.sha256).digest()
        self.assertEqual(actual_hmac, exp_hmac)

        # verify encrypted etag for container update
        self.assertIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', call.footers)
        parts = call.footers[
            'X-Object-Sysmeta-Container-Update-Override-Etag'].rsplit(';', 1)
        self.assertEqual(2, len(parts))

        # extract crypto_meta from end of etag for container update
        param = parts[1].strip()
        crypto_meta_tag = 'swift_meta='
        self.assertTrue(param.startswith(crypto_meta_tag), param)
        actual_meta = json.loads(
            urllib.parse.unquote_plus(param[len(crypto_meta_tag):]))
        self.assertEqual(Crypto().cipher, actual_meta['cipher'])
        self.assertEqual(fetch_crypto_keys()['id'], actual_meta['key_id'])

        cont_key = fetch_crypto_keys()['container']
        cont_etag_iv = base64.b64decode(actual_meta['iv'])
        self.assertEqual(FAKE_IV, cont_etag_iv)
        exp_etag = encrypt(plaintext_etag.encode('ascii'),
                           cont_key, cont_etag_iv)
        self.assertEqual(exp_etag, base64.b64decode(parts[0]))

        # content-type is not encrypted
        self.assertEqual('text/plain', call.headers['Content-Type'])

        # user meta is encrypted
        self._verify_user_metadata(
            call.headers, 'Test', 'encrypt me', object_key)
        self._verify_user_metadata(
            call.headers, 'Etag', 'not to be confused with the Etag!',
            object_key)

        # sysmeta is not encrypted
        self.assertEqual('do not encrypt me',
                         call.headers['X-Object-Sysmeta-Test'])

        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = get_req.get_response(self.app)
        self.assertEqual(ciphertext, resp.body)
        self.assertEqual(ciphertext_etag, resp.headers['Etag'])

    def test_PUT_zero_size_object(self):
        # object body encryption should be skipped for zero sized object body
        object_key = fetch_crypto_keys()['object']
        plaintext_etag = MD5_OF_EMPTY_STRING

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'etag': MD5_OF_EMPTY_STRING,
                'content-type': 'text/plain',
                'content-length': '0',
                'x-object-meta-etag': 'not to be confused with the Etag!',
                'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body='', headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})

        resp = req.get_response(self.encrypter)

        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual('PUT', call.method)

        # verify that there is no body crypto meta
        self.assertNotIn('X-Object-Sysmeta-Crypto-Meta', call.footers)
        # verify etag is md5 of plaintext
        self.assertEqual(MD5_OF_EMPTY_STRING, call.footers['Etag'])
        # verify there is no etag crypto meta
        self.assertNotIn('X-Object-Sysmeta-Crypto-Etag', call.footers)
        # verify there is no container update override for etag
        self.assertNotIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', call.footers)

        # user meta is still encrypted
        self._verify_user_metadata(
            call.headers, 'Test', 'encrypt me', object_key)
        self._verify_user_metadata(
            call.headers, 'Etag', 'not to be confused with the Etag!',
            object_key)

        # sysmeta is not encrypted
        self.assertEqual('do not encrypt me',
                         call.headers['X-Object-Sysmeta-Test'])

        # verify object is empty by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = get_req.get_response(self.app)
        self.assertEqual(b'', resp.body)
        self.assertEqual(MD5_OF_EMPTY_STRING, resp.headers['Etag'])

    def _test_PUT_with_other_footers(self, override_etag):
        # verify handling of another middleware's footer callback
        body_key = os.urandom(32)
        object_key = fetch_crypto_keys()['object']
        plaintext = b'FAKE APP'
        plaintext_etag = md5hex(plaintext)
        ciphertext = encrypt(plaintext, body_key, FAKE_IV)
        ciphertext_etag = md5hex(ciphertext)
        other_footers = {
            'Etag': plaintext_etag,
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Object-Sysmeta-Container-Update-Override-Size':
                'other override',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
                override_etag}

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'swift.callback.update_footers':
                   lambda footers: footers.update(other_footers)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'Etag': 'correct etag is in footers'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})

        with mock.patch(
            'swift.common.middleware.crypto.crypto_utils.'
            'Crypto.create_random_key',
                lambda *args: body_key):
            resp = req.get_response(self.encrypter)

        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])

        # verify metadata items
        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual(('PUT', '/v1/a/c/o'), (call.method, call.path))

        # verify that other middleware's footers made it to app, including any
        # container update overrides but nothing Etag-related
        other_footers.pop('Etag')
        other_footers.pop('X-Object-Sysmeta-Container-Update-Override-Etag')
        for k, v in other_footers.items():
            self.assertEqual(v, call.footers[k])

        # verify encryption footers are ok
        encrypted_etag, _junk, etag_meta = \
            call.footers['X-Object-Sysmeta-Crypto-Etag'].partition(
                '; swift_meta=')
        self.assertTrue(etag_meta)
        actual_meta = json.loads(urllib.parse.unquote_plus(etag_meta))
        self.assertEqual(Crypto().cipher, actual_meta['cipher'])

        self.assertEqual(ciphertext_etag, call.footers['Etag'])
        actual = base64.b64decode(encrypted_etag)
        etag_iv = base64.b64decode(actual_meta['iv'])
        exp_etag = encrypt(plaintext_etag.encode('ascii'), object_key, etag_iv)
        self.assertEqual(exp_etag, actual)

        # verify encrypted etag for container update
        self.assertIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', call.footers)
        parts = call.footers[
            'X-Object-Sysmeta-Container-Update-Override-Etag'].rsplit(';', 1)
        self.assertEqual(2, len(parts))

        # extract crypto_meta from end of etag for container update
        param = parts[1].strip()
        crypto_meta_tag = 'swift_meta='
        self.assertTrue(param.startswith(crypto_meta_tag), param)
        actual_meta = json.loads(
            urllib.parse.unquote_plus(param[len(crypto_meta_tag):]))
        self.assertEqual(Crypto().cipher, actual_meta['cipher'])

        cont_key = fetch_crypto_keys()['container']
        cont_etag_iv = base64.b64decode(actual_meta['iv'])
        self.assertEqual(FAKE_IV, cont_etag_iv)
        exp_etag = encrypt(override_etag.encode('ascii'),
                           cont_key, cont_etag_iv)
        self.assertEqual(exp_etag, base64.b64decode(parts[0]))

        # verify body crypto meta
        actual = call.footers['X-Object-Sysmeta-Crypto-Body-Meta']
        actual = json.loads(urllib.parse.unquote_plus(actual))
        self.assertEqual(Crypto().cipher, actual['cipher'])
        self.assertEqual(FAKE_IV, base64.b64decode(actual['iv']))

        # verify wrapped body key
        expected_wrapped_key = encrypt(body_key, object_key, FAKE_IV)
        self.assertEqual(expected_wrapped_key,
                         base64.b64decode(actual['body_key']['key']))
        self.assertEqual(FAKE_IV,
                         base64.b64decode(actual['body_key']['iv']))
        self.assertEqual(fetch_crypto_keys()['id'], actual['key_id'])

    def test_PUT_with_other_footers(self):
        self._test_PUT_with_other_footers('override etag')

    def test_PUT_with_other_footers_and_etag_of_empty_body(self):
        # verify that an override etag value of MD5_OF_EMPTY_STRING will be
        # encrypted when there was a non-zero body length
        self._test_PUT_with_other_footers(MD5_OF_EMPTY_STRING)

    def _test_PUT_with_etag_override_in_headers(self, override_etag):
        # verify handling of another middleware's
        # container-update-override-etag in headers
        plaintext = b'FAKE APP'
        plaintext_etag = md5hex(plaintext)

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'Etag': plaintext_etag,
                'X-Object-Sysmeta-Container-Update-Override-Etag':
                    override_etag}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)

        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])

        # verify metadata items
        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual(('PUT', '/v1/a/c/o'), (call.method, call.path))

        # verify encrypted etag for container update
        self.assertIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', call.footers)
        parts = call.footers[
            'X-Object-Sysmeta-Container-Update-Override-Etag'].rsplit(';', 1)
        self.assertEqual(2, len(parts))
        cont_key = fetch_crypto_keys()['container']

        # extract crypto_meta from end of etag for container update
        param = parts[1].strip()
        crypto_meta_tag = 'swift_meta='
        self.assertTrue(param.startswith(crypto_meta_tag), param)
        actual_meta = json.loads(
            urllib.parse.unquote_plus(param[len(crypto_meta_tag):]))
        self.assertEqual(Crypto().cipher, actual_meta['cipher'])
        self.assertEqual(fetch_crypto_keys()['id'], actual_meta['key_id'])

        cont_etag_iv = base64.b64decode(actual_meta['iv'])
        self.assertEqual(FAKE_IV, cont_etag_iv)
        exp_etag = encrypt(override_etag.encode('ascii'),
                           cont_key, cont_etag_iv)
        self.assertEqual(exp_etag, base64.b64decode(parts[0]))

    def test_PUT_with_etag_override_in_headers(self):
        self._test_PUT_with_etag_override_in_headers('override_etag')

    def test_PUT_with_etag_of_empty_body_override_in_headers(self):
        # verify that an override etag value of MD5_OF_EMPTY_STRING will be
        # encrypted when there was a non-zero body length
        self._test_PUT_with_etag_override_in_headers(MD5_OF_EMPTY_STRING)

    def _test_PUT_with_empty_etag_override_in_headers(self, plaintext):
        # verify that an override etag value of '' from other middleware is
        # passed through unencrypted
        plaintext_etag = md5hex(plaintext)
        override_etag = ''
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'Etag': plaintext_etag,
                'X-Object-Sysmeta-Container-Update-Override-Etag':
                    override_etag}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)

        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual(('PUT', '/v1/a/c/o'), (call.method, call.path))
        self.assertIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', call.headers)
        self.assertEqual(
            override_etag,
            call.headers['X-Object-Sysmeta-Container-Update-Override-Etag'])

    def test_PUT_with_empty_etag_override_in_headers(self):
        self._test_PUT_with_empty_etag_override_in_headers(b'body')

    def test_PUT_with_empty_etag_override_in_headers_no_body(self):
        self._test_PUT_with_empty_etag_override_in_headers(b'')

    def _test_PUT_with_empty_etag_override_in_footers(self, plaintext):
        # verify that an override etag value of '' from other middleware is
        # passed through unencrypted
        plaintext_etag = md5hex(plaintext)
        override_etag = ''
        other_footers = {
            'X-Object-Sysmeta-Container-Update-Override-Etag': override_etag}
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'swift.callback.update_footers':
                   lambda footers: footers.update(other_footers)}

        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'Etag': plaintext_etag}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)

        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])
        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual(('PUT', '/v1/a/c/o'), (call.method, call.path))
        self.assertIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', call.footers)
        self.assertEqual(
            override_etag,
            call.footers['X-Object-Sysmeta-Container-Update-Override-Etag'])

    def test_PUT_with_empty_etag_override_in_footers(self):
        self._test_PUT_with_empty_etag_override_in_footers(b'body')

    def test_PUT_with_empty_etag_override_in_footers_no_body(self):
        self._test_PUT_with_empty_etag_override_in_footers(b'')

    def test_PUT_with_bad_etag_in_other_footers(self):
        # verify that etag supplied in footers from other middleware overrides
        # header etag when validating inbound plaintext etags
        plaintext = b'FAKE APP'
        plaintext_etag = md5hex(plaintext)
        other_footers = {
            'Etag': 'bad etag',
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
                'other override'}

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'swift.callback.update_footers':
                   lambda footers: footers.update(other_footers)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'Etag': plaintext_etag}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('422 Unprocessable Entity', resp.status)
        self.assertNotIn('Etag', resp.headers)

    def test_PUT_with_bad_etag_in_headers_and_other_footers(self):
        # verify that etag supplied in headers from other middleware is used if
        # none is supplied in footers when validating inbound plaintext etags
        plaintext = 'FAKE APP'
        other_footers = {
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
                'other override'}

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'swift.callback.update_footers':
                   lambda footers: footers.update(other_footers)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'Etag': 'bad etag'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('422 Unprocessable Entity', resp.status)
        self.assertNotIn('Etag', resp.headers)

    def test_PUT_nothing_read(self):
        # simulate an artificial scenario of a downstream filter/app not
        # actually reading the input stream from encrypter.
        class NonReadingApp(object):
            def __call__(self, env, start_response):
                # note: no read from wsgi.input
                req = Request(env)
                env['swift.callback.update_footers'](req.headers)
                call_headers.append(req.headers)
                resp = HTTPCreated(req=req, headers={'Etag': 'response etag'})
                return resp(env, start_response)

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': 0,
                'etag': 'etag from client'}
        req = Request.blank('/v1/a/c/o', environ=env, body='', headers=hdrs)

        call_headers = []
        resp = req.get_response(encrypter.Encrypter(NonReadingApp(), {}))
        self.assertEqual('201 Created', resp.status)
        self.assertEqual('response etag', resp.headers['Etag'])
        self.assertEqual(1, len(call_headers))
        self.assertEqual('etag from client', call_headers[0]['etag'])
        # verify no encryption footers
        for k in call_headers[0]:
            self.assertFalse(k.lower().startswith('x-object-sysmeta-crypto-'))

        # check that an upstream footer callback gets called
        other_footers = {
            'Etag': MD5_OF_EMPTY_STRING,
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
                'other override'}
        env.update({'swift.callback.update_footers':
                    lambda footers: footers.update(other_footers)})
        req = Request.blank('/v1/a/c/o', environ=env, body='', headers=hdrs)

        call_headers = []
        resp = req.get_response(encrypter.Encrypter(NonReadingApp(), {}))

        self.assertEqual('201 Created', resp.status)
        self.assertEqual('response etag', resp.headers['Etag'])
        self.assertEqual(1, len(call_headers))

        # verify encrypted override etag for container update.
        self.assertIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', call_headers[0])
        parts = call_headers[0][
            'X-Object-Sysmeta-Container-Update-Override-Etag'].rsplit(';', 1)
        self.assertEqual(2, len(parts))
        cont_key = fetch_crypto_keys()['container']

        param = parts[1].strip()
        crypto_meta_tag = 'swift_meta='
        self.assertTrue(param.startswith(crypto_meta_tag), param)
        actual_meta = json.loads(
            urllib.parse.unquote_plus(param[len(crypto_meta_tag):]))
        self.assertEqual(Crypto().cipher, actual_meta['cipher'])
        self.assertEqual(fetch_crypto_keys()['id'], actual_meta['key_id'])

        cont_etag_iv = base64.b64decode(actual_meta['iv'])
        self.assertEqual(FAKE_IV, cont_etag_iv)
        self.assertEqual(encrypt(b'other override', cont_key, cont_etag_iv),
                         base64.b64decode(parts[0]))

        # verify that other middleware's footers made it to app
        other_footers.pop('X-Object-Sysmeta-Container-Update-Override-Etag')
        for k, v in other_footers.items():
            self.assertEqual(v, call_headers[0][k])
        # verify no encryption footers
        for k in call_headers[0]:
            self.assertFalse(k.lower().startswith('x-object-sysmeta-crypto-'))

        # if upstream footer override etag is for an empty body then check that
        # it is not encrypted
        other_footers = {
            'Etag': MD5_OF_EMPTY_STRING,
            'X-Object-Sysmeta-Container-Update-Override-Etag':
            MD5_OF_EMPTY_STRING}
        env.update({'swift.callback.update_footers':
                    lambda footers: footers.update(other_footers)})
        req = Request.blank('/v1/a/c/o', environ=env, body='', headers=hdrs)

        call_headers = []
        resp = req.get_response(encrypter.Encrypter(NonReadingApp(), {}))

        self.assertEqual('201 Created', resp.status)
        self.assertEqual('response etag', resp.headers['Etag'])
        self.assertEqual(1, len(call_headers))

        # verify that other middleware's footers made it to app
        for k, v in other_footers.items():
            self.assertEqual(v, call_headers[0][k])
        # verify no encryption footers
        for k in call_headers[0]:
            self.assertFalse(k.lower().startswith('x-object-sysmeta-crypto-'))

        # if upstream footer override etag is an empty string then check that
        # it is not encrypted
        other_footers = {
            'Etag': MD5_OF_EMPTY_STRING,
            'X-Object-Sysmeta-Container-Update-Override-Etag': ''}
        env.update({'swift.callback.update_footers':
                    lambda footers: footers.update(other_footers)})
        req = Request.blank('/v1/a/c/o', environ=env, body='', headers=hdrs)

        call_headers = []
        resp = req.get_response(encrypter.Encrypter(NonReadingApp(), {}))

        self.assertEqual('201 Created', resp.status)
        self.assertEqual('response etag', resp.headers['Etag'])
        self.assertEqual(1, len(call_headers))

        # verify that other middleware's footers made it to app
        for k, v in other_footers.items():
            self.assertEqual(v, call_headers[0][k])
        # verify no encryption footers
        for k in call_headers[0]:
            self.assertFalse(k.lower().startswith('x-object-sysmeta-crypto-'))

    def test_POST_req(self):
        body = b'FAKE APP'
        env = {'REQUEST_METHOD': 'POST',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'x-object-meta-test': 'encrypt me',
                'x-object-meta-test2': '',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank('/v1/a/c/o', environ=env, body=body, headers=hdrs)
        key = fetch_crypto_keys()['object']
        self.app.register('POST', '/v1/a/c/o', HTTPAccepted, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('202 Accepted', resp.status)
        self.assertNotIn('Etag', resp.headers)

        # verify metadata items
        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual(('POST', '/v1/a/c/o'), (call.method, call.path))

        # user meta is encrypted
        self._verify_user_metadata(call.headers, 'Test', 'encrypt me', key)
        # unless it had no value
        self.assertEqual('', call.headers['X-Object-Meta-Test2'])

        # sysmeta is not encrypted
        self.assertEqual('do not encrypt me',
                         call.headers['X-Object-Sysmeta-Test'])

    def _test_no_user_metadata(self, method):
        # verify that x-object-transient-sysmeta-crypto-meta is not set when
        # there is no user metadata
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env, body='body')
        self.app.register(method, '/v1/a/c/o', HTTPAccepted, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('202 Accepted', resp.status)
        self.assertEqual(1, len(self.app.calls), self.app.calls)
        self.assertEqual(method, self.app.calls[0][0])
        self.assertNotIn('x-object-transient-sysmeta-crypto-meta',
                         self.app.headers[0])

    def test_PUT_no_user_metadata(self):
        self._test_no_user_metadata('PUT')

    def test_POST_no_user_metadata(self):
        self._test_no_user_metadata('POST')

    def _test_if_match(self, method, match_header_name):
        def do_test(method, plain_etags, expected_plain_etags=None):
            env = {CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
            match_header_value = ', '.join(plain_etags)
            req = Request.blank(
                '/v1/a/c/o', environ=env, method=method,
                headers={match_header_name: match_header_value})
            app = FakeSwift()
            app.register(method, '/v1/a/c/o', HTTPOk, {})
            resp = req.get_response(encrypter.Encrypter(app, {}))
            self.assertEqual('200 OK', resp.status)

            self.assertEqual(1, len(app.call_list), app.calls)
            call = app.call_list[0]
            self.assertEqual(method, call.method)

            # verify the alternate etag location has been specified
            if match_header_value and match_header_value != '*':
                self.assertIn('X-Backend-Etag-Is-At', call.headers)
                self.assertEqual('X-Object-Sysmeta-Crypto-Etag-Mac',
                                 call.headers['X-Backend-Etag-Is-At'])

            # verify etags have been supplemented with masked values
            self.assertIn(match_header_name, call.headers)
            actual_etags = set(call.headers[match_header_name].split(', '))
            # masked values for secret_id None
            key = fetch_crypto_keys()['object']
            masked_etags = [
                '"%s"' % bytes_to_wsgi(base64.b64encode(hmac.new(
                    key, wsgi_to_bytes(etag.strip('"')),
                    hashlib.sha256).digest()))
                for etag in plain_etags if etag not in ('*', '')]
            # masked values for secret_id myid
            key = fetch_crypto_keys(key_id={'secret_id': 'myid'})['object']
            masked_etags_myid = [
                '"%s"' % bytes_to_wsgi(base64.b64encode(hmac.new(
                    key, wsgi_to_bytes(etag.strip('"')),
                    hashlib.sha256).digest()))
                for etag in plain_etags if etag not in ('*', '')]
            expected_etags = set((expected_plain_etags or plain_etags) +
                                 masked_etags + masked_etags_myid)
            self.assertEqual(expected_etags, actual_etags)
            # check that the request environ was returned to original state
            self.assertEqual(set(plain_etags),
                             set(req.headers[match_header_name].split(', ')))

        do_test(method, [''])
        do_test(method, ['"an etag"'])
        do_test(method, ['"an etag"', '"another_etag"'])
        do_test(method, ['*'])
        # rfc2616 does not allow wildcard *and* etag but test it anyway
        do_test(method, ['*', '"an etag"'])
        # etags should be quoted but check we can cope if they are not
        do_test(
            method, ['*', 'an etag', 'another_etag'],
            expected_plain_etags=['*', '"an etag"', '"another_etag"'])

    def test_GET_if_match(self):
        self._test_if_match('GET', 'If-Match')

    def test_HEAD_if_match(self):
        self._test_if_match('HEAD', 'If-Match')

    def test_GET_if_none_match(self):
        self._test_if_match('GET', 'If-None-Match')

    def test_HEAD_if_none_match(self):
        self._test_if_match('HEAD', 'If-None-Match')

    def _test_existing_etag_is_at_header(self, method, match_header_name):
        # if another middleware has already set X-Backend-Etag-Is-At then
        # encrypter should not override that value
        env = {CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank(
            '/v1/a/c/o', environ=env, method=method,
            headers={match_header_name: "an etag",
                     'X-Backend-Etag-Is-At': 'X-Object-Sysmeta-Other-Etag'})
        self.app.register(method, '/v1/a/c/o', HTTPOk, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('200 OK', resp.status)

        self.assertEqual(1, len(self.app.call_list), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual((method, '/v1/a/c/o'), (call.method, call.path))
        self.assertIn('X-Backend-Etag-Is-At', call.headers)
        self.assertEqual(
            'X-Object-Sysmeta-Other-Etag,X-Object-Sysmeta-Crypto-Etag-Mac',
            call.headers['X-Backend-Etag-Is-At'])
        actual_etags = set(call.headers[match_header_name].split(', '))
        self.assertIn('"an etag"', actual_etags)

    def test_GET_if_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('GET', 'If-Match')

    def test_HEAD_if_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('HEAD', 'If-Match')

    def test_GET_if_none_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('GET', 'If-None-Match')

    def test_HEAD_if_none_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('HEAD', 'If-None-Match')

    def _test_etag_is_at_not_duplicated(self, method):
        # verify only one occurrence of X-Object-Sysmeta-Crypto-Etag-Mac in
        # X-Backend-Etag-Is-At
        key = fetch_crypto_keys()['object']
        env = {CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank(
            '/v1/a/c/o', environ=env, method=method,
            headers={'If-Match': '"an etag"',
                     'If-None-Match': '"another etag"'})
        self.app.register(method, '/v1/a/c/o', HTTPOk, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('200 OK', resp.status)

        self.assertEqual(1, len(self.app.calls), self.app.calls)
        call = self.app.call_list[0]
        self.assertEqual((method, '/v1/a/c/o'), (call.method, call.path))
        self.assertIn('X-Backend-Etag-Is-At', call.headers)
        self.assertEqual('X-Object-Sysmeta-Crypto-Etag-Mac',
                         call.headers['X-Backend-Etag-Is-At'])

        self.assertIn('"%s"' % bytes_to_wsgi(base64.b64encode(
            hmac.new(key, b'an etag', hashlib.sha256).digest())),
            call.headers['If-Match'])
        self.assertIn('"another etag"', call.headers['If-None-Match'])
        self.assertIn('"%s"' % bytes_to_wsgi(base64.b64encode(
            hmac.new(key, b'another etag', hashlib.sha256).digest())),
            call.headers['If-None-Match'])

    def test_GET_etag_is_at_not_duplicated(self):
        self._test_etag_is_at_not_duplicated('GET')

    def test_HEAD_etag_is_at_not_duplicated(self):
        self._test_etag_is_at_not_duplicated('HEAD')

    def test_PUT_response_inconsistent_etag_is_not_replaced(self):
        # if response is success but etag does not match the ciphertext md5
        # then verify that we do *not* replace it with the plaintext etag
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank('/v1/a/c/o', environ=env, body=body, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated,
                          {'Etag': 'not the ciphertext etag'})
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual('not the ciphertext etag', resp.headers['Etag'])

    def test_PUT_multiseg_no_client_etag(self):
        body_key = os.urandom(32)
        chunks = [b'some', b'chunks', b'of data']
        body = b''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank('/v1/a/c/o', environ=env, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})

        with mock.patch(
            'swift.common.middleware.crypto.crypto_utils.'
            'Crypto.create_random_key',
                lambda *args: body_key):
            resp = req.get_response(self.encrypter)

        self.assertEqual('201 Created', resp.status)
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(encrypt(body, body_key, FAKE_IV),
                         get_req.get_response(self.app).body)

    def test_PUT_multiseg_good_client_etag(self):
        body_key = os.urandom(32)
        chunks = [b'some', b'chunks', b'of data']
        body = b''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': md5hex(body)}
        req = Request.blank('/v1/a/c/o', environ=env, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})

        with mock.patch(
            'swift.common.middleware.crypto.crypto_utils.'
            'Crypto.create_random_key',
                lambda *args: body_key):
            resp = req.get_response(self.encrypter)

        self.assertEqual('201 Created', resp.status)
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(encrypt(body, body_key, FAKE_IV),
                         get_req.get_response(self.app).body)

    def test_PUT_multiseg_bad_client_etag(self):
        chunks = [b'some', b'chunks', b'of data']
        body = b''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': 'badclientetag'}
        req = Request.blank('/v1/a/c/o', environ=env, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('422 Unprocessable Entity', resp.status)

    def test_PUT_missing_key_callback(self):
        body = b'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank('/v1/a/c/o', environ=env, body=body, headers=hdrs)
        resp = req.get_response(self.encrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn('missing callback',
                      self.encrypter.logger.get_lines_for_level('error')[0])
        self.assertEqual(b'Unable to retrieve encryption keys.', resp.body)

    def test_PUT_error_in_key_callback(self):
        def raise_exc(*args, **kwargs):
            raise Exception('Testing')

        body = b'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: raise_exc}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank('/v1/a/c/o', environ=env, body=body, headers=hdrs)
        resp = req.get_response(self.encrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn('from callback: Testing',
                      self.encrypter.logger.get_lines_for_level('error')[0])
        self.assertEqual(b'Unable to retrieve encryption keys.', resp.body)

    def test_PUT_encryption_override(self):
        # set crypto override to disable encryption.
        # simulate another middleware wanting to set footers
        other_footers = {
            'Etag': 'other etag',
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
                'other override'}
        body = b'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.override': True,
               'swift.callback.update_footers':
                   lambda footers: footers.update(other_footers)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank('/v1/a/c/o', environ=env, body=body, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)

        # verify that other middleware's footers made it to app
        self.assertEqual(1, len(self.app.call_list))
        call = self.app.call_list[0]
        for k, v in other_footers.items():
            self.assertEqual(v, call.footers[k])

        # verify object is NOT encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(body, get_req.get_response(self.app).body)

    def _test_constraints_checking(self, method):
        # verify that the check_metadata function is called on PUT and POST
        body = b'FAKE APP'
        env = {'REQUEST_METHOD': method,
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank('/v1/a/c/o', environ=env, body=body, headers=hdrs)
        mocked_func = 'swift.common.middleware.crypto.encrypter.check_metadata'
        with mock.patch(mocked_func) as mocked:
            mocked.side_effect = [HTTPBadRequest(b'testing')]
            resp = req.get_response(self.encrypter)
        self.assertEqual('400 Bad Request', resp.status)
        self.assertEqual(1, mocked.call_count)
        mocked.assert_called_once_with(mock.ANY, 'object')
        self.assertEqual(req.headers,
                         mocked.call_args_list[0][0][0].headers)

    def test_PUT_constraints_checking(self):
        self._test_constraints_checking('PUT')

    def test_POST_constraints_checking(self):
        self._test_constraints_checking('POST')

    def test_config_true_value_on_disable_encryption(self):
        app = FakeSwift()
        self.assertFalse(encrypter.Encrypter(app, {}).disable_encryption)
        for val in ('true', '1', 'yes', 'on', 't', 'y'):
            app = encrypter.Encrypter(app,
                                      {'disable_encryption': val})
            self.assertTrue(app.disable_encryption)

    def test_PUT_app_exception(self):
        app = encrypter.Encrypter(FakeAppThatExcepts(HTTPException), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        with self.assertRaises(HTTPException) as catcher:
            req.get_response(app)
        self.assertEqual(FakeAppThatExcepts.MESSAGE, catcher.exception.body)

    def test_encrypt_header_val(self):
        # Prepare key and Crypto instance
        object_key = fetch_crypto_keys()['object']

        # - Normal string can be crypted
        encrypted = encrypter.encrypt_header_val(Crypto(), 'aaa', object_key)
        # sanity: return value is 2 item tuple
        self.assertEqual(2, len(encrypted))
        crypted_val, crypt_info = encrypted
        expected_crypt_val = base64.b64encode(
            encrypt(b'aaa', object_key, FAKE_IV))
        expected_crypt_info = {
            'cipher': 'AES_CTR_256', 'iv': b'This is an IV123'}
        self.assertEqual(expected_crypt_val, wsgi_to_bytes(crypted_val))
        self.assertEqual(expected_crypt_info, crypt_info)

        # - Empty string raises a ValueError for safety
        with self.assertRaises(ValueError) as cm:
            encrypter.encrypt_header_val(Crypto(), '', object_key)

        self.assertEqual('empty value is not acceptable',
                         cm.exception.args[0])

        # - None also raises a ValueError for safety
        with self.assertRaises(ValueError) as cm:
            encrypter.encrypt_header_val(Crypto(), None, object_key)

        self.assertEqual('empty value is not acceptable',
                         cm.exception.args[0])


if __name__ == '__main__':
    unittest.main()
