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
import urllib

import unittest
import mock

from swift.common.middleware import encrypter
from swift.common.swob import (
    Request, HTTPException, HTTPCreated, HTTPAccepted, HTTPOk)
from swift.common.utils import FileLikeIter
from swift.common.crypto_utils import CRYPTO_KEY_CALLBACK
from swift.common.middleware.crypto import Crypto
from test.unit import FakeLogger

from test.unit.common.middleware.crypto_helpers import fetch_crypto_keys, \
    md5hex, FAKE_IV, encrypt
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts


@mock.patch('swift.common.middleware.crypto.Crypto._get_random_iv',
            lambda *args: FAKE_IV)
class TestEncrypter(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.encrypter = encrypter.Encrypter(self.app, {})
        self.encrypter.logger = FakeLogger()

    def test_PUT_req(self):
        key = fetch_crypto_keys()['object']
        plaintext = 'FAKE APP'
        plaintext_etag = md5hex(plaintext)
        ciphertext = encrypt(plaintext, key, FAKE_IV)
        ciphertext_etag = md5hex(ciphertext)

        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(plaintext)),
                'x-object-meta-etag': 'not to be confused with the Etag!',
                'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=plaintext, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])

        # verify metadata items
        self.assertEqual(1, len(self.app.calls), self.app.calls)
        self.assertEqual('PUT', self.app.calls[0][0])
        req_hdrs = self.app.headers[0]

        self.assertEqual(ciphertext_etag, req_hdrs['Etag'])

        # verify encrypted version of plaintext etag
        actual = base64.b64decode(req_hdrs['X-Object-Sysmeta-Crypto-Etag'])
        etag_iv = Crypto().create_iv(iv_base=req.path)
        enc_etag = encrypt(plaintext_etag, key, etag_iv)
        self.assertEqual(enc_etag, actual)
        # verify crypto_meta was not appended to this etag
        parts = req_hdrs['X-Object-Sysmeta-Crypto-Etag'].rsplit(';', 1)
        self.assertEqual(len(parts), 1)

        actual = json.loads(urllib.unquote_plus(
            req_hdrs['X-Object-Sysmeta-Crypto-Meta-Etag']))
        self.assertEqual(Crypto().get_cipher(), actual['cipher'])
        self.assertEqual(etag_iv, base64.b64decode(actual['iv']))

        # verify encrypted etag for container update
        self.assertIn(
            'X-Object-Sysmeta-Container-Update-Override-Etag', req_hdrs)
        parts = req_hdrs[
            'X-Object-Sysmeta-Container-Update-Override-Etag'].rsplit(';', 1)
        self.assertEqual(2, len(parts))
        cont_key = fetch_crypto_keys()['container']
        actual = base64.b64decode(parts[0])
        self.assertEqual(encrypt(plaintext_etag, cont_key, FAKE_IV), actual)

        # extract crypto_meta from end of etag for container update
        param = parts[1].strip()
        self.assertTrue(param.startswith('meta='), param)
        actual = json.loads(urllib.unquote_plus(param[5:]))
        self.assertEqual(Crypto().get_cipher(), actual['cipher'])
        self.assertEqual(FAKE_IV, base64.b64decode(actual['iv']))

        # content-type is not encrypted
        self.assertEqual('text/plain', req_hdrs['Content-Type'])

        # user meta is encrypted
        self.assertEqual(base64.b64encode(encrypt('encrypt me', key, FAKE_IV)),
                         req_hdrs['X-Object-Meta-Test'])
        actual = req_hdrs['X-Object-Transient-Sysmeta-Crypto-Meta-Test']
        actual = json.loads(urllib.unquote_plus(actual))
        self.assertEqual(Crypto().get_cipher(), actual['cipher'])
        self.assertEqual(FAKE_IV, base64.b64decode(actual['iv']))
        self.assertEqual(
            base64.b64encode(encrypt('not to be confused with the Etag!',
                                     key, FAKE_IV)),
            req_hdrs['X-Object-Meta-Etag'])
        actual = req_hdrs['X-Object-Transient-Sysmeta-Crypto-Meta-Etag']
        actual = json.loads(urllib.unquote_plus(actual))
        self.assertEqual(Crypto().get_cipher(), actual['cipher'])
        self.assertEqual(FAKE_IV, base64.b64decode(actual['iv']))

        # sysmeta is not encrypted
        self.assertEqual('do not encrypt me',
                         req_hdrs['X-Object-Sysmeta-Test'])

        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = get_req.get_response(self.app)
        self.assertEqual(ciphertext, resp.body)
        self.assertEqual(ciphertext_etag, resp.headers['Etag'])

    def test_PUT_with_other_footers(self):
        # verify handling of another middleware's footer callback
        key = fetch_crypto_keys()['object']
        plaintext = 'FAKE APP'
        plaintext_etag = md5hex(plaintext)
        ciphertext = encrypt(plaintext, key, FAKE_IV)
        ciphertext_etag = md5hex(ciphertext)
        other_footers = {
            'Etag': plaintext_etag,
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
                'other override'}

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
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(plaintext_etag, resp.headers['Etag'])

        # verify metadata items
        self.assertEqual(1, len(self.app.calls), self.app.calls)
        self.assertEqual('PUT', self.app.calls[0][0])
        req_hdrs = self.app.headers[0]

        # verify that other middleware's footers made it to app, including any
        # container update overrides but not Etag
        other_footers.pop('Etag')
        for k, v in other_footers.items():
            self.assertEqual(v, req_hdrs[k])

        # verify encryption footers are ok
        self.assertEqual(ciphertext_etag, req_hdrs['Etag'])
        actual = base64.b64decode(req_hdrs['X-Object-Sysmeta-Crypto-Etag'])
        etag_iv = Crypto().create_iv(iv_base=req.path)
        self.assertEqual(encrypt(plaintext_etag, key, etag_iv), actual)
        actual = json.loads(urllib.unquote_plus(
            req_hdrs['X-Object-Sysmeta-Crypto-Meta-Etag']))
        self.assertEqual(Crypto().get_cipher(), actual['cipher'])
        self.assertEqual(etag_iv, base64.b64decode(actual['iv']))

    def test_PUT_with_bad_etag_in_other_footers(self):
        # verify that etag supplied in footers from other middleware overrides
        # header etag when validating inbound plaintext etags
        plaintext = 'FAKE APP'
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
            'Etag': 'other etag',
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Backend-Container-Update-Override-Etag': 'other override'}
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
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'POST',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        key = fetch_crypto_keys()['object']
        self.app.register('POST', '/v1/a/c/o', HTTPAccepted, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('202 Accepted', resp.status)
        self.assertNotIn('Etag', resp.headers)

        # verify metadata items
        self.assertEqual(1, len(self.app.calls), self.app.calls)
        self.assertEqual('POST', self.app.calls[0][0])
        req_hdrs = self.app.headers[0]

        # user meta is encrypted
        self.assertEqual(base64.b64encode(encrypt('encrypt me', key, FAKE_IV)),
                         req_hdrs['X-Object-Meta-Test'])
        actual = req_hdrs['X-Object-Transient-Sysmeta-Crypto-Meta-Test']
        actual = json.loads(urllib.unquote_plus(actual))
        self.assertEqual(Crypto().get_cipher(), actual['cipher'])
        self.assertEqual(FAKE_IV, base64.b64decode(actual['iv']))

        # sysmeta is not encrypted
        self.assertEqual('do not encrypt me',
                         req_hdrs['X-Object-Sysmeta-Test'])

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

            self.assertEqual(1, len(app.calls), app.calls)
            self.assertEqual(method, app.calls[0][0])
            actual_headers = app.headers[0]

            # verify the alternate etag location has been specified
            if match_header_value:
                self.assertIn('X-Backend-Etag-Is-At', actual_headers)
                self.assertEqual('X-Object-Sysmeta-Crypto-Etag',
                                 actual_headers['X-Backend-Etag-Is-At'])

            # verify etags have been supplemented with encrypted values
            self.assertIn(match_header_name, actual_headers)
            actual_etags = set(actual_headers[match_header_name].split(', '))
            key = fetch_crypto_keys()['object']
            iv = Crypto().create_iv(iv_base=req.path)
            encrypted_etags = [
                '"%s"' % base64.b64encode(encrypt(etag.strip('"'), key, iv))
                for etag in plain_etags if etag not in ('*', '')]
            expected_etags = set((expected_plain_etags or plain_etags) +
                                 encrypted_etags)
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

        self.assertEqual(1, len(self.app.calls), self.app.calls)
        self.assertEqual(method, self.app.calls[0][0])
        actual_headers = self.app.headers[0]
        self.assertIn('X-Backend-Etag-Is-At', actual_headers)
        self.assertEqual('X-Object-Sysmeta-Other-Etag',
                         actual_headers['X-Backend-Etag-Is-At'])
        actual_etags = set(actual_headers[match_header_name].split(', '))
        self.assertIn('"an etag"', actual_etags)

    def test_GET_if_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('GET', 'If-Match')

    def test_HEAD_if_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('HEAD', 'If-Match')

    def test_GET_if_none_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('GET', 'If-None-Match')

    def test_HEAD_if_none_match_with_existing_etag_is_at_header(self):
        self._test_existing_etag_is_at_header('HEAD', 'If-None-Match')

    def test_PUT_backend_response_etag_is_replaced(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated,
                          {'Etag': 'ciphertextEtag'})
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])

    def test_PUT_multiseg_no_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(encrypt(body, fetch_crypto_keys()['object'], FAKE_IV),
                         get_req.get_response(self.app).body)

    def test_PUT_multiseg_good_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': md5hex(body)}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(encrypt(body, fetch_crypto_keys()['object'], FAKE_IV),
                         get_req.get_response(self.app).body)

    def test_PUT_multiseg_bad_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': 'badclientetag'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('422 Unprocessable Entity', resp.status)

    def test_PUT_missing_key_callback(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        resp = req.get_response(self.encrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn('%s not in env' % CRYPTO_KEY_CALLBACK,
                      self.encrypter.logger.get_lines_for_level('error')[0])
        self.assertEqual('Unable to retrieve encryption keys.', resp.body)

    def test_PUT_error_in_key_callback(self):
        def raise_exc():
            raise Exception('Testing')

        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: raise_exc}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        resp = req.get_response(self.encrypter)
        self.assertEqual('500 Internal Error', resp.status)
        self.assertIn('from %s: Testing' % CRYPTO_KEY_CALLBACK,
                      self.encrypter.logger.get_lines_for_level('error')[0])
        self.assertEqual('Unable to retrieve encryption keys.', resp.body)

    def test_PUT_encryption_override(self):
        # set crypto override to disable encryption.
        # simulate another middleware wanting to set footers
        other_footers = {
            'Etag': 'other etag',
            'X-Object-Sysmeta-Other': 'other sysmeta',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
                'other override'}
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.override': True,
               'swift.callback.update_footers':
                   lambda footers: footers.update(other_footers)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        self.app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(self.encrypter)
        self.assertEqual('201 Created', resp.status)

        # verify that other middleware's footers made it to app
        req_hdrs = self.app.headers[0]
        for k, v in other_footers.items():
            self.assertEqual(v, req_hdrs[k])

        # verify object is NOT encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(body, get_req.get_response(self.app).body)

    def test_filter(self):
        factory = encrypter.filter_factory({})
        self.assertTrue(callable(factory))
        self.assertIsInstance(factory({}), encrypter.Encrypter)

    def test_PUT_app_exception(self):
        app = encrypter.Encrypter(FakeAppThatExcepts(), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        with self.assertRaises(HTTPException) as catcher:
            req.get_response(app)
        self.assertEqual(FakeAppThatExcepts.MESSAGE, catcher.exception.body)


if __name__ == '__main__':
    unittest.main()
