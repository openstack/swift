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
import mock
import os
import unittest
import urllib
import uuid

from swift.common.middleware import encrypter, decrypter, keymaster
from swift.common.swob import Request, HTTPCreated, HTTPAccepted
from swift.common.crypto_utils import CRYPTO_KEY_CALLBACK

from test.unit.common.middleware.crypto_helpers import fetch_crypto_keys, \
    md5hex, encrypt
from test.unit.common.middleware.helpers import FakeSwift
from test.unit.helpers import setup_servers, teardown_servers


class TestCryptoPipelineChanges(unittest.TestCase):
    # Tests the consequences of crypto middleware being in/out of the pipeline
    # for PUT/GET requests on same object. Uses real backend servers so that
    # the handling of headers and sysmeta is verified to diskfile and back.
    _test_context = None

    @classmethod
    def setUpClass(cls):
        cls._test_context = setup_servers()
        cls.proxy_app = cls._test_context["test_servers"][0]

    @classmethod
    def tearDownClass(cls):
        if cls._test_context is not None:
            teardown_servers(cls._test_context)
            cls._test_context = None

    def setUp(self):
        self.container_path = 'http://localhost:8080/v1/a/' + uuid.uuid4().hex
        self.object_path = self.container_path + '/o'
        self.plaintext = 'unencrypted body content'
        self.plaintext_etag = md5hex(self.plaintext)

        # Set up a pipeline of crypto middleware ending in the proxy app so
        # that tests can make requests to either the proxy server directly or
        # via the crypto middleware. Make a fresh instance for each test to
        # avoid any state coupling.
        enc = encrypter.Encrypter(self.proxy_app, {})
        km = keymaster.KeyMaster(enc, {'encryption_root_secret': 's3cr3t'})
        self.crypto_app = decrypter.Decrypter(km, {})

    def _create_container(self, app, policy_name='one'):
        req = Request.blank(
            self.container_path, method='PUT',
            headers={'X-Storage-Policy': policy_name})
        resp = req.get_response(app)
        self.assertEqual('201 Created', resp.status)
        # sanity check
        req = Request.blank(
            self.container_path, method='HEAD',
            headers={'X-Storage-Policy': policy_name})
        resp = req.get_response(app)
        self.assertEqual(policy_name, resp.headers['X-Storage-Policy'])

    def _put_object(self, app, body):
        req = Request.blank(self.object_path, method='PUT', body=body)
        resp = req.get_response(app)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        return resp

    def _post_object(self, app):
        req = Request.blank(self.object_path, method='POST',
                            headers={'X-Object-Meta-Fruit': 'Kiwi'})
        resp = req.get_response(app)
        self.assertEqual('202 Accepted', resp.status)
        return resp

    def _check_GET_and_HEAD(self, app):
        req = Request.blank(self.object_path, method='GET')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(self.plaintext, resp.body)
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

        req = Request.blank(self.object_path, method='HEAD')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual('', resp.body)
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

    def _check_match_requests(self, method, app):
        # verify conditional match requests
        expected_body = self.plaintext if method == 'GET' else ''

        # If-Match matches
        req = Request.blank(self.object_path, method=method,
                            headers={'If-Match': '"%s"' % self.plaintext_etag})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(expected_body, resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

        # If-Match wildcard
        req = Request.blank(self.object_path, method=method,
                            headers={'If-Match': '*'})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(expected_body, resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

        # If-Match does not match
        req = Request.blank(self.object_path, method=method,
                            headers={'If-Match': '"not the etag"'})
        resp = req.get_response(app)
        self.assertEqual('412 Precondition Failed', resp.status)
        self.assertEqual('', resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])

        # If-None-Match matches
        req = Request.blank(
            self.object_path, method=method,
            headers={'If-None-Match': '"%s"' % self.plaintext_etag})
        resp = req.get_response(app)
        self.assertEqual('304 Not Modified', resp.status)
        self.assertEqual('', resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])

        # If-None-Match wildcard
        req = Request.blank(self.object_path, method=method,
                            headers={'If-None-Match': '*'})
        resp = req.get_response(app)
        self.assertEqual('304 Not Modified', resp.status)
        self.assertEqual('', resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])

        # If-None-Match does not match
        req = Request.blank(self.object_path, method=method,
                            headers={'If-None-Match': '"not the etag"'})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(expected_body, resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

    def _check_listing(self, app, expect_mismatch=False):
        req = Request.blank(
            self.container_path, method='GET', query_string='format=json')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        listing = json.loads(resp.body)
        self.assertEqual(1, len(listing))
        self.assertEqual('o', listing[0]['name'])
        self.assertEqual(len(self.plaintext), listing[0]['bytes'])
        if expect_mismatch:
            self.assertNotEqual(self.plaintext_etag, listing[0]['hash'])
        else:
            self.assertEqual(self.plaintext_etag, listing[0]['hash'])

    def test_write_with_crypto_read_with_crypto(self):
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)

    def test_write_with_crypto_read_with_crypto_ec(self):
        self._create_container(self.proxy_app, policy_name='ec')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)

    def test_write_without_crypto_read_with_crypto(self):
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.proxy_app, self.plaintext)
        self._post_object(self.proxy_app)
        self._check_GET_and_HEAD(self.proxy_app)  # sanity check
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.proxy_app)  # sanity check
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.proxy_app)  # sanity check
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)

    def _check_GET_and_HEAD_not_decrypted(self, app):
        req = Request.blank(self.object_path, method='GET')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertNotEqual(self.plaintext, resp.body)
        self.assertEqual('%s' % len(self.plaintext),
                         resp.headers['Content-Length'])
        self.assertNotEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

        req = Request.blank(self.object_path, method='HEAD')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual('', resp.body)
        self.assertNotEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

    def test_write_with_crypto_read_without_crypto(self):
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)  # sanity check
        # without crypto middleware, GET and HEAD returns ciphertext
        self._check_GET_and_HEAD_not_decrypted(self.proxy_app)
        self._check_listing(self.proxy_app, expect_mismatch=True)

    def test_write_with_crypto_read_without_crypto_ec(self):
        self._create_container(self.proxy_app, policy_name='ec')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)  # sanity check
        # without crypto middleware, GET and HEAD returns ciphertext
        self._check_GET_and_HEAD_not_decrypted(self.proxy_app)
        self._check_listing(self.proxy_app, expect_mismatch=True)


class TestCryptoPipelineChangesFastPost(TestCryptoPipelineChanges):
    @classmethod
    def setUpClass(cls):
        # set proxy config to use fast post
        extra_conf = {'object_post_as_copy': 'False'}
        cls._test_context = setup_servers(extra_conf=extra_conf)
        cls.proxy_app = cls._test_context["test_servers"][0]


class TestEncrypterDecrypter(unittest.TestCase):
    """
    Unit tests to verify round-trip encryption followed by decryption.
    These tests serve to complement the separate unit tests for encrypter and
    decrypter, which test each in isolation.
    However, the real Crypto implementation is used.
    """

    def test_basic_put_get_req(self):
        # pass the PUT request through the encrypter.
        body_key = os.urandom(32)
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})

        with mock.patch(
                'swift.common.middleware.crypto.Crypto.create_random_key',
                return_value=body_key):
            req.get_response(encrypter.Encrypter(app, {}))

        # Verify that at least the request body was indeed encrypted.
        # Otherwise, checking that input matches output after decryption is
        # not sufficient if encryption and decryption were both just the
        # identity function! (i.e. if they did nothing)
        encrypt_get_req = Request.blank('/v1/a/c/o',
                                        environ={'REQUEST_METHOD': 'GET'})
        encrypt_get_resp = encrypt_get_req.get_response(app)

        crypto_meta = json.loads(urllib.unquote_plus(
            encrypt_get_resp.headers['X-Object-Sysmeta-Crypto-Meta']))
        crypto_meta['iv'] = base64.b64decode(crypto_meta['iv'])
        exp_enc_body = encrypt(
            body, body_key, crypto_meta['iv'])
        self.assertEqual(exp_enc_body, encrypt_get_resp.body)
        self.assertNotEqual(body, encrypt_get_resp.body)  # sanity check

        decrypt_env = {'REQUEST_METHOD': 'GET',
                       CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        decrypt_req = Request.blank('/v1/a/c/o', environ=decrypt_env)
        decrypt_resp = decrypt_req.get_response(
            decrypter.Decrypter(app, {}))

        self.assertEqual(body, decrypt_resp.body)
        self.assertEqual('200 OK', decrypt_resp.status)
        self.assertEqual('text/plain', decrypt_resp.headers['Content-Type'])
        self.assertEqual(md5hex(body), decrypt_resp.headers['Etag'])
        self.assertEqual('encrypt me',
                         decrypt_resp.headers['x-object-meta-test'])
        self.assertEqual('do not encrypt me',
                         decrypt_resp.headers['x-object-sysmeta-test'])

        # do a POST update to verify updated metadata is encrypted
        env = {'REQUEST_METHOD': 'POST',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        hdrs = {'x-object-meta-test': 'encrypt me is updated'}
        req = Request.blank('/v1/a/c/o', environ=env, headers=hdrs)
        app.register('POST', '/v1/a/c/o', HTTPAccepted, {})
        req.get_response(encrypter.Encrypter(app, {}))

        # verify the metadata header was indeed encrypted by doing a GET
        # direct to the app
        encrypt_get_resp = encrypt_get_req.get_response(app)
        crypto_meta = json.loads(urllib.unquote_plus(
            encrypt_get_resp.headers[
                'X-Object-Transient-Sysmeta-Crypto-Meta-Test']))
        crypto_meta['iv'] = base64.b64decode(crypto_meta['iv'])
        exp_header_value = base64.b64encode(encrypt(
            'encrypt me is updated', fetch_crypto_keys()['object'],
            crypto_meta['iv']))
        self.assertEqual(exp_header_value,
                         encrypt_get_resp.headers['x-object-meta-test'])

        # do a GET to verify the updated metadata is decrypted
        env = {'REQUEST_METHOD': 'GET',
               CRYPTO_KEY_CALLBACK: fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        resp_dec = req.get_response(
            decrypter.Decrypter(app, {}))
        self.assertEqual('encrypt me is updated',
                         resp_dec.headers['x-object-meta-test'])


if __name__ == '__main__':
    unittest.main()
