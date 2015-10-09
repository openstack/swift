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

import unittest
import mock

from test.unit import(
    FakeRing, FakeMemcache, debug_logger, patch_policies)
from test.unit.proxy import test_server
from swift.proxy import server as proxy_server
from swift.common.middleware import encrypter
from swift.common.swob import Request, HTTPException, HTTPCreated, HTTPAccepted
from swift.common.utils import FileLikeIter
from swift.common.middleware.crypto import Crypto

from test.unit.common.middleware.crypto_helpers import fetch_crypto_keys, \
    md5hex, FakeCrypto, fake_encrypt
from test.unit.common.middleware.helpers import FakeSwift
from test.unit.common.middleware.test_proxy_logging import FakeAppThatExcepts


@mock.patch('swift.common.middleware.encrypter.Crypto', FakeCrypto)
class TestEncrypter(unittest.TestCase):

    def test_basic_put_req(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        self.assertEqual(resp.headers['Etag'], md5hex(body))

        # verify metadata items
        self.assertEqual(1, len(app.calls), app.calls)
        self.assertEqual('PUT', app.calls[0][0])
        req_hdrs = app.headers[0]

        # encrypted version of plaintext etag
        actual = base64.b64decode(req_hdrs['X-Object-Sysmeta-Crypto-Etag'])
        self.assertEqual(fake_encrypt(md5hex(body)), actual)
        actual = json.loads(req_hdrs['X-Object-Sysmeta-Crypto-Meta-Etag'])
        self.assertEqual('test_cipher', actual['cipher'])
        self.assertEqual('test_iv', base64.b64decode(actual['iv']))

        # encrypted version of plaintext etag for container update
        actual = req_hdrs['X-Backend-Container-Update-Override-Etag']
        self.assertEqual(md5hex(body), actual)
        # TODO: uncomment when container metadata is encrypted
        # actual_etag, actual_meta = base64.b64decode(actual).split(';')
        # self.assertEqual(fake_encrypt(md5hex(body)), actual_etag)
        # actual_meta = json.loads(actual_meta.lstrip(' meta='))
        # self.assertEqual('test_cipher', actual_meta['cipher'])
        # self.assertEqual('test_iv', base64.b64decode(actual_meta['iv']))

        # content-type is encrypted
        actual = req_hdrs['Content-Type']
        actual_ctype, actual_meta = actual.split(';')
        self.assertEqual(base64.b64encode(fake_encrypt('text/plain')),
                         actual_ctype)
        actual_meta = json.loads(actual_meta.lstrip(' meta='))
        self.assertEqual('test_cipher', actual_meta['cipher'])
        self.assertEqual('test_iv', base64.b64decode(actual_meta['iv']))

        # encrypted version of content-type for container update
        actual = req_hdrs['X-Backend-Container-Update-Override-Content-Type']
        self.assertEqual('text/plain', actual)
        # TODO: uncomment when container metadata is encrypted
        # actual_ctype, actual_meta = base64.b64decode(actual).split(';')
        # self.assertEqual(fake_encrypt('text/plain'), actual_ctype)
        # actual_meta = json.loads(actual_meta.lstrip(' meta='))
        # self.assertEqual('test_cipher', actual_meta['cipher'])
        # self.assertEqual('test_iv', base64.b64decode(actual_meta['iv']))

        # user meta is encrypted
        self.assertEqual(base64.b64encode(fake_encrypt('encrypt me')),
                         req_hdrs['X-Object-Meta-Test'])
        actual = req_hdrs['X-Object-Sysmeta-Crypto-Meta-Test']
        actual = json.loads(actual)
        self.assertEqual('test_cipher', actual['cipher'])
        self.assertEqual('test_iv', base64.b64decode(actual['iv']))

        # sysmeta is not encrypted
        self.assertEqual('do not encrypt me',
                         req_hdrs['X-Object-Sysmeta-Test'])

        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = get_req.get_response(app)
        self.assertEqual(resp.body, fake_encrypt(body))
        self.assertEqual(md5hex(fake_encrypt(body)), resp.headers['Etag'])

    def test_basic_post_req(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'POST',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        hdrs = {'x-object-meta-test': 'encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('POST', '/v1/a/c/o', HTTPAccepted, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '202 Accepted')

        # verify metadata items
        self.assertEqual(1, len(app.calls), app.calls)
        self.assertEqual('POST', app.calls[0][0])
        req_hdrs = app.headers[0]

        # user meta is encrypted
        self.assertEqual(base64.b64encode(fake_encrypt('encrypt me')),
                         req_hdrs['X-Object-Meta-Test'])
        actual = req_hdrs['X-Object-Sysmeta-Crypto-Meta-Test']
        actual = json.loads(actual)
        self.assertEqual('test_cipher', actual['cipher'])
        self.assertEqual('test_iv', base64.b64decode(actual['iv']))

        # sysmeta is not encrypted
        self.assertEqual('do not encrypt me',
                         req_hdrs['X-Object-Sysmeta-Test'])

    def test_backend_response_etag_is_replaced(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated,
                     {'Etag': 'ciphertextEtag'})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        self.assertEqual(resp.headers['Etag'], md5hex(body))

    def test_multiseg_no_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(get_req.get_response(app).body, fake_encrypt(body))

    def test_multiseg_good_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': md5hex(body)}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        # verify object is encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(get_req.get_response(app).body, fake_encrypt(body))

    def test_multiseg_bad_client_etag(self):
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'wsgi.input': FileLikeIter(chunks)}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body)),
                'Etag': 'badclientetag'}
        req = Request.blank(
            '/v1/a/c/o', environ=env, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '422 Unprocessable Entity')

    def test_missing_key_callback(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT'}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')
        self.assertEqual(
            resp.body, 'swift.crypto.fetch_crypto_keys not in env')

    def test_error_in_key_callback(self):
        def raise_exc():
            raise Exception('Testing')

        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.fetch_crypto_keys': raise_exc}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '500 Internal Error')
        self.assertEqual(
            resp.body, 'swift.crypto.fetch_crypto_keys had exception: Testing')

    def test_encryption_override(self):
        body = 'FAKE APP'
        env = {'REQUEST_METHOD': 'PUT',
               'swift.crypto.override': True}
        hdrs = {'content-type': 'text/plain',
                'content-length': str(len(body))}
        req = Request.blank(
            '/v1/a/c/o', environ=env, body=body, headers=hdrs)
        app = FakeSwift()
        app.register('PUT', '/v1/a/c/o', HTTPCreated, {})
        resp = req.get_response(encrypter.Encrypter(app, {}))
        self.assertEqual(resp.status, '201 Created')
        # verify object is NOT encrypted by getting direct from the app
        get_req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        self.assertEqual(get_req.get_response(app).body, body)

    def test_filter(self):
        factory = encrypter.filter_factory({})
        self.assertTrue(callable(factory))
        self.assertIsInstance(factory({}), encrypter.Encrypter)

    def test_app_exception(self):
        app = encrypter.Encrypter(
            FakeAppThatExcepts(), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        with self.assertRaises(HTTPException) as catcher:
            req.get_response(app)
        self.assertEqual(catcher.exception.body,
                         FakeAppThatExcepts.get_error_msg())


@patch_policies
class TestEncrypterContentType(unittest.TestCase):

    def setUp(self):
        self.proxy_app = proxy_server.Application(
            None, FakeMemcache(),
            logger=debug_logger('proxy-ut'),
            account_ring=FakeRing(),
            container_ring=FakeRing())
        self.app = encrypter.Encrypter(self.proxy_app, {})

    def _make_capture_headers(self, captured_headers):
        def on_connect(ipaddr, port, device, partition,
                       method, path, headers=None,
                       query_string=None):
            if path == '/a/c/o':
                captured_headers.append(headers)
        return on_connect

    def _verify_content_type(self, expected_ctype, expected_meta, value, iv):
        key = fetch_crypto_keys()['object']
        crypto_ctxt = Crypto({}).create_decryption_ctxt(key, iv, 0)

        self.assertIn(';', value)
        actual_ctype, param = value.split(';')
        actual_ctype = crypto_ctxt.update(base64.b64decode(actual_ctype))
        self.assertEqual(expected_ctype, actual_ctype)

        param = param.strip()
        self.assertTrue(param.startswith('meta='))
        actual_meta = json.loads(param[5:])
        self.assertDictEqual(expected_meta, actual_meta)

    def test_PUT_detect_content_type(self):
        # Tests content-type is guessed and encrypted if x-detect-content-type
        with test_server.save_globals():
            captured_headers = []
            on_connect = self._make_capture_headers(captured_headers)
            test_server.set_http_connect(204, 204, 201, 201, 201,
                                         give_connect=on_connect)

            headers = {'Content-Length': 0, 'content-type': 'something/wrong',
                       'X-Detect-Content-Type': 'True'}
            req = Request.blank('/v1/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'PUT',
                                         'swift.crypto.fetch_crypto_keys':
                                         fetch_crypto_keys})
            iv = '0123456789abcdef'
            with mock.patch(
                    'swift.common.middleware.encrypter.Crypto.create_iv',
                    lambda *args: iv):
                req.get_response(self.app)

            self.assertIs(True, req.environ['content_type_manually_set'])
            self.assertNotIn('x-detect-content-type', req.headers)
            self.assertEqual(3, len(captured_headers))
            enc_ct = captured_headers[0]['content-type']
            expected_meta = {"iv": base64.b64encode(iv),
                             "cipher": "AES_CTR_256"}
            self._verify_content_type(
                'application/octet-stream', expected_meta, enc_ct, iv)

    def test_PUT_no_content_type(self):
        # Tests content-type is guessed and encrypted when none is supplied
        with test_server.save_globals():
            captured_headers = []
            on_connect = self._make_capture_headers(captured_headers)
            test_server.set_http_connect(204, 204, 201, 201, 201,
                                         give_connect=on_connect)

            headers = {'Content-Length': 0}
            req = Request.blank('/v1/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'PUT',
                                         'swift.crypto.fetch_crypto_keys':
                                         fetch_crypto_keys})
            iv = '0123456789abcdef'
            with mock.patch(
                    'swift.common.middleware.encrypter.Crypto.create_iv',
                    lambda *args: iv):
                req.get_response(self.app)

            self.assertIs(False, req.environ['content_type_manually_set'])
            self.assertNotIn('x-detect-content-type', req.headers)
            self.assertEqual(3, len(captured_headers))
            enc_ct = captured_headers[0]['content-type']
            expected_meta = {"iv": base64.b64encode(iv),
                             "cipher": "AES_CTR_256"}
            self._verify_content_type(
                'application/octet-stream', expected_meta, enc_ct, iv)

    def test_PUT_detect_content_type_no_keys(self):
        # Tests content-type is guessed and NOT encrypted with crypto override
        with test_server.save_globals():
            captured_headers = []
            on_connect = self._make_capture_headers(captured_headers)
            test_server.set_http_connect(204, 204, 201, 201, 201,
                                         give_connect=on_connect)
            headers = {'Content-Length': 0, 'content-type': 'something/wrong',
                       'X-Detect-Content-Type': 'True'}
            req = Request.blank('/v1/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'PUT',
                                         'swift.crypto.override': True})

            req.get_response(self.app)

            self.assertIs(True, req.environ['content_type_manually_set'])
            self.assertNotIn('x-detect-content-type', req.headers)
            self.assertEqual(3, len(captured_headers))
            enc_ct = captured_headers[0]['content-type']
            expected = 'application/octet-stream'
            self.assertEqual(expected, enc_ct)

    def test_PUT_no_content_type_no_keys(self):
        # Tests content-type is guessed and NOT encrypted with crypto override
        with test_server.save_globals():
            captured_headers = []
            on_connect = self._make_capture_headers(captured_headers)
            test_server.set_http_connect(204, 204, 201, 201, 201,
                                         give_connect=on_connect)

            headers = {'Content-Length': 0}
            req = Request.blank('/v1/a/c/o', headers=headers,
                                environ={'REQUEST_METHOD': 'PUT',
                                         'swift.crypto.override': True})

            req.get_response(self.app)

            self.assertIs(False, req.environ['content_type_manually_set'])
            self.assertNotIn('x-detect-content-type', req.headers)
            self.assertEqual(3, len(captured_headers))
            enc_ct = captured_headers[0]['content-type']
            expected = 'application/octet-stream'
            self.assertEqual(expected, enc_ct)


if __name__ == '__main__':
    unittest.main()
