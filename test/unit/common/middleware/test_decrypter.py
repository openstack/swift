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

import unittest
from xml.dom import minidom
import mock
import base64
import json
import urllib

from test.unit.common.middleware.crypto_helpers import md5hex, \
    fetch_crypto_keys, fake_iv, encrypt, fake_get_crypto_meta
from swift.common.middleware.crypto import Crypto
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts
from swift.common.middleware import decrypter
from swift.common.swob import Request, HTTPException, HTTPOk, \
    HTTPPreconditionFailed, HTTPNotFound


def get_crypto_meta_header(crypto_meta=None):
    if crypto_meta is None:
        crypto_meta = fake_get_crypto_meta()
    return urllib.quote_plus(
        json.dumps({key: (base64.b64encode(value).decode()
                          if key == 'iv' else value)
                    for key, value in crypto_meta.items()}))


def encrypt_and_append_meta(value, key, crypto_meta=None):
    return '%s; meta=%s' % (
        base64.b64encode(encrypt(value, key, fake_iv())),
        get_crypto_meta_header(crypto_meta))


@mock.patch('swift.common.middleware.crypto.Crypto.create_iv',
            lambda *args: fake_iv())
class TestDecrypterObjectRequests(unittest.TestCase):

    def test_get_req_success(self):
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag':
                    base64.b64encode(encrypt(md5hex(body), key, fake_iv())),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header(),
                'x-object-meta-test':
                    base64.b64encode(encrypt('encrypt me', key, fake_iv())),
                'x-object-transient-sysmeta-crypto-meta-test':
                    get_crypto_meta_header(),
                'x-object-sysmeta-test': 'do not encrypt me'}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(body, resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])

    def _test_412_response(self, method):
        # simulate a 412 response to a conditional GET which has an Etag header
        data = 'the object content'
        env = {'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env, method=method)
        resp_body = 'I am sorry, you have failed to meet a precondition'
        key = fetch_crypto_keys()['object']
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(resp_body),
                'X-Object-Sysmeta-Crypto-Etag':
                    base64.b64encode(encrypt(md5hex(data), key, fake_iv())),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header(),
                'x-object-meta-test':
                    base64.b64encode(encrypt('encrypt me', key, fake_iv())),
                'x-object-transient-sysmeta-crypto-meta-test':
                    get_crypto_meta_header(),
                'x-object-sysmeta-test': 'do not encrypt me'}
        app.register(method, '/v1/a/c/o', HTTPPreconditionFailed,
                     body=resp_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))

        self.assertEqual('412 Precondition Failed', resp.status)
        # the response body should not be decrypted, it is already plaintext
        self.assertEqual(resp_body if method == 'GET' else '', resp.body)
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
        env = {'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env, method=method)
        resp_body = 'You still have not found what you are looking for'
        app = FakeSwift()
        hdrs = {'content-type': 'text/plain',
                'content-length': len(resp_body)}
        app.register(method, '/v1/a/c/o', HTTPNotFound,
                     body=resp_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))

        self.assertEqual('404 Not Found', resp.status)
        # the response body should not be decrypted, it is already plaintext
        self.assertEqual(resp_body if method == 'GET' else '', resp.body)
        # there should be no etag header inserted by decrypter
        self.assertNotIn('Etag', resp.headers)
        self.assertEqual('text/plain', resp.headers['Content-Type'])

    def test_GET_404_response(self):
        self._test_404_response('GET')

    def test_HEAD_404_response(self):
        self._test_404_response('HEAD')

    def _test_bad_key(self, method):
        # use bad key
        def bad_fetch_crypto_keys():
            keys = fetch_crypto_keys()
            keys['object'] = 'bad key'
            return keys

        env = {'REQUEST_METHOD': method,
               'swift.crypto.fetch_crypto_keys': bad_fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header(),
                'x-object-meta-test':
                    base64.b64encode(encrypt('encrypt me', key, fake_iv())),
                'x-object-transient-sysmeta-crypto-meta-test':
                    get_crypto_meta_header()}
        app.register(method, '/v1/a/c/o', HTTPOk, body=enc_body,
                     headers=hdrs)
        return req.get_response(decrypter.Decrypter(app, {}))

    def test_head_with_bad_key(self):
        resp = self._test_bad_key('HEAD')
        self.assertEqual('500 Internal Error', resp.status)

    def test_get_with_bad_key(self):
        resp = self._test_bad_key('GET')
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual('Error decrypting header value', resp.body)

    def _test_bad_iv_for_user_metadata(self, method):
        # use bad iv for metadata headers
        env = {'REQUEST_METHOD': method,
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['iv'] = 'bad_iv'
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header(),
                'x-object-meta-test':
                    base64.b64encode(encrypt('encrypt me', key, fake_iv())),
                'x-object-transient-sysmeta-crypto-meta-test':
                    get_crypto_meta_header(crypto_meta=bad_crypto_meta)}
        app.register(method, '/v1/a/c/o', HTTPOk, body=enc_body,
                     headers=hdrs)
        return req.get_response(decrypter.Decrypter(app, {}))

    def test_head_with_bad_iv_for_user_metadata(self):
        resp = self._test_bad_iv_for_user_metadata('HEAD')
        self.assertEqual('500 Internal Error', resp.status)

    def test_get_with_bad_iv_for_user_metadata(self):
        resp = self._test_bad_iv_for_user_metadata('GET')
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual('Error decrypting header value', resp.body)

    def test_get_with_bad_iv_for_object_body(self):
        # use bad iv for object body
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['iv'] = 'bad_iv'
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta':
                    get_crypto_meta_header(crypto_meta=bad_crypto_meta)}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body,
                     headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual('Error creating decryption context for object body',
                         resp.body)

    def test_basic_head_req(self):
        env = {'REQUEST_METHOD': 'HEAD',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag':
                    base64.b64encode(encrypt(md5hex(body), key, fake_iv())),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header(),
                'x-object-meta-test':
                    base64.b64encode(encrypt('encrypt me', key, fake_iv())),
                'x-object-transient-sysmeta-crypto-meta-test':
                    get_crypto_meta_header(),
                'x-object-sysmeta-test': 'do not encrypt me'}
        app.register('HEAD', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])

    def _test_req_content_type_not_encrypted(self, method):
        # check that content_type is not decrypted if it does not have crypto
        # meta (testing for future cases where content_type may be updated
        # as part of an unencrypted POST).
        env = {'REQUEST_METHOD': method,
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag':
                    base64.b64encode(encrypt(md5hex(body), key, fake_iv())),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register(method, '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])

    def test_head_req_content_type_not_encrypted(self):
        self._test_req_content_type_not_encrypted('HEAD')

    def test_get_req_content_type_not_encrypted(self):
        self._test_req_content_type_not_encrypted('GET')

    def _test_req_metadata_not_encrypted(self, method):
        # check that metadata is not decrypted if it does not have crypto meta;
        # testing for case of an unencrypted POST to an object.
        env = {'REQUEST_METHOD': method,
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag':
                    base64.b64encode(encrypt(md5hex(body), key, fake_iv())),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header(),
                'x-object-meta-test': 'plaintext'}
        app.register(method, '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('plaintext', resp.headers['x-object-meta-test'])

    def test_head_req_metadata_not_encrypted(self):
        self._test_req_metadata_not_encrypted('HEAD')

    def test_get_req_metadata_not_encrypted(self):
        self._test_req_metadata_not_encrypted('GET')

    def test_get_req_unencrypted_data(self):
        # testing case of an unencrypted object with encrypted metadata from
        # a later POST
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        app = FakeSwift()
        hdrs = {'Etag': md5hex(body),
                'content-type': 'text/plain',
                'content-length': len(body),
                'x-object-meta-test':
                    base64.b64encode(encrypt('encrypt me',
                                             fetch_crypto_keys()['object'],
                                             fake_iv())),
                'x-object-transient-sysmeta-crypto-meta-test':
                    get_crypto_meta_header(),
                'x-object-sysmeta-test': 'do not encrypt me'}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(body, resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        # POSTed user meta was encrypted
        self.assertEqual('encrypt me', resp.headers['x-object-meta-test'])
        # PUT sysmeta was not encrypted
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])

    def test_multiseg_get_obj(self):
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        chunks = ['some', 'chunks', 'of data']
        body = ''.join(chunks)
        key = fetch_crypto_keys()['object']
        ctxt = Crypto({}).create_encryption_ctxt(key, fake_iv())
        enc_body = [encrypt(chunk, ctxt=ctxt) for chunk in chunks]
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': sum(map(len, enc_body)),
                'X-Object-Sysmeta-Crypto-Etag':
                    base64.b64encode(encrypt(md5hex(body), key, fake_iv())),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(body, resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])

    def test_multiseg_get_range_obj(self):
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        req.headers['Content-Range'] = 'bytes 3-10/17'
        chunks = ['0123', '45678', '9abcdef']
        body = ''.join(chunks)
        key = fetch_crypto_keys()['object']
        ctxt = Crypto({}).create_encryption_ctxt(key, fake_iv())
        enc_body = [encrypt(chunk, ctxt=ctxt) for chunk in chunks]
        enc_body = [enc_body[0][3:], enc_body[1], enc_body[2][:2]]
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': sum(map(len, enc_body)),
                'content-range': req.headers['Content-Range'],
                'X-Object-Sysmeta-Crypto-Etag':
                    base64.b64encode(encrypt(md5hex(body), key,
                                             fake_iv())),
                'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('3456789a', resp.body)
        self.assertEqual('200 OK', resp.status)
        # TODO - how do we validate the range body if etag is for whole? Is
        # the test actually faking the correct Etag in response?
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])

    # Force the decrypter context updates to be less than one of our range
    # sizes to check that the decrypt context offset is setup correctly with
    # offset to first byte of range for first update and then re-used.
    # Do mocking here to have the mocked value have effect in the generator
    # function.
    @mock.patch.object(decrypter, 'DECRYPT_CHUNK_SIZE', 4)
    def test_multipart_get_obj(self):
        # build fake multipart response body
        key = fetch_crypto_keys()['object']
        ctxt = Crypto({}).create_encryption_ctxt(key, fake_iv())
        plaintext = 'Cwm fjord veg balks nth pyx quiz'
        ciphertext = encrypt(plaintext, ctxt=ctxt)
        parts = ((0, 3, 'text/plain'),
                 (4, 9, 'text/plain; charset=us-ascii'),
                 (24, 32, 'text/plain'))
        length = len(ciphertext)
        body = ''
        for start, end, ctype in parts:
            body += '--multipartboundary\r\n'
            body += 'Content-Type: %s\r\n' % ctype
            body += 'Content-Range: bytes %s-%s/%s' % (start, end - 1, length)
            body += '\r\n\r\n' + ciphertext[start:end] + '\r\n'
        body += '--multipartboundary--'

        # register request with fake swift
        app = FakeSwift()
        hdrs = {
            'Etag': 'hashOfCiphertext',
            'content-type': 'multipart/byteranges;boundary=multipartboundary',
            'content-length': len(body),
            'X-Object-Sysmeta-Crypto-Etag':
                base64.b64encode(encrypt(md5hex(body), key, fake_iv())),
            'X-Object-Sysmeta-Crypto-Meta-Etag': get_crypto_meta_header(),
            'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)

        # issue request
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        resp = req.get_response(decrypter.Decrypter(app, {}))

        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        self.assertEqual('multipart/byteranges;boundary=multipartboundary',
                         resp.headers['Content-Type'])

        # the multipart headers could be re-ordered, so parse response body to
        # verify expected content
        resp_lines = resp.body.split('\r\n')
        resp_lines.reverse()
        for start, end, ctype in parts:
            self.assertEqual('--multipartboundary', resp_lines.pop())
            expected_header_lines = {
                'Content-Type: %s' % ctype,
                'Content-Range: bytes %s-%s/%s' % (start, end - 1, length)}
            resp_header_lines = {resp_lines.pop(), resp_lines.pop()}
            self.assertEqual(expected_header_lines, resp_header_lines)
            self.assertEqual('', resp_lines.pop())
            self.assertEqual(plaintext[start:end], resp_lines.pop())
        self.assertEqual('--multipartboundary--', resp_lines.pop())

        # we should have consumed the whole response body
        self.assertFalse(resp_lines)

    def test_etag_no_match_on_get(self):
        self.skipTest('Etag verification not yet implemented')
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex('not the body'),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('500 Internal Error', resp.status)

    def test_missing_key_callback(self):
        # Do not provide keys, and do not set override flag
        env = {'REQUEST_METHOD': 'GET'}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = encrypt(body, fetch_crypto_keys()['object'], fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex('not the body'),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual('swift.crypto.fetch_crypto_keys not in env',
                         resp.body)

    def test_error_in_key_callback(self):
        def raise_exc():
            raise Exception('Testing')

        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': raise_exc}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = encrypt(body, fetch_crypto_keys()['object'], fake_iv())
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header()}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual('swift.crypto.fetch_crypto_keys had exception:'
                         ' Testing', resp.body)

    def test_cipher_mismatch_for_body(self):
        # Cipher does not match
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        enc_body = encrypt(body, fetch_crypto_keys()['object'], fake_iv())
        app = FakeSwift()
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['cipher'] = 'unknown_cipher'
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta':
                    get_crypto_meta_header(crypto_meta=bad_crypto_meta)}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual('Error creating decryption context for object body',
                         resp.body)

    def test_cipher_mismatch_for_metadata(self):
        # Cipher does not match
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        key = fetch_crypto_keys()['object']
        enc_body = encrypt(body, key, fake_iv())
        bad_crypto_meta = fake_get_crypto_meta()
        bad_crypto_meta['cipher'] = 'unknown_cipher'
        app = FakeSwift()
        hdrs = {'Etag': 'hashOfCiphertext',
                'content-type': 'text/plain',
                'content-length': len(enc_body),
                'X-Object-Sysmeta-Crypto-Etag': md5hex(body),
                'X-Object-Sysmeta-Crypto-Meta': get_crypto_meta_header(),
                'x-object-meta-test':
                    base64.b64encode(encrypt('encrypt me', key, fake_iv())),
                'x-object-transient-sysmeta-crypto-meta-test':
                    get_crypto_meta_header(crypto_meta=bad_crypto_meta)}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=enc_body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual('500 Internal Error', resp.status)
        self.assertEqual('Error decrypting header value', resp.body)

    def test_decryption_override(self):
        # This covers the case of an old un-encrypted object
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys,
               'swift.crypto.override': True}
        req = Request.blank('/v1/a/c/o', environ=env)
        body = 'FAKE APP'
        app = FakeSwift()
        hdrs = {'Etag': md5hex(body),
                'content-type': 'text/plain',
                'content-length': len(body),
                'x-object-meta-test': 'do not encrypt me',
                'x-object-sysmeta-test': 'do not encrypt me'}
        app.register('GET', '/v1/a/c/o', HTTPOk, body=body, headers=hdrs)
        resp = req.get_response(decrypter.Decrypter(app, {}))
        self.assertEqual(body, resp.body)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(md5hex(body), resp.headers['Etag'])
        self.assertEqual('text/plain', resp.headers['Content-Type'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-meta-test'])
        self.assertEqual('do not encrypt me',
                         resp.headers['x-object-sysmeta-test'])


@mock.patch('swift.common.middleware.crypto.Crypto.create_iv',
            lambda *args: fake_iv())
class TestDecrypterContainerRequests(unittest.TestCase):
    # TODO - update these tests to have etag to be encrypted and have
    # crypto-meta in response, and verify that the etag gets decrypted.
    def _make_cont_get_req(self, resp_body, format, override=False):
        path = '/v1/a/c'
        content_type = 'text/plain'
        if format:
            path = '%s/?format=%s' % (path, format)
            content_type = 'application/' + format
        env = {'REQUEST_METHOD': 'GET',
               'swift.crypto.fetch_crypto_keys': fetch_crypto_keys}
        if override:
            env['swift.crypto.override'] = True
        req = Request.blank(path, environ=env)
        app = FakeSwift()
        hdrs = {'content-type': content_type}
        app.register('GET', path, HTTPOk, body=resp_body, headers=hdrs)
        return req.get_response(decrypter.Decrypter(app, {}))

    def test_cont_get_simple_req(self):
        # no format requested, listing has names only
        fake_body = 'testfile1\ntestfile2\n'

        resp = self._make_cont_get_req(fake_body, None)

        self.assertEqual('200 OK', resp.status)
        names = resp.body.split('\n')
        self.assertEqual(3, len(names))
        self.assertIn('testfile1', names)
        self.assertIn('testfile2', names)
        self.assertIn('', names)

    def test_cont_get_json_req(self):
        content_type_1 = u'\uF10F\uD20D\uB30B\u9409'
        content_type_2 = 'text/plain; param=foo'

        obj_dict_1 = {"bytes": 16,
                      "last_modified": "2015-04-14T23:33:06.439040",
                      "hash": "c6e8196d7f0fff6444b90861fe8d609d",
                      "name": "testfile",
                      "content_type": content_type_1}

        obj_dict_2 = {"bytes": 24,
                      "last_modified": "2015-04-14T23:33:06.519020",
                      "hash": "ac0374ed4d43635f803c82469d0b5a10",
                      "name": "testfile2",
                      "content_type": content_type_2}

        listing = [obj_dict_1, obj_dict_2]
        fake_body = json.dumps(listing)

        resp = self._make_cont_get_req(fake_body, 'json')

        self.assertEqual('200 OK', resp.status)
        body = resp.body
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        body_json = json.loads(body)
        self.assertEqual(2, len(body_json))
        obj_dict_1['content_type'] = content_type_1
        self.assertDictEqual(obj_dict_1, body_json[0])
        obj_dict_2['content_type'] = content_type_2
        self.assertDictEqual(obj_dict_2, body_json[1])

    def test_cont_get_json_req_with_crypto_override(self):
        content_type_1 = 'image/jpeg'
        content_type_2 = 'text/plain; param=foo'

        obj_dict_1 = {"bytes": 16,
                      "last_modified": "2015-04-14T23:33:06.439040",
                      "hash": "c6e8196d7f0fff6444b90861fe8d609d",
                      "name": "testfile",
                      "content_type": content_type_1}

        obj_dict_2 = {"bytes": 24,
                      "last_modified": "2015-04-14T23:33:06.519020",
                      "hash": "ac0374ed4d43635f803c82469d0b5a10",
                      "name": "testfile2",
                      "content_type": content_type_2}

        listing = [obj_dict_1, obj_dict_2]
        fake_body = json.dumps(listing)

        resp = self._make_cont_get_req(fake_body, 'json', override=True)

        self.assertEqual('200 OK', resp.status)
        body = resp.body
        self.assertEqual(len(body), int(resp.headers['Content-Length']))
        body_json = json.loads(body)
        self.assertEqual(2, len(body_json))
        obj_dict_1['content_type'] = content_type_1
        self.assertDictEqual(obj_dict_1, body_json[0])
        obj_dict_2['content_type'] = content_type_2
        self.assertDictEqual(obj_dict_2, body_json[1])

    def _assert_element_contains_dict(self, expected, element):
        for k, v in expected.items():
            entry = element.getElementsByTagName(k)
            self.assertIsNotNone(entry, 'Key %s not found' % k)
            actual = entry[0].childNodes[0].nodeValue
            self.assertEqual(v, actual,
                             "Expected %s but got %s for key %s"
                             % (v, actual, k))

    def test_cont_get_xml_req(self):
        content_type_1 = u'\uF10F\uD20D\uB30B\u9409'
        content_type_2 = 'text/plain; param=foo'

        fake_body = '''<?xml version="1.0" encoding="UTF-8"?>
<container name="testc">\
<object><hash>c6e8196d7f0fff6444b90861fe8d609d</hash><content_type>\
''' + content_type_1 + '''\
</content_type><name>testfile</name><bytes>16</bytes>\
<last_modified>2015-04-19T02:37:39.601660</last_modified></object>\
<object><hash>ac0374ed4d43635f803c82469d0b5a10</hash><content_type>\
''' + content_type_2 + '''\
</content_type><name>testfile2</name><bytes>24</bytes>\
<last_modified>2015-04-19T02:37:39.684740</last_modified></object>\
</container>'''

        resp = self._make_cont_get_req(fake_body, 'xml')
        self.assertEqual('200 OK', resp.status)
        body = resp.body
        self.assertEqual(len(body), int(resp.headers['Content-Length']))

        tree = minidom.parseString(body)
        containers = tree.getElementsByTagName('container')
        self.assertEqual(1, len(containers))
        self.assertEqual('testc',
                         containers[0].attributes.getNamedItem("name").value)

        objs = tree.getElementsByTagName('object')
        self.assertEqual(2, len(objs))

        obj_dict_1 = {"bytes": "16",
                      "last_modified": "2015-04-19T02:37:39.601660",
                      "hash": "c6e8196d7f0fff6444b90861fe8d609d",
                      "name": "testfile",
                      "content_type": content_type_1}
        self._assert_element_contains_dict(obj_dict_1, objs[0])
        obj_dict_2 = {"bytes": "24",
                      "last_modified": "2015-04-19T02:37:39.684740",
                      "hash": "ac0374ed4d43635f803c82469d0b5a10",
                      "name": "testfile2",
                      "content_type": content_type_2}
        self._assert_element_contains_dict(obj_dict_2, objs[1])

    def test_cont_get_xml_req_with_crypto_override(self):
        content_type_1 = 'image/jpeg'
        content_type_2 = 'text/plain; param=foo'

        fake_body = '''<?xml version="1.0" encoding="UTF-8"?>
<container name="testc">\
<object><hash>c6e8196d7f0fff6444b90861fe8d609d</hash>\
<content_type>''' + content_type_1 + '''\
</content_type><name>testfile</name><bytes>16</bytes>\
<last_modified>2015-04-19T02:37:39.601660</last_modified></object>\
<object><hash>ac0374ed4d43635f803c82469d0b5a10</hash>\
<content_type>''' + content_type_2 + '''\
</content_type><name>testfile2</name><bytes>24</bytes>\
<last_modified>2015-04-19T02:37:39.684740</last_modified></object>\
</container>'''

        resp = self._make_cont_get_req(fake_body, 'xml', override=True)

        self.assertEqual('200 OK', resp.status)
        body = resp.body
        self.assertEqual(len(body), int(resp.headers['Content-Length']))

        tree = minidom.parseString(body)
        containers = tree.getElementsByTagName('container')
        self.assertEqual(1, len(containers))
        self.assertEqual('testc',
                         containers[0].attributes.getNamedItem("name").value)

        objs = tree.getElementsByTagName('object')
        self.assertEqual(2, len(objs))

        obj_dict_1 = {"bytes": "16",
                      "last_modified": "2015-04-19T02:37:39.601660",
                      "hash": "c6e8196d7f0fff6444b90861fe8d609d",
                      "name": "testfile",
                      "content_type": content_type_1}
        self._assert_element_contains_dict(obj_dict_1, objs[0])
        obj_dict_2 = {"bytes": "24",
                      "last_modified": "2015-04-19T02:37:39.684740",
                      "hash": "ac0374ed4d43635f803c82469d0b5a10",
                      "name": "testfile2",
                      "content_type": content_type_2}
        self._assert_element_contains_dict(obj_dict_2, objs[1])


class TestModuleMethods(unittest.TestCase):
    def test_filter_factory(self):
        factory = decrypter.filter_factory({})
        self.assertTrue(callable(factory))
        self.assertIsInstance(factory(None), decrypter.Decrypter)


class TestDecrypter(unittest.TestCase):
    def test_app_exception(self):
        app = decrypter.Decrypter(
            FakeAppThatExcepts(), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        with self.assertRaises(HTTPException) as catcher:
            req.get_response(app)
        self.assertEqual(FakeAppThatExcepts.get_error_msg(),
                         catcher.exception.body)


if __name__ == '__main__':
    unittest.main()
