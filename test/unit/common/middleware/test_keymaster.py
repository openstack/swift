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
import unittest

from swift.common import swob
from swift.common.middleware import keymaster
from swift.common.middleware.crypto_utils import CRYPTO_KEY_CALLBACK
from swift.common.swob import Request
from test.unit.common.middleware.helpers import FakeSwift, FakeAppThatExcepts


def capture_start_response():
    calls = []

    def start_response(*args):
        calls.append(args)
    return start_response, calls


class TestKeymaster(unittest.TestCase):

    def setUp(self):
        super(TestKeymaster, self).setUp()
        self.swift = FakeSwift()

    def test_object_path(self):
        self.verify_keys_for_path(
            '/v1/a/c/o', expected_keys=('object', 'container'),
            key_id=base64.b64encode('/a/c/o'))

    def test_container_path(self):
        self.verify_keys_for_path(
            '/v1/a/c', expected_keys=('container',))

    def verify_keys_for_path(self, path, expected_keys, key_id=None):
        put_keys = None
        app = keymaster.KeyMaster(self.swift,
                                  {'encryption_root_secret': 'secret'})
        for method, resp_class, status in (
                ('PUT', swob.HTTPCreated, '201'),
                ('POST', swob.HTTPAccepted, '202'),
                ('GET', swob.HTTPOk, '200'),
                ('HEAD', swob.HTTPNoContent, '204')):
            resp_headers = {}
            if key_id is not None:
                resp_headers.update({'X-Object-Sysmeta-Crypto-Id': key_id})
            self.swift.register(method, path, resp_class, resp_headers, '')
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertTrue(calls[0][0].startswith(status))
            self.assertNotIn('swift.crypto.override', req.environ)
            self.assertIn(CRYPTO_KEY_CALLBACK, req.environ,
                          '%s not set in env' % CRYPTO_KEY_CALLBACK)
            keys = req.environ.get(CRYPTO_KEY_CALLBACK)()
            self.assertListEqual(sorted(expected_keys), sorted(keys.keys()),
                                 '%s %s got keys %r, but expected %r'
                                 % (method, path, keys.keys(), expected_keys))
            if put_keys is not None:
                # check all key sets were consistent for this path
                self.assertDictEqual(put_keys, keys)
            else:
                put_keys = keys
        return put_keys

    def test_key_uniqueness(self):
        # a rudimentary check that different keys are made for different paths
        ref_path_parts = ('a1', 'c1', 'o1')
        path = '/' + '/'.join(ref_path_parts)
        ref_keys = self.verify_keys_for_path(
            '/v1' + path, expected_keys=('object', 'container'),
            key_id=base64.b64encode(path))

        # for same path and for each differing path check that keys are unique
        # when path to object or container is unique and vice-versa
        for path_parts in [(a, c, o) for a in ('a1', 'a2')
                           for c in ('c1', 'c2')
                           for o in ('o1', 'o2')]:
            path = '/' + '/'.join(path_parts)
            keys = self.verify_keys_for_path(
                '/v1' + path, expected_keys=('object', 'container'),
                key_id=base64.b64encode(path))
            # object keys should only be equal when complete paths are equal
            self.assertEqual(path_parts == ref_path_parts,
                             keys['object'] == ref_keys['object'],
                             'Path %s keys:\n%s\npath %s keys\n%s' %
                             (ref_path_parts, ref_keys, path_parts, keys))
            # container keys should only be equal when paths to container are
            # equal
            self.assertEqual(path_parts[:2] == ref_path_parts[:2],
                             keys['container'] == ref_keys['container'],
                             'Path %s keys:\n%s\npath %s keys\n%s' %
                             (ref_path_parts, ref_keys, path_parts, keys))

    def test_object_with_different_key_id(self):
        # object was put using different path; stored key_id should be used
        # to generate keys, not the GET path
        path = '/v1/a/c/o'
        key_id = base64.b64encode('/a/c/o')
        resp_headers = {'X-Object-Sysmeta-Crypto-Id': key_id}
        # first get keys when path matches key_id
        method = 'HEAD'
        self.swift.register(method, path, swob.HTTPOk, resp_headers, '')
        app = keymaster.KeyMaster(self.swift,
                                  {'encryption_root_secret': 'secret'})
        req = Request.blank(path, environ={'REQUEST_METHOD': method})
        start_response, calls = capture_start_response()
        app(req.environ, start_response)
        self.assertEqual(1, len(calls))
        self.assertEqual('200 OK', calls[0][0])
        self.assertIn(CRYPTO_KEY_CALLBACK, req.environ)
        expected_keys = req.environ.get(CRYPTO_KEY_CALLBACK)()

        # now change path but verify that keys match key_id, not path
        path = '/v1/a/got/relocated'
        for method in ('HEAD', 'GET'):
            self.swift.register(method, path, swob.HTTPOk, resp_headers, '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('200 OK', calls[0][0])
            self.assertIn(CRYPTO_KEY_CALLBACK, req.environ)
            actual_keys = req.environ.get(CRYPTO_KEY_CALLBACK)()
            self.assertDictEqual(expected_keys, actual_keys)

    def test_object_with_no_key_id(self):
        # object was not put using keymaster so has no key id, that's ok
        for method in ('HEAD', 'GET'):
            path = '/v1/a/c/o'
            self.swift.register(method, path, swob.HTTPOk, {}, '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('200 OK', calls[0][0])
            self.assertIn('swift.crypto.override', req.environ)
            self.assertNotIn(CRYPTO_KEY_CALLBACK, req.environ)

    def test_object_with_no_key_id_but_crypto_meta(self):
        # object should have a key id if it has any
        # x-object-sysmeta-crypto-meta or
        # x-object-transient-sysmeta-crypto-meta- header
        path = '/v1/a/c/o'
        for method in ('HEAD', 'GET'):
            # object has x-object-transient-sysmeta-crypto header but no key id
            self.swift.register(
                method, path, swob.HTTPOk,
                {'x-object-transient-sysmeta-crypto-meta-foo': 'gotcha',
                 'x-object-meta-foo': 'ciphertext of user meta value'},
                '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('422 Unprocessable Entity', calls[0][0])

            # object has x-object-sysmeta-crypto-meta but no key id
            self.swift.register(
                method, path, swob.HTTPOk,
                {'x-object-sysmeta-crypto-meta': 'gotcha'},
                '')
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('422 Unprocessable Entity', calls[0][0])

        # but "crypto-meta" in other headers is ok
        path = '/v1/a/c/o'
        for method in ('HEAD', 'GET'):
            self.swift.register(method, path, swob.HTTPOk,
                                {'x-object-sysmeta-foo-crypto-meta': 'ok',
                                 'x-object-sysmeta-foo-crypto-metabolic': 'ok',
                                 'x-object-meta-crypto-meta': 'no probs',
                                 'crypto-meta': 'pas de problem'},
                                '')
            app = keymaster.KeyMaster(self.swift,
                                      {'encryption_root_secret': 'secret'})
            req = Request.blank(path, environ={'REQUEST_METHOD': method})
            start_response, calls = capture_start_response()
            app(req.environ, start_response)
            self.assertEqual(1, len(calls))
            self.assertEqual('200 OK', calls[0][0])

    def test_filter(self):
        factory = keymaster.filter_factory(
            {'encryption_root_secret': 'secret'})
        self.assertTrue(callable(factory))
        self.assertTrue(callable(factory(self.swift)))

    def test_app_exception(self):
        app = keymaster.KeyMaster(
            FakeAppThatExcepts(), {'encryption_root_secret': 'secret'})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'PUT'})
        start_response, _ = capture_start_response()
        self.assertRaises(Exception, app, req.environ, start_response)

    def test_key_loaded(self):
        app = keymaster.KeyMaster(self.swift,
                                  {'encryption_root_secret': 'secret'})
        self.assertEqual('secret', app.root_secret)

    def test_no_root_secret_error(self):
        with self.assertRaises(ValueError) as err:
            keymaster.KeyMaster(self.swift, {})

        self.assertEqual('encryption_root_secret not set in proxy-server.conf',
                         err.exception.message)


if __name__ == '__main__':
    unittest.main()
