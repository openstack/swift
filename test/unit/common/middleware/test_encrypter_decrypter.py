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
import uuid

from swift.common import storage_policy
from swift.common.middleware import encrypter, decrypter, keymaster, crypto
from swift.common.ring import Ring
from swift.common.swob import Request
from swift.common.crypto_utils import load_crypto_meta

from test.unit.common.middleware.crypto_helpers import md5hex, encrypt
from test.unit.helpers import setup_servers, teardown_servers
from swift.obj import diskfile
from test.unit import FakeLogger


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
        self.container_name = uuid.uuid4().hex
        self.container_path = 'http://localhost:8080/v1/a/' + \
                              self.container_name
        self.object_path = self.container_path + '/o'
        self.plaintext = 'unencrypted body content'
        self.plaintext_etag = md5hex(self.plaintext)

        # Set up a pipeline of crypto middleware ending in the proxy app so
        # that tests can make requests to either the proxy server directly or
        # via the crypto middleware. Make a fresh instance for each test to
        # avoid any state coupling.
        enc = encrypter.Encrypter(self.proxy_app, {})
        self.km = keymaster.KeyMaster(enc,
                                      {'encryption_root_secret': 's3cr3t'})
        self.crypto_app = decrypter.Decrypter(self.km, {})

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
        req = Request.blank(self.object_path, method='PUT', body=body,
                            headers={'Content-Type': 'application/test'})
        resp = req.get_response(app)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        return resp

    def _post_object(self, app):
        req = Request.blank(self.object_path, method='POST',
                            headers={'Content-Type': 'application/test',
                                     'X-Object-Meta-Fruit': 'Kiwi'})
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

    def test_write_without_crypto_read_with_crypto_ec(self):
        self._create_container(self.proxy_app, policy_name='ec')
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

    def test_disable_encryption_config_option(self):
        # check that on disable_encryption = true, object is not encrypted
        enc = encrypter.Encrypter(
            self.proxy_app, {'disable_encryption': 'true'})
        km = keymaster.KeyMaster(enc, {'encryption_root_secret': 's3cr3t'})
        crypto_app = decrypter.Decrypter(km, {})
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(crypto_app, self.plaintext)
        self._post_object(crypto_app)
        self._check_GET_and_HEAD(crypto_app)
        # check as if no crypto middleware exists
        self._check_GET_and_HEAD(self.proxy_app)
        self._check_match_requests('GET', crypto_app)
        self._check_match_requests('HEAD', crypto_app)
        self._check_match_requests('GET', self.proxy_app)
        self._check_match_requests('HEAD', self.proxy_app)

    def test_write_with_crypto_read_with_disable_encryption_conf(self):
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)  # sanity check
        # turn on disable_encryption config option
        enc = encrypter.Encrypter(
            self.proxy_app, {'disable_encryption': 'true'})
        km = keymaster.KeyMaster(enc, {'encryption_root_secret': 's3cr3t'})
        crypto_app = decrypter.Decrypter(km, {})
        # GET and HEAD of encrypted objects should still work
        self._check_GET_and_HEAD(crypto_app)
        self._check_listing(crypto_app, expect_mismatch=False)
        self._check_match_requests('GET', crypto_app)
        self._check_match_requests('HEAD', crypto_app)

    def _test_ondisk_data_after_write_with_crypto(self, policy_name):
        policy = storage_policy.POLICIES.get_by_name(policy_name)
        self._create_container(self.proxy_app, policy_name=policy_name)
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)

        # Verify container listing etag is encrypted by direct GET to container
        # server. We can use any server for all nodes since they all share same
        # devices dir.
        cont_server = self._test_context['test_servers'][3]
        cont_ring = Ring(self._test_context['testdir'], ring_name='container')
        part, nodes = cont_ring.get_nodes('a', self.container_name)
        for node in nodes:
            req = Request.blank('/%s/%s/a/%s'
                                % (node['device'], part, self.container_name),
                                method='GET', query_string='format=json')
            resp = req.get_response(cont_server)
            listing = json.loads(resp.body)
            # sanity checks...
            self.assertEqual(1, len(listing))
            self.assertEqual('o', listing[0]['name'])
            self.assertEqual('application/test', listing[0]['content_type'])
            # verify encrypted etag value
            parts = listing[0]['hash'].rsplit(';', 1)
            crypto_meta_param = parts[1].strip()
            crypto_meta = crypto_meta_param[len('swift_meta='):]
            listing_etag_iv = load_crypto_meta(crypto_meta)['iv']
            exp_enc_listing_etag = base64.b64encode(
                encrypt(self.plaintext_etag,
                        self.km.create_key('/a/%s' % self.container_name),
                        listing_etag_iv))
            self.assertEqual(exp_enc_listing_etag, parts[0])

        # Verify diskfile data and metadata is encrypted
        ring_object = self.proxy_app.get_object_ring(int(policy))
        partition, nodes = ring_object.get_nodes('a', self.container_name, 'o')
        conf = {'devices': self._test_context["testdir"],
                'mount_check': 'false'}
        df_mgr = diskfile.DiskFileRouter(conf, FakeLogger())[policy]
        ondisk_data = []
        exp_enc_body = None
        for node_index, node in enumerate(nodes):
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', self.container_name, 'o',
                                     policy=policy)
            with df.open():
                meta = df.get_metadata()
                contents = ''.join(df.reader())
                metadata = dict((k.lower(), v) for k, v in meta.items())
                # verify on disk data - body
                body_iv = load_crypto_meta(
                    metadata['x-object-sysmeta-crypto-meta'])['iv']
                body_key_meta = load_crypto_meta(
                    metadata['x-object-sysmeta-crypto-meta'])['body_key']
                obj_key = self.km.create_key('/a/%s/o' % self.container_name)
                body_key = crypto.Crypto({}).unwrap_key(obj_key, body_key_meta)
                exp_enc_body = encrypt(self.plaintext, body_key, body_iv)
                ondisk_data.append((node, contents))
                # verify on disk user metadata
                metadata_iv = load_crypto_meta(
                    metadata['x-object-transient-sysmeta-crypto-meta-fruit']
                )['iv']
                exp_enc_meta = base64.b64encode(encrypt('Kiwi', obj_key,
                                                        metadata_iv))
                self.assertEqual(exp_enc_meta, metadata['x-object-meta-fruit'])
                # verify etag
                etag_iv = load_crypto_meta(
                    metadata['x-object-sysmeta-crypto-meta-etag'])['iv']
                etag_key = self.km.create_key('/a/%s' % self.container_name)
                exp_enc_etag = base64.b64encode(encrypt(self.plaintext_etag,
                                                        etag_key, etag_iv))
                self.assertEqual(exp_enc_etag,
                                 metadata['x-object-sysmeta-crypto-etag'])
                # verify etag override for container updates
                override = 'x-object-sysmeta-container-update-override-etag'
                parts = metadata[override].rsplit(';', 1)
                crypto_meta_param = parts[1].strip()
                crypto_meta = crypto_meta_param[len('swift_meta='):]
                listing_etag_iv = load_crypto_meta(crypto_meta)['iv']
                exp_enc_listing_etag = base64.b64encode(
                    encrypt(self.plaintext_etag, etag_key,
                            listing_etag_iv))
                self.assertEqual(exp_enc_listing_etag, parts[0])

        self._check_GET_and_HEAD(self.crypto_app)
        return exp_enc_body, ondisk_data

    def test_ondisk_data_after_write_with_crypto(self):
        exp_body, ondisk_data = self._test_ondisk_data_after_write_with_crypto(
            policy_name='one')
        for node, body in ondisk_data:
            self.assertEqual(exp_body, body)

    def test_ondisk_data_after_write_with_crypto_ec(self):
        exp_body, ondisk_data = self._test_ondisk_data_after_write_with_crypto(
            policy_name='ec')
        policy = storage_policy.POLICIES.get_by_name('ec')
        for frag_selection in (ondisk_data[:2], ondisk_data[1:]):
            frags = [frag for node, frag in frag_selection]
            self.assertEqual(exp_body, policy.pyeclib_driver.decode(frags))


class TestCryptoPipelineChangesFastPost(TestCryptoPipelineChanges):
    @classmethod
    def setUpClass(cls):
        # set proxy config to use fast post
        extra_conf = {'object_post_as_copy': 'False'}
        cls._test_context = setup_servers(extra_conf=extra_conf)
        cls.proxy_app = cls._test_context["test_servers"][0]


if __name__ == '__main__':
    unittest.main()
