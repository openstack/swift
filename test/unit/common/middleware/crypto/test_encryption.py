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
import unittest
import uuid

from swift.common import storage_policy, constraints
from swift.common.middleware import copy
from swift.common.middleware import crypto
from swift.common.middleware.crypto import keymaster
from swift.common.middleware.crypto.crypto_utils import (
    load_crypto_meta, Crypto)
from swift.common.ring import Ring
from swift.common.swob import Request, str_to_wsgi
from swift.obj import diskfile

from test.unit import FakeLogger, skip_if_no_xattrs
from test.unit.common.middleware.crypto.crypto_helpers import (
    md5hex, encrypt, TEST_KEYMASTER_CONF)
from test.unit.helpers import setup_servers, teardown_servers


class TestCryptoPipelineChanges(unittest.TestCase):
    # Tests the consequences of crypto middleware being in/out of the pipeline
    # or having encryption disabled for PUT/GET requests on same object. Uses
    # real backend servers so that the handling of headers and sysmeta is
    # verified to diskfile and back.
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
        skip_if_no_xattrs()
        self.plaintext = b'unencrypted body content'
        self.plaintext_etag = md5hex(self.plaintext)
        self._setup_crypto_app()

    def _setup_crypto_app(self, disable_encryption=False, root_secret_id=None):
        # Set up a pipeline of crypto middleware ending in the proxy app so
        # that tests can make requests to either the proxy server directly or
        # via the crypto middleware. Make a fresh instance for each test to
        # avoid any state coupling.
        conf = {'disable_encryption': disable_encryption}
        self.encryption = crypto.filter_factory(conf)(self.proxy_app)
        self.encryption.logger = self.proxy_app.logger
        km_conf = dict(TEST_KEYMASTER_CONF)
        if root_secret_id is not None:
            km_conf['active_root_secret_id'] = root_secret_id
        self.km = keymaster.KeyMaster(self.encryption, km_conf)
        self.crypto_app = self.km  # for clarity
        self.crypto_app.logger = self.encryption.logger

    def _create_container(self, app, policy_name='one', container_path=None):
        if not container_path:
            # choose new container name so that the policy can be specified
            self.container_name = uuid.uuid4().hex
            self.container_path = 'http://foo:8080/v1/a/' + self.container_name
            self.object_name = 'o'
            self.object_path = self.container_path + '/' + self.object_name
            container_path = self.container_path
        req = Request.blank(
            str_to_wsgi(container_path), method='PUT',
            headers={'X-Storage-Policy': policy_name})
        resp = req.get_response(app)
        self.assertEqual('201 Created', resp.status)
        # sanity check
        req = Request.blank(
            str_to_wsgi(container_path), method='HEAD',
            headers={'X-Storage-Policy': policy_name})
        resp = req.get_response(app)
        self.assertEqual(policy_name, resp.headers['X-Storage-Policy'])

    def _put_object(self, app, body):
        req = Request.blank(
            str_to_wsgi(self.object_path), method='PUT', body=body,
            headers={'Content-Type': 'application/test'})
        resp = req.get_response(app)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        return resp

    def _post_object(self, app):
        req = Request.blank(str_to_wsgi(self.object_path), method='POST',
                            headers={'Content-Type': 'application/test',
                                     'X-Object-Meta-Fruit': 'Kiwi'})
        resp = req.get_response(app)
        self.assertEqual('202 Accepted', resp.status)
        return resp

    def _copy_object(self, app, destination):
        req = Request.blank(str_to_wsgi(self.object_path), method='COPY',
                            headers={'Destination': destination})
        resp = req.get_response(app)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        return resp

    def _check_GET_and_HEAD(self, app, object_path=None):
        object_path = str_to_wsgi(object_path or self.object_path)
        req = Request.blank(object_path, method='GET')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(self.plaintext, resp.body)
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

        req = Request.blank(object_path, method='HEAD')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(b'', resp.body)
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

    def _check_match_requests(self, method, app, object_path=None):
        object_path = str_to_wsgi(object_path or self.object_path)
        # verify conditional match requests
        expected_body = self.plaintext if method == 'GET' else b''

        # If-Match matches
        req = Request.blank(object_path, method=method,
                            headers={'If-Match': '"%s"' % self.plaintext_etag})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(expected_body, resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

        # If-Match wildcard
        req = Request.blank(object_path, method=method,
                            headers={'If-Match': '*'})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(expected_body, resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

        # If-Match does not match
        req = Request.blank(object_path, method=method,
                            headers={'If-Match': '"not the etag"'})
        resp = req.get_response(app)
        self.assertEqual('412 Precondition Failed', resp.status)
        self.assertEqual(b'', resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])

        # If-None-Match matches
        req = Request.blank(
            object_path, method=method,
            headers={'If-None-Match': '"%s"' % self.plaintext_etag})
        resp = req.get_response(app)
        self.assertEqual('304 Not Modified', resp.status)
        self.assertEqual(b'', resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])

        # If-None-Match wildcard
        req = Request.blank(object_path, method=method,
                            headers={'If-None-Match': '*'})
        resp = req.get_response(app)
        self.assertEqual('304 Not Modified', resp.status)
        self.assertEqual(b'', resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])

        # If-None-Match does not match
        req = Request.blank(object_path, method=method,
                            headers={'If-None-Match': '"not the etag"'})
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        self.assertEqual(expected_body, resp.body)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        self.assertEqual('Kiwi', resp.headers['X-Object-Meta-Fruit'])

    def _check_listing(self, app, expect_mismatch=False, container_path=None):
        container_path = str_to_wsgi(container_path or self.container_path)
        req = Request.blank(
            container_path, method='GET', query_string='format=json')
        resp = req.get_response(app)
        self.assertEqual('200 OK', resp.status)
        listing = json.loads(resp.body)
        self.assertEqual(1, len(listing))
        self.assertEqual(self.object_name, listing[0]['name'])
        self.assertEqual(len(self.plaintext), listing[0]['bytes'])
        if expect_mismatch:
            self.assertNotEqual(self.plaintext_etag, listing[0]['hash'])
        else:
            self.assertEqual(self.plaintext_etag, listing[0]['hash'])

    def test_write_with_crypto_and_override_headers(self):
        self._create_container(self.proxy_app, policy_name='one')

        def verify_overrides():
            # verify object sysmeta
            req = Request.blank(
                self.object_path, method='GET')
            resp = req.get_response(self.crypto_app)
            for k, v in overrides.items():
                self.assertIn(k, resp.headers)
                self.assertEqual(overrides[k], resp.headers[k])

            # check container listing
            req = Request.blank(
                self.container_path, method='GET', query_string='format=json')
            resp = req.get_response(self.crypto_app)
            self.assertEqual('200 OK', resp.status)
            listing = json.loads(resp.body)
            self.assertEqual(1, len(listing))
            self.assertEqual('o', listing[0]['name'])
            self.assertEqual(
                overrides['x-object-sysmeta-container-update-override-size'],
                str(listing[0]['bytes']))
            self.assertEqual(
                overrides['x-object-sysmeta-container-update-override-etag'],
                listing[0]['hash'])

        # include overrides in headers
        overrides = {'x-object-sysmeta-container-update-override-etag': 'foo',
                     'x-object-sysmeta-container-update-override-size':
                         str(len(self.plaintext) + 1)}
        req = Request.blank(self.object_path, method='PUT',
                            body=self.plaintext, headers=overrides.copy())
        resp = req.get_response(self.crypto_app)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        verify_overrides()

        # include overrides in footers
        overrides = {'x-object-sysmeta-container-update-override-etag': 'bar',
                     'x-object-sysmeta-container-update-override-size':
                         str(len(self.plaintext) + 2)}

        def callback(footers):
            footers.update(overrides)

        req = Request.blank(
            self.object_path, method='PUT', body=self.plaintext)
        req.environ['swift.callback.update_footers'] = callback
        resp = req.get_response(self.crypto_app)
        self.assertEqual('201 Created', resp.status)
        self.assertEqual(self.plaintext_etag, resp.headers['Etag'])
        verify_overrides()

    def test_write_with_crypto_read_with_crypto(self):
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)

    def test_write_with_crypto_read_with_crypto_different_root_secrets(self):
        root_secret = self.crypto_app.root_secret
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.crypto_app, self.plaintext)
        # change root secret
        self._setup_crypto_app(root_secret_id='1')
        root_secret_1 = self.crypto_app.root_secret
        self.assertNotEqual(root_secret, root_secret_1)  # sanity check
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)
        # change root secret
        self._setup_crypto_app(root_secret_id='2')
        root_secret_2 = self.crypto_app.root_secret
        self.assertNotEqual(root_secret_2, root_secret_1)  # sanity check
        self.assertNotEqual(root_secret_2, root_secret)  # sanity check
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)
        # write object again
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

    def test_put_without_crypto_post_with_crypto_read_with_crypto(self):
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.proxy_app, self.plaintext)
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
        self.assertEqual(b'', resp.body)
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
        self._setup_crypto_app(disable_encryption=True)
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)
        # check as if no crypto middleware exists
        self._check_GET_and_HEAD(self.proxy_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_match_requests('GET', self.proxy_app)
        self._check_match_requests('HEAD', self.proxy_app)

    def test_write_with_crypto_read_with_disable_encryption_conf(self):
        self._create_container(self.proxy_app, policy_name='one')
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)  # sanity check
        # turn on disable_encryption config option
        self._setup_crypto_app(disable_encryption=True)
        # GET and HEAD of encrypted objects should still work
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_listing(self.crypto_app, expect_mismatch=False)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)

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
                encrypt(self.plaintext_etag.encode('ascii'),
                        self.km.create_key('/a/%s' % self.container_name),
                        listing_etag_iv)).decode('ascii')
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
                contents = b''.join(df.reader())
                metadata = dict((k.lower(), v) for k, v in meta.items())
                # verify on disk data - body
                body_iv = load_crypto_meta(
                    metadata['x-object-sysmeta-crypto-body-meta'])['iv']
                body_key_meta = load_crypto_meta(
                    metadata['x-object-sysmeta-crypto-body-meta'])['body_key']
                obj_key = self.km.create_key('/a/%s/o' % self.container_name)
                body_key = Crypto().unwrap_key(obj_key, body_key_meta)
                exp_enc_body = encrypt(self.plaintext, body_key, body_iv)
                ondisk_data.append((node, contents))

                # verify on disk user metadata
                enc_val, meta = metadata[
                    'x-object-transient-sysmeta-crypto-meta-fruit'].split(';')
                meta = meta.strip()[len('swift_meta='):]
                metadata_iv = load_crypto_meta(meta)['iv']
                exp_enc_meta = base64.b64encode(encrypt(
                    b'Kiwi', obj_key, metadata_iv)).decode('ascii')
                self.assertEqual(exp_enc_meta, enc_val)
                self.assertNotIn('x-object-meta-fruit', metadata)

                self.assertIn(
                    'x-object-transient-sysmeta-crypto-meta', metadata)
                meta = load_crypto_meta(
                    metadata['x-object-transient-sysmeta-crypto-meta'])
                self.assertIn('key_id', meta)
                self.assertIn('path', meta['key_id'])
                self.assertEqual(
                    '/a/%s/%s' % (self.container_name, self.object_name),
                    meta['key_id']['path'])
                self.assertIn('v', meta['key_id'])
                self.assertEqual('2', meta['key_id']['v'])
                self.assertIn('cipher', meta)
                self.assertEqual(Crypto.cipher, meta['cipher'])

                # verify etag
                actual_enc_etag, _junk, actual_etag_meta = metadata[
                    'x-object-sysmeta-crypto-etag'].partition('; swift_meta=')
                etag_iv = load_crypto_meta(actual_etag_meta)['iv']
                exp_enc_etag = base64.b64encode(encrypt(
                    self.plaintext_etag.encode('ascii'),
                    obj_key, etag_iv)).decode('ascii')
                self.assertEqual(exp_enc_etag, actual_enc_etag)

                # verify etag hmac
                exp_etag_mac = hmac.new(
                    obj_key, self.plaintext_etag.encode('ascii'),
                    digestmod=hashlib.sha256).digest()
                exp_etag_mac = base64.b64encode(exp_etag_mac).decode('ascii')
                self.assertEqual(exp_etag_mac,
                                 metadata['x-object-sysmeta-crypto-etag-mac'])

                # verify etag override for container updates
                override = 'x-object-sysmeta-container-update-override-etag'
                parts = metadata[override].rsplit(';', 1)
                crypto_meta_param = parts[1].strip()
                crypto_meta = crypto_meta_param[len('swift_meta='):]
                listing_etag_iv = load_crypto_meta(crypto_meta)['iv']
                cont_key = self.km.create_key('/a/%s' % self.container_name)
                exp_enc_listing_etag = base64.b64encode(
                    encrypt(self.plaintext_etag.encode('ascii'), cont_key,
                            listing_etag_iv)).decode('ascii')
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

    def _test_copy_encrypted_to_encrypted(
            self, src_policy_name, dest_policy_name):
        self._create_container(self.proxy_app, policy_name=src_policy_name)
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)

        copy_crypto_app = copy.ServerSideCopyMiddleware(self.crypto_app, {})

        dest_container = uuid.uuid4().hex
        dest_container_path = 'http://localhost:8080/v1/a/' + dest_container
        self._create_container(copy_crypto_app, policy_name=dest_policy_name,
                               container_path=dest_container_path)
        dest_obj_path = dest_container_path + '/o'
        dest = '/%s/%s' % (dest_container, 'o')
        self._copy_object(copy_crypto_app, dest)

        self._check_GET_and_HEAD(copy_crypto_app, object_path=dest_obj_path)
        self._check_listing(
            copy_crypto_app, container_path=dest_container_path)
        self._check_match_requests(
            'GET', copy_crypto_app, object_path=dest_obj_path)
        self._check_match_requests(
            'HEAD', copy_crypto_app, object_path=dest_obj_path)

    def test_copy_encrypted_to_encrypted(self):
        self._test_copy_encrypted_to_encrypted('ec', 'ec')
        self._test_copy_encrypted_to_encrypted('one', 'ec')
        self._test_copy_encrypted_to_encrypted('ec', 'one')
        self._test_copy_encrypted_to_encrypted('one', 'one')

    def _test_copy_encrypted_to_unencrypted(
            self, src_policy_name, dest_policy_name):
        self._create_container(self.proxy_app, policy_name=src_policy_name)
        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)

        # make a pipeline with encryption disabled, use it to copy object
        self._setup_crypto_app(disable_encryption=True)
        copy_app = copy.ServerSideCopyMiddleware(self.crypto_app, {})

        dest_container = uuid.uuid4().hex
        dest_container_path = 'http://localhost:8080/v1/a/' + dest_container
        self._create_container(self.crypto_app, policy_name=dest_policy_name,
                               container_path=dest_container_path)
        dest_obj_path = dest_container_path + '/o'
        dest = '/%s/%s' % (dest_container, 'o')
        self._copy_object(copy_app, dest)

        self._check_GET_and_HEAD(copy_app, object_path=dest_obj_path)
        self._check_GET_and_HEAD(self.proxy_app, object_path=dest_obj_path)
        self._check_listing(copy_app, container_path=dest_container_path)
        self._check_listing(self.proxy_app, container_path=dest_container_path)
        self._check_match_requests(
            'GET', self.proxy_app, object_path=dest_obj_path)
        self._check_match_requests(
            'HEAD', self.proxy_app, object_path=dest_obj_path)

    def test_copy_encrypted_to_unencrypted(self):
        self._test_copy_encrypted_to_unencrypted('ec', 'ec')
        self._test_copy_encrypted_to_unencrypted('one', 'ec')
        self._test_copy_encrypted_to_unencrypted('ec', 'one')
        self._test_copy_encrypted_to_unencrypted('one', 'one')

    def _test_copy_unencrypted_to_encrypted(
            self, src_policy_name, dest_policy_name):
        self._create_container(self.proxy_app, policy_name=src_policy_name)
        self._put_object(self.proxy_app, self.plaintext)
        self._post_object(self.proxy_app)

        copy_crypto_app = copy.ServerSideCopyMiddleware(self.crypto_app, {})

        dest_container = uuid.uuid4().hex
        dest_container_path = 'http://localhost:8080/v1/a/' + dest_container
        self._create_container(copy_crypto_app, policy_name=dest_policy_name,
                               container_path=dest_container_path)
        dest_obj_path = dest_container_path + '/o'
        dest = '/%s/%s' % (dest_container, 'o')
        self._copy_object(copy_crypto_app, dest)

        self._check_GET_and_HEAD(copy_crypto_app, object_path=dest_obj_path)
        self._check_listing(
            copy_crypto_app, container_path=dest_container_path)
        self._check_match_requests(
            'GET', copy_crypto_app, object_path=dest_obj_path)
        self._check_match_requests(
            'HEAD', copy_crypto_app, object_path=dest_obj_path)

    def test_copy_unencrypted_to_encrypted(self):
        self._test_copy_unencrypted_to_encrypted('ec', 'ec')
        self._test_copy_unencrypted_to_encrypted('one', 'ec')
        self._test_copy_unencrypted_to_encrypted('ec', 'one')
        self._test_copy_unencrypted_to_encrypted('one', 'one')

    def test_crypto_max_length_path(self):
        # the path is stashed in the key_id in crypto meta; check that a long
        # path is ok
        self.container_name = 'c' * constraints.MAX_CONTAINER_NAME_LENGTH
        self.object_name = 'o' * constraints.MAX_OBJECT_NAME_LENGTH
        self.container_path = 'http://foo:8080/v1/a/' + self.container_name
        self.object_path = '%s/%s' % (self.container_path, self.object_name)

        self._create_container(self.proxy_app, policy_name='one',
                               container_path=self.container_path)

        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)

    def test_crypto_UTF8_path(self):
        # check that UTF8 path is ok
        self.container_name = self.object_name = u'\u010brypto'
        self.container_path = 'http://foo:8080/v1/a/' + self.container_name
        self.object_path = '%s/%s' % (self.container_path, self.object_name)

        self._create_container(self.proxy_app, policy_name='one',
                               container_path=self.container_path)

        self._put_object(self.crypto_app, self.plaintext)
        self._post_object(self.crypto_app)
        self._check_GET_and_HEAD(self.crypto_app)
        self._check_match_requests('GET', self.crypto_app)
        self._check_match_requests('HEAD', self.crypto_app)
        self._check_listing(self.crypto_app)


if __name__ == '__main__':
    unittest.main()
