#!/usr/bin/python -u
# Copyright (c) 2010-2016 OpenStack Foundation
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
import functools
import hmac
import hashlib
import json
from copy import deepcopy
import urllib.parse
from time import time, strftime, gmtime
from unittest import SkipTest

import test.functional as tf
from swift.common.middleware import tempurl
from test.functional import cluster_info
from test.functional.tests import Utils, Base, Base2, BaseEnv
from test.functional import requires_acls
from test.functional.swift_test_client import Account, Connection, \
    ResponseError


def tempurl_parms(method, expires, path, key, digest=None):
    path = urllib.parse.unquote(path)
    sig = hmac.new(
        key.encode('utf8'),
        b'%s\n%d\n%s' % (method.encode('utf8'), expires, path.encode('utf8')),
        digest or hashlib.sha256).hexdigest()
    return {'temp_url_sig': sig, 'temp_url_expires': str(expires)}


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestTempurlBaseEnv(BaseEnv):
    original_account_meta = None

    @classmethod
    def setUp(cls):
        super(TestTempurlBaseEnv, cls).setUp()
        cls.original_account_meta = cls.account.info()

    @classmethod
    def tearDown(cls):
        if cls.original_account_meta:
            # restore any tempurl keys that the tests may have overwritten
            cls.account.update_metadata(
                dict((k, cls.original_account_meta.get(k, ''))
                     for k in ('temp-url-key', 'temp-url-key-2',)))


class TestTempurlEnv(TestTempurlBaseEnv):
    tempurl_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        if cls.tempurl_enabled is None:
            cls.tempurl_enabled = 'tempurl' in cluster_info
            if not cls.tempurl_enabled:
                return

        super(TestTempurlEnv, cls).setUp()

        cls.tempurl_key = Utils.create_name()
        cls.tempurl_key2 = Utils.create_name()

        cls.account.update_metadata({
            'temp-url-key': cls.tempurl_key,
            'temp-url-key-2': cls.tempurl_key2
        })

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create():
            raise ResponseError(cls.conn.response)

        cls.obj = cls.container.file(Utils.create_name())
        cls.obj.write(b"obj contents")
        cls.other_obj = cls.container.file(Utils.create_name())
        cls.other_obj.write(b"other obj contents")


class TestTempurl(Base):
    env = TestTempurlEnv
    digest_name = 'sha256'

    def setUp(self):
        super(TestTempurl, self).setUp()
        if self.env.tempurl_enabled is False:
            raise SkipTest("TempURL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

        # N.B. The default to 'sha1' in case the info has nothing is for
        # extremely old clusters, which presumably use SHA1.
        if self.digest_name not in cluster_info['tempurl'].get(
                'allowed_digests', ['sha1']):
            raise SkipTest("tempurl does not support %s signatures" %
                           self.digest_name)

        self.digest = getattr(hashlib, self.digest_name)
        self.expires = int(time()) + 86400
        self.expires_8601 = strftime(
            tempurl.EXPIRES_ISO8601_FORMAT, gmtime(self.expires))
        self.obj_tempurl_parms = self.tempurl_parms(
            'GET', self.expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)

    def tempurl_parms(self, method, expires, path, key):
        return tempurl_parms(method, expires, path, key, self.digest)

    def test_GET(self):
        for e in (str(self.expires), self.expires_8601):
            self.obj_tempurl_parms['temp_url_expires'] = e

            contents = self.env.obj.read(
                parms=self.obj_tempurl_parms,
                cfg={'no_auth_token': True})
            self.assertEqual(contents, b"obj contents")

            # GET tempurls also allow HEAD requests
            self.assertTrue(self.env.obj.info(parms=self.obj_tempurl_parms,
                                              cfg={'no_auth_token': True}))

    def test_GET_with_key_2(self):
        expires = int(time()) + 86400
        parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key2)

        contents = self.env.obj.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

    def test_GET_DLO_inside_container(self):
        seg1 = self.env.container.file(
            "get-dlo-inside-seg1" + Utils.create_name())
        seg2 = self.env.container.file(
            "get-dlo-inside-seg2" + Utils.create_name())
        seg1.write(b"one fish two fish ")
        seg2.write(b"red fish blue fish")

        manifest = self.env.container.file("manifest" + Utils.create_name())
        manifest.write(
            b'',
            hdrs={"X-Object-Manifest": "%s/get-dlo-inside-seg" %
                  (self.env.container.name,)})

        expires = int(time()) + 86400
        parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)

        contents = manifest.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, b"one fish two fish red fish blue fish")

    def test_GET_DLO_outside_container(self):
        seg1 = self.env.container.file(
            "get-dlo-outside-seg1" + Utils.create_name())
        seg2 = self.env.container.file(
            "get-dlo-outside-seg2" + Utils.create_name())
        seg1.write(b"one fish two fish ")
        seg2.write(b"red fish blue fish")

        container2 = self.env.account.container(Utils.create_name())
        container2.create()

        manifest = container2.file("manifest" + Utils.create_name())
        manifest.write(
            b'',
            hdrs={"X-Object-Manifest": "%s/get-dlo-outside-seg" %
                  (self.env.container.name,)})

        expires = int(time()) + 86400
        parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)

        # cross container tempurl works fine for account tempurl key
        contents = manifest.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, b"one fish two fish red fish blue fish")
        self.assert_status([200])

    def test_PUT(self):
        new_obj = self.env.container.file(Utils.create_name())

        expires = int(time()) + 86400
        expires_8601 = strftime(
            tempurl.EXPIRES_ISO8601_FORMAT, gmtime(expires))

        put_parms = self.tempurl_parms(
            'PUT', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)
        for e in (str(expires), expires_8601):
            put_parms['temp_url_expires'] = e

            new_obj.write(b'new obj contents',
                          parms=put_parms, cfg={'no_auth_token': True})
            self.assertEqual(new_obj.read(), b"new obj contents")

            # PUT tempurls also allow HEAD requests
            self.assertTrue(new_obj.info(parms=put_parms,
                                         cfg={'no_auth_token': True}))

    def test_PUT_manifest_access(self):
        new_obj = self.env.container.file(Utils.create_name())

        # give out a signature which allows a PUT to new_obj
        expires = int(time()) + 86400
        put_parms = self.tempurl_parms(
            'PUT', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)

        # try to create manifest pointing to some random container
        try:
            new_obj.write(b'', {
                'x-object-manifest': '%s/foo' % 'some_random_container'
            }, parms=put_parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

        # create some other container
        other_container = self.env.account.container(Utils.create_name())
        if not other_container.create():
            raise ResponseError(self.conn.response)

        # try to create manifest pointing to new container
        try:
            new_obj.write(b'', {
                'x-object-manifest': '%s/foo' % other_container
            }, parms=put_parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

        # try again using a tempurl POST to an already created object
        new_obj.write(b'', {}, parms=put_parms, cfg={'no_auth_token': True})
        expires = int(time()) + 86400
        post_parms = self.tempurl_parms(
            'POST', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)
        try:
            new_obj.post({'x-object-manifest': '%s/foo' % other_container},
                         parms=post_parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 400)
        else:
            self.fail('request did not error')

    def test_HEAD(self):
        expires = int(time()) + 86400
        head_parms = self.tempurl_parms(
            'HEAD', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)

        self.assertTrue(self.env.obj.info(parms=head_parms,
                                          cfg={'no_auth_token': True}))

        # HEAD tempurls don't allow PUT or GET requests, despite the fact that
        # PUT and GET tempurls both allow HEAD requests
        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=head_parms)
        self.assert_status([401])

        self.assertRaises(ResponseError, self.env.other_obj.write,
                          b'new contents',
                          cfg={'no_auth_token': True},
                          parms=head_parms)
        self.assert_status([401])

    def test_different_object(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

    def test_changing_sig(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_sig'][0] == 'a':
            parms['temp_url_sig'] = 'b' + parms['temp_url_sig'][1:]
        else:
            parms['temp_url_sig'] = 'a' + parms['temp_url_sig'][1:]

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    def test_changing_expires(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_expires'][-1] == '0':
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '1'
        else:
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '0'

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])


class TestTempurlPrefix(TestTempurl):
    def tempurl_parms(self, method, expires, path, key,
                      prefix=None):
        path_parts = urllib.parse.unquote(path).split('/')

        if prefix is None:
            # Choose the first 4 chars of object name as prefix.
            prefix = path_parts[4][:4]
        prefix_to_hash = '/'.join(path_parts[0:4]) + '/' + prefix
        parms = tempurl_parms(
            method, expires,
            'prefix:' + prefix_to_hash,
            key, self.digest)
        parms['temp_url_prefix'] = prefix
        return parms

    def test_empty_prefix(self):
        parms = self.tempurl_parms(
            'GET', self.expires,
            self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key, '')

        contents = self.env.obj.read(
            parms=parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

    def test_no_prefix_match(self):
        prefix = 'b' if self.env.obj.name[0] == 'a' else 'a'

        parms = self.tempurl_parms(
            'GET', self.expires,
            self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key, prefix)

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    def test_object_url_with_prefix(self):
        parms = super(TestTempurlPrefix, self).tempurl_parms(
            'GET', self.expires,
            self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        parms['temp_url_prefix'] = self.env.obj.name

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    def test_missing_query_parm(self):
        del self.obj_tempurl_parms['temp_url_prefix']

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])


class TestTempurlUTF8(Base2, TestTempurl):
    pass


class TestTempurlPrefixUTF8(Base2, TestTempurlPrefix):
    pass


class TestContainerTempurlEnv(BaseEnv):
    tempurl_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        if cls.tempurl_enabled is None:
            cls.tempurl_enabled = 'tempurl' in cluster_info
            if not cls.tempurl_enabled:
                return

        super(TestContainerTempurlEnv, cls).setUp()

        cls.tempurl_key = Utils.create_name()
        cls.tempurl_key2 = Utils.create_name()

        if not tf.skip2:
            # creating another account and connection
            # for ACL tests
            config2 = deepcopy(tf.config)
            config2['account'] = tf.config['account2']
            config2['username'] = tf.config['username2']
            config2['password'] = tf.config['password2']
            cls.conn2 = Connection(config2)
            cls.conn2.authenticate()
            cls.account2 = Account(
                cls.conn2, config2.get('account', config2['username']))
            cls.account2 = cls.conn2.get_account()

        cls.container = cls.account.container(Utils.create_name())
        if not tf.skip2:
            if not cls.container.create({
                    'x-container-meta-temp-url-key': cls.tempurl_key,
                    'x-container-meta-temp-url-key-2': cls.tempurl_key2,
                    'x-container-read': cls.account2.name}):
                raise ResponseError(cls.conn.response)
        else:
            if not cls.container.create({
                    'x-container-meta-temp-url-key': cls.tempurl_key,
                    'x-container-meta-temp-url-key-2': cls.tempurl_key2}):
                raise ResponseError(cls.conn.response)

        cls.obj = cls.container.file(Utils.create_name())
        cls.obj.write(b"obj contents")
        cls.other_obj = cls.container.file(Utils.create_name())
        cls.other_obj.write(b"other obj contents")


class TestContainerTempurl(Base):
    env = TestContainerTempurlEnv
    digest_name = 'sha256'

    def setUp(self):
        super(TestContainerTempurl, self).setUp()
        if self.env.tempurl_enabled is False:
            raise SkipTest("TempURL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

        if self.digest_name not in cluster_info['tempurl'].get(
                'allowed_digests', ['sha1']):
            raise SkipTest("tempurl does not support %s signatures" %
                           self.digest_name)

        self.digest = getattr(hashlib, self.digest_name)
        expires = int(time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        self.obj_tempurl_parms = {'temp_url_sig': sig,
                                  'temp_url_expires': str(expires)}

    def tempurl_sig(self, method, expires, path, key):
        path = urllib.parse.unquote(path)
        method = method.encode('utf8')
        path = path.encode('utf8')
        key = key.encode('utf8')
        return hmac.new(
            key,
            b'%s\n%d\n%s' % (method, expires, path),
            self.digest).hexdigest()

    def test_GET(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        # GET tempurls also allow HEAD requests
        self.assertTrue(self.env.obj.info(parms=self.obj_tempurl_parms,
                                          cfg={'no_auth_token': True}))

    def test_GET_with_key_2(self):
        expires = int(time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key2)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = self.env.obj.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

    def test_PUT(self):
        new_obj = self.env.container.file(Utils.create_name())

        expires = int(time()) + 86400
        sig = self.tempurl_sig(
            'PUT', expires, self.env.conn.make_path(new_obj.path),
            self.env.tempurl_key)
        put_parms = {'temp_url_sig': sig,
                     'temp_url_expires': str(expires)}

        new_obj.write(b'new obj contents',
                      parms=put_parms, cfg={'no_auth_token': True})
        self.assertEqual(new_obj.read(), b"new obj contents")

        # PUT tempurls also allow HEAD requests
        self.assertTrue(new_obj.info(parms=put_parms,
                                     cfg={'no_auth_token': True}))

    def test_HEAD(self):
        expires = int(time()) + 86400
        sig = self.tempurl_sig(
            'HEAD', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        head_parms = {'temp_url_sig': sig,
                      'temp_url_expires': str(expires)}

        self.assertTrue(self.env.obj.info(parms=head_parms,
                                          cfg={'no_auth_token': True}))
        # HEAD tempurls don't allow PUT or GET requests, despite the fact that
        # PUT and GET tempurls both allow HEAD requests
        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

        self.assertRaises(ResponseError, self.env.other_obj.write,
                          b'new contents',
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

    def test_different_object(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        self.assertRaises(ResponseError, self.env.other_obj.read,
                          cfg={'no_auth_token': True},
                          parms=self.obj_tempurl_parms)
        self.assert_status([401])

    def test_changing_sig(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_sig'][0] == 'a':
            parms['temp_url_sig'] = 'b' + parms['temp_url_sig'][1:]
        else:
            parms['temp_url_sig'] = 'a' + parms['temp_url_sig'][1:]

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    def test_changing_expires(self):
        contents = self.env.obj.read(
            parms=self.obj_tempurl_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        parms = self.obj_tempurl_parms.copy()
        if parms['temp_url_expires'][-1] == '0':
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '1'
        else:
            parms['temp_url_expires'] = parms['temp_url_expires'][:-1] + '0'

        self.assertRaises(ResponseError, self.env.obj.read,
                          cfg={'no_auth_token': True},
                          parms=parms)
        self.assert_status([401])

    @requires_acls
    def test_tempurl_keys_visible_to_account_owner(self):
        metadata = self.env.container.info()
        self.assertEqual(metadata.get('tempurl_key'), self.env.tempurl_key)
        self.assertEqual(metadata.get('tempurl_key2'), self.env.tempurl_key2)

    @requires_acls
    def test_tempurl_keys_hidden_from_acl_readonly(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        metadata = self.env.container.info(cfg={
            'use_token': self.env.conn2.storage_token})

        self.assertNotIn(
            'tempurl_key', metadata,
            'Container TempURL key found, should not be visible '
            'to readonly ACLs')
        self.assertNotIn(
            'tempurl_key2', metadata,
            'Container TempURL key-2 found, should not be visible '
            'to readonly ACLs')

    def test_GET_DLO_inside_container(self):
        seg1 = self.env.container.file(
            "get-dlo-inside-seg1" + Utils.create_name())
        seg2 = self.env.container.file(
            "get-dlo-inside-seg2" + Utils.create_name())
        seg1.write(b"one fish two fish ")
        seg2.write(b"red fish blue fish")

        manifest = self.env.container.file("manifest" + Utils.create_name())
        manifest.write(
            b'',
            hdrs={"X-Object-Manifest": "%s/get-dlo-inside-seg" %
                  (self.env.container.name,)})

        expires = int(time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        contents = manifest.read(parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(contents, b"one fish two fish red fish blue fish")

    def test_GET_DLO_outside_container(self):
        container2 = self.env.account.container(Utils.create_name())
        container2.create()
        seg1 = container2.file(
            "get-dlo-outside-seg1" + Utils.create_name())
        seg2 = container2.file(
            "get-dlo-outside-seg2" + Utils.create_name())
        seg1.write(b"one fish two fish ")
        seg2.write(b"red fish blue fish")

        manifest = self.env.container.file("manifest" + Utils.create_name())
        manifest.write(
            b'',
            hdrs={"X-Object-Manifest": "%s/get-dlo-outside-seg" %
                  (container2.name,)})

        expires = int(time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig,
                 'temp_url_expires': str(expires)}

        # cross container tempurl does not work for container tempurl key
        try:
            manifest.read(parms=parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 401)
        else:
            self.fail('request did not error')
        try:
            manifest.info(parms=parms, cfg={'no_auth_token': True})
        except ResponseError as e:
            self.assertEqual(e.status, 401)
        else:
            self.fail('request did not error')


class TestContainerTempurlUTF8(Base2, TestContainerTempurl):
    pass


class TestSloTempurlEnv(TestTempurlBaseEnv):
    enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        super(TestSloTempurlEnv, cls).setUp()
        if cls.enabled is None:
            cls.enabled = 'tempurl' in cluster_info and 'slo' in cluster_info

        cls.tempurl_key = Utils.create_name()

        cls.account.update_metadata({'temp-url-key': cls.tempurl_key})

        cls.manifest_container = cls.account.container(Utils.create_name())
        cls.segments_container = cls.account.container(Utils.create_name())
        if not cls.manifest_container.create():
            raise ResponseError(cls.conn.response)
        if not cls.segments_container.create():
            raise ResponseError(cls.conn.response)

        seg1 = cls.segments_container.file(Utils.create_name())
        seg1.write(b'1' * 1024 * 1024)

        seg2 = cls.segments_container.file(Utils.create_name())
        seg2.write(b'2' * 1024 * 1024)

        cls.manifest_data = [{'size_bytes': 1024 * 1024,
                              'etag': seg1.md5,
                              'path': '/%s/%s' % (cls.segments_container.name,
                                                  seg1.name)},
                             {'size_bytes': 1024 * 1024,
                              'etag': seg2.md5,
                              'path': '/%s/%s' % (cls.segments_container.name,
                                                  seg2.name)}]

        cls.manifest = cls.manifest_container.file(Utils.create_name())
        cls.manifest.write(
            json.dumps(cls.manifest_data).encode('ascii'),
            parms={'multipart-manifest': 'put'})


class TestSloTempurl(Base):
    env = TestSloTempurlEnv
    digest_name = 'sha256'

    def setUp(self):
        super(TestSloTempurl, self).setUp()
        if self.env.enabled is False:
            raise SkipTest("TempURL and SLO not both enabled")
        elif self.env.enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected enabled to be True/False, got %r" %
                (self.env.enabled,))

        if self.digest_name not in cluster_info['tempurl'].get(
                'allowed_digests', ['sha1']):
            raise SkipTest("tempurl does not support %s signatures" %
                           self.digest_name)
        self.digest = getattr(hashlib, self.digest_name)

    def tempurl_sig(self, method, expires, path, key):
        path = urllib.parse.unquote(path)
        method = method.encode('utf8')
        path = path.encode('utf8')
        key = key.encode('utf8')
        return hmac.new(
            key,
            b'%s\n%d\n%s' % (method, expires, path),
            self.digest).hexdigest()

    def test_GET(self):
        expires = int(time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.manifest.path),
            self.env.tempurl_key)
        parms = {'temp_url_sig': sig, 'temp_url_expires': str(expires)}

        contents = self.env.manifest.read(
            parms=parms,
            cfg={'no_auth_token': True})
        self.assertEqual(len(contents), 2 * 1024 * 1024)

        # GET tempurls also allow HEAD requests
        self.assertTrue(self.env.manifest.info(
            parms=parms, cfg={'no_auth_token': True}))


class TestSloTempurlUTF8(Base2, TestSloTempurl):
    pass


def requires_digest(digest):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if digest not in cluster_info['tempurl'].get(
                    'allowed_digests', ['sha1']):
                raise SkipTest("tempurl does not support %s signatures" %
                               digest)
            return func(*args, **kwargs)
        return wrapper
    return decorator


class TestTempurlAlgorithms(Base):
    env = TestTempurlEnv

    def setUp(self):
        super(TestTempurlAlgorithms, self).setUp()
        if self.env.tempurl_enabled is False:
            raise SkipTest("TempURL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

    def get_sig(self, expires, digest, encoding):
        path = urllib.parse.unquote(self.env.conn.make_path(self.env.obj.path))
        path = path.encode('utf8')
        key = self.env.tempurl_key.encode('utf8')
        sig = hmac.new(
            key,
            b'GET\n%d\n%s' % (expires, path),
            getattr(hashlib, digest))

        if encoding == 'hex':
            return sig.hexdigest()
        elif encoding == 'base64':
            return digest + ':' + base64.b64encode(
                sig.digest()).decode('ascii')
        elif encoding == 'base64-no-padding':
            return digest + ':' + base64.b64encode(
                sig.digest()).decode('ascii').strip('=')
        elif encoding == 'url-safe-base64':
            return digest + ':' + base64.urlsafe_b64encode(
                sig.digest()).decode('ascii')
        else:
            raise ValueError('Unrecognized encoding: %r' % encoding)

    def _do_test(self, digest, encoding):
        expires = int(time()) + 86400
        sig = self.get_sig(expires, digest, encoding)

        if encoding == 'url-safe-base64':
            # Make sure that we're actually testing url-safe-ness
            while '-' not in sig and '_' not in sig:
                expires += 1
                sig = self.get_sig(expires, digest, encoding)

        parms = {'temp_url_sig': sig, 'temp_url_expires': str(expires)}

        contents = self.env.obj.read(
            parms=parms,
            cfg={'no_auth_token': True})
        self.assertEqual(contents, b"obj contents")

        # GET tempurls also allow HEAD requests
        self.assertTrue(self.env.obj.info(
            parms=parms, cfg={'no_auth_token': True}))

    @requires_digest('sha1')
    def test_sha1(self):
        self._do_test('sha1', 'hex')
        self._do_test('sha1', 'base64')
        self._do_test('sha1', 'base64-no-padding')
        self._do_test('sha1', 'url-safe-base64')

    @requires_digest('sha256')
    def test_sha256(self):
        # apparently Cloud Files supports hex-encoded SHA-256
        # let's not break that just for the sake of being different
        self._do_test('sha256', 'hex')
        self._do_test('sha256', 'base64')
        self._do_test('sha256', 'base64-no-padding')
        self._do_test('sha256', 'url-safe-base64')

    @requires_digest('sha512')
    def test_sha512(self):
        self._do_test('sha512', 'hex')
        self._do_test('sha512', 'base64')
        self._do_test('sha512', 'base64-no-padding')
        self._do_test('sha512', 'url-safe-base64')
