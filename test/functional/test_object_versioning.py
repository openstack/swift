#!/usr/bin/python -u
# Copyright (c) 2010-2012 OpenStack Foundation
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

import hashlib
import hmac
import json
import time

from copy import deepcopy
from urllib.parse import quote, unquote
from unittest import SkipTest

import test.functional as tf

from swift.common.swob import normalize_etag
from swift.common.utils import MD5_OF_EMPTY_STRING, config_true_value, md5
from swift.common.middleware.versioned_writes.object_versioning import \
    DELETE_MARKER_CONTENT_TYPE

from test.functional.tests import Base, Base2, BaseEnv, Utils
from test.functional import cluster_info
from test.functional.swift_test_client import Connection, \
    ResponseError
from test.functional.test_tempurl import TestContainerTempurlEnv, \
    TestTempurlEnv


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestObjectVersioningEnv(BaseEnv):
    account2 = None
    versions_header_key = 'X-Versions-Enabled'

    @classmethod
    def setUp(cls):
        super(TestObjectVersioningEnv, cls).setUp()

        if not tf.skip2:
            # Second connection for ACL tests
            config2 = deepcopy(tf.config)
            config2['account'] = tf.config['account2']
            config2['username'] = tf.config['username2']
            config2['password'] = tf.config['password2']
            cls.conn2 = Connection(config2)
            cls.conn2.authenticate()

        prefix = Utils.create_name()[:10]
        cls.container = cls.account.container(prefix + "-objs")
        container_headers = {cls.versions_header_key: 'True'}
        if not cls.container.create(hdrs=container_headers):
            raise ResponseError(cls.conn.response)

        cls.unversioned_container = cls.account.container(
            prefix + "-unversioned")
        if not cls.unversioned_container.create():
            raise ResponseError(cls.conn.response)

        if not tf.skip2:
            # setup another account to test ACLs
            config2 = deepcopy(tf.config)
            config2['account'] = tf.config['account2']
            config2['username'] = tf.config['username2']
            config2['password'] = tf.config['password2']
            cls.conn2 = Connection(config2)
            cls.storage_url2, cls.storage_token2 = cls.conn2.authenticate()
            cls.account2 = cls.conn2.get_account()
            cls.account2.delete_containers()

        if not tf.skip3:
            # setup another account with no access to anything to test ACLs
            config3 = deepcopy(tf.config)
            config3['account'] = tf.config['account']
            config3['username'] = tf.config['username3']
            config3['password'] = tf.config['password3']
            cls.conn3 = Connection(config3)
            cls.storage_url3, cls.storage_token3 = cls.conn3.authenticate()
            cls.account3 = cls.conn3.get_account()

        # the allowed headers are configurable in object server, so we cannot
        # assert that content-encoding or content-disposition get *copied* to
        # the object version unless they were set on the original PUT, so
        # populate expected_headers by making a HEAD on the original object
        precheck_container = cls.account.container('header-precheck-cont')
        if not precheck_container.create():
            raise ResponseError(cls.conn.response)
        test_obj = precheck_container.file('test_allowed_headers')
        put_headers = {'Content-Type': 'text/jibberish01',
                       'Content-Encoding': 'gzip',
                       'Content-Disposition': 'attachment; filename=myfile'}
        test_obj.write(b"aaaaa", hdrs=put_headers)
        test_obj.initialize()
        resp_headers = {
            h.lower(): v for h, v in test_obj.conn.response.getheaders()}
        cls.expected_headers = {}
        for k, v in put_headers.items():
            if k.lower() in resp_headers:
                cls.expected_headers[k] = v
        precheck_container.delete_recursive()

    @classmethod
    def tearDown(cls):
        if cls.account:
            cls.account.delete_containers()
        if cls.account2:
            cls.account2.delete_containers()


class TestObjectVersioningBase(Base):
    env = TestObjectVersioningEnv

    def setUp(self):
        super(TestObjectVersioningBase, self).setUp()
        if 'object_versioning' not in tf.cluster_info:
            raise SkipTest("Object Versioning not enabled")

        self._account_name = None

        # make sure versioning is enabled,
        # since it gets disabled in tearDown
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'True'})

    def _tear_down_files(self, container):
        try:
            # only delete files and not containers
            # as they were configured in self.env
            # get rid of any versions so they aren't restored

            container.update_metadata(
                hdrs={self.env.versions_header_key: 'False'})

            # get rid of originals
            container.delete_files()

            # delete older versions
            listing_parms = {'versions': None, 'format': 'json'}
            for obj_info in container.files(parms=listing_parms):
                prev_version = container.file(obj_info['name'])
                prev_version.delete(
                    parms={'version-id': obj_info['version_id']})

        except ResponseError:
            pass

    def tearDown(self):
        super(TestObjectVersioningBase, self).tearDown()
        self._tear_down_files(self.env.container)

    def assertTotalVersions(self, container, count):
        listing_parms = {'versions': None}
        self.assertEqual(count, len(container.files(parms=listing_parms)))

    def assertContentTypes(self, container, expected_content_types):
        listing_parms = {'versions': None,
                         'format': 'json',
                         'reverse': 'true'}
        self.assertEqual(expected_content_types, [
            o['content_type']
            for o in container.files(parms=listing_parms)])


class TestObjectVersioning(TestObjectVersioningBase):

    @property
    def account_name(self):
        if not self._account_name:
            self._account_name = unquote(
                self.env.conn.storage_path.rsplit('/', 1)[-1])
        return self._account_name

    def test_disable_version(self):
        # sanity
        self.assertTrue(
            config_true_value(self.env.container.info()['versions_enabled']))

        # disable it
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})
        self.assertFalse(
            config_true_value(self.env.container.info()['versions_enabled']))

        # enabled it back
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'True'})
        self.assertTrue(
            config_true_value(self.env.container.info()['versions_enabled']))

    def test_account_list_containers(self):
        cont_listing = self.env.account.containers()
        self.assertEqual(cont_listing, [self.env.container.name,
                                        self.env.unversioned_container.name])
        self.env.account.delete_containers()
        prefix = Utils.create_name()

        def get_name(i):
            return prefix + '-%02d' % i

        num_container = [15, 20]
        for i in range(num_container[1]):
            name = get_name(i)
            container = self.env.account.container(name)
            container.create()

        limit = 5
        cont_listing = self.env.account.containers(parms={'limit': limit})
        self.assertEqual(cont_listing, [get_name(i) for i in range(limit)])

        for i in range(num_container[0], num_container[1]):
            name = get_name(i)
            container = self.env.account.container(name)
            container.update_metadata(
                hdrs={self.env.versions_header_key: 'True'})

        cont_listing = self.env.account.containers(parms={'limit': limit})
        self.assertEqual(cont_listing, [get_name(i) for i in range(limit)])

        # we're in charge of getting everything back to normal
        self.env.account.delete_containers()
        self.env.container.create()
        self.env.unversioned_container.create()

    def assert_previous_version(self, object_name, version_id, content,
                                content_type, expected_headers={},
                                not_expected_header_keys=[],
                                check_env_expected_headers=False):
        '''
        Find previous version of an object using the ?versions API
        then, assert object data and metadata using ?version-id API
        '''
        prev_version = self.env.container.file(object_name)
        prev_version.initialize(parms={'version-id': version_id})
        self.assertEqual(content, prev_version.read(
            parms={'version-id': version_id}))
        self.assertEqual(content_type, prev_version.content_type)
        # make sure the new obj metadata did not leak to the prev. version
        resp_headers = {
            h.lower(): v for h, v in prev_version.conn.response.getheaders()}

        for k in not_expected_header_keys:
            self.assertNotIn(k, resp_headers)

        for k, v in expected_headers.items():
            self.assertIn(k.lower(), resp_headers)
            self.assertEqual(v, resp_headers[k.lower()])

        # also check env expected_headers
        if check_env_expected_headers:
            for k, v in self.env.expected_headers.items():
                self.assertIn(k.lower(), resp_headers)
                self.assertEqual(v, resp_headers[k.lower()])

    def test_expiry(self):
        # sanity
        container = self.env.container
        self.assertTrue(
            config_true_value(self.env.container.info()['versions_enabled']))

        versioned_obj1 = container.file(Utils.create_name())
        put_headers = {'Content-Type': 'text/blah-blah-blah',
                       'X-Delete-After': '1',
                       'X-Object-Meta-Color': 'blue'}
        resp = versioned_obj1.write(b"aaaaa", hdrs=put_headers,
                                    return_resp=True)
        version_id1 = resp.getheader('x-object-version-id')

        versioned_obj2 = container.file(Utils.create_name())
        resp = versioned_obj2.write(b"aaaaa", hdrs={}, return_resp=True)
        version_id2 = resp.getheader('x-object-version-id')

        # swift_test_client's File API doesn't really allow for POSTing
        # arbitrary headers, so...
        def post(url, token, parsed, conn):
            conn.request('POST', '%s/%s/%s' % (parsed.path, container,
                                               versioned_obj2.name),
                         '', {'X-Auth-Token': token,
                              'Content-Length': '0',
                              'X-Object-Meta-Color': 'red',
                              'X-Delete-After': '1'})
            return tf.check_response(conn)
        resp = tf.retry(post)
        resp.read()
        self.assertEqual(resp.status, 202)

        time.sleep(1)

        # Links have expired
        with self.assertRaises(ResponseError) as cm:
            versioned_obj1.info()
        self.assertEqual(404, cm.exception.status)

        with self.assertRaises(ResponseError) as cm:
            versioned_obj2.info()
        self.assertEqual(404, cm.exception.status)

        # But data are still there
        versioned_obj1.initialize(parms={'version-id': version_id1})
        self.assertEqual('text/blah-blah-blah', versioned_obj1.content_type)
        self.assertEqual('blue', versioned_obj1.metadata['color'])

        versioned_obj2.initialize(parms={'version-id': version_id2})
        self.assertEqual('application/octet-stream',
                         versioned_obj2.content_type)
        self.assertEqual('red', versioned_obj2.metadata['color'])

        # Note that links may still show up in listings, depending on how
        # aggressive the object-expirer is. When doing a version-aware
        # listing, though, we'll only ever have the two entries.
        self.assertTotalVersions(container, 2)

    def test_get_if_match(self):
        body = b'data'
        oname = Utils.create_name()
        obj = self.env.unversioned_container.file(oname)
        resp = obj.write(body, return_resp=True)
        etag = resp.getheader('etag')
        self.assertEqual(
            md5(body, usedforsecurity=False).hexdigest(),
            normalize_etag(etag))

        # un-versioned object is cool with with if-match
        self.assertEqual(body, obj.read(hdrs={'if-match': etag}))
        with self.assertRaises(ResponseError) as cm:
            obj.read(hdrs={'if-match': 'not-the-etag'})
        self.assertEqual(412, cm.exception.status)

        v_obj = self.env.container.file(oname)
        resp = v_obj.write(body, return_resp=True)
        self.assertEqual(resp.getheader('etag'), etag)

        # versioned object is too with with if-match
        self.assertEqual(body, v_obj.read(hdrs={
            'if-match': normalize_etag(etag)}))
        # works quoted, too
        self.assertEqual(body, v_obj.read(hdrs={
            'if-match': '"%s"' % normalize_etag(etag)}))
        with self.assertRaises(ResponseError) as cm:
            v_obj.read(hdrs={'if-match': 'not-the-etag'})
        self.assertEqual(412, cm.exception.status)

    def test_container_acls(self):
        if tf.skip3:
            raise SkipTest('Username3 not set')

        obj = self.env.container.file(Utils.create_name())
        resp = obj.write(b"data", return_resp=True)
        version_id = resp.getheader('x-object-version-id')
        self.assertIsNotNone(version_id)

        with self.assertRaises(ResponseError) as cm:
            obj.read(hdrs={'X-Auth-Token': self.env.conn3.storage_token})
        self.assertEqual(403, cm.exception.status)

        # Container ACLs work more or less like they always have
        self.env.container.update_metadata(
            hdrs={'X-Container-Read': self.env.conn3.user_acl})
        self.assertEqual(b"data", obj.read(hdrs={
            'X-Auth-Token': self.env.conn3.storage_token}))

        # But the version-specifc GET still requires a swift owner
        with self.assertRaises(ResponseError) as cm:
            obj.read(hdrs={'X-Auth-Token': self.env.conn3.storage_token},
                     parms={'version-id': version_id})
        self.assertEqual(403, cm.exception.status)

        # If it's pointing to a symlink that points elsewhere, that still needs
        # to be authed
        tgt_name = Utils.create_name()
        self.env.unversioned_container.file(tgt_name).write(b'link')
        sym_tgt_header = quote(unquote('%s/%s' % (
            self.env.unversioned_container.name, tgt_name)))
        obj.write(hdrs={'X-Symlink-Target': sym_tgt_header})

        # So, user1's good...
        self.assertEqual(b'link', obj.read())
        # ...but user3 can't
        with self.assertRaises(ResponseError) as cm:
            obj.read(hdrs={'X-Auth-Token': self.env.conn3.storage_token})
        self.assertEqual(403, cm.exception.status)

        # unless we add the acl to the unversioned_container
        self.env.unversioned_container.update_metadata(
            hdrs={'X-Container-Read': self.env.conn3.user_acl})
        self.assertEqual(b'link', obj.read(
            hdrs={'X-Auth-Token': self.env.conn3.storage_token}))

    def _test_overwriting_setup(self, obj_name=None):
        # sanity
        container = self.env.container
        self.assertTrue(
            config_true_value(self.env.container.info()['versions_enabled']))

        expected_content_types = []
        self.assertTotalVersions(container, 0)
        obj_name = obj_name or Utils.create_name()

        versioned_obj = container.file(obj_name)
        put_headers = {'Content-Type': 'text/jibberish01',
                       'Content-Encoding': 'gzip',
                       'Content-Disposition': 'attachment; filename=myfile'}
        resp = versioned_obj.write(b"aaaaa", hdrs=put_headers,
                                   return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')
        expected_content_types.append('text/jibberish01')
        self.assertContentTypes(container, expected_content_types)
        obj_info = versioned_obj.info()
        self.assertEqual('text/jibberish01', obj_info['content_type'])

        self.assertTotalVersions(container, 1)
        resp = versioned_obj.write(
            b"bbbbb",
            hdrs={'Content-Type': 'text/jibberish02',
                  'X-Object-Meta-Foo': 'Bar'},
            return_resp=True)
        v2_version_id = resp.getheader('x-object-version-id')
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish02')
        self.assertEqual(versioned_obj.metadata['foo'], 'Bar')
        resp_headers = {
            h.lower(): v for h, v in versioned_obj.conn.response.getheaders()}
        content_location = quote('/v1/%s/%s/%s' % (
            self.account_name, container.name, obj_name
        )) + '?version-id=%s' % (v2_version_id,)
        self.assertEqual(content_location, resp_headers['content-location'])
        expected_content_types.append('text/jibberish02')
        self.assertContentTypes(container, expected_content_types)

        # the old version got saved off
        self.assertTotalVersions(container, 2)

        self.assert_previous_version(
            obj_name, v1_version_id, b'aaaaa', 'text/jibberish01',
            not_expected_header_keys=['X-Object-Meta-Foo'],
            check_env_expected_headers=True)

        # check that POST does not create a new version
        versioned_obj.sync_metadata(metadata={'fu': 'baz'})
        self.assertTotalVersions(container, 2)

        self.assert_previous_version(
            obj_name, v2_version_id, b'bbbbb', 'text/jibberish02',
            expected_headers={'X-Object-Meta-Fu': 'baz'})

        # if we overwrite it again, there are three versions
        resp = versioned_obj.write(b"ccccc", return_resp=True)
        v3_version_id = resp.getheader('x-object-version-id')
        expected_content_types.append('text/jibberish02')
        self.assertContentTypes(container, expected_content_types)
        self.assertTotalVersions(self.env.container, 3)

        # versioned_obj keeps the newest content
        self.assertEqual(b"ccccc", versioned_obj.read())

        # test copy from a different container
        src_container = self.env.account.container(Utils.create_name())
        self.assertTrue(src_container.create())
        src_name = Utils.create_name()
        src_obj = src_container.file(src_name)
        src_obj.write(b"ddddd", hdrs={'Content-Type': 'text/jibberish04'})
        src_obj.copy(container.name, obj_name)
        expected_content_types.append('text/jibberish04')
        self.assertContentTypes(container, expected_content_types)

        self.assertEqual(b"ddddd", versioned_obj.read())
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish04')

        # make sure versions container has the previous version
        self.assertTotalVersions(self.env.container, 4)
        self.assert_previous_version(
            obj_name, v3_version_id, b'ccccc', 'text/jibberish02')

        # test delete
        # at first, delete will succeed with 204
        versioned_obj.delete()
        expected_content_types.append(
            'application/x-deleted;swift_versions_deleted=1')

        # after that, any time the delete doesn't restore the old version
        # and we will get 404 NotFound
        for x in range(3):
            with self.assertRaises(ResponseError) as cm:
                versioned_obj.delete()
            self.assertEqual(404, cm.exception.status)
            expected_content_types.append(
                'application/x-deleted;swift_versions_deleted=1')

        # finally, we have 4 versioned items and 4 delete markers total in
        # the versions container
        self.assertTotalVersions(self.env.container, 8)
        self.assertContentTypes(self.env.container, expected_content_types)

        # update versioned_obj
        versioned_obj.write(b"eeee", hdrs={'Content-Type': 'text/thanksgiving',
                            'X-Object-Meta-Bar': 'foo'})

        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/thanksgiving', obj_info['content_type'])

        # 8 plus one more write
        self.assertTotalVersions(self.env.container, 9)

        # update versioned_obj
        versioned_obj.write(b"ffff", hdrs={'Content-Type': 'text/teriyaki',
                            'X-Object-Meta-Food': 'chickin'})

        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/teriyaki', obj_info['content_type'])

        # 9 plus one more write
        self.assertTotalVersions(self.env.container, 10)

        versioned_obj.delete()
        with self.assertRaises(ResponseError) as cm:
            versioned_obj.read()
        self.assertEqual(404, cm.exception.status)

        # 10 plus delete marker
        self.assertTotalVersions(self.env.container, 11)

        return (versioned_obj, expected_content_types)

    def test_overwriting(self):
        versioned_obj, expected_content_types = \
            self._test_overwriting_setup()

    def test_make_old_version_latest(self):
        obj_name = Utils.create_name()
        versioned_obj = self.env.container.file(obj_name)
        versions = [{
            'content_type': 'text/jibberish01',
            'body': b'aaaaa',
        }, {
            'content_type': 'text/jibberish02',
            'body': b'bbbbbb',
        }, {
            'content_type': 'text/jibberish03',
            'body': b'ccccccc',
        }]
        for version in versions:
            resp = versioned_obj.write(version['body'], hdrs={
                'Content-Type': version['content_type']}, return_resp=True)
            version['version_id'] = resp.getheader('x-object-version-id')
        expected = [{
            'name': obj_name,
            'content_type': version['content_type'],
            'version_id': version['version_id'],
            'hash': md5(version['body'], usedforsecurity=False).hexdigest(),
            'bytes': len(version['body'],)
        } for version in reversed(versions)]
        for item, is_latest in zip(expected, (True, False, False)):
            item['is_latest'] = is_latest
        versions_listing = self.env.container.files(parms={
            'versions': 'true', 'format': 'json'})
        for item in versions_listing:
            item.pop('last_modified')
        self.assertEqual(expected, versions_listing)

        versioned_obj.write(b'', parms={
            'version-id': versions[1]['version_id']})
        self.assertEqual(b'bbbbbb', versioned_obj.read())
        for item, is_latest in zip(expected, (False, True, False)):
            item['is_latest'] = is_latest
        versions_listing = self.env.container.files(parms={
            'versions': 'true', 'format': 'json'})
        for item in versions_listing:
            item.pop('last_modified')
        self.assertEqual(expected, versions_listing)

    def test_overwriting_with_url_encoded_object_name(self):
        obj_name = Utils.create_name() + '%25ff'
        versioned_obj, expected_content_types = \
            self._test_overwriting_setup(obj_name)

    def _test_versioning_dlo_setup(self):
        if tf.in_process:
            tf.skip_if_no_xattrs()

        container = self.env.container
        obj_name = Utils.create_name()

        for i in ('1', '2', '3'):
            time.sleep(.01)  # guarantee that the timestamp changes
            obj_name_seg = 'segs_' + obj_name + '/' + i
            versioned_obj = container.file(obj_name_seg)
            versioned_obj.write(i.encode('ascii'))
            # immediately overwrite
            versioned_obj.write((i + i).encode('ascii'))

        # three objects 2 versions each
        self.assertTotalVersions(self.env.container, 6)

        man_file = container.file(obj_name)

        # write a normal file first
        resp = man_file.write(
            b'old content', hdrs={'Content-Type': 'text/jibberish01'},
            return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')
        self.assertEqual(b'old content', man_file.read())

        # guarantee that the timestamp changes
        time.sleep(.01)

        # overwrite with a dlo manifest
        dlo_prefix = quote(unquote('%s/segs_%s/' % (
            self.env.container.name, obj_name)))
        resp = man_file.write(
            b'', hdrs={'Content-Type': 'text/jibberish02',
                       'X-Object-Manifest': dlo_prefix},
            return_resp=True)
        v2_version_id = resp.getheader('x-object-version-id')

        self.assertTotalVersions(self.env.container, 8)
        self.assertEqual(b'112233', man_file.read())

        self.assert_previous_version(
            obj_name, v1_version_id, b'old content', 'text/jibberish01')

        # overwrite the manifest with a normal file
        man_file.write(b'new content')
        self.assertTotalVersions(self.env.container, 9)
        self.assertEqual(b'new content', man_file.read())

        # new most-recent archive is the dlo
        self.assert_previous_version(
            obj_name, v2_version_id, b'112233', 'text/jibberish02',
            expected_headers={'X-Object-Manifest': dlo_prefix})
        return obj_name, man_file

    def test_versioning_dlo(self):
        obj_name, man_file = \
            self._test_versioning_dlo_setup()

        man_file.delete()
        with self.assertRaises(ResponseError) as cm:
            man_file.read()
        self.assertEqual(404, cm.exception.status)

        # 9 plus one more write
        self.assertTotalVersions(self.env.container, 10)

        expected = [b'old content', b'112233', b'new content']

        bodies = []
        listing_parms = {'versions': None, 'format': 'json',
                         'reverse': 'true', 'prefix': obj_name}
        for obj_info in self.env.container.files(parms=listing_parms)[:3]:
            bodies.append(man_file.read(
                parms={'version-id': obj_info['version_id']}))
        self.assertEqual(expected, bodies)

    def _check_overwriting_symlink(self):
        # sanity
        container = self.env.container
        self.assertTrue(
            config_true_value(self.env.container.info()['versions_enabled']))

        tgt_a_name = Utils.create_name()
        tgt_b_name = Utils.create_name()
        expected_count = 0

        tgt_a = container.file(tgt_a_name)
        tgt_a.write(b'aaaaa', hdrs={'Content-Type': 'text/jibberish01'})
        expected_count += 1

        tgt_b = container.file(tgt_b_name)
        tgt_b.write(b"bbbbb")
        expected_count += 1

        symlink_name = Utils.create_name()
        sym_tgt_header = quote(unquote('%s/%s' % (container.name, tgt_a_name)))
        sym_headers_a = {'X-Symlink-Target': sym_tgt_header}
        symlink = container.file(symlink_name)
        resp = symlink.write(b'', hdrs=sym_headers_a, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')
        expected_count += 1
        self.assertEqual(b"aaaaa", symlink.read())

        sym_headers_b = {'X-Symlink-Target': '%s/%s' % (container.name,
                                                        tgt_b_name)}
        symlink.write(b"", hdrs=sym_headers_b)
        expected_count += 1
        self.assertEqual(b"bbbbb", symlink.read())

        self.assertTotalVersions(container, expected_count)
        self.assert_previous_version(
            symlink_name, v1_version_id, b'aaaaa', 'text/jibberish01')
        return symlink, tgt_a

    def test_overwriting_symlink(self):
        if 'symlink' not in cluster_info:
            raise SkipTest("Symlinks not enabled")

        symlink, target = self._check_overwriting_symlink()
        # test delete
        symlink.delete()
        with self.assertRaises(ResponseError) as cm:
            symlink.read()
        self.assertEqual(404, cm.exception.status)

    def _setup_symlink(self):
        tgt_name = 'target-' + Utils.create_name()
        target = self.env.container.file(tgt_name)
        target.write(b'target object data',
                     hdrs={'Content-Type': 'text/jibberish01'})
        symlink = self.env.container.file('symlink')
        resp = symlink.write(b'', hdrs={
            'Content-Type': 'application/symlink',
            'X-Symlink-Target': '%s/%s' % (
                self.env.container.name, target.name)},
            return_resp=True)
        symlink_version_id = resp.getheader('x-object-version-id')
        return symlink, symlink_version_id, target

    def _check_copy_destination_symlink(self):
        symlink, sym_version_id, target = self._setup_symlink()
        self.assertEqual(b'target object data', symlink.read())
        symlink.write(b'this is not a symlink')

        # target, symlink, and new 'not a symlink' overwritten by write
        self.assertTotalVersions(self.env.container, 3)
        self.assert_previous_version(
            symlink.name, sym_version_id,
            b'target object data', 'text/jibberish01')

        # the symlink is still a symlink
        prev_version = self.env.container.file(symlink.name)
        prev_version.initialize(parms={'version-id': sym_version_id})
        self.assertEqual('application/symlink',
                         prev_version.info(parms={
                             'version-id': sym_version_id,
                             'symlink': 'get'})['content_type'])
        prev_version.copy(self.env.container.name, symlink.name,
                          parms={'version-id': sym_version_id,
                                 'symlink': 'get'})
        self.assertEqual(b'target object data', symlink.read())
        self.assertTotalVersions(self.env.container, 4)

        return symlink, target

    def test_copy_destination_restore_symlink(self):
        if 'symlink' not in cluster_info:
            raise SkipTest("Symlinks not enabled")

        symlink, target = self._check_copy_destination_symlink()
        symlink.delete()
        with self.assertRaises(ResponseError) as cm:
            symlink.read()
        self.assertEqual(404, cm.exception.status)
        # symlink & target, plus overwrite and restore, then delete marker
        self.assertTotalVersions(self.env.container, 5)

    def test_versioned_staticlink(self):
        tgt_name = 'target-' + Utils.create_name()
        link_name = 'staticlink-' + Utils.create_name()
        target = self.env.container.file(tgt_name)
        staticlink = self.env.container.file(link_name)

        target_resp = target.write(b'target object data', hdrs={
            'Content-Type': 'text/jibberish01'}, return_resp=True)
        staticlink.write(b'', hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.env.container.name, target.name),
            'X-Symlink-Target-Etag': target_resp.getheader('etag'),
        }, cfg={'no_content_type': True})
        self.assertEqual(b'target object data', staticlink.read())

        listing_parms = {'format': 'json', 'versions': 'true'}
        prev_versions = self.env.container.files(parms=listing_parms)
        expected = [{
            'name': link_name,
            'bytes': 0,
            'content_type': 'text/jibberish01',
            'is_latest': True,
        }, {
            'name': tgt_name,
            'bytes': 18,
            'content_type': 'text/jibberish01',
            'is_latest': True,
        }]
        self.assertEqual(expected, [{
            k: i[k] for k in (
                'name', 'bytes', 'content_type', 'is_latest',
            )} for i in prev_versions])

        target_resp = target.write(b'updated target data', hdrs={
            'Content-Type': 'text/jibberish02'}, return_resp=True)
        with self.assertRaises(ResponseError) as caught:
            staticlink.read()
        self.assertEqual(409, caught.exception.status)
        staticlink.write(b'', hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.env.container.name, target.name),
            'X-Symlink-Target-Etag': target_resp.getheader('etag'),
        }, cfg={'no_content_type': True})
        self.assertEqual(b'updated target data', staticlink.read())

        listing_parms = {'format': 'json', 'versions': 'true'}
        prev_versions = self.env.container.files(parms=listing_parms)
        expected = [{
            'name': link_name,
            'bytes': 0,
            'content_type': 'text/jibberish02',
            'is_latest': True,
        }, {
            'name': link_name,
            'bytes': 0,
            'content_type': 'text/jibberish01',
            'is_latest': False,
        }, {
            'name': tgt_name,
            'bytes': 19,
            'content_type': 'text/jibberish02',
            'is_latest': True,
        }, {
            'name': tgt_name,
            'bytes': 18,
            'content_type': 'text/jibberish01',
            'is_latest': False,
        }]
        self.assertEqual(expected, [{
            k: i[k] for k in (
                'name', 'bytes', 'content_type', 'is_latest',
            )} for i in prev_versions])

    def test_link_to_versioned_object(self):

        # setup target object
        tgt_name = 'target-' + Utils.create_name()
        target = self.env.container.file(tgt_name)
        target_resp = target.write(b'target object data', hdrs={
            'Content-Type': 'text/jibberish01'}, return_resp=True)

        # setup dynamic link object from a non-versioned container
        link_container_name = 'link-container-' + Utils.create_name()
        link_name = 'link-' + Utils.create_name()
        link_cont = self.env.account.container(link_container_name)
        self.assertTrue(link_cont.create())
        link = link_cont.file(link_name)
        self.assertTrue(link.write(b'', hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.env.container.name, tgt_name),
        }, cfg={'no_content_type': True}))
        self.assertEqual(b'target object data', link.read())

        # setup static link object from a non-versioned container
        staticlink_name = 'staticlink-' + Utils.create_name()
        staticlink = link_cont.file(staticlink_name)
        self.assertTrue(staticlink.write(b'', hdrs={
            'X-Symlink-Target': '%s/%s' % (
                self.env.container.name, tgt_name),
            'X-Symlink-Target-Etag': target_resp.getheader('etag'),
        }, cfg={'no_content_type': True}))
        self.assertEqual(b'target object data', link.read())

    def test_versioned_post(self):
        # first we'll create a versioned object
        obj_name = Utils.create_name()
        obj = self.env.container.file(obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish10'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')

        # send post request
        obj.post(hdrs={'Content-Type': 'text/updated20'})

        # head request should show updated content-type
        obj_info = obj.info()
        self.assertEqual(obj_info['content_type'], 'text/updated20')

        listing_parms = {'format': 'json', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(1, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj_name,
            'bytes': 8,
            'content_type': 'text/updated20',
            'hash': '966634ebf2fc135707d6753692bf4b1e',
            'version_id': v1_version_id,
            'is_latest': True,
        }])

    def test_unversioned_post(self):
        # first we'll create a versioned object
        obj_name = Utils.create_name()
        obj = self.env.container.file(obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish10'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')

        # now, turn off versioning
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})

        obj.post(hdrs={'Content-Type': 'text/updated20'})

        # head request should show updated content-type
        obj_info = obj.info()
        self.assertEqual(obj_info['content_type'], 'text/updated20')

        listing_parms = {'format': 'json', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(1, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj_name,
            'bytes': 8,
            'content_type': 'text/updated20',
            'hash': '966634ebf2fc135707d6753692bf4b1e',
            'is_latest': True,
            'version_id': v1_version_id,
            'is_latest': True,
        }])

    def test_unversioned_overwrite_and_delete(self):
        # first we'll create a versioned object
        obj_name = Utils.create_name()
        obj = self.env.container.file(obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish18'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')
        self.assertTotalVersions(self.env.container, 1)

        # now, turn off versioning, and delete source obj
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})
        obj.delete()

        # no delete markers, archive listing is unchanged
        self.assertTotalVersions(self.env.container, 1)

        # sanity, object is gone
        self.assertRaises(ResponseError, obj.read)
        self.assertEqual(404, obj.conn.response.status)

        # but, archive version is unmodified
        self.assert_previous_version(obj_name, v1_version_id, b'version1',
                                     'text/jibberish18')

        # a new overwrites will not have a version-id
        resp = obj.write(b'version2', hdrs={
            'Content-Type': 'text/jibberish19'
        }, return_resp=True)
        self.assertIsNone(resp.getheader('x-object-version-id'))
        self.assertTotalVersions(self.env.container, 2)

        resp = obj.write(b'version3', hdrs={
            'Content-Type': 'text/jibberish20'
        }, return_resp=True)
        self.assertIsNone(resp.getheader('x-object-version-id'))
        self.assertTotalVersions(self.env.container, 2)

        obj.delete()
        self.assertTotalVersions(self.env.container, 1)

        obj.delete(tolerate_missing=True)
        self.assertTotalVersions(self.env.container, 1)

    def test_versioned_overwrite_from_old_version(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish32'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')
        v1_etag = normalize_etag(resp.getheader('etag'))

        resp = obj.write(b'version2', hdrs={
            'Content-Type': 'text/jibberish33'
        }, return_resp=True)
        v2_version_id = resp.getheader('x-object-version-id')
        v2_etag = normalize_etag(resp.getheader('etag'))

        # sanity
        self.assertEqual(b'version2', obj.read())

        self.assertTotalVersions(self.env.container, 2)
        listing_parms = {'format': 'json', 'reverse': 'true', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(2, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': versioned_obj_name,
            'bytes': 8,
            'content_type': 'text/jibberish32',
            'hash': v1_etag,
            'version_id': v1_version_id,
            'is_latest': False,
        }, {
            'name': versioned_obj_name,
            'bytes': 8,
            'content_type': 'text/jibberish33',
            'hash': v2_etag,
            'version_id': v2_version_id,
            'is_latest': True,
        }])

        # restore old version1 back in place with a copy request
        # should get a new version-id
        old_version_obj = self.env.container.file(versioned_obj_name)
        resp = old_version_obj.copy(self.env.container.name,
                                    versioned_obj_name,
                                    parms={'version-id': v1_version_id},
                                    return_resp=True)
        v3_version_id = resp.getheader('x-object-version-id')

        listing_parms = {'format': 'json', 'reverse': 'true', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(3, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': versioned_obj_name,
            'bytes': 8,
            'content_type': 'text/jibberish32',
            'hash': v1_etag,
            'version_id': v1_version_id,
            'is_latest': False,
        }, {
            'name': versioned_obj_name,
            'bytes': 8,
            'content_type': 'text/jibberish33',
            'hash': v2_etag,
            'version_id': v2_version_id,
            'is_latest': False,
        }, {
            'name': versioned_obj_name,
            'bytes': 8,
            'content_type': 'text/jibberish32',
            'hash': v1_etag,
            'version_id': v3_version_id,
            'is_latest': True,
        }])

        self.assertEqual(b'version1', obj.read())
        obj_info = obj.info()
        self.assertEqual('text/jibberish32', obj_info['content_type'])
        self.assertEqual(v1_etag, normalize_etag(obj_info['etag']))

    def test_delete_with_version_api_old_object(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish32'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')

        obj.write(b'version2', hdrs={'Content-Type': 'text/jibberish33'})

        # sanity
        self.assertEqual(b'version2', obj.read())

        self.assertTotalVersions(self.env.container, 2)
        obj.delete(parms={'version-id': v1_version_id})

        self.assertEqual(b'version2', obj.read())
        self.assertTotalVersions(self.env.container, 1)

    def test_delete_with_version_api_current_object(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        obj.write(b'version1', hdrs={'Content-Type': 'text/jibberish32'})

        resp = obj.write(b'version2', hdrs={
            'Content-Type': 'text/jibberish33'
        }, return_resp=True)
        v2_version_id = resp.getheader('x-object-version-id')

        # sanity
        self.assertEqual(b'version2', obj.read())

        self.assertTotalVersions(self.env.container, 2)
        obj.delete(parms={'version-id': v2_version_id})

        with self.assertRaises(ResponseError) as cm:
            obj.read()
        self.assertEqual(404, cm.exception.status)
        self.assertTotalVersions(self.env.container, 1)

    def test_delete_delete_marker_with_version_api(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        obj.write(b'version1', hdrs={'Content-Type': 'text/jibberish32'})

        obj.delete()
        resp_headers = {
            h.lower(): v for h, v in obj.conn.response.getheaders()}
        self.assertIn('x-object-version-id', resp_headers)
        dm_version_id = resp_headers['x-object-version-id']

        # sanity
        with self.assertRaises(ResponseError) as cm:
            obj.info(parms={'version-id': dm_version_id})
        resp_headers = {
            h.lower(): v for h, v in cm.exception.headers}
        self.assertEqual(dm_version_id,
                         resp_headers['x-object-version-id'])
        self.assertEqual(DELETE_MARKER_CONTENT_TYPE,
                         resp_headers['content-type'])

        obj.delete(parms={'version-id': dm_version_id})
        resp_headers = {
            h.lower(): v for h, v in obj.conn.response.getheaders()}
        self.assertEqual(dm_version_id,
                         resp_headers['x-object-version-id'])

    def test_delete_with_version_api_last_object(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish1'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')

        # sanity
        self.assertEqual(b'version1', obj.read())
        self.assertTotalVersions(self.env.container, 1)

        # delete
        obj.delete(parms={'version-id': v1_version_id})

        with self.assertRaises(ResponseError) as cm:
            obj.read()
        self.assertEqual(404, cm.exception.status)
        self.assertTotalVersions(self.env.container, 0)

    def test_delete_with_version_api_null_version(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        obj.write(b'version1', hdrs={'Content-Type': 'text/jibberish32'})
        obj.write(b'version2', hdrs={'Content-Type': 'text/jibberish33'})

        # sanity
        self.assertEqual(b'version2', obj.read())
        self.assertTotalVersions(self.env.container, 2)

        obj.delete(parms={'version-id': 'null'})
        with self.assertRaises(ResponseError) as caught:
            obj.read()
        self.assertEqual(404, caught.exception.status)

        # no versions removed
        self.assertTotalVersions(self.env.container, 2)

    def test_delete_with_version_api_old_object_disabled(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish32'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')

        obj.write(b'version2', hdrs={'Content-Type': 'text/jibberish33'})

        # disabled versioning
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})

        # sanity
        self.assertEqual(b'version2', obj.read())

        self.assertTotalVersions(self.env.container, 2)
        obj.delete(parms={'version-id': v1_version_id})

        self.assertEqual(b'version2', obj.read())
        self.assertTotalVersions(self.env.container, 1)

    def test_delete_with_version_api_current_object_disabled(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        obj.write(b'version1', hdrs={'Content-Type': 'text/jibberish32'})

        resp = obj.write(b'version2', hdrs={
            'Content-Type': 'text/jibberish33'
        }, return_resp=True)
        v2_version_id = resp.getheader('x-object-version-id')

        # disabled versioning
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})

        # sanity
        self.assertEqual(b'version2', obj.read())

        self.assertTotalVersions(self.env.container, 2)
        obj.delete(parms={'version-id': v2_version_id})

        with self.assertRaises(ResponseError) as cm:
            obj.read()
        self.assertEqual(404, cm.exception.status)
        self.assertTotalVersions(self.env.container, 1)

    def test_delete_with_version_api_old_object_current_unversioned(self):
        versioned_obj_name = Utils.create_name()
        obj = self.env.container.file(versioned_obj_name)
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish32'
        }, return_resp=True)
        v1_version_id = resp.getheader('x-object-version-id')

        # disabled versioning
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})

        # write unversioned object (i.e., version-id='null')
        obj.write(b'version2', hdrs={'Content-Type': 'text/jibberish33'})

        # sanity
        self.assertEqual(b'version2', obj.read())

        self.assertTotalVersions(self.env.container, 2)
        obj.delete(parms={'version-id': v1_version_id})

        self.assertEqual(b'version2', obj.read())
        self.assertTotalVersions(self.env.container, 1)


class TestObjectVersioningUTF8(Base2, TestObjectVersioning):
    pass


class TestContainerOperations(TestObjectVersioningBase):

    def _prep_object_versions(self):

        # object with multiple versions and currently deleted
        obj1_v1 = {}
        obj1_v1['name'] = 'c' + Utils.create_name()
        obj = self.env.container.file(obj1_v1['name'])

        # v1
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish11',
            'ETag': md5(b'version1', usedforsecurity=False).hexdigest(),
        }, return_resp=True)
        obj1_v1['id'] = resp.getheader('x-object-version-id')

        # v2
        resp = obj.write(b'version2', hdrs={
            'Content-Type': 'text/jibberish12',
            'ETag': md5(b'version2', usedforsecurity=False).hexdigest(),
        }, return_resp=True)
        obj1_v2 = {}
        obj1_v2['name'] = obj1_v1['name']
        obj1_v2['id'] = resp.getheader('x-object-version-id')

        # v3
        resp = obj.write(b'version3', hdrs={
            'Content-Type': 'text/jibberish13',
            'ETag': md5(b'version3', usedforsecurity=False).hexdigest(),
        }, return_resp=True)
        obj1_v3 = {}
        obj1_v3['name'] = obj1_v1['name']
        obj1_v3['id'] = resp.getheader('x-object-version-id')

        with self.assertRaises(ResponseError) as cm:
            obj.write(b'version4', hdrs={
                'Content-Type': 'text/jibberish11',
                'ETag': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            })
        self.assertEqual(422, cm.exception.status)

        # v4
        obj.delete()
        resp_headers = {
            h.lower(): v for h, v in obj.conn.response.getheaders()}
        obj1_v4 = {}
        obj1_v4['name'] = obj1_v1['name']
        obj1_v4['id'] = resp_headers.get('x-object-version-id')

        # object with just a single version
        obj2_v1 = {}
        obj2_v1['name'] = 'b' + Utils.create_name()
        obj = self.env.container.file(obj2_v1['name'])
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish20',
            'ETag': '966634ebf2fc135707d6753692bf4b1e',
        }, return_resp=True)
        obj2_v1['id'] = resp.getheader('x-object-version-id')

        # object never existed, just a delete marker
        obj3_v1 = {}
        obj3_v1['name'] = 'a' + Utils.create_name()
        obj = self.env.container.file(obj3_v1['name'])
        obj.delete(tolerate_missing=True)
        self.assertEqual(obj.conn.response.status, 404)
        resp_headers = {
            h.lower(): v for h, v in obj.conn.response.getheaders()}
        obj3_v1['id'] = resp_headers.get('x-object-version-id')

        return (obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1)

    def _prep_unversioned_objects(self):
        objs = (
            'deleted' + Utils.create_name(),
            'in' + Utils.create_name(),
            'order' + Utils.create_name(),
        )

        # object with multiple writes and currently deleted
        obj = self.env.unversioned_container.file(objs[0])
        obj.write(b'data', hdrs={
            'Content-Type': 'text/jibberish11',
            'ETag': md5(b'data', usedforsecurity=False).hexdigest(),
        })
        obj.delete()

        obj = self.env.unversioned_container.file(objs[1])
        obj.write(b'first', hdrs={
            'Content-Type': 'text/blah-blah-blah',
            'ETag': md5(b'first', usedforsecurity=False).hexdigest(),
        })

        obj = self.env.unversioned_container.file(objs[2])
        obj.write(b'second', hdrs={
            'Content-Type': 'text/plain',
            'ETag': md5(b'second', usedforsecurity=False).hexdigest(),
        })
        return objs

    def test_list_all_versions(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions in container
        listing_parms = {'format': 'json', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(6, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj3_v1['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj3_v1['id'],
        }, {
            'name': obj2_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish20',
            'hash': '966634ebf2fc135707d6753692bf4b1e',
            'is_latest': True,
            'version_id': obj2_v1['id'],
        }, {
            'name': obj1_v4['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }, {
            'name': obj1_v3['name'],
            'bytes': 8,
            'content_type': 'text/jibberish13',
            'hash': md5(b'version3', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v3['id'],
        }, {
            'name': obj1_v2['name'],
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v2['id'],
        }, {
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v1['id'],
        }])

    def test_list_all_versions_reverse(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions in container in reverse order
        listing_parms = {'format': 'json', 'reverse': 'true', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(6, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v1['id'],
        }, {
            'name': obj1_v2['name'],
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v2['id'],
        }, {
            'name': obj1_v3['name'],
            'bytes': 8,
            'content_type': 'text/jibberish13',
            'hash': md5(b'version3', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v3['id'],
        }, {
            'name': obj1_v4['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }, {
            'name': obj2_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish20',
            'hash': '966634ebf2fc135707d6753692bf4b1e',
            'is_latest': True,
            'version_id': obj2_v1['id'],
        }, {
            'name': obj3_v1['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj3_v1['id'],
        }])

    def test_list_versions_prefix(self):

        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions for a given object
        listing_parms = {'format': 'json',
                         'versions': None, 'prefix': obj1_v1['name']}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(4, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v4['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }, {
            'name': obj1_v3['name'],
            'bytes': 8,
            'content_type': 'text/jibberish13',
            'hash': md5(b'version3', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v3['id'],
        }, {
            'name': obj1_v2['name'],
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v2['id'],
        }, {
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v1['id'],
        }])

    def test_list_versions_prefix_reverse(self):

        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions for a given object in reverse order
        listing_parms = {'format': 'json', 'reverse': 'true',
                         'versions': None, 'prefix': obj1_v1['name']}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(4, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v1['id'],
        }, {
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v2['id'],
        }, {
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish13',
            'hash': md5(b'version3', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v3['id'],
        }, {
            'name': obj1_v1['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }])

    def test_list_limit(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions in container
        listing_parms = {'format': 'json',
                         'limit': 3,
                         'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(3, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj3_v1['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj3_v1['id'],
        }, {
            'name': obj2_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish20',
            'hash': '966634ebf2fc135707d6753692bf4b1e',
            'is_latest': True,
            'version_id': obj2_v1['id'],
        }, {
            'name': obj1_v4['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }])

    def test_list_limit_marker(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions in container
        listing_parms = {'format': 'json',
                         'limit': 2,
                         'marker': obj2_v1['name'],
                         'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(2, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v4['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }, {
            'name': obj1_v3['name'],
            'bytes': 8,
            'content_type': 'text/jibberish13',
            'hash': md5(b'version3', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v3['id'],
        }])

    def test_list_version_marker(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions starting with version_marker
        listing_parms = {'format': 'json',
                         'marker': obj1_v3['name'],
                         'version_marker': obj1_v3['id'],
                         'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(2, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v2['name'],
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v2['id'],
        }, {
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v1['id'],
        }])

    def test_list_version_marker_reverse(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list all versions starting with version_marker in reverse order
        listing_parms = {'format': 'json',
                         'marker': obj1_v3['name'],
                         'version_marker': obj1_v3['id'],
                         'reverse': 'true',
                         'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(3, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v4['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }, {
            'name': obj2_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish20',
            'hash': '966634ebf2fc135707d6753692bf4b1e',
            'is_latest': True,
            'version_id': obj2_v1['id'],
        }, {
            'name': obj3_v1['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj3_v1['id'],
        }])

    def test_list_prefix_version_marker(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list versions with prefix starting with version_marker
        listing_parms = {'format': 'json',
                         'prefix': obj1_v3['name'],
                         'marker': obj1_v3['name'],
                         'version_marker': obj1_v3['id'],
                         'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(2, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v2['name'],
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v2['id'],
        }, {
            'name': obj1_v1['name'],
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj1_v1['id'],
        }])

    def test_list_prefix_version_marker_reverse(self):
        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        # list versions with prefix starting with version_marker
        # in reverse order
        listing_parms = {'format': 'json',
                         'prefix': obj1_v3['name'],
                         'marker': obj1_v3['name'],
                         'version_marker': obj1_v3['id'],
                         'reverse': 'true',
                         'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(1, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj1_v4['name'],
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': True,
            'version_id': obj1_v4['id'],
        }])

    def test_unacceptable(self):
        def do_test(format):
            with self.assertRaises(ResponseError) as caught:
                self.env.container.files(parms={
                    'format': format, 'versions': None})
            self.assertEqual(caught.exception.status, 406)

        do_test('plain')
        do_test('xml')

        def do_test(accept):
            with self.assertRaises(ResponseError) as caught:
                self.env.container.files(hdrs={'Accept': accept},
                                         parms={'versions': None})
            self.assertEqual(caught.exception.status, 406)

        do_test('text/plain')
        do_test('text/xml')
        do_test('application/xml')
        do_test('foo/bar')

    def testFileListingLimitMarkerPrefix(self):
        cont = self.env.container

        files = ['apple', 'banana', 'cacao', 'date', 'elderberry']
        for f in files:
            file_item = cont.file(f)
            self.assertTrue(file_item.write_random())
            # immediately ovewrite
            self.assertTrue(file_item.write_random())
            time.sleep(.01)  # guarantee that the timestamp changes

        # sanity
        for i in range(len(files)):
            f = files[i]
            for j in range(1, len(files) - i):
                self.assertEqual(cont.files(parms={'limit': j, 'marker': f}),
                                 files[i + 1: i + j + 1])
            self.assertEqual(cont.files(parms={'marker': f}), files[i + 1:])
            self.assertEqual(cont.files(parms={'marker': f, 'prefix': f}), [])
            self.assertEqual(cont.files(parms={'prefix': f}), [f])

        # repeat items in files list
        versions = [f2 for f1 in files for f2 in (f1,) * 2]

        # now list versions too
        v = 0
        for i in range(len(files)):
            f = files[i]
            for j in range(1, len(files) - i):
                self.assertEqual(versions[i + v + 2: i + j + v + 2], [
                    item['name'] for item in cont.files(parms={
                        'limit': j, 'marker': f, 'versions': None})])
            self.assertEqual(versions[v + i + 2:], [
                item['name'] for item in cont.files(parms={
                    'marker': f, 'versions': None})])
            self.assertEqual(cont.files(parms={'marker': f, 'prefix': f,
                                               'versions': None}), [])
            self.assertEqual([f, f], [
                item['name'] for item in cont.files(parms={
                    'prefix': f, 'versions': None})])
            v = v + 1

    def testPrefixAndLimit(self):
        cont = self.env.container

        prefix_file_count = 10
        limit_count = 2
        prefixs = ['apple/', 'banana/', 'cacao/']
        prefix_files = {}

        for prefix in prefixs:
            prefix_files[prefix] = []

            for i in range(prefix_file_count):
                file_item = cont.file(prefix + Utils.create_name())
                self.assertTrue(file_item.write_random())
                self.assertTrue(file_item.write_random())
                prefix_files[prefix].append(file_item.name)
                time.sleep(.01)  # guarantee that the timestamp changes

        versions_prefix_files = {}
        for prefix in prefixs:
            versions_prefix_files[prefix] = [f2 for f1 in prefix_files[prefix]
                                             for f2 in (f1,) * 2]
        # sanity
        for format_type in [None, 'json', 'xml']:
            for prefix in prefixs:
                files = cont.files(parms={'prefix': prefix,
                                          'format': format_type})
                if isinstance(files[0], dict):
                    files = [x.get('name', x.get('subdir')) for x in files]
                self.assertEqual(files, sorted(prefix_files[prefix]))

        # list versions
        for format_type in [None, 'json']:
            for prefix in prefixs:
                files = cont.files(parms={'prefix': prefix,
                                          'versions': None,
                                          'format': format_type})
                if isinstance(files[0], dict):
                    files = [x.get('name', x.get('subdir')) for x in files]
                self.assertEqual(files, sorted(versions_prefix_files[prefix]))

        # list versions
        for format_type in [None, 'json']:
            for prefix in prefixs:
                files = cont.files(parms={'limit': limit_count,
                                          'versions': None,
                                          'prefix': prefix,
                                          'format': format_type})
                if isinstance(files[0], dict):
                    files = [x.get('name', x.get('subdir')) for x in files]
                self.assertEqual(len(files), limit_count)

                for file_item in files:
                    self.assertTrue(file_item.startswith(prefix))

    def testListDelimiter(self):
        cont = self.env.container

        delimiter = '-'
        files = ['test', delimiter.join(['test', 'bar']),
                 delimiter.join(['test', 'foo'])]
        for f in files:
            file_item = cont.file(f)
            self.assertTrue(file_item.write_random())

        # object with no current version, just a delete marker
        del_file = 'del-baz'
        obj = self.env.container.file(del_file)
        obj.delete(tolerate_missing=True)
        self.assertEqual(obj.conn.response.status, 404)

        # now, turn off versioning and write a un-versioned obj
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})

        # a new write will not have a version-id
        off_file = 'off-xyz'
        obj = self.env.container.file(off_file)
        resp = obj.write(b'unversioned', return_resp=True)
        self.assertIsNone(resp.getheader('x-object-version-id'))

        # sanity
        # list latest, delete marker should not show-up
        for format_type in [None, 'json', 'xml']:
            results = cont.files(parms={'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['off-xyz', 'test', 'test-bar',
                                       'test-foo'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['off-', 'test', 'test-'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'format': format_type,
                                        'reverse': 'yes'})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['test-', 'test', 'off-'])

        # list versions, we should see delete marker here
        for format_type in [None, 'json']:
            results = cont.files(parms={'versions': None,
                                        'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['del-baz', 'off-xyz', 'test',
                                       'test-bar', 'test-foo'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'versions': None,
                                        'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['del-', 'off-', 'test', 'test-'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'versions': None,
                                        'format': format_type,
                                        'reverse': 'yes'})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['test-', 'test', 'off-', 'del-'])

    def testListMultiCharDelimiter(self):
        cont = self.env.container

        delimiter = '-&'
        files = ['test', delimiter.join(['test', 'bar']),
                 delimiter.join(['test', 'foo'])]
        for f in files:
            file_item = cont.file(f)
            self.assertTrue(file_item.write_random())

        # object with no current version, just a delete marker
        del_file = 'del-&baz'
        obj = self.env.container.file(del_file)
        obj.delete(tolerate_missing=True)
        self.assertEqual(obj.conn.response.status, 404)

        # now, turn off versioning and write a un-versioned obj
        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})

        # a new write will not have a version-id
        off_file = 'off-&xyz'
        obj = self.env.container.file(off_file)
        resp = obj.write(b'unversioned', return_resp=True)
        self.assertIsNone(resp.getheader('x-object-version-id'))

        # sanity
        # list latest, delete marker should not show-up
        for format_type in [None, 'json', 'xml']:
            results = cont.files(parms={'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['off-&xyz', 'test', 'test-&bar',
                                       'test-&foo'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['off-&', 'test', 'test-&'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'format': format_type,
                                        'reverse': 'yes'})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['test-&', 'test', 'off-&'])

        # list versions, we should see delete marker here
        for format_type in [None, 'json']:
            results = cont.files(parms={'versions': None,
                                        'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['del-&baz', 'off-&xyz', 'test',
                                       'test-&bar', 'test-&foo'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'versions': None,
                                        'format': format_type})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['del-&', 'off-&', 'test', 'test-&'])

            results = cont.files(parms={'delimiter': delimiter,
                                        'versions': None,
                                        'format': format_type,
                                        'reverse': 'yes'})
            if isinstance(results[0], dict):
                results = [x.get('name', x.get('subdir')) for x in results]
            self.assertEqual(results, ['test-&', 'test', 'off-&', 'del-&'])

    def test_bytes_count(self):

        container = self.env.container

        # first store a non-versioned object
        # disable versioning
        container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})
        self.assertFalse(
            config_true_value(container.info()['versions_enabled']))

        obj = container.file(Utils.create_name())
        self.assertTrue(obj.write(b'not-versioned'))
        self.assertTotalVersions(container, 1)

        # enable versioning
        container.update_metadata(
            hdrs={self.env.versions_header_key: 'True'})
        self.assertTrue(
            config_true_value(container.info()['versions_enabled']))

        obj1_v1, obj1_v2, obj1_v3, obj1_v4, obj2_v1, obj3_v1 = \
            self._prep_object_versions()

        self.assertEqual(int(container.info()['bytes_used']), 32 + obj.size)
        self.assertEqual(int(container.info()['object_count']), 2)
        self.assertTotalVersions(container, 7)

    def test_container_quota_bytes(self):
        if 'container_quotas' not in tf.cluster_info:
            raise SkipTest('Container quotas not enabled')

        if tf.in_process:
            tf.skip_if_no_xattrs()

        container = self.env.container

        # write two versions of 5 bytes each
        obj = container.file(Utils.create_name())
        self.assertTrue(obj.write(b'aaaaa'))
        self.assertTrue(obj.write(b'bbbbb'))
        self.assertTotalVersions(container, 2)

        # set X-Container-Meta-Quota-Bytes is 10
        container.update_metadata(
            hdrs={'X-Container-Meta-Quota-Bytes': '10'})
        self.assertEqual(container.info()['container_quota_bytes'], '10')

        with self.assertRaises(ResponseError) as cm:
            obj.write(b'ccccc')
        self.assertEqual(413, cm.exception.status)

        # reset container quota
        container.update_metadata(
            hdrs={'X-Container-Meta-Quota-Bytes': ''})

    def test_list_unversioned_container(self):
        _obj1, obj2, obj3 = self._prep_unversioned_objects()
        # _obj1 got deleted, so won't show up at all
        item2 = {
            'name': obj2,
            'bytes': 5,
            'content_type': 'text/blah-blah-blah',
            'hash': md5(b'first', usedforsecurity=False).hexdigest(),
            'is_latest': True,
            'version_id': 'null',
        }
        item3 = {
            'name': obj3,
            'bytes': 6,
            'content_type': 'text/plain',
            'hash': md5(b'second', usedforsecurity=False).hexdigest(),
            'is_latest': True,
            'version_id': 'null',
        }

        # version-aware listing works for unversioned containers
        listing_parms = {'format': 'json',
                         'versions': None}
        listing = self.env.unversioned_container.files(parms=listing_parms)
        for item in listing:
            item.pop('last_modified')
        self.assertEqual(listing, [item2, item3])

        listing_parms = {'format': 'json',
                         'prefix': obj2[:2],
                         'versions': None}
        listing = self.env.unversioned_container.files(parms=listing_parms)
        for item in listing:
            item.pop('last_modified')
        self.assertEqual(listing, [item2])

        listing_parms = {'format': 'json',
                         'marker': obj2,
                         'versions': None}
        listing = self.env.unversioned_container.files(parms=listing_parms)
        for item in listing:
            item.pop('last_modified')
        self.assertEqual(listing, [item3])

        listing_parms = {'format': 'json',
                         'delimiter': 'er',
                         'versions': None}
        listing = self.env.unversioned_container.files(parms=listing_parms)
        for item in listing:
            if 'name' in item:
                item.pop('last_modified')
        self.assertEqual(listing, [item2, {'subdir': 'order'}])

        listing_parms = {'format': 'json',
                         'reverse': 'true',
                         'versions': None}
        listing = self.env.unversioned_container.files(parms=listing_parms)
        for item in listing:
            item.pop('last_modified')
        self.assertEqual(listing, [item3, item2])

    def test_is_latest(self):
        obj = self.env.container.file(Utils.create_name())

        # v1
        resp = obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish11',
            'ETag': md5(b'version1', usedforsecurity=False).hexdigest(),
        }, return_resp=True)
        obj_v1 = resp.getheader('x-object-version-id')

        # v2
        resp = obj.write(b'version2', hdrs={
            'Content-Type': 'text/jibberish12',
            'ETag': md5(b'version2', usedforsecurity=False).hexdigest(),
        }, return_resp=True)
        obj_v2 = resp.getheader('x-object-version-id')

        obj.delete()
        resp_headers = {
            h.lower(): v for h, v in obj.conn.response.getheaders()}
        obj_v3 = resp_headers.get('x-object-version-id')

        resp = obj.write(b'version4', hdrs={
            'Content-Type': 'text/jibberish14',
            'ETag': md5(b'version4', usedforsecurity=False).hexdigest(),
        }, return_resp=True)
        obj_v4 = resp.getheader('x-object-version-id')

        listing_parms = {'format': 'json', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(4, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj.name,
            'bytes': 8,
            'content_type': 'text/jibberish14',
            'hash': md5(b'version4', usedforsecurity=False).hexdigest(),
            'is_latest': True,
            'version_id': obj_v4,
        }, {
            'name': obj.name,
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': False,
            'version_id': obj_v3,
        }, {
            'name': obj.name,
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj_v2,
        }, {
            'name': obj.name,
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj_v1,
        }])

        self.env.container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})

        # v5 - non-versioned
        obj.write(b'version5', hdrs={
            'Content-Type': 'text/jibberish15',
            'ETag': md5(b'version5', usedforsecurity=False).hexdigest(),
        })

        listing_parms = {'format': 'json', 'versions': None}
        prev_versions = self.env.container.files(parms=listing_parms)
        self.assertEqual(5, len(prev_versions))
        for pv in prev_versions:
            pv.pop('last_modified')
        self.assertEqual(prev_versions, [{
            'name': obj.name,
            'bytes': 8,
            'content_type': 'text/jibberish15',
            'hash': md5(b'version5', usedforsecurity=False).hexdigest(),
            'is_latest': True,
            'version_id': 'null',
        }, {
            'name': obj.name,
            'bytes': 8,
            'content_type': 'text/jibberish14',
            'hash': md5(b'version4', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj_v4,
        }, {
            'name': obj.name,
            'bytes': 0,
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
            'hash': MD5_OF_EMPTY_STRING,
            'is_latest': False,
            'version_id': obj_v3,
        }, {
            'name': obj.name,
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj_v2,
        }, {
            'name': obj.name,
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': False,
            'version_id': obj_v1,
        }])


class TestContainerOperationsUTF8(Base2, TestContainerOperations):
    pass


class TestDeleteContainer(TestObjectVersioningBase):
    def tearDown(self):
        # do nothing since test will delete all data + container
        pass

    def test_delete_container(self):
        # sanity
        container = self.env.container
        self.assertTrue(
            config_true_value(container.info()['versions_enabled']))
        self.assertTotalVersions(container, 0)

        # write an object to be versioned
        obj = container.file(Utils.create_name)
        obj.write(b"foo")
        self.assertTotalVersions(container, 1)

        # delete object and attempt to delete container
        obj.delete()
        self.assertTotalVersions(container, 2)

        # expect failure because versioning is enabled and
        # old versions still exist
        self.assertFalse(container.delete())

        # disable it
        container.update_metadata(
            hdrs={self.env.versions_header_key: 'False'})
        self.assertFalse(
            config_true_value(container.info()['versions_enabled']))

        # expect failure because old versions still exist
        self.assertFalse(container.delete())

        # delete older versions
        self._tear_down_files(container)
        self.assertTotalVersions(container, 0)

        # and finally delete container
        self.assertTrue(container.delete())


class TestSloWithVersioning(TestObjectVersioningBase):

    def setUp(self):
        super(TestSloWithVersioning, self).setUp()

        if 'slo' not in cluster_info:
            raise SkipTest("SLO not enabled")
        if tf.in_process:
            tf.skip_if_no_xattrs()

        # create a container with versioning
        self.env.versions_header_key = 'X-Versions-Enabled'
        self.container = self.env.account.container(Utils.create_name())
        container_headers = {self.env.versions_header_key: 'True'}
        if not self.container.create(hdrs=container_headers):
            raise ResponseError(self.conn.response)

        self.segments_container = self.env.account.container(
            Utils.create_name())
        if not self.segments_container.create():
            raise ResponseError(self.conn.response)

        # create some segments
        self.seg_info = {}
        for letter, size in (('a', 1024 * 1024),
                             ('b', 1024 * 1024)):
            seg_name = letter
            file_item = self.segments_container.file(seg_name)
            file_item.write((letter * size).encode('ascii'))
            self.seg_info[seg_name] = {
                'size_bytes': size,
                'etag': file_item.md5,
                'path': '/%s/%s' % (self.segments_container.name, seg_name)}

    @property
    def account_name(self):
        if not self._account_name:
            self._account_name = self.env.account.conn.storage_path.rsplit(
                '/', 1)[-1]
        return self._account_name

    def _create_manifest(self, seg_names):
        # create a manifest in the versioning container
        file_item = self.container.file("my-slo-manifest")
        manifest = [self.seg_info[seg_name] for seg_name in seg_names]
        resp = file_item.write(
            json.dumps(manifest).encode('ascii'),
            parms={'multipart-manifest': 'put'},
            return_resp=True)
        version_id = resp.getheader('x-object-version-id')
        return file_item, version_id

    def _assert_is_manifest(self, file_item, seg_name, version_id=None):
        if version_id:
            read_params = {'multipart-manifest': 'get',
                           'version-id': version_id}
        else:
            read_params = {'multipart-manifest': 'get'}
        manifest_body = file_item.read(parms=read_params)
        resp_headers = {
            h.lower(): v for h, v in file_item.conn.response.getheaders()}
        self.assertIn('x-static-large-object', resp_headers)
        self.assertEqual('application/json; charset=utf-8',
                         file_item.content_type)
        try:
            manifest = json.loads(manifest_body)
        except ValueError:
            self.fail("GET with multipart-manifest=get got invalid json")

        self.assertEqual(1, len(manifest))
        key_map = {'etag': 'hash', 'size_bytes': 'bytes', 'path': 'name'}

        for k_client, k_slo in key_map.items():
            self.assertEqual(self.seg_info[seg_name][k_client],
                             manifest[0][k_slo])

    def _assert_is_object(self, file_item, seg_data, version_id=None):
        if version_id:
            file_contents = file_item.read(parms={'version-id': version_id})
        else:
            file_contents = file_item.read()
        self.assertEqual(1024 * 1024, len(file_contents))
        self.assertEqual(seg_data, file_contents[:1])
        self.assertEqual(seg_data, file_contents[-1:])

    def tearDown(self):
        self._tear_down_files(self.container)

    def test_slo_manifest_version(self):
        file_item, v1_version_id = self._create_manifest(['a'])
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, b'a')

        # upload new manifest
        file_item, v2_version_id = self._create_manifest(['b'])
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'b')
        self._assert_is_object(file_item, b'b')

        # we wrote two versions
        self.assertTotalVersions(self.container, 2)

        # check the version 1 is still a manifest
        self._assert_is_manifest(file_item, 'a', v1_version_id)
        self._assert_is_object(file_item, b'a', v1_version_id)

        # listing looks good
        file_info = file_item.info()
        manifest_info = file_item.info(parms={'multipart-manifest': 'get'})
        obj_list = self.container.files(parms={'format': 'json'})
        for o in obj_list:
            o.pop('last_modified')
            # TODO: add symlink_path back in expected
            o.pop('symlink_path')
        expected = {
            'bytes': file_info['content_length'],
            'content_type': 'application/octet-stream',
            'hash': normalize_etag(manifest_info['etag']),
            'name': 'my-slo-manifest',
            'slo_etag': file_info['etag'],
            'version_symlink': True,
        }
        self.assertEqual([expected], obj_list)

        # delete the newest manifest
        file_item.delete()

        # expect to have 3 versions now, last one being a delete-marker
        self.assertTotalVersions(self.container, 3)

        # restore version 1
        file_item.copy(self.container.name, file_item.name,
                       parms={'multipart-manifest': 'get',
                              'version-id': v1_version_id})
        self.assertTotalVersions(self.container, 4)
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, b'a')

        # versioned container listing still looks slo-like
        file_info = file_item.info()
        manifest_info = file_item.info(parms={'multipart-manifest': 'get'})
        obj_list = self.container.files(parms={'format': 'json'})
        for o in obj_list:
            o.pop('last_modified')
            # TODO: add symlink_path back in expected
            o.pop('symlink_path')
        expected = {
            'bytes': file_info['content_length'],
            'content_type': 'application/octet-stream',
            'hash': normalize_etag(manifest_info['etag']),
            'name': 'my-slo-manifest',
            'slo_etag': file_info['etag'],
            'version_symlink': True,
        }
        self.assertEqual([expected], obj_list)

        status = file_item.conn.make_request(
            'DELETE', file_item.path,
            hdrs={'Accept': 'application/json'},
            parms={'multipart-manifest': 'delete',
                   'version-id': v1_version_id})
        body = file_item.conn.response.read()
        self.assertEqual(status, 200, body)
        resp = json.loads(body)
        self.assertEqual(resp['Response Status'], '200 OK')
        self.assertEqual(resp['Errors'], [])
        self.assertEqual(resp['Number Deleted'], 2)

        self.assertTotalVersions(self.container, 3)
        # Since we included the ?multipart-manifest=delete, segments
        # got cleaned up and now the current version is busted
        with self.assertRaises(ResponseError) as caught:
            file_item.read()
        self.assertEqual(409, caught.exception.status)

    def test_links_to_slo(self):
        file_item, v1_version_id = self._create_manifest(['a'])
        slo_info = file_item.info()

        symlink_name = Utils.create_name()
        sym_tgt_header = quote(unquote('%s/%s' % (
            self.container.name, file_item.name)))
        symlink = self.container.file(symlink_name)

        # symlink to the slo
        sym_headers = {'X-Symlink-Target': sym_tgt_header}
        symlink.write(b'', hdrs=sym_headers)
        self.assertEqual(slo_info, symlink.info())

        # hardlink to the slo
        sym_headers['X-Symlink-Target-Etag'] = slo_info['x_manifest_etag']
        symlink.write(b'', hdrs=sym_headers)
        self.assertEqual(slo_info, symlink.info())

    def test_slo_HEAD_part_number_with_version(self):
        file_item, version_id = self._create_manifest(['a', 'b'])
        file_item.info(parms={'part-number': '1',
                              'version-id': version_id},
                       exp_status=206)
        sizes = [seg['size_bytes']
                 for seg in (self.seg_info['a'], self.seg_info['b'])]
        total_size = sum(sizes)
        resp = file_item.conn.response
        self.assertEqual(version_id, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('2', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes 0-%s/%s' % (sizes[0] - 1, total_size),
                         resp.getheader('Content-Range'))

        file_item.info(parms={'part-number': '2',
                              'version-id': version_id},
                       exp_status=206)
        resp = file_item.conn.response
        self.assertEqual(version_id, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('2', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes %s-%s/%s'
                         % (sizes[1], total_size - 1, total_size),
                         resp.getheader('Content-Range'))

        file_item.info(parms={'part-number': '3',
                              'version-id': version_id},
                       exp_status=416)
        resp = file_item.conn.response
        self.assertEqual(version_id, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('2', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes */%s' % total_size,
                         resp.getheader('Content-Range'))

    def test_slo_GET_part_number_with_version(self):
        file_item, version_id = self._create_manifest(['a', 'b'])
        body = file_item.read(parms={'part-number': '1',
                                     'version-id': version_id})
        sizes = [seg['size_bytes']
                 for seg in (self.seg_info['a'], self.seg_info['b'])]
        total_size = sum(sizes)
        resp = file_item.conn.response
        self.assertEqual(version_id, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('2', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes 0-%s/%s' % (sizes[0] - 1, total_size),
                         resp.getheader('Content-Range'))
        self.assertEqual(('a' * sizes[0]).encode('ascii'), body)

        body = file_item.read(parms={'part-number': '2',
                                     'version-id': version_id})
        resp = file_item.conn.response
        self.assertEqual(version_id, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('2', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes %s-%s/%s'
                         % (sizes[1], total_size - 1, total_size),
                         resp.getheader('Content-Range'))
        self.assertEqual(('b' * sizes[0]).encode('ascii'), body)

        with self.assertRaises(ResponseError):
            file_item.read(parms={'part-number': '3',
                                  'version-id': version_id})
        self.assertEqual(416, file_item.conn.response.status)
        resp = file_item.conn.response
        self.assertEqual(version_id, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('2', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes */%s' % total_size,
                         resp.getheader('Content-Range'))

    def test_slo_HEAD_part_number_multiple_versions(self):
        file_item, version_id_1 = self._create_manifest(['a', 'b'])
        file_item, version_id_2 = self._create_manifest(['a'])
        # older version has 2 parts
        file_item.info(parms={'part-number': '2',
                              'version-id': version_id_1},
                       exp_status=206)
        sizes = [seg['size_bytes']
                 for seg in (self.seg_info['a'], self.seg_info['b'])]
        total_size = sum(sizes)
        resp = file_item.conn.response
        self.assertEqual(version_id_1, resp.getheader('x-object-version-id'))
        self.assertEqual('2', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes %s-%s/%s'
                         % (sizes[1], total_size - 1, total_size),
                         resp.getheader('Content-Range'))

        # newer version has only 1 part
        file_item.info(parms={'part-number': '1',
                              'version-id': version_id_2},
                       exp_status=206)
        resp = file_item.conn.response
        self.assertEqual(version_id_2, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('1', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes %s-%s/%s'
                         % (0, sizes[0] - 1, sizes[0]),
                         resp.getheader('Content-Range'))

        file_item.info(parms={'part-number': '2',
                              'version-id': version_id_2},
                       exp_status=416)
        resp = file_item.conn.response
        self.assertEqual(version_id_2, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('1', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes */%s' % sizes[0],
                         resp.getheader('Content-Range'))

        # current version == newer version has only 1 part
        file_item.info(parms={'part-number': '2'},
                       exp_status=416)
        resp = file_item.conn.response
        self.assertEqual(version_id_2, resp.getheader('X-Object-Version-Id'))
        self.assertEqual('1', resp.getheader('X-Parts-Count'))
        self.assertEqual('bytes */%s' % sizes[0],
                         resp.getheader('Content-Range'))


class TestSloWithVersioningUTF8(Base2, TestSloWithVersioning):
    pass


class TestVersionsLocationWithVersioning(TestObjectVersioningBase):

    # create a container with versioned writes
    location_header_key = 'X-Versions-Location'

    def setUp(self):
        super(TestVersionsLocationWithVersioning, self).setUp()

        prefix = Utils.create_name()[:10]
        self.versions_container = self.env.account.container(
            prefix + "-versions")
        if not self.versions_container.create():
            raise ResponseError(self.conn.response)

        self.container = self.env.account.container(prefix + "-objs")
        container_headers = {
            self.location_header_key: quote(self.versions_container.name)}
        if not self.container.create(hdrs=container_headers):
            raise ResponseError(self.conn.response)

    def _prep_object_versions(self):

        # object with multiple versions
        object_name = Utils.create_name()
        obj = self.container.file(object_name)

        # v1
        obj.write(b'version1', hdrs={
            'Content-Type': 'text/jibberish11',
            'ETag': md5(b'version1', usedforsecurity=False).hexdigest(),
        })

        # v2
        obj.write(b'version2', hdrs={
            'Content-Type': 'text/jibberish12',
            'ETag': md5(b'version2', usedforsecurity=False).hexdigest(),
        })

        # v3
        obj.write(b'version3', hdrs={
            'Content-Type': 'text/jibberish13',
            'ETag': md5(b'version3', usedforsecurity=False).hexdigest(),
        })

        return obj

    def test_list_with_versions_param(self):
        obj = self._prep_object_versions()
        obj_name = obj.name

        listing_parms = {'format': 'json', 'versions': None}
        current_versions = self.container.files(parms=listing_parms)
        self.assertEqual(1, len(current_versions))
        for pv in current_versions:
            pv.pop('last_modified')
        self.assertEqual(current_versions, [{
            'name': obj_name,
            'bytes': 8,
            'content_type': 'text/jibberish13',
            'hash': md5(b'version3', usedforsecurity=False).hexdigest(),
            'is_latest': True,
            'version_id': 'null'
        }])

        prev_versions = self.versions_container.files(parms=listing_parms)
        self.assertEqual(2, len(prev_versions))

        for pv in prev_versions:
            pv.pop('last_modified')
            name = pv.pop('name')
            self.assertTrue(name.startswith('%03x%s/' % (len(obj_name),
                                                         obj_name)))

        self.assertEqual(prev_versions, [{
            'bytes': 8,
            'content_type': 'text/jibberish11',
            'hash': md5(b'version1', usedforsecurity=False).hexdigest(),
            'is_latest': True,
            'version_id': 'null',
        }, {
            'bytes': 8,
            'content_type': 'text/jibberish12',
            'hash': md5(b'version2', usedforsecurity=False).hexdigest(),
            'is_latest': True,
            'version_id': 'null'
        }])

    def test_delete_with_null_version_id(self):
        obj = self._prep_object_versions()

        # sanity
        self.assertEqual(b'version3', obj.read())

        obj.delete(parms={'version-id': 'null'})
        if self.location_header_key == 'X-Versions-Location':
            self.assertEqual(b'version2', obj.read())
        else:
            with self.assertRaises(ResponseError) as caught:
                obj.read()
            self.assertEqual(404, caught.exception.status)


class TestHistoryLocationWithVersioning(TestVersionsLocationWithVersioning):

    # create a container with versioned writes
    location_header_key = 'X-History-Location'


class TestVersioningAccountTempurl(TestObjectVersioningBase):
    env = TestTempurlEnv
    digest_name = 'sha256'

    def setUp(self):
        self.env.versions_header_key = 'X-Versions-Enabled'
        super(TestVersioningAccountTempurl, self).setUp()
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
        self.expires = int(time.time()) + 86400
        self.obj_tempurl_parms = self.tempurl_parms(
            'GET', self.expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)

    def tempurl_parms(self, method, expires, path, key):
        path = unquote(path)
        method = method.encode('utf8')
        path = path.encode('utf8')
        key = key.encode('utf8')
        sig = hmac.new(
            key,
            b'%s\n%d\n%s' % (method, expires, path),
            self.digest).hexdigest()
        return {'temp_url_sig': sig, 'temp_url_expires': str(expires)}

    def test_PUT(self):
        obj = self.env.obj

        # give out a signature which allows a PUT to obj
        expires = int(time.time()) + 86400
        put_parms = self.tempurl_parms(
            'PUT', expires, self.env.conn.make_path(obj.path),
            self.env.tempurl_key)

        # try to overwrite existing object
        resp = obj.write(b"version2", parms=put_parms,
                         cfg={'no_auth_token': True},
                         return_resp=True)
        resp_headers = {
            h.lower(): v for h, v in resp.getheaders()}
        self.assertIn('x-object-version-id', resp_headers)

    def test_GET_latest(self):
        obj = self.env.obj

        expires = int(time.time()) + 86400
        get_parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(obj.path),
            self.env.tempurl_key)

        # get v1 object (., version-id=null, no symlinks involved)
        contents = obj.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"obj contents")

        # give out a signature which allows a PUT to obj
        expires = int(time.time()) + 86400
        put_parms = self.tempurl_parms(
            'PUT', expires, self.env.conn.make_path(obj.path),
            self.env.tempurl_key)

        # try to overwrite existing object
        resp = obj.write(b"version2", parms=put_parms,
                         cfg={'no_auth_token': True},
                         return_resp=True)
        resp_headers = {
            h.lower(): v for h, v in resp.getheaders()}
        self.assertIn('x-object-version-id', resp_headers)

        # get v2 object
        contents = obj.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"version2")

    def test_GET_version_id(self):
        # N.B.: The test is not intended to imply the desired behavior
        # of a tempurl GET with version-id. Currently version-id is simply
        # ignored as the latest version is always returned. In the future,
        # users should be able to create a tempurl with version-id as a
        # parameter.

        # overwrite object a couple more times
        obj = self.env.obj
        resp = obj.write(b"version2", return_resp=True)
        v2_version_id = resp.getheader('x-object-version-id')
        obj.write(b"version3!!!")

        expires = int(time.time()) + 86400
        get_parms = self.tempurl_parms(
            'GET', expires, self.env.conn.make_path(obj.path),
            self.env.tempurl_key)
        get_parms['version-id'] = v2_version_id

        contents = obj.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"version3!!!")


class TestVersioningContainerTempurl(TestObjectVersioningBase):
    env = TestContainerTempurlEnv
    digest_name = 'sha256'

    def setUp(self):
        self.env.versions_header_key = 'X-Versions-Enabled'
        super(TestVersioningContainerTempurl, self).setUp()
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
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(self.env.obj.path),
            self.env.tempurl_key)
        self.obj_tempurl_parms = {'temp_url_sig': sig,
                                  'temp_url_expires': str(expires)}

    def tempurl_sig(self, method, expires, path, key):
        path = unquote(path)
        method = method.encode('utf8')
        path = path.encode('utf8')
        key = key.encode('utf8')
        return hmac.new(
            key,
            b'%s\n%d\n%s' % (method, expires, path),
            self.digest).hexdigest()

    def test_PUT(self):
        obj = self.env.obj

        # give out a signature which allows a PUT to new_obj
        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'PUT', expires, self.env.conn.make_path(obj.path),
            self.env.tempurl_key)
        put_parms = {'temp_url_sig': sig,
                     'temp_url_expires': str(expires)}

        # try to overwrite existing object
        resp = obj.write(b"version2", parms=put_parms,
                         cfg={'no_auth_token': True},
                         return_resp=True)
        resp_headers = {
            h.lower(): v for h, v in resp.getheaders()}
        self.assertIn('x-object-version-id', resp_headers)

    def test_GET_latest(self):
        obj = self.env.obj

        expires = int(time.time()) + 86400
        sig = self.tempurl_sig(
            'GET', expires, self.env.conn.make_path(obj.path),
            self.env.tempurl_key)
        get_parms = {'temp_url_sig': sig,
                     'temp_url_expires': str(expires)}

        # get v1 object (., version-id=null, no symlinks involved)
        contents = obj.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"obj contents")

        # overwrite existing object
        obj.write(b"version2")

        # get v2 object (reading from versions container)
        # versioning symlink allows us to bypass the normal
        # container-tempurl-key scoping
        contents = obj.read(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
        self.assertEqual(contents, b"version2")
        # HEAD works, too
        obj.info(parms=get_parms, cfg={'no_auth_token': True})
        self.assert_status([200])
