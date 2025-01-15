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

from copy import deepcopy
import json
import time
import unittest
from urllib.parse import quote, unquote
from unittest import SkipTest

import test.functional as tf

from swift.common.utils import MD5_OF_EMPTY_STRING
from test.functional.tests import Base, Base2, BaseEnv, Utils
from test.functional import cluster_info
from test.functional.swift_test_client import Account, Connection, \
    ResponseError


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestObjectVersioningEnv(BaseEnv):
    versioning_enabled = None  # tri-state: None initially, then True/False
    location_header_key = 'X-Versions-Location'
    account2 = None

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
        cls.versions_container = cls.account.container(prefix + "-versions")
        if not cls.versions_container.create():
            raise ResponseError(cls.conn.response)

        cls.container = cls.account.container(prefix + "-objs")
        container_headers = {
            cls.location_header_key: quote(cls.versions_container.name)}
        if not cls.container.create(hdrs=container_headers):
            if cls.conn.response.status == 412:
                cls.versioning_enabled = False
                return
            raise ResponseError(cls.conn.response)

        container_info = cls.container.info()
        # if versioning is off, then cls.location_header_key won't persist
        cls.versioning_enabled = 'versions' in container_info

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

    @classmethod
    def tearDown(cls):
        if cls.account:
            cls.account.delete_containers()
        if cls.account2:
            cls.account2.delete_containers()


class TestCrossPolicyObjectVersioningEnv(BaseEnv):
    # tri-state: None initially, then True/False
    versioning_enabled = None
    multiple_policies_enabled = None
    policies = None
    location_header_key = 'X-Versions-Location'
    account2 = None

    @classmethod
    def setUp(cls):
        super(TestCrossPolicyObjectVersioningEnv, cls).setUp()
        if cls.multiple_policies_enabled is None:
            try:
                cls.policies = tf.FunctionalStoragePolicyCollection.from_info()
            except AssertionError:
                pass

        if cls.policies and len(cls.policies) > 1:
            cls.multiple_policies_enabled = True
        else:
            cls.multiple_policies_enabled = False
            cls.versioning_enabled = True
            # We don't actually know the state of versioning, but without
            # multiple policies the tests should be skipped anyway. Claiming
            # versioning support lets us report the right reason for skipping.
            return

        policy = cls.policies.select()
        version_policy = cls.policies.exclude(name=policy['name']).select()

        if not tf.skip2:
            # Second connection for ACL tests
            config2 = deepcopy(tf.config)
            config2['account'] = tf.config['account2']
            config2['username'] = tf.config['username2']
            config2['password'] = tf.config['password2']
            cls.conn2 = Connection(config2)
            cls.conn2.authenticate()

        prefix = Utils.create_name()[:10]
        cls.versions_container = cls.account.container(prefix + "-versions")
        if not cls.versions_container.create(
                {'X-Storage-Policy': policy['name']}):
            raise ResponseError(cls.conn.response)

        cls.container = cls.account.container(prefix + "-objs")
        if not cls.container.create(
                hdrs={cls.location_header_key: cls.versions_container.name,
                      'X-Storage-Policy': version_policy['name']}):
            if cls.conn.response.status == 412:
                cls.versioning_enabled = False
                return
            raise ResponseError(cls.conn.response)

        container_info = cls.container.info()
        # if versioning is off, then X-Versions-Location won't persist
        cls.versioning_enabled = 'versions' in container_info

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

    @classmethod
    def tearDown(cls):
        if cls.account:
            cls.account.delete_containers()
        if cls.account2:
            cls.account2.delete_containers()


class TestObjectVersioningHistoryModeEnv(TestObjectVersioningEnv):
    location_header_key = 'X-History-Location'


class TestObjectVersioning(Base):
    env = TestObjectVersioningEnv

    def setUp(self):
        super(TestObjectVersioning, self).setUp()
        if self.env.versioning_enabled is False:
            raise SkipTest("Object versioning not enabled")
        elif self.env.versioning_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected versioning_enabled to be True/False, got %r" %
                (self.env.versioning_enabled,))

    def _tear_down_files(self):
        try:
            # only delete files and not containers
            # as they were configured in self.env
            # get rid of any versions so they aren't restored
            self.env.versions_container.delete_files()
            # get rid of originals
            self.env.container.delete_files()
            # in history mode, deleted originals got copied to versions, so
            # clear that again
            self.env.versions_container.delete_files()
        except ResponseError:
            pass

    def tearDown(self):
        super(TestObjectVersioning, self).tearDown()
        self._tear_down_files()

    def test_clear_version_option(self):
        # sanity
        header_val = quote(self.env.versions_container.name)
        self.assertEqual(self.env.container.info()['versions'], header_val)
        self.env.container.update_metadata(
            hdrs={self.env.location_header_key: ''})
        self.assertIsNone(self.env.container.info().get('versions'))

        # set location back to the way it was
        self.env.container.update_metadata(
            hdrs={self.env.location_header_key: header_val})
        self.assertEqual(self.env.container.info()['versions'], header_val)

    def _test_overwriting_setup(self, obj_name=None):
        container = self.env.container
        versions_container = self.env.versions_container
        cont_info = container.info()
        self.assertEqual(cont_info['versions'], quote(versions_container.name))
        expected_content_types = []
        obj_name = obj_name or Utils.create_name()

        versioned_obj = container.file(obj_name)
        put_headers = {'Content-Type': 'text/jibberish01',
                       'Content-Encoding': 'gzip',
                       'Content-Disposition': 'attachment; filename=myfile'}
        versioned_obj.write(b"aaaaa", hdrs=put_headers)
        obj_info = versioned_obj.info()
        self.assertEqual('text/jibberish01', obj_info['content_type'])
        expected_content_types.append('text/jibberish01')

        # the allowed headers are configurable in object server, so we cannot
        # assert that content-encoding or content-disposition get *copied* to
        # the object version unless they were set on the original PUT, so
        # populate expected_headers by making a HEAD on the original object
        resp_headers = {
            h.lower(): v for h, v in versioned_obj.conn.response.getheaders()}
        expected_headers = {}
        for k, v in put_headers.items():
            if k.lower() in resp_headers:
                expected_headers[k] = v

        self.assertEqual(0, versions_container.info()['object_count'])
        versioned_obj.write(b"bbbbb", hdrs={'Content-Type': 'text/jibberish02',
                            'X-Object-Meta-Foo': 'Bar'})
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish02')
        expected_content_types.append('text/jibberish02')
        self.assertEqual(versioned_obj.metadata['foo'], 'Bar')

        # the old version got saved off
        self.assertEqual(1, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[0]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual(b"aaaaa", prev_version.read())
        self.assertEqual(prev_version.content_type, 'text/jibberish01')

        resp_headers = {
            h.lower(): v for h, v in prev_version.conn.response.getheaders()}
        for k, v in expected_headers.items():
            self.assertIn(k.lower(), resp_headers)
            self.assertEqual(v, resp_headers[k.lower()])

        # make sure the new obj metadata did not leak to the prev. version
        self.assertNotIn('foo', prev_version.metadata)

        # check that POST does not create a new version
        versioned_obj.sync_metadata(metadata={'fu': 'baz'})
        self.assertEqual(1, versions_container.info()['object_count'])

        # if we overwrite it again, there are two versions
        versioned_obj.write(b"ccccc")
        self.assertEqual(2, versions_container.info()['object_count'])
        expected_content_types.append('text/jibberish02')
        versioned_obj_name = versions_container.files()[1]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual(b"bbbbb", prev_version.read())
        self.assertEqual(prev_version.content_type, 'text/jibberish02')
        self.assertNotIn('foo', prev_version.metadata)
        self.assertIn('fu', prev_version.metadata)

        # versioned_obj keeps the newest content
        self.assertEqual(b"ccccc", versioned_obj.read())

        # test copy from a different container
        src_container = self.env.account.container(Utils.create_name())
        self.assertTrue(src_container.create())
        src_name = Utils.create_name()
        src_obj = src_container.file(src_name)
        src_obj.write(b"ddddd", hdrs={'Content-Type': 'text/jibberish04'})
        src_obj.copy(container.name, obj_name)

        self.assertEqual(b"ddddd", versioned_obj.read())
        versioned_obj.initialize()
        self.assertEqual(versioned_obj.content_type, 'text/jibberish04')
        expected_content_types.append('text/jibberish04')

        # make sure versions container has the previous version
        self.assertEqual(3, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[2]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual(b"ccccc", prev_version.read())

        # for further use in the mode-specific tests
        return (versioned_obj, expected_headers, expected_content_types)

    def test_overwriting(self):
        versions_container = self.env.versions_container
        versioned_obj, expected_headers, expected_content_types = \
            self._test_overwriting_setup()

        # pop one for the current version
        expected_content_types.pop()
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        # test delete
        versioned_obj.delete()
        self.assertEqual(b"ccccc", versioned_obj.read())
        expected_content_types.pop()
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        versioned_obj.delete()
        self.assertEqual(b"bbbbb", versioned_obj.read())
        expected_content_types.pop()
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        versioned_obj.delete()
        self.assertEqual(b"aaaaa", versioned_obj.read())
        self.assertEqual(0, versions_container.info()['object_count'])

        # verify that all the original object headers have been copied back
        obj_info = versioned_obj.info()
        self.assertEqual('text/jibberish01', obj_info['content_type'])
        resp_headers = {
            h.lower(): v for h, v in versioned_obj.conn.response.getheaders()}
        for k, v in expected_headers.items():
            self.assertIn(k.lower(), resp_headers)
            self.assertEqual(v, resp_headers[k.lower()])

        versioned_obj.delete()
        self.assertRaises(ResponseError, versioned_obj.read)

    def test_overwriting_with_url_encoded_object_name(self):
        versions_container = self.env.versions_container
        obj_name = Utils.create_name() + '%25ff'
        versioned_obj, expected_headers, expected_content_types = \
            self._test_overwriting_setup(obj_name)

        # pop one for the current version
        expected_content_types.pop()
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        # test delete
        versioned_obj.delete()
        self.assertEqual(b"ccccc", versioned_obj.read())
        expected_content_types.pop()
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        versioned_obj.delete()
        self.assertEqual(b"bbbbb", versioned_obj.read())
        expected_content_types.pop()
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        versioned_obj.delete()
        self.assertEqual(b"aaaaa", versioned_obj.read())
        self.assertEqual(0, versions_container.info()['object_count'])

        # verify that all the original object headers have been copied back
        obj_info = versioned_obj.info()
        self.assertEqual('text/jibberish01', obj_info['content_type'])
        resp_headers = {
            h.lower(): v for h, v in versioned_obj.conn.response.getheaders()}
        for k, v in expected_headers.items():
            self.assertIn(k.lower(), resp_headers)
            self.assertEqual(v, resp_headers[k.lower()])

        versioned_obj.delete()
        self.assertRaises(ResponseError, versioned_obj.read)

    def assert_most_recent_version(self, obj_name, content,
                                   should_be_dlo=False):
        name_len = len(obj_name.encode('utf8'))
        archive_versions = self.env.versions_container.files(parms={
            'prefix': '%03x%s/' % (name_len, obj_name),
            'reverse': 'yes'})
        archive_file = self.env.versions_container.file(archive_versions[0])
        self.assertEqual(content, archive_file.read())
        resp_headers = {
            h.lower(): v for h, v in archive_file.conn.response.getheaders()}
        if should_be_dlo:
            self.assertIn('x-object-manifest', resp_headers)
        else:
            self.assertNotIn('x-object-manifest', resp_headers)

    def _test_versioning_dlo_setup(self):
        if tf.in_process:
            tf.skip_if_no_xattrs()

        container = self.env.container
        versions_container = self.env.versions_container
        obj_name = Utils.create_name()

        for i in ('1', '2', '3'):
            time.sleep(.01)  # guarantee that the timestamp changes
            obj_name_seg = obj_name + '/' + i
            versioned_obj = container.file(obj_name_seg)
            versioned_obj.write(i.encode('ascii'))
            # immediately overwrite
            versioned_obj.write((i + i).encode('ascii'))

        self.assertEqual(3, versions_container.info()['object_count'])

        man_file = container.file(obj_name)

        # write a normal file first
        man_file.write(b'old content')

        # guarantee that the timestamp changes
        time.sleep(.01)

        # overwrite with a dlo manifest
        man_file.write(b'', hdrs={"X-Object-Manifest": "%s/%s/" %
                       (self.env.container.name, obj_name)})

        self.assertEqual(4, versions_container.info()['object_count'])
        self.assertEqual(b"112233", man_file.read())
        self.assert_most_recent_version(obj_name, b'old content')

        # overwrite the manifest with a normal file
        man_file.write(b'new content')
        self.assertEqual(5, versions_container.info()['object_count'])

        # new most-recent archive is the dlo
        self.assert_most_recent_version(
            obj_name, b'112233', should_be_dlo=True)

        return obj_name, man_file

    def test_versioning_dlo(self):
        obj_name, man_file = self._test_versioning_dlo_setup()

        # verify that restore works properly
        man_file.delete()
        self.assertEqual(4, self.env.versions_container.info()['object_count'])
        self.assertEqual(b"112233", man_file.read())
        resp_headers = {
            h.lower(): v for h, v in man_file.conn.response.getheaders()}
        self.assertIn('x-object-manifest', resp_headers)

        self.assert_most_recent_version(obj_name, b'old content')

        man_file.delete()
        self.assertEqual(3, self.env.versions_container.info()['object_count'])
        self.assertEqual(b"old content", man_file.read())

    def test_versioning_container_acl(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        # create versions container and DO NOT give write access to account2
        versions_container = self.env.account.container(Utils.create_name())
        location_header_val = quote(str(versions_container))
        self.assertTrue(versions_container.create(hdrs={
            'X-Container-Write': ''
        }))

        # check account2 cannot write to versions container
        fail_obj_name = Utils.create_name()
        fail_obj = versions_container.file(fail_obj_name)
        self.assertRaises(ResponseError, fail_obj.write, b"should fail",
                          cfg={'use_token': self.env.storage_token2})

        # create container and give write access to account2
        # don't set X-Versions-Location just yet
        container = self.env.account.container(Utils.create_name())
        self.assertTrue(container.create(hdrs={
            'X-Container-Write': self.env.conn2.user_acl}))

        # check account2 cannot set X-Versions-Location on container
        self.assertRaises(ResponseError, container.update_metadata, hdrs={
            self.env.location_header_key: location_header_val},
            cfg={'use_token': self.env.storage_token2})

        # good! now let admin set the X-Versions-Location
        # p.s.: sticking a 'x-remove' header here to test precedence
        # of both headers. Setting the location should succeed.
        self.assertTrue(container.update_metadata(hdrs={
            'X-Remove-' + self.env.location_header_key[len('X-'):]:
            location_header_val,
            self.env.location_header_key: location_header_val}))

        # write object twice to container and check version
        obj_name = Utils.create_name()
        versioned_obj = container.file(obj_name)
        self.assertTrue(versioned_obj.write(b"never argue with the data",
                        cfg={'use_token': self.env.storage_token2}))
        self.assertEqual(versioned_obj.read(), b"never argue with the data")

        self.assertTrue(
            versioned_obj.write(b"we don't have no beer, just tequila",
                                cfg={'use_token': self.env.storage_token2}))
        self.assertEqual(versioned_obj.read(),
                         b"we don't have no beer, just tequila")
        self.assertEqual(1, versions_container.info()['object_count'])

        # read the original uploaded object
        for filename in versions_container.files():
            backup_file = versions_container.file(filename)
            break
        self.assertEqual(backup_file.read(), b"never argue with the data")

        if not tf.skip3:
            # user3 (some random user with no access to any of account1)
            # tries to read from versioned container
            self.assertRaises(ResponseError, backup_file.read,
                              cfg={'use_token': self.env.storage_token3})

            # create an object user3 can try to copy
            a2_container = self.env.account2.container(Utils.create_name())
            a2_container.create(
                hdrs={'X-Container-Read': self.env.conn3.user_acl},
                cfg={'use_token': self.env.storage_token2})
            a2_obj = a2_container.file(Utils.create_name())
            self.assertTrue(a2_obj.write(b"unused",
                            cfg={'use_token': self.env.storage_token2}))

            # user3 also cannot write, delete, or copy to/from source container
            number_of_versions = versions_container.info()['object_count']
            self.assertRaises(ResponseError, versioned_obj.write,
                              b"some random user trying to write data",
                              cfg={'use_token': self.env.storage_token3})
            self.assertEqual(number_of_versions,
                             versions_container.info()['object_count'])
            self.assertRaises(ResponseError, versioned_obj.delete,
                              cfg={'use_token': self.env.storage_token3})
            self.assertEqual(number_of_versions,
                             versions_container.info()['object_count'])
            self.assertRaises(
                ResponseError, versioned_obj.write,
                hdrs={
                    'X-Copy-From': '%s/%s' % (a2_container.name, a2_obj.name),
                    'X-Copy-From-Account': self.env.conn2.account_name},
                cfg={'use_token': self.env.storage_token3})
            self.assertEqual(number_of_versions,
                             versions_container.info()['object_count'])
            self.assertRaises(
                ResponseError, a2_obj.copy_account,
                self.env.conn.account_name, container.name, obj_name,
                cfg={'use_token': self.env.storage_token3})
            self.assertEqual(number_of_versions,
                             versions_container.info()['object_count'])

        # user2 can't read or delete from versions-location
        self.assertRaises(ResponseError, backup_file.read,
                          cfg={'use_token': self.env.storage_token2})
        self.assertRaises(ResponseError, backup_file.delete,
                          cfg={'use_token': self.env.storage_token2})

        # but is able to delete from the source container
        # this could be a helpful scenario for dev ops that want to setup
        # just one container to hold object versions of multiple containers
        # and each one of those containers are owned by different users
        self.assertTrue(versioned_obj.delete(
                        cfg={'use_token': self.env.storage_token2}))

        # tear-down since we create these containers here
        # and not in self.env
        if not tf.skip3:
            a2_container.delete_recursive()
        versions_container.delete_recursive()
        container.delete_recursive()

    def _test_versioning_check_acl_setup(self):
        container = self.env.container
        versions_container = self.env.versions_container
        versions_container.create(hdrs={'X-Container-Read': '.r:*,.rlistings'})

        obj_name = Utils.create_name()
        versioned_obj = container.file(obj_name)
        versioned_obj.write(b"aaaaa")
        self.assertEqual(b"aaaaa", versioned_obj.read())

        versioned_obj.write(b"bbbbb")
        self.assertEqual(b"bbbbb", versioned_obj.read())

        # Use token from second account and try to delete the object
        org_token = self.env.account.conn.storage_token
        self.env.account.conn.storage_token = self.env.conn2.storage_token
        try:
            with self.assertRaises(ResponseError) as cm:
                versioned_obj.delete()
            self.assertEqual(403, cm.exception.status)
        finally:
            self.env.account.conn.storage_token = org_token

        # Verify with token from first account
        self.assertEqual(b"bbbbb", versioned_obj.read())
        return versioned_obj

    def test_versioning_check_acl(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        versioned_obj = self._test_versioning_check_acl_setup()
        versioned_obj.delete()
        self.assertEqual(b"aaaaa", versioned_obj.read())

    def _check_overwriting_symlink(self):
        # assertions common to x-versions-location and x-history-location modes
        container = self.env.container
        versions_container = self.env.versions_container

        tgt_a_name = Utils.create_name()
        tgt_b_name = Utils.create_name()

        tgt_a = container.file(tgt_a_name)
        tgt_a.write(b"aaaaa")

        tgt_b = container.file(tgt_b_name)
        tgt_b.write(b"bbbbb")

        symlink_name = Utils.create_name()
        sym_tgt_header = quote(unquote('%s/%s' % (container.name, tgt_a_name)))
        sym_headers_a = {'X-Symlink-Target': sym_tgt_header}
        symlink = container.file(symlink_name)
        symlink.write(b"", hdrs=sym_headers_a)
        self.assertEqual(b"aaaaa", symlink.read())

        sym_headers_b = {'X-Symlink-Target': '%s/%s' % (container.name,
                                                        tgt_b_name)}
        symlink.write(b"", hdrs=sym_headers_b)
        self.assertEqual(b"bbbbb", symlink.read())

        # the old version got saved off
        self.assertEqual(1, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[0]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version_info = prev_version.info(parms={'symlink': 'get'})
        self.assertEqual(b"aaaaa", prev_version.read())
        symlink_etag = prev_version_info['etag']
        if symlink_etag.startswith('"') and symlink_etag.endswith('"') and \
                symlink_etag[1:-1]:
            symlink_etag = symlink_etag[1:-1]
        self.assertEqual(MD5_OF_EMPTY_STRING, symlink_etag)
        self.assertEqual(sym_tgt_header,
                         prev_version_info['x_symlink_target'])
        return symlink, tgt_a

    def test_overwriting_symlink(self):
        if 'symlink' not in cluster_info:
            raise SkipTest("Symlinks not enabled")

        symlink, target = self._check_overwriting_symlink()
        # test delete
        symlink.delete()
        sym_info = symlink.info(parms={'symlink': 'get'})
        self.assertEqual(b"aaaaa", symlink.read())
        if tf.cluster_info.get('etag_quoter', {}).get('enable_by_default'):
            self.assertEqual('"%s"' % MD5_OF_EMPTY_STRING, sym_info['etag'])
        else:
            self.assertEqual(MD5_OF_EMPTY_STRING, sym_info['etag'])
        self.assertEqual(
            quote(unquote('%s/%s' % (self.env.container.name, target.name))),
            sym_info['x_symlink_target'])

    def _setup_symlink(self):
        target = self.env.container.file('target-object')
        target.write(b'target object data')
        symlink = self.env.container.file('symlink')
        symlink.write(b'', hdrs={
            'Content-Type': 'application/symlink',
            'X-Symlink-Target': '%s/%s' % (
                self.env.container.name, target.name)})
        return symlink, target

    def _assert_symlink(self, symlink, target):
        self.assertEqual(b'target object data', symlink.read())
        self.assertEqual(target.info(), symlink.info())
        self.assertEqual('application/symlink',
                         symlink.info(parms={
                             'symlink': 'get'})['content_type'])

    def _check_copy_destination_restore_symlink(self):
        # assertions common to x-versions-location and x-history-location modes
        symlink, target = self._setup_symlink()
        symlink.write(b'this is not a symlink')
        # the symlink is versioned
        version_container_files = self.env.versions_container.files(
            parms={'format': 'json'})
        self.assertEqual(1, len(version_container_files))
        versioned_obj_info = version_container_files[0]
        self.assertEqual('application/symlink',
                         versioned_obj_info['content_type'])
        versioned_obj = self.env.versions_container.file(
            versioned_obj_info['name'])
        # the symlink is still a symlink
        self._assert_symlink(versioned_obj, target)
        # test manual restore (this creates a new backup of the overwrite)
        versioned_obj.copy(self.env.container.name, symlink.name,
                           parms={'symlink': 'get'})
        self._assert_symlink(symlink, target)
        # symlink overwritten by write then copy -> 2 versions
        self.assertEqual(2, self.env.versions_container.info()['object_count'])
        return symlink, target

    def test_copy_destination_restore_symlink(self):
        if 'symlink' not in cluster_info:
            raise SkipTest("Symlinks not enabled")

        symlink, target = self._check_copy_destination_restore_symlink()
        # and versioned writes restore
        symlink.delete()
        self.assertEqual(1, self.env.versions_container.info()['object_count'])
        self.assertEqual(b'this is not a symlink', symlink.read())
        symlink.delete()
        self.assertEqual(0, self.env.versions_container.info()['object_count'])
        self._assert_symlink(symlink, target)

    def test_put_x_copy_from_restore_symlink(self):
        if 'symlink' not in cluster_info:
            raise SkipTest("Symlinks not enabled")

        symlink, target = self._setup_symlink()
        symlink.write(b'this is not a symlink')
        version_container_files = self.env.versions_container.files()
        self.assertEqual(1, len(version_container_files))
        versioned_obj = self.env.versions_container.file(
            version_container_files[0])
        symlink.write(parms={'symlink': 'get'}, cfg={
            'no_content_type': True}, hdrs={
                'X-Copy-From': '%s/%s' % (
                    self.env.versions_container, versioned_obj.name)})
        self._assert_symlink(symlink, target)


class TestObjectVersioningUTF8(Base2, TestObjectVersioning):

    def tearDown(self):
        self._tear_down_files()
        super(TestObjectVersioningUTF8, self).tearDown()


class TestCrossPolicyObjectVersioning(TestObjectVersioning):
    env = TestCrossPolicyObjectVersioningEnv

    def setUp(self):
        super(TestCrossPolicyObjectVersioning, self).setUp()
        if self.env.multiple_policies_enabled is False:
            raise SkipTest('Cross policy test requires multiple policies')
        elif self.env.multiple_policies_enabled is not True:
            # just some sanity checking
            raise Exception("Expected multiple_policies_enabled "
                            "to be True/False, got %r" % (
                                self.env.versioning_enabled,))


class TestObjectVersioningHistoryMode(TestObjectVersioning):
    env = TestObjectVersioningHistoryModeEnv

    # those override tests includes assertions for delete versioned objects
    # behaviors different from default object versioning using
    # x-versions-location.

    def test_overwriting(self):
        versions_container = self.env.versions_container
        versioned_obj, expected_headers, expected_content_types = \
            self._test_overwriting_setup()

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
        self.assertEqual(8, versions_container.info()['object_count'])
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        # update versioned_obj
        versioned_obj.write(b"eeee", hdrs={'Content-Type': 'text/thanksgiving',
                            'X-Object-Meta-Bar': 'foo'})
        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/thanksgiving', obj_info['content_type'])

        # we still have delete-marker there
        self.assertEqual(8, versions_container.info()['object_count'])

        # update versioned_obj
        versioned_obj.write(b"ffff", hdrs={'Content-Type': 'text/teriyaki',
                            'X-Object-Meta-Food': 'chickin'})
        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/teriyaki', obj_info['content_type'])

        # new obj will be inserted after delete-marker there
        self.assertEqual(9, versions_container.info()['object_count'])

        versioned_obj.delete()
        with self.assertRaises(ResponseError) as cm:
            versioned_obj.read()
        self.assertEqual(404, cm.exception.status)
        self.assertEqual(11, versions_container.info()['object_count'])

    def test_overwriting_with_url_encoded_object_name(self):
        versions_container = self.env.versions_container
        obj_name = Utils.create_name() + '%25ff'
        versioned_obj, expected_headers, expected_content_types = \
            self._test_overwriting_setup(obj_name)

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
        self.assertEqual(8, versions_container.info()['object_count'])
        self.assertEqual(expected_content_types, [
            o['content_type'] for o in versions_container.files(
                parms={'format': 'json'})])

        # update versioned_obj
        versioned_obj.write(b"eeee", hdrs={'Content-Type': 'text/thanksgiving',
                            'X-Object-Meta-Bar': 'foo'})
        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/thanksgiving', obj_info['content_type'])

        # we still have delete-marker there
        self.assertEqual(8, versions_container.info()['object_count'])

        # update versioned_obj
        versioned_obj.write(b"ffff", hdrs={'Content-Type': 'text/teriyaki',
                            'X-Object-Meta-Food': 'chickin'})
        # verify the PUT object is kept successfully
        obj_info = versioned_obj.info()
        self.assertEqual('text/teriyaki', obj_info['content_type'])

        # new obj will be inserted after delete-marker there
        self.assertEqual(9, versions_container.info()['object_count'])

        versioned_obj.delete()
        with self.assertRaises(ResponseError) as cm:
            versioned_obj.read()
        self.assertEqual(404, cm.exception.status)

    def test_versioning_dlo(self):
        obj_name, man_file = \
            self._test_versioning_dlo_setup()

        man_file.delete()
        with self.assertRaises(ResponseError) as cm:
            man_file.read()
        self.assertEqual(404, cm.exception.status)
        self.assertEqual(7, self.env.versions_container.info()['object_count'])

        expected = [b'old content', b'112233', b'new content', b'']

        name_len = len(obj_name.encode('utf8'))
        bodies = [
            self.env.versions_container.file(f).read()
            for f in self.env.versions_container.files(parms={
                'prefix': '%03x%s/' % (name_len, obj_name)})]
        self.assertEqual(expected, bodies)

    def test_versioning_check_acl(self):
        if tf.skip2:
            raise SkipTest('Account2 not set')
        versioned_obj = self._test_versioning_check_acl_setup()
        versioned_obj.delete()
        with self.assertRaises(ResponseError) as cm:
            versioned_obj.read()
        self.assertEqual(404, cm.exception.status)

        # we have 3 objects in the versions_container, 'aaaaa', 'bbbbb'
        # and delete-marker with empty content
        self.assertEqual(3, self.env.versions_container.info()['object_count'])
        files = self.env.versions_container.files()
        for actual, expected in zip(files, [b'aaaaa', b'bbbbb', b'']):
            prev_version = self.env.versions_container.file(actual)
            self.assertEqual(expected, prev_version.read())

    def test_overwriting_symlink(self):
        if 'symlink' not in cluster_info:
            raise SkipTest("Symlinks not enabled")

        symlink, target = self._check_overwriting_symlink()
        # test delete
        symlink.delete()
        with self.assertRaises(ResponseError) as cm:
            symlink.read()
        self.assertEqual(404, cm.exception.status)

    def test_copy_destination_restore_symlink(self):
        if 'symlink' not in cluster_info:
            raise SkipTest("Symlinks not enabled")

        symlink, target = self._check_copy_destination_restore_symlink()
        symlink.delete()
        with self.assertRaises(ResponseError) as cm:
            symlink.read()
        self.assertEqual(404, cm.exception.status)
        # 2 versions plus delete marker and deleted version
        self.assertEqual(4, self.env.versions_container.info()['object_count'])


class TestObjectVersioningHistoryModeUTF8(
        Base2, TestObjectVersioningHistoryMode):
    pass


class TestSloWithVersioning(unittest.TestCase):

    def setUp(self):
        if 'slo' not in cluster_info:
            raise SkipTest("SLO not enabled")
        if tf.in_process:
            tf.skip_if_no_xattrs()

        self.conn = Connection(tf.config)
        self.conn.authenticate()
        self.account = Account(
            self.conn, tf.config.get('account', tf.config['username']))
        self.account.delete_containers()

        # create a container with versioning
        self.versions_container = self.account.container(Utils.create_name())
        self.container = self.account.container(Utils.create_name())
        self.segments_container = self.account.container(Utils.create_name())
        if not self.container.create(
                hdrs={'X-Versions-Location': self.versions_container.name}):
            if self.conn.response.status == 412:
                raise SkipTest("Object versioning not enabled")
            else:
                raise ResponseError(self.conn.response)
        if 'versions' not in self.container.info():
            raise SkipTest("Object versioning not enabled")

        for cont in (self.versions_container, self.segments_container):
            if not cont.create():
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

    def _create_manifest(self, seg_name):
        # create a manifest in the versioning container
        file_item = self.container.file("my-slo-manifest")
        file_item.write(
            json.dumps([self.seg_info[seg_name]]).encode('ascii'),
            parms={'multipart-manifest': 'put'})
        return file_item

    def _assert_is_manifest(self, file_item, seg_name):
        manifest_body = file_item.read(parms={'multipart-manifest': 'get'})
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
        key_map = {'etag': 'hash', 'size_bytes': 'bytes'}
        for k_client, k_slo in key_map.items():
            self.assertEqual(self.seg_info[seg_name][k_client],
                             manifest[0][k_slo])
        self.assertEqual(self.seg_info[seg_name]['path'],
                         manifest[0]['name'])

    def _assert_is_object(self, file_item, seg_data):
        file_contents = file_item.read()
        self.assertEqual(1024 * 1024, len(file_contents))
        self.assertEqual(seg_data, file_contents[:1])
        self.assertEqual(seg_data, file_contents[-1:])

    def tearDown(self):
        # remove versioning to allow simple container delete
        self.container.update_metadata(hdrs={'X-Versions-Location': ''})
        self.account.delete_containers()

    def test_slo_manifest_version(self):
        file_item = self._create_manifest('a')
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, b'a')

        # upload new manifest
        file_item = self._create_manifest('b')
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'b')
        self._assert_is_object(file_item, b'b')

        versions_list = self.versions_container.files()
        self.assertEqual(1, len(versions_list))
        version_file = self.versions_container.file(versions_list[0])
        # check the version is still a manifest
        self._assert_is_manifest(version_file, 'a')
        self._assert_is_object(version_file, b'a')

        # delete the newest manifest
        file_item.delete()

        # expect the original manifest file to be restored
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, b'a')

    def test_slo_manifest_version_size(self):
        file_item = self._create_manifest('a')
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, b'a')

        # original manifest size
        primary_list = self.container.files(parms={'format': 'json'})
        self.assertEqual(1, len(primary_list))
        org_size = primary_list[0]['bytes']

        # upload new manifest
        file_item = self._create_manifest('b')
        # sanity check: read the manifest, then the large object
        self._assert_is_manifest(file_item, 'b')
        self._assert_is_object(file_item, b'b')

        versions_list = self.versions_container.files(parms={'format': 'json'})
        self.assertEqual(1, len(versions_list))
        version_file = self.versions_container.file(versions_list[0]['name'])
        version_file_size = versions_list[0]['bytes']
        # check the version is still a manifest
        self._assert_is_manifest(version_file, 'a')
        self._assert_is_object(version_file, b'a')

        # check the version size is correct
        self.assertEqual(version_file_size, org_size)

        # delete the newest manifest
        file_item.delete()

        # expect the original manifest file to be restored
        self._assert_is_manifest(file_item, 'a')
        self._assert_is_object(file_item, b'a')

        primary_list = self.container.files(parms={'format': 'json'})
        self.assertEqual(1, len(primary_list))
        primary_file_size = primary_list[0]['bytes']
        # expect the original manifest file size to be the same
        self.assertEqual(primary_file_size, org_size)


class TestSloWithVersioningUTF8(Base2, TestSloWithVersioning):
    pass


class TestObjectVersioningChangingMode(Base):
    env = TestObjectVersioningHistoryModeEnv

    def setUp(self):
        super(TestObjectVersioningChangingMode, self).setUp()
        if 'versioned_writes' not in cluster_info:
            raise SkipTest("versioned_writes not enabled")

    def test_delete_while_changing_mode(self):
        container = self.env.container
        versions_container = self.env.versions_container
        cont_info = container.info()
        self.assertEqual(cont_info['versions'], quote(versions_container.name))

        obj_name = Utils.create_name()
        versioned_obj = container.file(obj_name)

        versioned_obj.write(
            b"version1", hdrs={'Content-Type': 'text/jibberish01'})
        versioned_obj.write(
            b"version2", hdrs={'Content-Type': 'text/jibberish01'})

        # sanity, version1 object should have moved to versions_container
        self.assertEqual(1, versions_container.info()['object_count'])

        versioned_obj.delete()

        # version2 and the delete marker should have put in versions_container
        self.assertEqual(3, versions_container.info()['object_count'])
        delete_marker_name = versions_container.files()[2]
        delete_marker = versions_container.file(delete_marker_name)
        delete_marker.initialize()
        self.assertEqual(
            delete_marker.content_type,
            'application/x-deleted;swift_versions_deleted=1')

        # change to stack mode
        hdrs = {'X-Versions-Location': versions_container.name}
        container.update_metadata(hdrs=hdrs)

        versioned_obj.delete()

        # version2 object should have been moved in container
        self.assertEqual(b"version2", versioned_obj.read())

        # and there's only one version1 is left in versions_container
        self.assertEqual(1, versions_container.info()['object_count'])
        versioned_obj_name = versions_container.files()[0]
        prev_version = versions_container.file(versioned_obj_name)
        prev_version.initialize()
        self.assertEqual(b"version1", prev_version.read())
        self.assertEqual(prev_version.content_type, 'text/jibberish01')

        # reset and test double delete
        # change back to history mode
        hdrs = {'X-History-Location': versions_container.name}
        container.update_metadata(hdrs=hdrs)

        # double delete, second DELETE returns a 404 as expected
        versioned_obj.delete()
        with self.assertRaises(ResponseError) as cm:
            versioned_obj.delete()
        self.assertEqual(404, cm.exception.status)

        # There should now be 4 objects total in versions_container
        # 2 are delete markers
        self.assertEqual(4, versions_container.info()['object_count'])

        # change to stack mode
        hdrs = {'X-Versions-Location': versions_container.name}
        container.update_metadata(hdrs=hdrs)

        # a delete, just deletes one delete marker, it doesn't yet pop
        # version2 back in the container
        # This DELETE doesn't return a 404!
        versioned_obj.delete()
        self.assertEqual(3, versions_container.info()['object_count'])
        self.assertEqual(0, container.info()['object_count'])

        # neither does this one!
        versioned_obj.delete()

        # version2 object should have been moved in container
        self.assertEqual(b"version2", versioned_obj.read())

        # and there's only one version1 is left in versions_container
        self.assertEqual(1, versions_container.info()['object_count'])


class TestObjectVersioningChangingModeUTF8(
        Base2, TestObjectVersioningChangingMode):
    pass
