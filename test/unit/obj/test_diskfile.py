# -*- coding:utf-8 -*-
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

"""Tests for swift.obj.diskfile"""

import cPickle as pickle
import os
import errno
import itertools
import mock
import unittest
import email
import tempfile
import uuid
import xattr
import re
from collections import defaultdict
from random import shuffle, randint
from shutil import rmtree
from time import time
from tempfile import mkdtemp
from hashlib import md5
from contextlib import closing, nested
from gzip import GzipFile

from eventlet import hubs, timeout, tpool
from test.unit import (FakeLogger, mock as unit_mock, temptree,
                       patch_policies, debug_logger, EMPTY_ETAG)

from nose import SkipTest
from swift.obj import diskfile
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, Timestamp
from swift.common import ring
from swift.common.splice import splice
from swift.common.exceptions import DiskFileNotExist, DiskFileQuarantined, \
    DiskFileDeviceUnavailable, DiskFileDeleted, DiskFileNotOpen, \
    DiskFileError, ReplicationLockTimeout, DiskFileCollision, \
    DiskFileExpired, SwiftException, DiskFileNoSpace, DiskFileXattrNotSupported
from swift.common.storage_policy import (
    POLICIES, get_policy_string, StoragePolicy, ECStoragePolicy,
    BaseStoragePolicy, REPL_POLICY, EC_POLICY)


test_policies = [
    StoragePolicy(0, name='zero', is_default=True),
    ECStoragePolicy(1, name='one', is_default=False,
                    ec_type='jerasure_rs_vand',
                    ec_ndata=10, ec_nparity=4),
]


def find_paths_with_matching_suffixes(needed_matches=2, needed_suffixes=3):
    paths = defaultdict(list)
    while True:
        path = ('a', 'c', uuid.uuid4().hex)
        hash_ = hash_path(*path)
        suffix = hash_[-3:]
        paths[suffix].append(path)
        if len(paths) < needed_suffixes:
            # in the extreamly unlikely situation where you land the matches
            # you need before you get the total suffixes you need - it's
            # simpler to just ignore this suffix for now
            continue
        if len(paths[suffix]) >= needed_matches:
            break
    return paths, suffix


def _create_test_ring(path, policy):
    ring_name = get_policy_string('object', policy)
    testgz = os.path.join(path, ring_name + '.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2, 3, 4, 5, 6],
        [1, 2, 3, 0, 5, 6, 4],
        [2, 3, 0, 1, 6, 4, 5]]
    intended_devs = [
        {'id': 0, 'device': 'sda1', 'zone': 0, 'ip': '127.0.0.0',
         'port': 6000},
        {'id': 1, 'device': 'sda1', 'zone': 1, 'ip': '127.0.0.1',
         'port': 6000},
        {'id': 2, 'device': 'sda1', 'zone': 2, 'ip': '127.0.0.2',
         'port': 6000},
        {'id': 3, 'device': 'sda1', 'zone': 4, 'ip': '127.0.0.3',
         'port': 6000},
        {'id': 4, 'device': 'sda1', 'zone': 5, 'ip': '127.0.0.4',
         'port': 6000},
        {'id': 5, 'device': 'sda1', 'zone': 6,
         'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6000},
        {'id': 6, 'device': 'sda1', 'zone': 7,
         'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
         'port': 6000}]
    intended_part_shift = 30
    intended_reload_time = 15
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id, intended_devs,
                          intended_part_shift),
            f)
    return ring.Ring(path, ring_name=ring_name,
                     reload_time=intended_reload_time)


@patch_policies
class TestDiskFileModuleMethods(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        # Setup a test ring per policy (stolen from common/test_ring.py)
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        self.existing_device = 'sda1'
        os.mkdir(os.path.join(self.devices, self.existing_device))
        self.objects = os.path.join(self.devices, self.existing_device,
                                    'objects')
        os.mkdir(self.objects)
        self.parts = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(os.path.join(self.objects, part))
        self.ring = _create_test_ring(self.testdir, POLICIES.legacy)
        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')
        self.df_mgr = diskfile.DiskFileManager(self.conf, FakeLogger())

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def _create_diskfile(self, policy):
        return self.df_mgr.get_diskfile(self.existing_device,
                                        '0', 'a', 'c', 'o',
                                        policy=policy)

    def test_extract_policy(self):
        # good path names
        pn = 'objects/0/606/1984527ed7ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy(pn), POLICIES[0])
        pn = 'objects-1/0/606/198452b6ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy(pn), POLICIES[1])

        # leading slash
        pn = '/objects/0/606/1984527ed7ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy(pn), POLICIES[0])
        pn = '/objects-1/0/606/198452b6ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy(pn), POLICIES[1])

        # full paths
        good_path = '/srv/node/sda1/objects-1/1/abc/def/1234.data'
        self.assertEqual(diskfile.extract_policy(good_path), POLICIES[1])
        good_path = '/srv/node/sda1/objects/1/abc/def/1234.data'
        self.assertEqual(diskfile.extract_policy(good_path), POLICIES[0])

        # short paths
        path = '/srv/node/sda1/objects/1/1234.data'
        self.assertEqual(diskfile.extract_policy(path), POLICIES[0])
        path = '/srv/node/sda1/objects-1/1/1234.data'
        self.assertEqual(diskfile.extract_policy(path), POLICIES[1])

        # well formatted but, unknown policy index
        pn = 'objects-2/0/606/198427efcff042c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy(pn), None)

        # malformed path
        self.assertEqual(diskfile.extract_policy(''), None)
        bad_path = '/srv/node/sda1/objects-t/1/abc/def/1234.data'
        self.assertEqual(diskfile.extract_policy(bad_path), None)
        pn = 'XXXX/0/606/1984527ed42b6ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy(pn), None)
        bad_path = '/srv/node/sda1/foo-1/1/abc/def/1234.data'
        self.assertEqual(diskfile.extract_policy(bad_path), None)
        bad_path = '/srv/node/sda1/obj1/1/abc/def/1234.data'
        self.assertEqual(diskfile.extract_policy(bad_path), None)

    def test_quarantine_renamer(self):
        for policy in POLICIES:
            # we use this for convenience, not really about a diskfile layout
            df = self._create_diskfile(policy=policy)
            mkdirs(df._datadir)
            exp_dir = os.path.join(self.devices, 'quarantined',
                                   diskfile.get_data_dir(policy),
                                   os.path.basename(df._datadir))
            qbit = os.path.join(df._datadir, 'qbit')
            with open(qbit, 'w') as f:
                f.write('abc')
            to_dir = diskfile.quarantine_renamer(self.devices, qbit)
            self.assertEqual(to_dir, exp_dir)
            self.assertRaises(OSError, diskfile.quarantine_renamer,
                              self.devices, qbit)

    def test_get_data_dir(self):
        self.assertEquals(diskfile.get_data_dir(POLICIES[0]),
                          diskfile.DATADIR_BASE)
        self.assertEquals(diskfile.get_data_dir(POLICIES[1]),
                          diskfile.DATADIR_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_data_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_data_dir, 99)

    def test_get_async_dir(self):
        self.assertEquals(diskfile.get_async_dir(POLICIES[0]),
                          diskfile.ASYNCDIR_BASE)
        self.assertEquals(diskfile.get_async_dir(POLICIES[1]),
                          diskfile.ASYNCDIR_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_async_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_async_dir, 99)

    def test_get_tmp_dir(self):
        self.assertEquals(diskfile.get_tmp_dir(POLICIES[0]),
                          diskfile.TMP_BASE)
        self.assertEquals(diskfile.get_tmp_dir(POLICIES[1]),
                          diskfile.TMP_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_tmp_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_tmp_dir, 99)

    def test_pickle_async_update_tmp_dir(self):
        for policy in POLICIES:
            if int(policy) == 0:
                tmp_part = 'tmp'
            else:
                tmp_part = 'tmp-%d' % policy
            tmp_path = os.path.join(
                self.devices, self.existing_device, tmp_part)
            self.assertFalse(os.path.isdir(tmp_path))
            pickle_args = (self.existing_device, 'a', 'c', 'o',
                           'data', 0.0, policy)
            # async updates don't create their tmpdir on their own
            self.assertRaises(OSError, self.df_mgr.pickle_async_update,
                              *pickle_args)
            os.makedirs(tmp_path)
            # now create a async update
            self.df_mgr.pickle_async_update(*pickle_args)
            # check tempdir
            self.assertTrue(os.path.isdir(tmp_path))


@patch_policies
class TestObjectAuditLocationGenerator(unittest.TestCase):
    def _make_file(self, path):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise

        with open(path, 'w'):
            pass

    def test_audit_location_class(self):
        al = diskfile.AuditLocation('abc', '123', '_-_',
                                    policy=POLICIES.legacy)
        self.assertEqual(str(al), 'abc')

    def test_finding_of_hashdirs(self):
        with temptree([]) as tmpdir:
            # the good
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                                     "5c1fdc1ffb12e5eaf84edc30d8b67aca"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                                     "fdfd184d39080020bc8b487f8a7beaca"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "1519", "df2",
                                     "b0fe7af831cc7b1af5bf486b1c841df2"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "9720", "ca5",
                                     "4a943bc72c2e647c4675923d58cf4ca5"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects", "3071", "8eb",
                                     "fcd938702024c25fef6c32fef05298eb"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects-1", "9970", "ca5",
                                     "4a943bc72c2e647c4675923d58cf4ca5"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-2", "9971", "8eb",
                                     "fcd938702024c25fef6c32fef05298eb"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-99", "9972",
                                     "8eb",
                                     "fcd938702024c25fef6c32fef05298eb"))
            # the bad
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-", "1135",
                                     "6c3",
                                     "fcd938702024c25fef6c32fef05298eb"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-fud", "foo"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-+1", "foo"))

            self._make_file(os.path.join(tmpdir, "sdp", "objects", "1519",
                                         "fed"))
            self._make_file(os.path.join(tmpdir, "sdq", "objects", "9876"))

            # the empty
            os.makedirs(os.path.join(tmpdir, "sdr"))
            os.makedirs(os.path.join(tmpdir, "sds", "objects"))
            os.makedirs(os.path.join(tmpdir, "sdt", "objects", "9601"))
            os.makedirs(os.path.join(tmpdir, "sdu", "objects", "6499", "f80"))

            # the irrelevant
            os.makedirs(os.path.join(tmpdir, "sdv", "accounts", "77", "421",
                                     "4b8c86149a6d532f4af018578fd9f421"))
            os.makedirs(os.path.join(tmpdir, "sdw", "containers", "28", "51e",
                                     "4f9eee668b66c6f0250bfa3c7ab9e51e"))

            logger = debug_logger()
            locations = [(loc.path, loc.device, loc.partition, loc.policy)
                         for loc in diskfile.object_audit_location_generator(
                             devices=tmpdir, mount_check=False,
                             logger=logger)]
            locations.sort()

            # expect some warnings about those bad dirs
            warnings = logger.get_lines_for_level('warning')
            self.assertEqual(set(warnings), set([
                ("Directory 'objects-' does not map to a valid policy "
                 "(Unknown policy, for index '')"),
                ("Directory 'objects-2' does not map to a valid policy "
                 "(Unknown policy, for index '2')"),
                ("Directory 'objects-99' does not map to a valid policy "
                 "(Unknown policy, for index '99')"),
                ("Directory 'objects-fud' does not map to a valid policy "
                 "(Unknown policy, for index 'fud')"),
                ("Directory 'objects-+1' does not map to a valid policy "
                 "(Unknown policy, for index '+1')"),
            ]))

            expected =  \
                [(os.path.join(tmpdir, "sdp", "objects-1", "9970", "ca5",
                               "4a943bc72c2e647c4675923d58cf4ca5"),
                  "sdp", "9970", POLICIES[1]),
                 (os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                               "5c1fdc1ffb12e5eaf84edc30d8b67aca"),
                  "sdp", "1519", POLICIES[0]),
                 (os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                               "fdfd184d39080020bc8b487f8a7beaca"),
                  "sdp", "1519", POLICIES[0]),
                 (os.path.join(tmpdir, "sdp", "objects", "1519", "df2",
                               "b0fe7af831cc7b1af5bf486b1c841df2"),
                  "sdp", "1519", POLICIES[0]),
                 (os.path.join(tmpdir, "sdp", "objects", "9720", "ca5",
                               "4a943bc72c2e647c4675923d58cf4ca5"),
                  "sdp", "9720", POLICIES[0]),
                 (os.path.join(tmpdir, "sdq", "objects", "3071", "8eb",
                               "fcd938702024c25fef6c32fef05298eb"),
                  "sdq", "3071", POLICIES[0]),
                 ]
            self.assertEqual(locations, expected)

            # now without a logger
            locations = [(loc.path, loc.device, loc.partition, loc.policy)
                         for loc in diskfile.object_audit_location_generator(
                             devices=tmpdir, mount_check=False)]
            locations.sort()
            self.assertEqual(locations, expected)

    def test_skipping_unmounted_devices(self):
        def mock_ismount(path):
            return path.endswith('sdp')

        with mock.patch('swift.obj.diskfile.ismount', mock_ismount):
            with temptree([]) as tmpdir:
                os.makedirs(os.path.join(tmpdir, "sdp", "objects",
                                         "2607", "df3",
                                         "ec2871fe724411f91787462f97d30df3"))
                os.makedirs(os.path.join(tmpdir, "sdq", "objects",
                                         "9785", "a10",
                                         "4993d582f41be9771505a8d4cb237a10"))

                locations = [
                    (loc.path, loc.device, loc.partition, loc.policy)
                    for loc in diskfile.object_audit_location_generator(
                        devices=tmpdir, mount_check=True)]
                locations.sort()

                self.assertEqual(
                    locations,
                    [(os.path.join(tmpdir, "sdp", "objects",
                                   "2607", "df3",
                                   "ec2871fe724411f91787462f97d30df3"),
                      "sdp", "2607", POLICIES[0])])

                # Do it again, this time with a logger.
                ml = mock.MagicMock()
                locations = [
                    (loc.path, loc.device, loc.partition, loc.policy)
                    for loc in diskfile.object_audit_location_generator(
                        devices=tmpdir, mount_check=True, logger=ml)]
                ml.debug.assert_called_once_with(
                    'Skipping %s as it is not mounted',
                    'sdq')

    def test_only_catch_expected_errors(self):
        # Crazy exceptions should still escape object_audit_location_generator
        # so that errors get logged and a human can see what's going wrong;
        # only normal FS corruption should be skipped over silently.

        def list_locations(dirname):
            return [(loc.path, loc.device, loc.partition, loc.policy)
                    for loc in diskfile.object_audit_location_generator(
                        devices=dirname, mount_check=False)]

        real_listdir = os.listdir

        def splode_if_endswith(suffix):
            def sploder(path):
                if path.endswith(suffix):
                    raise OSError(errno.EACCES, "don't try to ad-lib")
                else:
                    return real_listdir(path)
            return sploder

        with temptree([]) as tmpdir:
            os.makedirs(os.path.join(tmpdir, "sdf", "objects",
                                     "2607", "b54",
                                     "fe450ec990a88cc4b252b181bab04b54"))
            with mock.patch('os.listdir', splode_if_endswith("sdf/objects")):
                self.assertRaises(OSError, list_locations, tmpdir)
            with mock.patch('os.listdir', splode_if_endswith("2607")):
                self.assertRaises(OSError, list_locations, tmpdir)
            with mock.patch('os.listdir', splode_if_endswith("b54")):
                self.assertRaises(OSError, list_locations, tmpdir)


class TestDiskFileRouter(unittest.TestCase):

    def test_register(self):
        with mock.patch.dict(
                diskfile.DiskFileRouter.policy_type_to_manager_cls, {}):
            @diskfile.DiskFileRouter.register('test-policy')
            class TestDiskFileManager(diskfile.DiskFileManager):
                pass

            @BaseStoragePolicy.register('test-policy')
            class TestStoragePolicy(BaseStoragePolicy):
                pass

            with patch_policies([TestStoragePolicy(0, 'test')]):
                router = diskfile.DiskFileRouter({}, debug_logger('test'))
                manager = router[POLICIES.default]
                self.assertTrue(isinstance(manager, TestDiskFileManager))


class BaseDiskFileTestMixin(object):
    """
    Bag of helpers that are useful in the per-policy DiskFile test classes.
    """

    def _manager_mock(self, manager_attribute_name, df=None):
        mgr_cls = df._manager.__class__ if df else self.mgr_cls
        return '.'.join([
            mgr_cls.__module__, mgr_cls.__name__, manager_attribute_name])


class DiskFileManagerMixin(BaseDiskFileTestMixin):
    """
    Abstract test method mixin for concrete test cases - this class
    won't get picked up by test runners because it doesn't subclass
    unittest.TestCase and doesn't have [Tt]est in the name.
    """

    # set mgr_cls on subclasses
    mgr_cls = None

    def setUp(self):
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(
            self.tmpdir, 'tmp_test_obj_server_DiskFile')
        self.existing_device1 = 'sda1'
        self.existing_device2 = 'sda2'
        for policy in POLICIES:
            mkdirs(os.path.join(self.testdir, self.existing_device1,
                                diskfile.get_tmp_dir(policy)))
            mkdirs(os.path.join(self.testdir, self.existing_device2,
                                diskfile.get_tmp_dir(policy)))
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.conf = dict(devices=self.testdir, mount_check='false',
                         keep_cache_size=2 * 1024)
        self.logger = debug_logger('test-' + self.__class__.__name__)
        self.df_mgr = self.mgr_cls(self.conf, self.logger)
        self.df_router = diskfile.DiskFileRouter(self.conf, self.logger)

    def tearDown(self):
        rmtree(self.tmpdir, ignore_errors=1)

    def _get_diskfile(self, policy, frag_index=None):
        df_mgr = self.df_router[policy]
        return df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                   policy=policy, frag_index=frag_index)

    def _test_get_ondisk_files(self, scenarios, policy,
                               frag_index=None):
        class_under_test = self._get_diskfile(policy, frag_index=frag_index)
        with mock.patch('swift.obj.diskfile.os.listdir',
                        lambda _: []):
            self.assertEqual((None, None, None),
                             class_under_test._get_ondisk_file())

        returned_ext_order = ('.data', '.meta', '.ts')
        for test in scenarios:
            chosen = dict((f[1], os.path.join(class_under_test._datadir, f[0]))
                          for f in test if f[1])
            expected = tuple(chosen.get(ext) for ext in returned_ext_order)
            files = list(zip(*test)[0])
            for _order in ('ordered', 'shuffled', 'shuffled'):
                class_under_test = self._get_diskfile(policy, frag_index)
                try:
                    with mock.patch('swift.obj.diskfile.os.listdir',
                                    lambda _: files):
                        actual = class_under_test._get_ondisk_file()
                        self.assertEqual(expected, actual,
                                         'Expected %s from %s but got %s'
                                         % (expected, files, actual))
                except AssertionError as e:
                    self.fail('%s with files %s' % (str(e), files))
                shuffle(files)

    def _test_hash_cleanup_listdir_files(self, scenarios, policy,
                                         reclaim_age=None):
        # check that expected files are left in hashdir after cleanup
        for test in scenarios:
            class_under_test = self.df_router[policy]
            files = list(zip(*test)[0])
            hashdir = os.path.join(self.testdir, str(uuid.uuid4()))
            os.mkdir(hashdir)
            for fname in files:
                open(os.path.join(hashdir, fname), 'w')
            expected_after_cleanup = set([f[0] for f in test
                                          if (f[2] if len(f) > 2 else f[1])])
            if reclaim_age:
                class_under_test.hash_cleanup_listdir(
                    hashdir, reclaim_age=reclaim_age)
            else:
                with mock.patch('swift.obj.diskfile.time') as mock_time:
                    # don't reclaim anything
                    mock_time.time.return_value = 0.0
                    class_under_test.hash_cleanup_listdir(hashdir)
            after_cleanup = set(os.listdir(hashdir))
            errmsg = "expected %r, got %r for test %r" % (
                sorted(expected_after_cleanup), sorted(after_cleanup), test
            )
            self.assertEqual(expected_after_cleanup, after_cleanup, errmsg)

    def _test_yield_hashes_cleanup(self, scenarios, policy):
        # opportunistic test to check that yield_hashes cleans up dir using
        # same scenarios as passed to _test_hash_cleanup_listdir_files
        for test in scenarios:
            class_under_test = self.df_router[policy]
            files = list(zip(*test)[0])
            dev_path = os.path.join(self.testdir, str(uuid.uuid4()))
            hashdir = os.path.join(
                dev_path, diskfile.get_data_dir(policy),
                '0', 'abc', '9373a92d072897b136b3fc06595b4abc')
            os.makedirs(hashdir)
            for fname in files:
                open(os.path.join(hashdir, fname), 'w')
            expected_after_cleanup = set([f[0] for f in test
                                          if f[1] or len(f) > 2 and f[2]])
            with mock.patch('swift.obj.diskfile.time') as mock_time:
                # don't reclaim anything
                mock_time.time.return_value = 0.0
                mock_func = 'swift.obj.diskfile.DiskFileManager.get_dev_path'
                with mock.patch(mock_func) as mock_path:
                    mock_path.return_value = dev_path
                    for _ in class_under_test.yield_hashes(
                            'ignored', '0', policy, suffixes=['abc']):
                        # return values are tested in test_yield_hashes_*
                        pass
            after_cleanup = set(os.listdir(hashdir))
            errmsg = "expected %r, got %r for test %r" % (
                sorted(expected_after_cleanup), sorted(after_cleanup), test
            )
            self.assertEqual(expected_after_cleanup, after_cleanup, errmsg)

    def test_construct_dev_path(self):
        res_path = self.df_mgr.construct_dev_path('abc')
        self.assertEqual(os.path.join(self.df_mgr.devices, 'abc'), res_path)

    def test_pickle_async_update(self):
        self.df_mgr.logger.increment = mock.MagicMock()
        ts = Timestamp(10000.0).internal
        with mock.patch('swift.obj.diskfile.write_pickle') as wp:
            self.df_mgr.pickle_async_update(self.existing_device1,
                                            'a', 'c', 'o',
                                            dict(a=1, b=2), ts, POLICIES[0])
            dp = self.df_mgr.construct_dev_path(self.existing_device1)
            ohash = diskfile.hash_path('a', 'c', 'o')
            wp.assert_called_with({'a': 1, 'b': 2},
                                  os.path.join(
                                      dp, diskfile.get_async_dir(POLICIES[0]),
                                      ohash[-3:], ohash + '-' + ts),
                                  os.path.join(dp, 'tmp'))
        self.df_mgr.logger.increment.assert_called_with('async_pendings')

    def test_object_audit_location_generator(self):
        locations = list(self.df_mgr.object_audit_location_generator())
        self.assertEqual(locations, [])

    def test_replication_lock_on(self):
        # Double check settings
        self.df_mgr.replication_one_per_device = True
        self.df_mgr.replication_lock_timeout = 0.1
        dev_path = os.path.join(self.testdir, self.existing_device1)
        with self.df_mgr.replication_lock(self.existing_device1):
            lock_exc = None
            exc = None
            try:
                with self.df_mgr.replication_lock(self.existing_device1):
                    raise Exception(
                        '%r was not replication locked!' % dev_path)
            except ReplicationLockTimeout as err:
                lock_exc = err
            except Exception as err:
                exc = err
            self.assertTrue(lock_exc is not None)
            self.assertTrue(exc is None)

    def test_replication_lock_off(self):
        # Double check settings
        self.df_mgr.replication_one_per_device = False
        self.df_mgr.replication_lock_timeout = 0.1
        dev_path = os.path.join(self.testdir, self.existing_device1)
        with self.df_mgr.replication_lock(dev_path):
            lock_exc = None
            exc = None
            try:
                with self.df_mgr.replication_lock(dev_path):
                    raise Exception(
                        '%r was not replication locked!' % dev_path)
            except ReplicationLockTimeout as err:
                lock_exc = err
            except Exception as err:
                exc = err
            self.assertTrue(lock_exc is None)
            self.assertTrue(exc is not None)

    def test_replication_lock_another_device_fine(self):
        # Double check settings
        self.df_mgr.replication_one_per_device = True
        self.df_mgr.replication_lock_timeout = 0.1
        with self.df_mgr.replication_lock(self.existing_device1):
            lock_exc = None
            try:
                with self.df_mgr.replication_lock(self.existing_device2):
                    pass
            except ReplicationLockTimeout as err:
                lock_exc = err
            self.assertTrue(lock_exc is None)

    def test_missing_splice_warning(self):
        logger = FakeLogger()
        with mock.patch('swift.common.splice.splice._c_splice', None):
            self.conf['splice'] = 'yes'
            mgr = diskfile.DiskFileManager(self.conf, logger)

        warnings = logger.get_lines_for_level('warning')
        self.assertTrue(len(warnings) > 0)
        self.assertTrue('splice()' in warnings[-1])
        self.assertFalse(mgr.use_splice)

    def test_get_diskfile_from_hash_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileDeviceUnavailable,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_not_dir(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata'),
                mock.patch(self._manager_mock('quarantine_renamer'))) as \
                (dfclass, hclistdir, readmeta, quarantine_renamer):
            osexc = OSError()
            osexc.errno = errno.ENOTDIR
            hclistdir.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])
            quarantine_renamer.assert_called_once_with(
                '/srv/dev/',
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900')

    def test_get_diskfile_from_hash_no_dir(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            osexc = OSError()
            osexc.errno = errno.ENOENT
            hclistdir.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_other_oserror(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            osexc = OSError()
            hclistdir.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                OSError,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_no_actual_files(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = []
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_read_metadata_problem(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.side_effect = EOFError()
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_no_meta_name(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {}
            try:
                self.df_mgr.get_diskfile_from_hash(
                    'dev', '9', '9a7175077c01a23ade5956b8a2bba900',
                    POLICIES[0])
            except DiskFileNotExist as err:
                exc = err
            self.assertEqual(str(exc), '')

    def test_get_diskfile_from_hash_bad_meta_name(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {'name': 'bad'}
            try:
                self.df_mgr.get_diskfile_from_hash(
                    'dev', '9', '9a7175077c01a23ade5956b8a2bba900',
                    POLICIES[0])
            except DiskFileNotExist as err:
                exc = err
            self.assertEqual(str(exc), '')

    def test_get_diskfile_from_hash(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch(self._manager_mock('diskfile_cls')),
                mock.patch(self._manager_mock('hash_cleanup_listdir')),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {'name': '/a/c/o'}
            self.df_mgr.get_diskfile_from_hash(
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])
            dfclass.assert_called_once_with(
                self.df_mgr, '/srv/dev/', self.df_mgr.threadpools['dev'], '9',
                'a', 'c', 'o', policy=POLICIES[0])
            hclistdir.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900',
                604800)
            readmeta.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900/'
                '1381679759.90941.data')

    def test_listdir_enoent(self):
        oserror = OSError()
        oserror.errno = errno.ENOENT
        self.df_mgr.logger.error = mock.MagicMock()
        with mock.patch('os.listdir', side_effect=oserror):
            self.assertEqual(self.df_mgr._listdir('path'), [])
            self.assertEqual(self.df_mgr.logger.error.mock_calls, [])

    def test_listdir_other_oserror(self):
        oserror = OSError()
        self.df_mgr.logger.error = mock.MagicMock()
        with mock.patch('os.listdir', side_effect=oserror):
            self.assertEqual(self.df_mgr._listdir('path'), [])
            self.df_mgr.logger.error.assert_called_once_with(
                'ERROR: Skipping %r due to error with listdir attempt: %s',
                'path', oserror)

    def test_listdir(self):
        self.df_mgr.logger.error = mock.MagicMock()
        with mock.patch('os.listdir', return_value=['abc', 'def']):
            self.assertEqual(self.df_mgr._listdir('path'), ['abc', 'def'])
            self.assertEqual(self.df_mgr.logger.error.mock_calls, [])

    def test_yield_suffixes_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        exc = None
        try:
            list(self.df_mgr.yield_suffixes(self.existing_device1, '9', 0))
        except DiskFileDeviceUnavailable as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_yield_suffixes(self):
        self.df_mgr._listdir = mock.MagicMock(return_value=[
            'abc', 'def', 'ghi', 'abcd', '012'])
        dev = self.existing_device1
        self.assertEqual(
            list(self.df_mgr.yield_suffixes(dev, '9', POLICIES[0])),
            [(self.testdir + '/' + dev + '/objects/9/abc', 'abc'),
             (self.testdir + '/' + dev + '/objects/9/def', 'def'),
             (self.testdir + '/' + dev + '/objects/9/012', '012')])

    def test_yield_hashes_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        exc = None
        try:
            list(self.df_mgr.yield_hashes(self.existing_device1, '9',
                                          POLICIES[0]))
        except DiskFileDeviceUnavailable as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_yield_hashes_empty(self):
        def _listdir(path):
            return []

        with mock.patch('os.listdir', _listdir):
            self.assertEqual(list(self.df_mgr.yield_hashes(
                self.existing_device1, '9', POLICIES[0])), [])

    def test_yield_hashes_empty_suffixes(self):
        def _listdir(path):
            return []

        with mock.patch('os.listdir', _listdir):
            self.assertEqual(
                list(self.df_mgr.yield_hashes(self.existing_device1, '9',
                                              POLICIES[0],
                                              suffixes=['456'])), [])

    def _check_yield_hashes(self, policy, suffix_map, expected, **kwargs):
        device = self.existing_device1
        part = '9'
        part_path = os.path.join(
            self.testdir, device, diskfile.get_data_dir(policy), part)

        def _listdir(path):
            if path == part_path:
                return suffix_map.keys()
            for suff, hash_map in suffix_map.items():
                if path == os.path.join(part_path, suff):
                    return hash_map.keys()
                for hash_, files in hash_map.items():
                    if path == os.path.join(part_path, suff, hash_):
                        return files
            self.fail('Unexpected listdir of %r' % path)
        expected_items = [
            (os.path.join(part_path, hash_[-3:], hash_), hash_,
             Timestamp(ts).internal)
            for hash_, ts in expected.items()]
        with nested(
                mock.patch('os.listdir', _listdir),
                mock.patch('os.unlink')):
            df_mgr = self.df_router[policy]
            hash_items = list(df_mgr.yield_hashes(
                device, part, policy, **kwargs))
            expected = sorted(expected_items)
            actual = sorted(hash_items)
            self.assertEqual(actual, expected,
                             'Expected %s but got %s' % (expected, actual))

    def test_yield_hashes_tombstones(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        ts3 = next(ts_iter)
        suffix_map = {
            '27e': {
                '1111111111111111111111111111127e': [
                    ts1.internal + '.ts'],
                '2222222222222222222222222222227e': [
                    ts2.internal + '.ts'],
            },
            'd41': {
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaad41': []
            },
            'd98': {},
            '00b': {
                '3333333333333333333333333333300b': [
                    ts1.internal + '.ts',
                    ts2.internal + '.ts',
                    ts3.internal + '.ts',
                ]
            },
            '204': {
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbb204': [
                    ts3.internal + '.ts',
                ]
            }
        }
        expected = {
            '1111111111111111111111111111127e': ts1.internal,
            '2222222222222222222222222222227e': ts2.internal,
            '3333333333333333333333333333300b': ts3.internal,
        }
        for policy in POLICIES:
            self._check_yield_hashes(policy, suffix_map, expected,
                                     suffixes=['27e', '00b'])


@patch_policies
class TestDiskFileManager(DiskFileManagerMixin, unittest.TestCase):

    mgr_cls = diskfile.DiskFileManager

    def test_get_ondisk_files_with_repl_policy(self):
        # Each scenario specifies a list of (filename, extension) tuples. If
        # extension is set then that filename should be returned by the method
        # under test for that extension type.
        scenarios = [[('0000000007.00000.data', '.data')],

                     [('0000000007.00000.ts', '.ts')],

                     # older tombstone is ignored
                     [('0000000007.00000.ts', '.ts'),
                      ('0000000006.00000.ts', False)],

                     # older data is ignored
                     [('0000000007.00000.data', '.data'),
                      ('0000000006.00000.data', False),
                      ('0000000004.00000.ts', False)],

                     # newest meta trumps older meta
                     [('0000000009.00000.meta', '.meta'),
                      ('0000000008.00000.meta', False),
                      ('0000000007.00000.data', '.data'),
                      ('0000000004.00000.ts', False)],

                     # meta older than data is ignored
                     [('0000000007.00000.data', '.data'),
                      ('0000000006.00000.meta', False),
                      ('0000000004.00000.ts', False)],

                     # meta without data is ignored
                     [('0000000007.00000.meta', False, True),
                      ('0000000006.00000.ts', '.ts'),
                      ('0000000004.00000.data', False)],

                     # tombstone trumps meta and data at same timestamp
                     [('0000000006.00000.meta', False),
                      ('0000000006.00000.ts', '.ts'),
                      ('0000000006.00000.data', False)],
                     ]

        self._test_get_ondisk_files(scenarios, POLICIES[0], None)
        self._test_hash_cleanup_listdir_files(scenarios, POLICIES[0])
        self._test_yield_hashes_cleanup(scenarios, POLICIES[0])

    def test_get_ondisk_files_with_stray_meta(self):
        # get_ondisk_files does not tolerate a stray .meta file

        class_under_test = self._get_diskfile(POLICIES[0])
        files = ['0000000007.00000.meta']

        self.assertRaises(AssertionError,
                          class_under_test.manager.get_ondisk_files, files,
                          self.testdir)

    def test_yield_hashes(self):
        old_ts = '1383180000.12345'
        fresh_ts = Timestamp(time() - 10).internal
        fresher_ts = Timestamp(time() - 1).internal
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    fresh_ts + '.ts'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    old_ts + '.data'],
                '9373a92d072897b136b3fc06595b7456': [
                    fresh_ts + '.ts',
                    fresher_ts + '.data'],
            },
            'def': {},
        }
        expected = {
            '9373a92d072897b136b3fc06595b4abc': fresh_ts,
            '9373a92d072897b136b3fc06595b0456': old_ts,
            '9373a92d072897b136b3fc06595b7456': fresher_ts,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

    def test_yield_hashes_yields_meta_timestamp(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        ts3 = next(ts_iter)
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    ts1.internal + '.ts',
                    ts2.internal + '.meta'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '.data',
                    ts2.internal + '.meta',
                    ts3.internal + '.meta'],
                '9373a92d072897b136b3fc06595b7456': [
                    ts1.internal + '.data',
                    ts2.internal + '.meta'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b4abc': ts2,
            '9373a92d072897b136b3fc06595b0456': ts3,
            '9373a92d072897b136b3fc06595b7456': ts2,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

    def test_yield_hashes_suffix_filter(self):
        # test again with limited suffixes
        old_ts = '1383180000.12345'
        fresh_ts = Timestamp(time() - 10).internal
        fresher_ts = Timestamp(time() - 1).internal
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    fresh_ts + '.ts'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    old_ts + '.data'],
                '9373a92d072897b136b3fc06595b7456': [
                    fresh_ts + '.ts',
                    fresher_ts + '.data'],
            },
            'def': {},
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': old_ts,
            '9373a92d072897b136b3fc06595b7456': fresher_ts,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 suffixes=['456'])

    def test_yield_hashes_fails_with_bad_ondisk_filesets(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        suffix_map = {
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '.data'],
                '9373a92d072897b136b3fc06595ba456': [
                    ts1.internal + '.meta'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': ts1,
        }
        try:
            self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                     frag_index=2)
            self.fail('Expected AssertionError')
        except AssertionError:
            pass


@patch_policies(with_ec_default=True)
class TestECDiskFileManager(DiskFileManagerMixin, unittest.TestCase):

    mgr_cls = diskfile.ECDiskFileManager

    def test_get_ondisk_files_with_ec_policy(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set then that filename should be returned by
        # the method under test for that extension type. If the optional
        # 'survives' is True, the filename should still be in the dir after
        # cleanup.
        scenarios = [[('0000000007.00000.ts', '.ts')],

                     [('0000000007.00000.ts', '.ts'),
                      ('0000000006.00000.ts', False)],

                     # highest frag index is chosen by default
                     [('0000000007.00000.durable', '.durable'),
                      ('0000000007.00000#1.data', '.data'),
                      ('0000000007.00000#0.data', False, True)],

                     # data with no durable is ignored
                     [('0000000007.00000#0.data', False, True)],

                     # data newer than durable is ignored
                     [('0000000008.00000#1.data', False, True),
                      ('0000000007.00000.durable', '.durable'),
                      ('0000000007.00000#1.data', '.data'),
                      ('0000000007.00000#0.data', False, True)],

                     # data newer than durable ignored, even if its only data
                     [('0000000008.00000#1.data', False, True),
                      ('0000000007.00000.durable', False, False)],

                     # data older than durable is ignored
                     [('0000000007.00000.durable', '.durable'),
                      ('0000000007.00000#1.data', '.data'),
                      ('0000000006.00000#1.data', False),
                      ('0000000004.00000.ts', False)],

                     # data older than durable ignored, even if its only data
                     [('0000000007.00000.durable', False, False),
                      ('0000000006.00000#1.data', False),
                      ('0000000004.00000.ts', False)],

                     # newer meta trumps older meta
                     [('0000000009.00000.meta', '.meta'),
                      ('0000000008.00000.meta', False),
                      ('0000000007.00000.durable', '.durable'),
                      ('0000000007.00000#14.data', '.data'),
                      ('0000000004.00000.ts', False)],

                     # older meta is ignored
                     [('0000000007.00000.durable', '.durable'),
                      ('0000000007.00000#14.data', '.data'),
                      ('0000000006.00000.meta', False),
                      ('0000000004.00000.ts', False)],

                     # tombstone trumps meta, data, durable at older timestamp
                     [('0000000006.00000.ts', '.ts'),
                      ('0000000005.00000.meta', False),
                      ('0000000004.00000.durable', False),
                      ('0000000004.00000#0.data', False)],

                     # tombstone trumps meta, data, durable at same timestamp
                     [('0000000006.00000.meta', False),
                      ('0000000006.00000.ts', '.ts'),
                      ('0000000006.00000.durable', False),
                      ('0000000006.00000#0.data', False)],

                     # missing durable invalidates data
                     [('0000000006.00000.meta', False, True),
                      ('0000000006.00000#0.data', False, True)]
                     ]

        self._test_get_ondisk_files(scenarios, POLICIES.default, None)
        self._test_hash_cleanup_listdir_files(scenarios, POLICIES.default)
        self._test_yield_hashes_cleanup(scenarios, POLICIES.default)

    def test_get_ondisk_files_with_ec_policy_and_frag_index(self):
        # Each scenario specifies a list of (filename, extension) tuples. If
        # extension is set then that filename should be returned by the method
        # under test for that extension type.
        scenarios = [[('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#1.data', '.data'),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000007.00000.durable', '.durable')],

                     # specific frag newer than durable is ignored
                     [('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#1.data', False, True),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000006.00000.durable', '.durable')],

                     # specific frag older than durable is ignored
                     [('0000000007.00000#2.data', False),
                      ('0000000007.00000#1.data', False),
                      ('0000000007.00000#0.data', False),
                      ('0000000008.00000.durable', '.durable')],

                     # specific frag older than newest durable is ignored
                     # even if is also has a durable
                     [('0000000007.00000#2.data', False),
                      ('0000000007.00000#1.data', False),
                      ('0000000007.00000.durable', False),
                      ('0000000008.00000#0.data', False),
                      ('0000000008.00000.durable', '.durable')],

                     # meta included when frag index is specified
                     [('0000000009.00000.meta', '.meta'),
                      ('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#1.data', '.data'),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000007.00000.durable', '.durable')],

                     # specific frag older than tombstone is ignored
                     [('0000000009.00000.ts', '.ts'),
                      ('0000000007.00000#2.data', False),
                      ('0000000007.00000#1.data', False),
                      ('0000000007.00000#0.data', False),
                      ('0000000007.00000.durable', False)],

                     # no data file returned if specific frag index missing
                     [('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#14.data', False, True),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000007.00000.durable', '.durable')],

                     # meta ignored if specific frag index missing
                     [('0000000008.00000.meta', False, True),
                      ('0000000007.00000#14.data', False, True),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000007.00000.durable', '.durable')],

                     # meta ignored if no data files
                     # Note: this is anomalous, because we are specifying a
                     # frag_index, get_ondisk_files will tolerate .meta with
                     # no .data
                     [('0000000088.00000.meta', False, True),
                      ('0000000077.00000.durable', '.durable')]
                     ]

        self._test_get_ondisk_files(scenarios, POLICIES.default, frag_index=1)
        # note: not calling self._test_hash_cleanup_listdir_files(scenarios, 0)
        # here due to the anomalous scenario as commented above

    def test_hash_cleanup_listdir_reclaim(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set or 'survives' is True, the filename
        # should still be in the dir after cleanup.
        much_older = Timestamp(time() - 2000).internal
        older = Timestamp(time() - 1001).internal
        newer = Timestamp(time() - 900).internal
        scenarios = [[('%s.ts' % older, False, False)],

                     # fresh tombstone is preserved
                     [('%s.ts' % newer, '.ts', True)],

                     # isolated .durable is cleaned up immediately
                     [('%s.durable' % newer, False, False)],

                     # ...even when other older files are in dir
                     [('%s.durable' % older, False, False),
                     ('%s.ts' % much_older, False, False)],

                     # isolated .data files are cleaned up when stale
                     [('%s#2.data' % older, False, False),
                      ('%s#4.data' % older, False, False)],

                     # ...even when there is an older durable fileset
                     [('%s#2.data' % older, False, False),
                      ('%s#4.data' % older, False, False),
                      ('%s#2.data' % much_older, '.data', True),
                      ('%s#4.data' % much_older, False, True),
                      ('%s.durable' % much_older, '.durable', True)],

                     # ... but preserved if still fresh
                     [('%s#2.data' % newer, False, True),
                      ('%s#4.data' % newer, False, True)],

                     # ... and we could have a mixture of fresh and stale .data
                     [('%s#2.data' % newer, False, True),
                      ('%s#4.data' % older, False, False)],

                     # TODO these remaining scenarios exhibit different
                     # behavior than the legacy replication DiskFileManager
                     # behavior...

                     # tombstone reclaimed despite newer non-durable data
                     [('%s#2.data' % newer, False, True),
                      ('%s#4.data' % older, False, False),
                      ('%s.ts' % much_older, '.ts', False)],

                     # tombstone reclaimed despite newer non-durable data
                     [('%s.ts' % older, '.ts', False),
                      ('%s.durable' % much_older, False, False)],

                     # tombstone reclaimed despite junk file
                     [('junk', False, True),
                      ('%s.ts' % much_older, '.ts', False)],
                     ]

        self._test_hash_cleanup_listdir_files(scenarios, POLICIES.default,
                                              reclaim_age=1000)

    def test_get_ondisk_files_with_stray_meta(self):
        # get_ondisk_files does not tolerate a stray .meta file
        scenarios = [['0000000007.00000.meta'],

                     ['0000000007.00000.meta',
                      '0000000006.00000.durable'],

                     ['0000000007.00000.meta',
                     '0000000006.00000#1.data'],

                     ['0000000007.00000.meta',
                     '0000000006.00000.durable',
                     '0000000005.00000#1.data']
                     ]
        for files in scenarios:
            class_under_test = self._get_diskfile(POLICIES.default)
            self.assertRaises(DiskFileNotExist, class_under_test.open)

    def test_parse_on_disk_filename(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            for frag in (0, 2, 14):
                fname = '%s#%s.data' % (ts.internal, frag)
                info = mgr.parse_on_disk_filename(fname)
                self.assertEqual(ts, info['timestamp'])
                self.assertEqual(frag, info['frag_index'])
                self.assertEqual(mgr.make_on_disk_filename(**info), fname)

            for ext in ('.meta', '.durable', '.ts'):
                fname = '%s%s' % (ts.internal, ext)
                info = mgr.parse_on_disk_filename(fname)
                self.assertEqual(ts, info['timestamp'])
                self.assertEqual(None, info['frag_index'])
                self.assertEqual(mgr.make_on_disk_filename(**info), fname)

    def test_parse_on_disk_filename_errors(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            fname = '%s.data' % ts.internal
            try:
                mgr.parse_on_disk_filename(fname)
                msg = 'Expected DiskFileError for filename %s' % fname
                self.fail(msg)
            except DiskFileError:
                pass

            expected = {
                '': 'bad',
                'foo': 'bad',
                '1.314': 'bad',
                1.314: 'bad',
                -2: 'negative',
                '-2': 'negative',
                None: 'bad',
                'None': 'bad',
            }

            for frag, msg in expected.items():
                fname = '%s#%s.data' % (ts.internal, frag)
                try:
                    mgr.parse_on_disk_filename(fname)
                except DiskFileError as e:
                    self.assertTrue(msg in str(e).lower())
                else:
                    msg = 'Expected DiskFileError for filename %s' % fname
                    self.fail(msg)

    def test_make_on_disk_filename(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            for frag in (0, '0', 2, '2', 14, '14'):
                expected = '%s#%s.data' % (ts.internal, frag)
                actual = mgr.make_on_disk_filename(
                    ts, '.data', frag_index=frag)
                self.assertEqual(expected, actual)
                parsed = mgr.parse_on_disk_filename(actual)
                self.assertEqual(parsed, {
                    'timestamp': ts,
                    'frag_index': int(frag),
                    'ext': '.data',
                })
                # these functions are inverse
                self.assertEqual(
                    mgr.make_on_disk_filename(**parsed),
                    expected)

                for ext in ('.meta', '.durable', '.ts'):
                    expected = '%s%s' % (ts.internal, ext)
                    # frag index should not be required
                    actual = mgr.make_on_disk_filename(ts, ext)
                    self.assertEqual(expected, actual)
                    # frag index should be ignored
                    actual = mgr.make_on_disk_filename(
                        ts, ext, frag_index=frag)
                    self.assertEqual(expected, actual)
                    parsed = mgr.parse_on_disk_filename(actual)
                    self.assertEqual(parsed, {
                        'timestamp': ts,
                        'frag_index': None,
                        'ext': ext,
                    })
                    # these functions are inverse
                    self.assertEqual(
                        mgr.make_on_disk_filename(**parsed),
                        expected)

            actual = mgr.make_on_disk_filename(ts)
            self.assertEqual(ts, actual)

    def test_make_on_disk_filename_with_bad_frag_index(self):
        mgr = self.df_router[POLICIES.default]
        ts = Timestamp('1234567890.00001')
        try:
            # .data requires a frag_index kwarg
            mgr.make_on_disk_filename(ts, '.data')
            self.fail('Expected DiskFileError for missing frag_index')
        except DiskFileError:
            pass

        for frag in (None, 'foo', '1.314', 1.314, -2, '-2'):
            try:
                mgr.make_on_disk_filename(ts, '.data', frag_index=frag)
                self.fail('Expected DiskFileError for frag_index %s' % frag)
            except DiskFileError:
                pass
            for ext in ('.meta', '.durable', '.ts'):
                expected = '%s%s' % (ts.internal, ext)
                # bad frag index should be ignored
                actual = mgr.make_on_disk_filename(ts, ext, frag_index=frag)
                self.assertEqual(expected, actual)

    def test_is_obsolete(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            for ts2 in (Timestamp('1234567890.99999'),
                        Timestamp('1234567890.99999', offset=17),
                        ts):
                f_2 = mgr.make_on_disk_filename(ts, '.durable')
                for fi in (0, 2):
                    for ext in ('.data', '.meta', '.durable', '.ts'):
                        f_1 = mgr.make_on_disk_filename(
                            ts2, ext, frag_index=fi)
                        self.assertFalse(mgr.is_obsolete(f_1, f_2),
                                         '%s should not be obsolete w.r.t. %s'
                                         % (f_1, f_2))

            for ts2 in (Timestamp('1234567890.00000'),
                        Timestamp('1234500000.00000', offset=0),
                        Timestamp('1234500000.00000', offset=17)):
                f_2 = mgr.make_on_disk_filename(ts, '.durable')
                for fi in (0, 2):
                    for ext in ('.data', '.meta', '.durable', '.ts'):
                        f_1 = mgr.make_on_disk_filename(
                            ts2, ext, frag_index=fi)
                        self.assertTrue(mgr.is_obsolete(f_1, f_2),
                                        '%s should not be w.r.t. %s'
                                        % (f_1, f_2))

    def test_yield_hashes(self):
        old_ts = '1383180000.12345'
        fresh_ts = Timestamp(time() - 10).internal
        fresher_ts = Timestamp(time() - 1).internal
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    fresh_ts + '.ts'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    old_ts + '#2.data',
                    old_ts + '.durable'],
                '9373a92d072897b136b3fc06595b7456': [
                    fresh_ts + '.ts',
                    fresher_ts + '#2.data',
                    fresher_ts + '.durable'],
            },
            'def': {},
        }
        expected = {
            '9373a92d072897b136b3fc06595b4abc': fresh_ts,
            '9373a92d072897b136b3fc06595b0456': old_ts,
            '9373a92d072897b136b3fc06595b7456': fresher_ts,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_yields_meta_timestamp(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        ts3 = next(ts_iter)
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    ts1.internal + '.ts',
                    ts2.internal + '.meta'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable',
                    ts2.internal + '.meta',
                    ts3.internal + '.meta'],
                '9373a92d072897b136b3fc06595b7456': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable',
                    ts2.internal + '.meta'],
            },
        }
        expected = {
            # TODO: differs from repl DiskFileManager which *will*
            # return meta timestamp when only meta and ts on disk
            '9373a92d072897b136b3fc06595b4abc': ts1,
            '9373a92d072897b136b3fc06595b0456': ts3,
            '9373a92d072897b136b3fc06595b7456': ts2,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # but meta timestamp is not returned if specified frag index
        # is not found
        expected = {
            # TODO: differs from repl DiskFileManager which *will*
            # return meta timestamp when only meta and ts on disk
            '9373a92d072897b136b3fc06595b4abc': ts1,
            '9373a92d072897b136b3fc06595b0456': ts3,
            '9373a92d072897b136b3fc06595b7456': ts2,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=3)

    def test_yield_hashes_suffix_filter(self):
        # test again with limited suffixes
        old_ts = '1383180000.12345'
        fresh_ts = Timestamp(time() - 10).internal
        fresher_ts = Timestamp(time() - 1).internal
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    fresh_ts + '.ts'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    old_ts + '#2.data',
                    old_ts + '.durable'],
                '9373a92d072897b136b3fc06595b7456': [
                    fresh_ts + '.ts',
                    fresher_ts + '#2.data',
                    fresher_ts + '.durable'],
            },
            'def': {},
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': old_ts,
            '9373a92d072897b136b3fc06595b7456': fresher_ts,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 suffixes=['456'], frag_index=2)

    def test_yield_hashes_skips_missing_durable(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        suffix_map = {
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable'],
                '9373a92d072897b136b3fc06595b7456': [
                    ts1.internal + '#2.data'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': ts1,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

        # if we add a durable it shows up
        suffix_map['456']['9373a92d072897b136b3fc06595b7456'].append(
            ts1.internal + '.durable')
        expected = {
            '9373a92d072897b136b3fc06595b0456': ts1,
            '9373a92d072897b136b3fc06595b7456': ts1,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_skips_data_without_durable(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        ts3 = next(ts_iter)
        suffix_map = {
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable',
                    ts2.internal + '#2.data',
                    ts3.internal + '#2.data'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': ts1,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=None)
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

        # if we add a durable then newer data shows up
        suffix_map['456']['9373a92d072897b136b3fc06595b0456'].append(
            ts2.internal + '.durable')
        expected = {
            '9373a92d072897b136b3fc06595b0456': ts2,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=None)
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_ignores_bad_ondisk_filesets(self):
        # this differs from DiskFileManager.yield_hashes which will fail
        # when encountering a bad on-disk file set
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        suffix_map = {
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable'],
                '9373a92d072897b136b3fc06595b7456': [
                    ts1.internal + '.data'],
                '9373a92d072897b136b3fc06595b8456': [
                    'junk_file'],
                '9373a92d072897b136b3fc06595b9456': [
                    ts1.internal + '.data',
                    ts2.internal + '.meta'],
                '9373a92d072897b136b3fc06595ba456': [
                    ts1.internal + '.meta'],
                '9373a92d072897b136b3fc06595bb456': [
                    ts1.internal + '.meta',
                    ts2.internal + '.meta'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': ts1,
            '9373a92d072897b136b3fc06595ba456': ts1,
            '9373a92d072897b136b3fc06595bb456': ts2,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_filters_frag_index(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        ts3 = next(ts_iter)
        suffix_map = {
            '27e': {
                '1111111111111111111111111111127e': [
                    ts1.internal + '#2.data',
                    ts1.internal + '#3.data',
                    ts1.internal + '.durable',
                ],
                '2222222222222222222222222222227e': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable',
                    ts2.internal + '#2.data',
                    ts2.internal + '.durable',
                ],
            },
            'd41': {
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaad41': [
                    ts1.internal + '#3.data',
                    ts1.internal + '.durable',
                ],
            },
            '00b': {
                '3333333333333333333333333333300b': [
                    ts1.internal + '#2.data',
                    ts2.internal + '#2.data',
                    ts3.internal + '#2.data',
                    ts3.internal + '.durable',
                ],
            },
        }
        expected = {
            '1111111111111111111111111111127e': ts1,
            '2222222222222222222222222222227e': ts2,
            '3333333333333333333333333333300b': ts3,
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_get_diskfile_from_hash_frag_index_filter(self):
        df = self._get_diskfile(POLICIES.default)
        hash_ = os.path.basename(df._datadir)
        self.assertRaises(DiskFileNotExist,
                          self.df_mgr.get_diskfile_from_hash,
                          self.existing_device1, '0', hash_,
                          POLICIES.default)  # sanity
        frag_index = 7
        timestamp = Timestamp(time())
        for frag_index in (4, 7):
            with df.create() as writer:
                data = 'test_data'
                writer.write(data)
                metadata = {
                    'ETag': md5(data).hexdigest(),
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': len(data),
                    'X-Object-Sysmeta-Ec-Frag-Index': str(frag_index),
                }
                writer.put(metadata)
                writer.commit(timestamp)

        df4 = self.df_mgr.get_diskfile_from_hash(
            self.existing_device1, '0', hash_, POLICIES.default, frag_index=4)
        self.assertEqual(df4._frag_index, 4)
        self.assertEqual(
            df4.read_metadata()['X-Object-Sysmeta-Ec-Frag-Index'], '4')
        df7 = self.df_mgr.get_diskfile_from_hash(
            self.existing_device1, '0', hash_, POLICIES.default, frag_index=7)
        self.assertEqual(df7._frag_index, 7)
        self.assertEqual(
            df7.read_metadata()['X-Object-Sysmeta-Ec-Frag-Index'], '7')


class DiskFileMixin(BaseDiskFileTestMixin):

    # set mgr_cls on subclasses
    mgr_cls = None

    def setUp(self):
        """Set up for testing swift.obj.diskfile"""
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(
            self.tmpdir, 'tmp_test_obj_server_DiskFile')
        self.existing_device = 'sda1'
        for policy in POLICIES:
            mkdirs(os.path.join(self.testdir, self.existing_device,
                                diskfile.get_tmp_dir(policy)))
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.conf = dict(devices=self.testdir, mount_check='false',
                         keep_cache_size=2 * 1024, mb_per_sync=1)
        self.logger = debug_logger('test-' + self.__class__.__name__)
        self.df_mgr = self.mgr_cls(self.conf, self.logger)
        self.df_router = diskfile.DiskFileRouter(self.conf, self.logger)
        self._ts_iter = (Timestamp(t) for t in
                         itertools.count(int(time())))

    def ts(self):
        """
        Timestamps - forever.
        """
        return next(self._ts_iter)

    def tearDown(self):
        """Tear down for testing swift.obj.diskfile"""
        rmtree(self.tmpdir, ignore_errors=1)
        tpool.execute = self._orig_tpool_exc

    def _create_ondisk_file(self, df, data, timestamp, metadata=None,
                            ext='.data'):
        mkdirs(df._datadir)
        if timestamp is None:
            timestamp = time()
        timestamp = Timestamp(timestamp)
        if not metadata:
            metadata = {}
        if 'X-Timestamp' not in metadata:
            metadata['X-Timestamp'] = timestamp.internal
        if 'ETag' not in metadata:
            etag = md5()
            etag.update(data)
            metadata['ETag'] = etag.hexdigest()
        if 'name' not in metadata:
            metadata['name'] = '/a/c/o'
        if 'Content-Length' not in metadata:
            metadata['Content-Length'] = str(len(data))
        filename = timestamp.internal + ext
        if ext == '.data' and df.policy.policy_type == EC_POLICY:
            filename = '%s#%s.data' % (timestamp.internal, df._frag_index)
        data_file = os.path.join(df._datadir, filename)
        with open(data_file, 'wb') as f:
            f.write(data)
            xattr.setxattr(f.fileno(), diskfile.METADATA_KEY,
                           pickle.dumps(metadata, diskfile.PICKLE_PROTOCOL))

    def _simple_get_diskfile(self, partition='0', account='a', container='c',
                             obj='o', policy=None, frag_index=None):
        policy = policy or POLICIES.default
        df_mgr = self.df_router[policy]
        if policy.policy_type == EC_POLICY and frag_index is None:
            frag_index = 2
        return df_mgr.get_diskfile(self.existing_device, partition,
                                   account, container, obj,
                                   policy=policy, frag_index=frag_index)

    def _create_test_file(self, data, timestamp=None, metadata=None,
                          account='a', container='c', obj='o'):
        if metadata is None:
            metadata = {}
        metadata.setdefault('name', '/%s/%s/%s' % (account, container, obj))
        df = self._simple_get_diskfile(account=account, container=container,
                                       obj=obj)
        if timestamp is None:
            timestamp = time()
        timestamp = Timestamp(timestamp)
        with df.create() as writer:
            new_metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': timestamp.internal,
                'Content-Length': len(data),
            }
            new_metadata.update(metadata)
            writer.write(data)
            writer.put(new_metadata)
            writer.commit(timestamp)
        df.open()
        return df

    def test_get_dev_path(self):
        self.df_mgr.devices = '/srv'
        device = 'sda1'
        dev_path = os.path.join(self.df_mgr.devices, device)

        mount_check = None
        self.df_mgr.mount_check = True
        with mock.patch('swift.obj.diskfile.check_mount',
                        mock.MagicMock(return_value=False)):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             None)
        with mock.patch('swift.obj.diskfile.check_mount',
                        mock.MagicMock(return_value=True)):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             dev_path)

        self.df_mgr.mount_check = False
        with mock.patch('swift.obj.diskfile.check_dir',
                        mock.MagicMock(return_value=False)):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             None)
        with mock.patch('swift.obj.diskfile.check_dir',
                        mock.MagicMock(return_value=True)):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             dev_path)

        mount_check = True
        with mock.patch('swift.obj.diskfile.check_mount',
                        mock.MagicMock(return_value=False)):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             None)
        with mock.patch('swift.obj.diskfile.check_mount',
                        mock.MagicMock(return_value=True)):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             dev_path)

        mount_check = False
        self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                         dev_path)

    def test_open_not_exist(self):
        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileNotExist, df.open)

    def test_open_expired(self):
        self.assertRaises(DiskFileExpired,
                          self._create_test_file,
                          '1234567890', metadata={'X-Delete-At': '0'})

    def test_open_not_expired(self):
        try:
            self._create_test_file(
                '1234567890', metadata={'X-Delete-At': str(2 * int(time()))})
        except SwiftException as err:
            self.fail("Unexpected swift exception raised: %r" % err)

    def test_get_metadata(self):
        timestamp = self.ts().internal
        df = self._create_test_file('1234567890', timestamp=timestamp)
        md = df.get_metadata()
        self.assertEqual(md['X-Timestamp'], timestamp)

    def test_read_metadata(self):
        timestamp = self.ts().internal
        self._create_test_file('1234567890', timestamp=timestamp)
        df = self._simple_get_diskfile()
        md = df.read_metadata()
        self.assertEqual(md['X-Timestamp'], timestamp)

    def test_read_metadata_no_xattr(self):
        def mock_getxattr(*args, **kargs):
            error_num = errno.ENOTSUP if hasattr(errno, 'ENOTSUP') else \
                errno.EOPNOTSUPP
            raise IOError(error_num, "Operation not supported")

        with mock.patch('xattr.getxattr', mock_getxattr):
            self.assertRaises(
                DiskFileXattrNotSupported,
                diskfile.read_metadata, 'n/a')

    def test_get_metadata_not_opened(self):
        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileNotOpen, df.get_metadata)

    def test_not_opened(self):
        df = self._simple_get_diskfile()
        try:
            with df:
                pass
        except DiskFileNotOpen:
            pass
        else:
            self.fail("Expected DiskFileNotOpen exception")

    def test_disk_file_default_disallowed_metadata(self):
        # build an object with some meta (at t0+1s)
        orig_metadata = {'X-Object-Meta-Key1': 'Value1',
                         'Content-Type': 'text/garbage'}
        df = self._get_open_disk_file(ts=self.ts().internal,
                                      extra_metadata=orig_metadata)
        with df.open():
            self.assertEquals('1024', df._metadata['Content-Length'])
        # write some new metadata (fast POST, don't send orig meta, at t0+1)
        df = self._simple_get_diskfile()
        df.write_metadata({'X-Timestamp': self.ts().internal,
                           'X-Object-Meta-Key2': 'Value2'})
        df = self._simple_get_diskfile()
        with df.open():
            # non-fast-post updateable keys are preserved
            self.assertEquals('text/garbage', df._metadata['Content-Type'])
            # original fast-post updateable keys are removed
            self.assert_('X-Object-Meta-Key1' not in df._metadata)
            # new fast-post updateable keys are added
            self.assertEquals('Value2', df._metadata['X-Object-Meta-Key2'])

    def test_disk_file_preserves_sysmeta(self):
        # build an object with some meta (at t0)
        orig_metadata = {'X-Object-Sysmeta-Key1': 'Value1',
                         'Content-Type': 'text/garbage'}
        df = self._get_open_disk_file(ts=self.ts().internal,
                                      extra_metadata=orig_metadata)
        with df.open():
            self.assertEquals('1024', df._metadata['Content-Length'])
        # write some new metadata (fast POST, don't send orig meta, at t0+1s)
        df = self._simple_get_diskfile()
        df.write_metadata({'X-Timestamp': self.ts().internal,
                           'X-Object-Sysmeta-Key1': 'Value2',
                           'X-Object-Meta-Key3': 'Value3'})
        df = self._simple_get_diskfile()
        with df.open():
            # non-fast-post updateable keys are preserved
            self.assertEquals('text/garbage', df._metadata['Content-Type'])
            # original sysmeta keys are preserved
            self.assertEquals('Value1', df._metadata['X-Object-Sysmeta-Key1'])

    def test_disk_file_reader_iter(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(''.join(reader), '1234567890')
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_reader_iter_w_quarantine(self):
        df = self._create_test_file('1234567890')

        def raise_dfq(m):
            raise DiskFileQuarantined(m)

        reader = df.reader(_quarantine_hook=raise_dfq)
        reader._obj_size += 1
        self.assertRaises(DiskFileQuarantined, ''.join, reader)

    def test_disk_file_app_iter_corners(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEquals(''.join(reader.app_iter_range(0, None)),
                          '1234567890')
        self.assertEquals(quarantine_msgs, [])
        df = self._simple_get_diskfile()
        with df.open():
            reader = df.reader()
            self.assertEqual(''.join(reader.app_iter_range(5, None)), '67890')

    def test_disk_file_app_iter_range_w_none(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(''.join(reader.app_iter_range(None, None)),
                         '1234567890')
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_partial_closes(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_range(0, 5)
        self.assertEqual(''.join(it), '12345')
        self.assertEqual(quarantine_msgs, [])
        self.assertTrue(reader._fp is None)

    def test_disk_file_app_iter_ranges(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(0, 10), (10, 20), (20, 30)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('0123456789' in value)
        self.assertTrue('1123456789' in value)
        self.assertTrue('2123456789' in value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_ranges_w_quarantine(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        reader._obj_size += 1
        it = reader.app_iter_ranges([(0, 30)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('0123456789' in value)
        self.assertTrue('1123456789' in value)
        self.assertTrue('2123456789' in value)
        self.assertEqual(quarantine_msgs,
                         ["Bytes read: 30, does not match metadata: 31"])

    def test_disk_file_app_iter_ranges_w_no_etag_quarantine(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(0, 10)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('0123456789' in value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_ranges_edges(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(3, 10), (0, 2)], 'application/whatever',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('3456789' in value)
        self.assertTrue('01' in value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_large_app_iter_ranges(self):
        # This test case is to make sure that the disk file app_iter_ranges
        # method all the paths being tested.
        long_str = '01234567890' * 65536
        target_strs = ['3456789', long_str[0:65590]]
        df = self._create_test_file(long_str)
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(3, 10), (0, 65590)], 'plain/text',
                                    '5e816ff8b8b8e9a5d355497e5d9e0301', 655360)

        # The produced string actually missing the MIME headers
        # need to add these headers to make it as real MIME message.
        # The body of the message is produced by method app_iter_ranges
        # off of DiskFile object.
        header = ''.join(['Content-Type: multipart/byteranges;',
                          'boundary=',
                          '5e816ff8b8b8e9a5d355497e5d9e0301\r\n'])

        value = header + ''.join(it)
        self.assertEquals(quarantine_msgs, [])

        parts = map(lambda p: p.get_payload(decode=True),
                    email.message_from_string(value).walk())[1:3]
        self.assertEqual(parts, target_strs)

    def test_disk_file_app_iter_ranges_empty(self):
        # This test case tests when empty value passed into app_iter_ranges
        # When ranges passed into the method is either empty array or None,
        # this method will yield empty string
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([], 'application/whatever',
                                    '\r\n--someheader\r\n', 100)
        self.assertEqual(''.join(it), '')

        df = self._simple_get_diskfile()
        with df.open():
            reader = df.reader()
            it = reader.app_iter_ranges(None, 'app/something',
                                        '\r\n--someheader\r\n', 150)
            self.assertEqual(''.join(it), '')
            self.assertEqual(quarantine_msgs, [])

    def test_disk_file_mkstemp_creates_dir(self):
        for policy in POLICIES:
            tmpdir = os.path.join(self.testdir, self.existing_device,
                                  diskfile.get_tmp_dir(policy))
            os.rmdir(tmpdir)
            df = self._simple_get_diskfile(policy=policy)
            with df.create():
                self.assert_(os.path.exists(tmpdir))

    def _get_open_disk_file(self, invalid_type=None, obj_name='o', fsize=1024,
                            csize=8, mark_deleted=False, prealloc=False,
                            ts=None, mount_check=False, extra_metadata=None,
                            policy=None, frag_index=None):
        '''returns a DiskFile'''
        policy = policy or POLICIES.legacy
        df = self._simple_get_diskfile(obj=obj_name, policy=policy,
                                       frag_index=frag_index)
        data = '0' * fsize
        etag = md5()
        if ts:
            timestamp = Timestamp(ts)
        else:
            timestamp = Timestamp(time())
        if prealloc:
            prealloc_size = fsize
        else:
            prealloc_size = None

        with df.create(size=prealloc_size) as writer:
            upload_size = writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp.internal,
                'Content-Length': str(upload_size),
            }
            metadata.update(extra_metadata or {})
            writer.put(metadata)
            if invalid_type == 'ETag':
                etag = md5()
                etag.update('1' + '0' * (fsize - 1))
                etag = etag.hexdigest()
                metadata['ETag'] = etag
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Content-Length':
                metadata['Content-Length'] = fsize - 1
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Bad-Content-Length':
                metadata['Content-Length'] = 'zero'
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Missing-Content-Length':
                del metadata['Content-Length']
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Bad-X-Delete-At':
                metadata['X-Delete-At'] = 'bad integer'
                diskfile.write_metadata(writer._fd, metadata)
            writer.commit(timestamp)

        if mark_deleted:
            df.delete(timestamp)

        data_files = [os.path.join(df._datadir, fname)
                      for fname in sorted(os.listdir(df._datadir),
                                          reverse=True)
                      if fname.endswith('.data')]
        if invalid_type == 'Corrupt-Xattrs':
            # We have to go below read_metadata/write_metadata to get proper
            # corruption.
            meta_xattr = xattr.getxattr(data_files[0], "user.swift.metadata")
            wrong_byte = 'X' if meta_xattr[0] != 'X' else 'Y'
            xattr.setxattr(data_files[0], "user.swift.metadata",
                           wrong_byte + meta_xattr[1:])
        elif invalid_type == 'Truncated-Xattrs':
            meta_xattr = xattr.getxattr(data_files[0], "user.swift.metadata")
            xattr.setxattr(data_files[0], "user.swift.metadata",
                           meta_xattr[:-1])
        elif invalid_type == 'Missing-Name':
            md = diskfile.read_metadata(data_files[0])
            del md['name']
            diskfile.write_metadata(data_files[0], md)
        elif invalid_type == 'Bad-Name':
            md = diskfile.read_metadata(data_files[0])
            md['name'] = md['name'] + 'garbage'
            diskfile.write_metadata(data_files[0], md)

        self.conf['disk_chunk_size'] = csize
        self.conf['mount_check'] = mount_check
        self.df_mgr = self.mgr_cls(self.conf, self.logger)
        self.df_router = diskfile.DiskFileRouter(self.conf, self.logger)

        # actual on disk frag_index may have been set by metadata
        frag_index = metadata.get('X-Object-Sysmeta-Ec-Frag-Index',
                                  frag_index)
        df = self._simple_get_diskfile(obj=obj_name, policy=policy,
                                       frag_index=frag_index)
        df.open()

        if invalid_type == 'Zero-Byte':
            fp = open(df._data_file, 'w')
            fp.close()
        df.unit_test_len = fsize
        return df

    def test_keep_cache(self):
        df = self._get_open_disk_file(fsize=65)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as foo:
            for _ in df.reader():
                pass
            self.assertTrue(foo.called)

        df = self._get_open_disk_file(fsize=65)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as bar:
            for _ in df.reader(keep_cache=False):
                pass
            self.assertTrue(bar.called)

        df = self._get_open_disk_file(fsize=65)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as boo:
            for _ in df.reader(keep_cache=True):
                pass
            self.assertFalse(boo.called)

        df = self._get_open_disk_file(fsize=5 * 1024, csize=256)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as goo:
            for _ in df.reader(keep_cache=True):
                pass
            self.assertTrue(goo.called)

    def test_quarantine_valids(self):

        def verify(*args, **kwargs):
            try:
                df = self._get_open_disk_file(**kwargs)
                reader = df.reader()
                for chunk in reader:
                    pass
            except DiskFileQuarantined:
                self.fail(
                    "Unexpected quarantining occurred: args=%r, kwargs=%r" % (
                        args, kwargs))
            else:
                pass

        verify(obj_name='1')

        verify(obj_name='2', csize=1)

        verify(obj_name='3', csize=100000)

    def run_quarantine_invalids(self, invalid_type):

        def verify(*args, **kwargs):
            open_exc = invalid_type in ('Content-Length', 'Bad-Content-Length',
                                        'Corrupt-Xattrs', 'Truncated-Xattrs',
                                        'Missing-Name', 'Bad-X-Delete-At')
            open_collision = invalid_type == 'Bad-Name'
            reader = None
            quarantine_msgs = []
            try:
                df = self._get_open_disk_file(**kwargs)
                reader = df.reader(_quarantine_hook=quarantine_msgs.append)
            except DiskFileQuarantined as err:
                if not open_exc:
                    self.fail(
                        "Unexpected DiskFileQuarantine raised: %r" % err)
                return
            except DiskFileCollision as err:
                if not open_collision:
                    self.fail(
                        "Unexpected DiskFileCollision raised: %r" % err)
                return
            else:
                if open_exc:
                    self.fail("Expected DiskFileQuarantine exception")
            try:
                for chunk in reader:
                    pass
            except DiskFileQuarantined as err:
                self.fail("Unexpected DiskFileQuarantine raised: :%r" % err)
            else:
                if not open_exc:
                    self.assertEqual(1, len(quarantine_msgs))

        verify(invalid_type=invalid_type, obj_name='1')

        verify(invalid_type=invalid_type, obj_name='2', csize=1)

        verify(invalid_type=invalid_type, obj_name='3', csize=100000)

        verify(invalid_type=invalid_type, obj_name='4')

        def verify_air(params, start=0, adjustment=0):
            """verify (a)pp (i)ter (r)ange"""
            open_exc = invalid_type in ('Content-Length', 'Bad-Content-Length',
                                        'Corrupt-Xattrs', 'Truncated-Xattrs',
                                        'Missing-Name', 'Bad-X-Delete-At')
            open_collision = invalid_type == 'Bad-Name'
            reader = None
            try:
                df = self._get_open_disk_file(**params)
                reader = df.reader()
            except DiskFileQuarantined as err:
                if not open_exc:
                    self.fail(
                        "Unexpected DiskFileQuarantine raised: %r" % err)
                return
            except DiskFileCollision as err:
                if not open_collision:
                    self.fail(
                        "Unexpected DiskFileCollision raised: %r" % err)
                return
            else:
                if open_exc:
                    self.fail("Expected DiskFileQuarantine exception")
            try:
                for chunk in reader.app_iter_range(
                        start,
                        df.unit_test_len + adjustment):
                    pass
            except DiskFileQuarantined as err:
                self.fail("Unexpected DiskFileQuarantine raised: :%r" % err)

        verify_air(dict(invalid_type=invalid_type, obj_name='5'))

        verify_air(dict(invalid_type=invalid_type, obj_name='6'), 0, 100)

        verify_air(dict(invalid_type=invalid_type, obj_name='7'), 1)

        verify_air(dict(invalid_type=invalid_type, obj_name='8'), 0, -1)

        verify_air(dict(invalid_type=invalid_type, obj_name='8'), 1, 1)

    def test_quarantine_corrupt_xattrs(self):
        self.run_quarantine_invalids('Corrupt-Xattrs')

    def test_quarantine_truncated_xattrs(self):
        self.run_quarantine_invalids('Truncated-Xattrs')

    def test_quarantine_invalid_etag(self):
        self.run_quarantine_invalids('ETag')

    def test_quarantine_invalid_missing_name(self):
        self.run_quarantine_invalids('Missing-Name')

    def test_quarantine_invalid_bad_name(self):
        self.run_quarantine_invalids('Bad-Name')

    def test_quarantine_invalid_bad_x_delete_at(self):
        self.run_quarantine_invalids('Bad-X-Delete-At')

    def test_quarantine_invalid_content_length(self):
        self.run_quarantine_invalids('Content-Length')

    def test_quarantine_invalid_content_length_bad(self):
        self.run_quarantine_invalids('Bad-Content-Length')

    def test_quarantine_invalid_zero_byte(self):
        self.run_quarantine_invalids('Zero-Byte')

    def test_quarantine_deleted_files(self):
        try:
            self._get_open_disk_file(invalid_type='Content-Length')
        except DiskFileQuarantined:
            pass
        else:
            self.fail("Expected DiskFileQuarantined exception")
        try:
            self._get_open_disk_file(invalid_type='Content-Length',
                                     mark_deleted=True)
        except DiskFileQuarantined as err:
            self.fail("Unexpected DiskFileQuarantined exception"
                      " encountered: %r" % err)
        except DiskFileNotExist:
            pass
        else:
            self.fail("Expected DiskFileNotExist exception")
        try:
            self._get_open_disk_file(invalid_type='Content-Length',
                                     mark_deleted=True)
        except DiskFileNotExist:
            pass
        else:
            self.fail("Expected DiskFileNotExist exception")

    def test_quarantine_missing_content_length(self):
        self.assertRaises(
            DiskFileQuarantined,
            self._get_open_disk_file,
            invalid_type='Missing-Content-Length')

    def test_quarantine_bad_content_length(self):
        self.assertRaises(
            DiskFileQuarantined,
            self._get_open_disk_file,
            invalid_type='Bad-Content-Length')

    def test_quarantine_fstat_oserror(self):
        invocations = [0]
        orig_os_fstat = os.fstat

        def bad_fstat(fd):
            invocations[0] += 1
            if invocations[0] == 4:
                # FIXME - yes, this an icky way to get code coverage ... worth
                # it?
                raise OSError()
            return orig_os_fstat(fd)

        with mock.patch('os.fstat', bad_fstat):
            self.assertRaises(
                DiskFileQuarantined,
                self._get_open_disk_file)

    def test_quarantine_hashdir_not_a_directory(self):
        df = self._create_test_file('1234567890', account="abc",
                                    container='123', obj='xyz')
        hashdir = df._datadir
        rmtree(hashdir)
        with open(hashdir, 'w'):
            pass

        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        self.assertRaises(DiskFileQuarantined, df.open)

        # make sure the right thing got quarantined; the suffix dir should not
        # have moved, as that could have many objects in it
        self.assertFalse(os.path.exists(hashdir))
        self.assertTrue(os.path.exists(os.path.dirname(hashdir)))

    def test_create_prealloc(self):
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        with mock.patch("swift.obj.diskfile.fallocate") as fa:
            with df.create(size=200) as writer:
                used_fd = writer._fd
        fa.assert_called_with(used_fd, 200)

    def test_create_prealloc_oserror(self):
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        for e in (errno.ENOSPC, errno.EDQUOT):
            with mock.patch("swift.obj.diskfile.fallocate",
                            mock.MagicMock(side_effect=OSError(
                                e, os.strerror(e)))):
                try:
                    with df.create(size=200):
                        pass
                except DiskFileNoSpace:
                    pass
                else:
                    self.fail("Expected exception DiskFileNoSpace")

        # Other OSErrors must not be raised as DiskFileNoSpace
        with mock.patch("swift.obj.diskfile.fallocate",
                        mock.MagicMock(side_effect=OSError(
                            errno.EACCES, os.strerror(errno.EACCES)))):
            try:
                with df.create(size=200):
                    pass
            except OSError:
                pass
            else:
                self.fail("Expected exception OSError")

    def test_create_mkstemp_no_space(self):
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        for e in (errno.ENOSPC, errno.EDQUOT):
            with mock.patch("swift.obj.diskfile.mkstemp",
                            mock.MagicMock(side_effect=OSError(
                                e, os.strerror(e)))):
                try:
                    with df.create(size=200):
                        pass
                except DiskFileNoSpace:
                    pass
                else:
                    self.fail("Expected exception DiskFileNoSpace")

        # Other OSErrors must not be raised as DiskFileNoSpace
        with mock.patch("swift.obj.diskfile.mkstemp",
                        mock.MagicMock(side_effect=OSError(
                            errno.EACCES, os.strerror(errno.EACCES)))):
            try:
                with df.create(size=200):
                    pass
            except OSError:
                pass
            else:
                self.fail("Expected exception OSError")

    def test_create_close_oserror(self):
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        with mock.patch("swift.obj.diskfile.os.close",
                        mock.MagicMock(side_effect=OSError(
                            errno.EACCES, os.strerror(errno.EACCES)))):
            try:
                with df.create(size=200):
                    pass
            except Exception as err:
                self.fail("Unexpected exception raised: %r" % err)
            else:
                pass

    def test_write_metadata(self):
        df = self._create_test_file('1234567890')
        file_count = len(os.listdir(df._datadir))
        timestamp = Timestamp(time()).internal
        metadata = {'X-Timestamp': timestamp, 'X-Object-Meta-test': 'data'}
        df.write_metadata(metadata)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), file_count + 1)
        exp_name = '%s.meta' % timestamp
        self.assertTrue(exp_name in set(dl))

    def test_write_metadata_no_xattr(self):
        timestamp = Timestamp(time()).internal
        metadata = {'X-Timestamp': timestamp, 'X-Object-Meta-test': 'data'}

        def mock_setxattr(*args, **kargs):
            error_num = errno.ENOTSUP if hasattr(errno, 'ENOTSUP') else \
                errno.EOPNOTSUPP
            raise IOError(error_num, "Operation not supported")

        with mock.patch('xattr.setxattr', mock_setxattr):
            self.assertRaises(
                DiskFileXattrNotSupported,
                diskfile.write_metadata, 'n/a', metadata)

    def test_write_metadata_disk_full(self):
        timestamp = Timestamp(time()).internal
        metadata = {'X-Timestamp': timestamp, 'X-Object-Meta-test': 'data'}

        def mock_setxattr_ENOSPC(*args, **kargs):
            raise IOError(errno.ENOSPC, "No space left on device")

        def mock_setxattr_EDQUOT(*args, **kargs):
            raise IOError(errno.EDQUOT, "Exceeded quota")

        with mock.patch('xattr.setxattr', mock_setxattr_ENOSPC):
            self.assertRaises(
                DiskFileNoSpace,
                diskfile.write_metadata, 'n/a', metadata)

        with mock.patch('xattr.setxattr', mock_setxattr_EDQUOT):
            self.assertRaises(
                DiskFileNoSpace,
                diskfile.write_metadata, 'n/a', metadata)

    def _create_diskfile_dir(self, timestamp, policy):
        timestamp = Timestamp(timestamp)
        df = self._simple_get_diskfile(account='a', container='c',
                                       obj='o_%s' % policy,
                                       policy=policy)

        with df.create() as writer:
            metadata = {
                'ETag': 'bogus_etag',
                'X-Timestamp': timestamp.internal,
                'Content-Length': '0',
            }
            if policy.policy_type == EC_POLICY:
                metadata['X-Object-Sysmeta-Ec-Frag-Index'] = \
                    df._frag_index or 7
            writer.put(metadata)
            writer.commit(timestamp)
        return writer._datadir

    def test_commit(self):
        for policy in POLICIES:
            # create first fileset as starting state
            timestamp = Timestamp(time()).internal
            datadir = self._create_diskfile_dir(timestamp, policy)
            dl = os.listdir(datadir)
            expected = ['%s.data' % timestamp]
            if policy.policy_type == EC_POLICY:
                expected = ['%s#2.data' % timestamp,
                            '%s.durable' % timestamp]
            self.assertEquals(len(dl), len(expected),
                              'Unexpected dir listing %s' % dl)
            self.assertEqual(sorted(expected), sorted(dl))

    def test_write_cleanup(self):
        for policy in POLICIES:
            # create first fileset as starting state
            timestamp_1 = Timestamp(time()).internal
            datadir_1 = self._create_diskfile_dir(timestamp_1, policy)
            # second write should clean up first fileset
            timestamp_2 = Timestamp(time() + 1).internal
            datadir_2 = self._create_diskfile_dir(timestamp_2, policy)
            # sanity check
            self.assertEqual(datadir_1, datadir_2)
            dl = os.listdir(datadir_2)
            expected = ['%s.data' % timestamp_2]
            if policy.policy_type == EC_POLICY:
                expected = ['%s#2.data' % timestamp_2,
                            '%s.durable' % timestamp_2]
            self.assertEquals(len(dl), len(expected),
                              'Unexpected dir listing %s' % dl)
            self.assertEqual(sorted(expected), sorted(dl))

    def test_commit_fsync(self):
        for policy in POLICIES:
            mock_fsync = mock.MagicMock()
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o', policy=policy)

            timestamp = Timestamp(time())
            with df.create() as writer:
                metadata = {
                    'ETag': 'bogus_etag',
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': '0',
                }
                writer.put(metadata)
                with mock.patch('swift.obj.diskfile.fsync', mock_fsync):
                    writer.commit(timestamp)
            expected = {
                EC_POLICY: 1,
                REPL_POLICY: 0,
            }[policy.policy_type]
            self.assertEqual(expected, mock_fsync.call_count)
            if policy.policy_type == EC_POLICY:
                durable_file = '%s.durable' % timestamp.internal
                self.assertTrue(durable_file in str(mock_fsync.call_args[0]))

    def test_commit_ignores_hash_cleanup_listdir_error(self):
        for policy in POLICIES:
            # Check OSError from hash_cleanup_listdir is caught and ignored
            mock_hcl = mock.MagicMock(side_effect=OSError)
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o_hcl_error', policy=policy)

            timestamp = Timestamp(time())
            with df.create() as writer:
                metadata = {
                    'ETag': 'bogus_etag',
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': '0',
                }
                writer.put(metadata)
                with mock.patch(self._manager_mock(
                        'hash_cleanup_listdir', df), mock_hcl):
                    writer.commit(timestamp)
            expected = {
                EC_POLICY: 1,
                REPL_POLICY: 0,
            }[policy.policy_type]
            self.assertEqual(expected, mock_hcl.call_count)
            expected = ['%s.data' % timestamp.internal]
            if policy.policy_type == EC_POLICY:
                expected = ['%s#2.data' % timestamp.internal,
                            '%s.durable' % timestamp.internal]
            dl = os.listdir(df._datadir)
            self.assertEquals(len(dl), len(expected),
                              'Unexpected dir listing %s' % dl)
            self.assertEqual(sorted(expected), sorted(dl))

    def test_delete(self):
        for policy in POLICIES:
            if policy.policy_type == EC_POLICY:
                metadata = {'X-Object-Sysmeta-Ec-Frag-Index': '1'}
                fi = 1
            else:
                metadata = {}
                fi = None
            df = self._get_open_disk_file(policy=policy, frag_index=fi,
                                          extra_metadata=metadata)

            ts = Timestamp(time())
            df.delete(ts)
            exp_name = '%s.ts' % ts.internal
            dl = os.listdir(df._datadir)
            self.assertEquals(len(dl), 1)
            self.assertTrue(exp_name in set(dl),
                            'Expected file %s missing in %s' % (exp_name, dl))
            # cleanup before next policy
            os.unlink(os.path.join(df._datadir, exp_name))

    def test_open_deleted(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileDeleted, df.open)

    def test_open_deleted_with_corrupt_tombstone(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        # it's pickle-format, so removing the last byte is sufficient to
        # corrupt it
        ts_fullpath = os.path.join(df._datadir, exp_name)
        self.assertTrue(os.path.exists(ts_fullpath))  # sanity check
        meta_xattr = xattr.getxattr(ts_fullpath, "user.swift.metadata")
        xattr.setxattr(ts_fullpath, "user.swift.metadata", meta_xattr[:-1])

        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileNotExist, df.open)
        self.assertFalse(os.path.exists(ts_fullpath))

    def test_from_audit_location(self):
        hashdir = self._create_test_file(
            'blah blah',
            account='three', container='blind', obj='mice')._datadir
        df = self.df_mgr.get_diskfile_from_audit_location(
            diskfile.AuditLocation(hashdir, self.existing_device, '0',
                                   policy=POLICIES.default))
        df.open()
        self.assertEqual(df._name, '/three/blind/mice')

    def test_from_audit_location_with_mismatched_hash(self):
        hashdir = self._create_test_file(
            'blah blah',
            account='this', container='is', obj='right')._datadir
        datafilename = [f for f in os.listdir(hashdir)
                        if f.endswith('.data')][0]
        datafile = os.path.join(hashdir, datafilename)
        meta = diskfile.read_metadata(datafile)
        meta['name'] = '/this/is/wrong'
        diskfile.write_metadata(datafile, meta)

        df = self.df_mgr.get_diskfile_from_audit_location(
            diskfile.AuditLocation(hashdir, self.existing_device, '0',
                                   policy=POLICIES.default))
        self.assertRaises(DiskFileQuarantined, df.open)

    def test_close_error(self):

        def mock_handle_close_quarantine():
            raise Exception("Bad")

        df = self._get_open_disk_file(fsize=1024 * 1024 * 2, csize=1024)
        reader = df.reader()
        reader._handle_close_quarantine = mock_handle_close_quarantine
        for chunk in reader:
            pass
        # close is called at the end of the iterator
        self.assertEquals(reader._fp, None)
        error_lines = df._logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        self.assertTrue('close failure' in error_lines[0])
        self.assertTrue('Bad' in error_lines[0])

    def test_mount_checking(self):

        def _mock_cm(*args, **kwargs):
            return False

        with mock.patch("swift.common.constraints.check_mount", _mock_cm):
            self.assertRaises(
                DiskFileDeviceUnavailable,
                self._get_open_disk_file,
                mount_check=True)

    def test_ondisk_search_loop_ts_meta_data(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, '', ext='.ts', timestamp=10)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=9)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=8)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=7)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=6)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=5)
        df = self._simple_get_diskfile()
        try:
            df.open()
        except DiskFileDeleted as d:
            self.assertEquals(d.timestamp, Timestamp(10).internal)
        else:
            self.fail("Expected DiskFileDeleted exception")

    def test_ondisk_search_loop_meta_ts_data(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, '', ext='.meta', timestamp=10)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=9)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=6)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=5)
        df = self._simple_get_diskfile()
        try:
            df.open()
        except DiskFileDeleted as d:
            self.assertEquals(d.timestamp, Timestamp(8).internal)
        else:
            self.fail("Expected DiskFileDeleted exception")

    def test_ondisk_search_loop_meta_data_ts(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, '', ext='.meta', timestamp=10)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=9)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=8)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=7)
        if df.policy.policy_type == EC_POLICY:
            self._create_ondisk_file(df, '', ext='.durable', timestamp=8)
            self._create_ondisk_file(df, '', ext='.durable', timestamp=7)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=6)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEquals(df._metadata['X-Timestamp'],
                              Timestamp(10).internal)
            self.assertTrue('deleted' not in df._metadata)

    def test_ondisk_search_loop_data_meta_ts(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=10)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=9)
        if df.policy.policy_type == EC_POLICY:
            self._create_ondisk_file(df, '', ext='.durable', timestamp=10)
            self._create_ondisk_file(df, '', ext='.durable', timestamp=9)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=6)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEquals(df._metadata['X-Timestamp'],
                              Timestamp(10).internal)
            self.assertTrue('deleted' not in df._metadata)

    def test_ondisk_search_loop_wayward_files_ignored(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, 'X', ext='.bar', timestamp=11)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=10)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=9)
        if df.policy.policy_type == EC_POLICY:
            self._create_ondisk_file(df, '', ext='.durable', timestamp=10)
            self._create_ondisk_file(df, '', ext='.durable', timestamp=9)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=6)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEquals(df._metadata['X-Timestamp'],
                              Timestamp(10).internal)
            self.assertTrue('deleted' not in df._metadata)

    def test_ondisk_search_loop_listdir_error(self):
        df = self._simple_get_diskfile()

        def mock_listdir_exp(*args, **kwargs):
            raise OSError(errno.EACCES, os.strerror(errno.EACCES))

        with mock.patch("os.listdir", mock_listdir_exp):
            self._create_ondisk_file(df, 'X', ext='.bar', timestamp=11)
            self._create_ondisk_file(df, 'B', ext='.data', timestamp=10)
            self._create_ondisk_file(df, 'A', ext='.data', timestamp=9)
            if df.policy.policy_type == EC_POLICY:
                self._create_ondisk_file(df, '', ext='.durable', timestamp=10)
                self._create_ondisk_file(df, '', ext='.durable', timestamp=9)
            self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
            self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
            self._create_ondisk_file(df, '', ext='.meta', timestamp=6)
            self._create_ondisk_file(df, '', ext='.meta', timestamp=5)
            df = self._simple_get_diskfile()
            self.assertRaises(DiskFileError, df.open)

    def test_exception_in_handle_close_quarantine(self):
        df = self._get_open_disk_file()

        def blow_up():
            raise Exception('a very special error')

        reader = df.reader()
        reader._handle_close_quarantine = blow_up
        for _ in reader:
            pass
        reader.close()
        log_lines = df._logger.get_lines_for_level('error')
        self.assert_('a very special error' in log_lines[-1])

    def test_diskfile_names(self):
        df = self._simple_get_diskfile()
        self.assertEqual(df.account, 'a')
        self.assertEqual(df.container, 'c')
        self.assertEqual(df.obj, 'o')

    def test_diskfile_content_length_not_open(self):
        df = self._simple_get_diskfile()
        exc = None
        try:
            df.content_length
        except DiskFileNotOpen as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_content_length_deleted(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        df = self._simple_get_diskfile()
        exc = None
        try:
            with df.open():
                df.content_length
        except DiskFileDeleted as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_content_length(self):
        self._get_open_disk_file()
        df = self._simple_get_diskfile()
        with df.open():
            self.assertEqual(df.content_length, 1024)

    def test_diskfile_timestamp_not_open(self):
        df = self._simple_get_diskfile()
        exc = None
        try:
            df.timestamp
        except DiskFileNotOpen as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_timestamp_deleted(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        df = self._simple_get_diskfile()
        exc = None
        try:
            with df.open():
                df.timestamp
        except DiskFileDeleted as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_timestamp(self):
        ts = Timestamp(time())
        self._get_open_disk_file(ts=ts.internal)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertEqual(df.timestamp, ts.internal)

    def test_error_in_hash_cleanup_listdir(self):

        def mock_hcl(*args, **kwargs):
            raise OSError()

        df = self._get_open_disk_file()
        file_count = len(os.listdir(df._datadir))
        ts = time()
        with mock.patch(self._manager_mock('hash_cleanup_listdir'), mock_hcl):
            try:
                df.delete(ts)
            except OSError:
                self.fail("OSError raised when it should have been swallowed")
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), file_count + 1)
        self.assertTrue(exp_name in set(dl))

    def _system_can_zero_copy(self):
        if not splice.available:
            return False

        try:
            utils.get_md5_socket()
        except IOError:
            return False

        return True

    def test_zero_copy_cache_dropping(self):
        if not self._system_can_zero_copy():
            raise SkipTest("zero-copy support is missing")

        self.conf['splice'] = 'on'
        self.conf['keep_cache_size'] = 16384
        self.conf['disk_chunk_size'] = 4096

        df = self._get_open_disk_file(fsize=16385)
        reader = df.reader()
        self.assertTrue(reader.can_zero_copy_send())
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as dbc:
            with mock.patch("swift.obj.diskfile.DROP_CACHE_WINDOW", 4095):
                with open('/dev/null', 'w') as devnull:
                    reader.zero_copy_send(devnull.fileno())
                self.assertEqual(len(dbc.mock_calls), 5)

    def test_zero_copy_turns_off_when_md5_sockets_not_supported(self):
        if not self._system_can_zero_copy():
            raise SkipTest("zero-copy support is missing")
        df_mgr = self.df_router[POLICIES.default]
        self.conf['splice'] = 'on'
        with mock.patch('swift.obj.diskfile.get_md5_socket') as mock_md5sock:
            mock_md5sock.side_effect = IOError(
                errno.EAFNOSUPPORT, "MD5 socket busted")
            df = self._get_open_disk_file(fsize=128)
            reader = df.reader()
            self.assertFalse(reader.can_zero_copy_send())

            log_lines = df_mgr.logger.get_lines_for_level('warning')
            self.assert_('MD5 sockets' in log_lines[-1])

    def test_tee_to_md5_pipe_length_mismatch(self):
        if not self._system_can_zero_copy():
            raise SkipTest("zero-copy support is missing")

        self.conf['splice'] = 'on'

        df = self._get_open_disk_file(fsize=16385)
        reader = df.reader()
        self.assertTrue(reader.can_zero_copy_send())

        with mock.patch('swift.obj.diskfile.tee') as mock_tee:
            mock_tee.side_effect = lambda _1, _2, _3, cnt: cnt - 1

            with open('/dev/null', 'w') as devnull:
                exc_re = (r'tee\(\) failed: tried to move \d+ bytes, but only '
                          'moved -?\d+')
                try:
                    reader.zero_copy_send(devnull.fileno())
                except Exception as e:
                    self.assertTrue(re.match(exc_re, str(e)))
                else:
                    self.fail('Expected Exception was not raised')

    def test_splice_to_wsockfd_blocks(self):
        if not self._system_can_zero_copy():
            raise SkipTest("zero-copy support is missing")

        self.conf['splice'] = 'on'

        df = self._get_open_disk_file(fsize=16385)
        reader = df.reader()
        self.assertTrue(reader.can_zero_copy_send())

        def _run_test():
            # Set up mock of `splice`
            splice_called = [False]  # State hack

            def fake_splice(fd_in, off_in, fd_out, off_out, len_, flags):
                if fd_out == devnull.fileno() and not splice_called[0]:
                    splice_called[0] = True
                    err = errno.EWOULDBLOCK
                    raise IOError(err, os.strerror(err))

                return splice(fd_in, off_in, fd_out, off_out,
                              len_, flags)

            mock_splice.side_effect = fake_splice

            # Set up mock of `trampoline`
            # There are 2 reasons to mock this:
            #
            # - We want to ensure it's called with the expected arguments at
            #   least once
            # - When called with our write FD (which points to `/dev/null`), we
            #   can't actually call `trampoline`, because adding such FD to an
            #   `epoll` handle results in `EPERM`
            def fake_trampoline(fd, read=None, write=None, timeout=None,
                                timeout_exc=timeout.Timeout,
                                mark_as_closed=None):
                if write and fd == devnull.fileno():
                    return
                else:
                    hubs.trampoline(fd, read=read, write=write,
                                    timeout=timeout, timeout_exc=timeout_exc,
                                    mark_as_closed=mark_as_closed)

            mock_trampoline.side_effect = fake_trampoline

            reader.zero_copy_send(devnull.fileno())

            # Assert the end of `zero_copy_send` was reached
            mock_close.assert_called()
            # Assert there was at least one call to `trampoline` waiting for
            # `write` access to the output FD
            mock_trampoline.assert_any_call(devnull.fileno(), write=True)
            # Assert at least one call to `splice` with the output FD we expect
            for call in mock_splice.call_args_list:
                args = call[0]
                if args[2] == devnull.fileno():
                    break
            else:
                self.fail('`splice` not called with expected arguments')

        with mock.patch('swift.obj.diskfile.splice') as mock_splice:
            with mock.patch.object(
                    reader, 'close', side_effect=reader.close) as mock_close:
                with open('/dev/null', 'w') as devnull:
                    with mock.patch('swift.obj.diskfile.trampoline') as \
                            mock_trampoline:
                        _run_test()

    def test_create_unlink_cleanup_DiskFileNoSpace(self):
        # Test cleanup when DiskFileNoSpace() is raised.
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        _m_fallocate = mock.MagicMock(side_effect=OSError(errno.ENOSPC,
                                      os.strerror(errno.ENOSPC)))
        _m_unlink = mock.Mock()
        with mock.patch("swift.obj.diskfile.fallocate", _m_fallocate):
            with mock.patch("os.unlink", _m_unlink):
                try:
                    with df.create(size=100):
                        pass
                except DiskFileNoSpace:
                    pass
                else:
                    self.fail("Expected exception DiskFileNoSpace")
        self.assertTrue(_m_fallocate.called)
        self.assertTrue(_m_unlink.called)
        self.assertTrue('error' not in self.logger.all_log_lines())

    def test_create_unlink_cleanup_renamer_fails(self):
        # Test cleanup when renamer fails
        _m_renamer = mock.MagicMock(side_effect=OSError(errno.ENOENT,
                                    os.strerror(errno.ENOENT)))
        _m_unlink = mock.Mock()
        df = self._simple_get_diskfile()
        data = '0' * 100
        metadata = {
            'ETag': md5(data).hexdigest(),
            'X-Timestamp': Timestamp(time()).internal,
            'Content-Length': str(100),
        }
        with mock.patch("swift.obj.diskfile.renamer", _m_renamer):
            with mock.patch("os.unlink", _m_unlink):
                try:
                    with df.create(size=100) as writer:
                        writer.write(data)
                        writer.put(metadata)
                except OSError:
                    pass
                else:
                    self.fail("Expected OSError exception")
        self.assertFalse(writer.put_succeeded)
        self.assertTrue(_m_renamer.called)
        self.assertTrue(_m_unlink.called)
        self.assertTrue('error' not in self.logger.all_log_lines())

    def test_create_unlink_cleanup_logging(self):
        # Test logging of os.unlink() failures.
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        _m_fallocate = mock.MagicMock(side_effect=OSError(errno.ENOSPC,
                                      os.strerror(errno.ENOSPC)))
        _m_unlink = mock.MagicMock(side_effect=OSError(errno.ENOENT,
                                   os.strerror(errno.ENOENT)))
        with mock.patch("swift.obj.diskfile.fallocate", _m_fallocate):
            with mock.patch("os.unlink", _m_unlink):
                try:
                    with df.create(size=100):
                        pass
                except DiskFileNoSpace:
                    pass
                else:
                    self.fail("Expected exception DiskFileNoSpace")
        self.assertTrue(_m_fallocate.called)
        self.assertTrue(_m_unlink.called)
        error_lines = self.logger.get_lines_for_level('error')
        for line in error_lines:
            self.assertTrue(line.startswith("Error removing tempfile:"))


@patch_policies(test_policies)
class TestDiskFile(DiskFileMixin, unittest.TestCase):

    mgr_cls = diskfile.DiskFileManager


@patch_policies(with_ec_default=True)
class TestECDiskFile(DiskFileMixin, unittest.TestCase):

    mgr_cls = diskfile.ECDiskFileManager

    def test_commit_raises_DiskFileErrors(self):
        scenarios = ((errno.ENOSPC, DiskFileNoSpace),
                     (errno.EDQUOT, DiskFileNoSpace),
                     (errno.ENOTDIR, DiskFileError),
                     (errno.EPERM, DiskFileError))

        # Check IOErrors from open() is handled
        for err_number, expected_exception in scenarios:
            io_error = IOError()
            io_error.errno = err_number
            mock_open = mock.MagicMock(side_effect=io_error)
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o_%s' % err_number,
                                           policy=POLICIES.default)
            timestamp = Timestamp(time())
            with df.create() as writer:
                metadata = {
                    'ETag': 'bogus_etag',
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': '0',
                }
                writer.put(metadata)
                with mock.patch('__builtin__.open', mock_open):
                    self.assertRaises(expected_exception,
                                      writer.commit,
                                      timestamp)
            dl = os.listdir(df._datadir)
            self.assertEqual(1, len(dl), dl)
            rmtree(df._datadir)

        # Check OSError from fsync() is handled
        mock_fsync = mock.MagicMock(side_effect=OSError)
        df = self._simple_get_diskfile(account='a', container='c',
                                       obj='o_fsync_error')

        timestamp = Timestamp(time())
        with df.create() as writer:
            metadata = {
                'ETag': 'bogus_etag',
                'X-Timestamp': timestamp.internal,
                'Content-Length': '0',
            }
            writer.put(metadata)
            with mock.patch('swift.obj.diskfile.fsync', mock_fsync):
                self.assertRaises(DiskFileError,
                                  writer.commit, timestamp)

    def test_data_file_has_frag_index(self):
        policy = POLICIES.default
        for good_value in (0, '0', 2, '2', 14, '14'):
            # frag_index set by constructor arg
            ts = self.ts().internal
            expected = ['%s#%s.data' % (ts, good_value), '%s.durable' % ts]
            df = self._get_open_disk_file(ts=ts, policy=policy,
                                          frag_index=good_value)
            self.assertEqual(expected, sorted(os.listdir(df._datadir)))
            # frag index should be added to object sysmeta
            actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
            self.assertEqual(int(good_value), int(actual))

            # metadata value overrides the constructor arg
            ts = self.ts().internal
            expected = ['%s#%s.data' % (ts, good_value), '%s.durable' % ts]
            meta = {'X-Object-Sysmeta-Ec-Frag-Index': good_value}
            df = self._get_open_disk_file(ts=ts, policy=policy,
                                          frag_index='99',
                                          extra_metadata=meta)
            self.assertEqual(expected, sorted(os.listdir(df._datadir)))
            actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
            self.assertEqual(int(good_value), int(actual))

            # metadata value alone is sufficient
            ts = self.ts().internal
            expected = ['%s#%s.data' % (ts, good_value), '%s.durable' % ts]
            meta = {'X-Object-Sysmeta-Ec-Frag-Index': good_value}
            df = self._get_open_disk_file(ts=ts, policy=policy,
                                          frag_index=None,
                                          extra_metadata=meta)
            self.assertEqual(expected, sorted(os.listdir(df._datadir)))
            actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
            self.assertEqual(int(good_value), int(actual))

    def test_sysmeta_frag_index_is_immutable(self):
        # the X-Object-Sysmeta-Ec-Frag-Index should *only* be set when
        # the .data file is written.
        policy = POLICIES.default
        orig_frag_index = 14
        # frag_index set by constructor arg
        ts = self.ts().internal
        expected = ['%s#%s.data' % (ts, orig_frag_index), '%s.durable' % ts]
        df = self._get_open_disk_file(ts=ts, policy=policy, obj_name='my_obj',
                                      frag_index=orig_frag_index)
        self.assertEqual(expected, sorted(os.listdir(df._datadir)))
        # frag index should be added to object sysmeta
        actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
        self.assertEqual(int(orig_frag_index), int(actual))

        # open the same diskfile with no frag_index passed to constructor
        df = self.df_router[policy].get_diskfile(
            self.existing_device, 0, 'a', 'c', 'my_obj', policy=policy,
            frag_index=None)
        df.open()
        actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
        self.assertEqual(int(orig_frag_index), int(actual))

        # write metadata to a meta file
        ts = self.ts().internal
        metadata = {'X-Timestamp': ts,
                    'X-Object-Meta-Fruit': 'kiwi'}
        df.write_metadata(metadata)
        # sanity check we did write a meta file
        expected.append('%s.meta' % ts)
        actual_files = sorted(os.listdir(df._datadir))
        self.assertEqual(expected, actual_files)

        # open the same diskfile, check frag index is unchanged
        df = self.df_router[policy].get_diskfile(
            self.existing_device, 0, 'a', 'c', 'my_obj', policy=policy,
            frag_index=None)
        df.open()
        # sanity check we have read the meta file
        self.assertEqual(ts, df.get_metadata().get('X-Timestamp'))
        self.assertEqual('kiwi', df.get_metadata().get('X-Object-Meta-Fruit'))
        # check frag index sysmeta is unchanged
        actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
        self.assertEqual(int(orig_frag_index), int(actual))

        # attempt to overwrite frag index sysmeta
        ts = self.ts().internal
        metadata = {'X-Timestamp': ts,
                    'X-Object-Sysmeta-Ec-Frag-Index': 99,
                    'X-Object-Meta-Fruit': 'apple'}
        df.write_metadata(metadata)

        # open the same diskfile, check frag index is unchanged
        df = self.df_router[policy].get_diskfile(
            self.existing_device, 0, 'a', 'c', 'my_obj', policy=policy,
            frag_index=None)
        df.open()
        # sanity check we have read the meta file
        self.assertEqual(ts, df.get_metadata().get('X-Timestamp'))
        self.assertEqual('apple', df.get_metadata().get('X-Object-Meta-Fruit'))
        actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
        self.assertEqual(int(orig_frag_index), int(actual))

    def test_data_file_errors_bad_frag_index(self):
        policy = POLICIES.default
        df_mgr = self.df_router[policy]
        for bad_value in ('foo', '-2', -2, '3.14', 3.14):
            # check that bad frag_index set by constructor arg raises error
            # as soon as diskfile is constructed, before data is written
            self.assertRaises(DiskFileError, self._simple_get_diskfile,
                              policy=policy, frag_index=bad_value)

            # bad frag_index set by metadata value
            # (drive-by check that it is ok for constructor arg to be None)
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                     policy=policy, frag_index=None)
            ts = self.ts()
            meta = {'X-Object-Sysmeta-Ec-Frag-Index': bad_value,
                    'X-Timestamp': ts.internal,
                    'Content-Length': 0,
                    'Etag': EMPTY_ETAG,
                    'Content-Type': 'plain/text'}
            with df.create() as writer:
                try:
                    writer.put(meta)
                    self.fail('Expected DiskFileError for frag_index %s'
                              % bad_value)
                except DiskFileError:
                    pass

            # bad frag_index set by metadata value overrides ok constructor arg
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                     policy=policy, frag_index=2)
            ts = self.ts()
            meta = {'X-Object-Sysmeta-Ec-Frag-Index': bad_value,
                    'X-Timestamp': ts.internal,
                    'Content-Length': 0,
                    'Etag': EMPTY_ETAG,
                    'Content-Type': 'plain/text'}
            with df.create() as writer:
                try:
                    writer.put(meta)
                    self.fail('Expected DiskFileError for frag_index %s'
                              % bad_value)
                except DiskFileError:
                    pass

    def test_purge_one_fragment_index(self):
        ts = self.ts()
        for frag_index in (1, 2):
            df = self._simple_get_diskfile(frag_index=frag_index)
            with df.create() as writer:
                data = 'test data'
                writer.write(data)
                metadata = {
                    'ETag': md5(data).hexdigest(),
                    'X-Timestamp': ts.internal,
                    'Content-Length': len(data),
                }
                writer.put(metadata)
                writer.commit(ts)

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1.data',
            ts.internal + '#2.data',
            ts.internal + '.durable',
        ])
        df.purge(ts, 2)
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1.data',
            ts.internal + '.durable',
        ])

    def test_purge_last_fragment_index(self):
        ts = self.ts()
        frag_index = 0
        df = self._simple_get_diskfile(frag_index=frag_index)
        with df.create() as writer:
            data = 'test data'
            writer.write(data)
            metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': ts.internal,
                'Content-Length': len(data),
            }
            writer.put(metadata)
            writer.commit(ts)

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#0.data',
            ts.internal + '.durable',
        ])
        df.purge(ts, 0)
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '.durable',
        ])

    def test_purge_non_existant_fragment_index(self):
        ts = self.ts()
        frag_index = 7
        df = self._simple_get_diskfile(frag_index=frag_index)
        with df.create() as writer:
            data = 'test data'
            writer.write(data)
            metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': ts.internal,
                'Content-Length': len(data),
            }
            writer.put(metadata)
            writer.commit(ts)

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#7.data',
            ts.internal + '.durable',
        ])
        df.purge(ts, 3)
        # no effect
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#7.data',
            ts.internal + '.durable',
        ])

    def test_purge_old_timestamp_frag_index(self):
        old_ts = self.ts()
        ts = self.ts()
        frag_index = 1
        df = self._simple_get_diskfile(frag_index=frag_index)
        with df.create() as writer:
            data = 'test data'
            writer.write(data)
            metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': ts.internal,
                'Content-Length': len(data),
            }
            writer.put(metadata)
            writer.commit(ts)

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1.data',
            ts.internal + '.durable',
        ])
        df.purge(old_ts, 1)
        # no effect
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1.data',
            ts.internal + '.durable',
        ])

    def test_purge_tombstone(self):
        ts = self.ts()
        df = self._simple_get_diskfile(frag_index=3)
        df.delete(ts)

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '.ts',
        ])
        df.purge(ts, 3)
        self.assertEqual(sorted(os.listdir(df._datadir)), [])

    def test_purge_old_tombstone(self):
        old_ts = self.ts()
        ts = self.ts()
        df = self._simple_get_diskfile(frag_index=5)
        df.delete(ts)

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '.ts',
        ])
        df.purge(old_ts, 5)
        # no effect
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '.ts',
        ])

    def test_purge_already_removed(self):
        df = self._simple_get_diskfile(frag_index=6)

        df.purge(self.ts(), 6)  # no errors

        # sanity
        os.makedirs(df._datadir)
        self.assertEqual(sorted(os.listdir(df._datadir)), [])
        df.purge(self.ts(), 6)
        # no effect
        self.assertEqual(sorted(os.listdir(df._datadir)), [])

    def test_open_most_recent_durable(self):
        policy = POLICIES.default
        df_mgr = self.df_router[policy]

        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)

        ts = self.ts()
        with df.create() as writer:
            data = 'test data'
            writer.write(data)
            metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': ts.internal,
                'Content-Length': len(data),
                'X-Object-Sysmeta-Ec-Frag-Index': 3,
            }
            writer.put(metadata)
            writer.commit(ts)

        # add some .meta stuff
        extra_meta = {
            'X-Object-Meta-Foo': 'Bar',
            'X-Timestamp': self.ts().internal,
        }
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        df.write_metadata(extra_meta)

        # sanity
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        metadata.update(extra_meta)
        self.assertEqual(metadata, df.read_metadata())

        # add a newer datafile
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        ts = self.ts()
        with df.create() as writer:
            data = 'test data'
            writer.write(data)
            new_metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': ts.internal,
                'Content-Length': len(data),
                'X-Object-Sysmeta-Ec-Frag-Index': 3,
            }
            writer.put(new_metadata)
            # N.B. don't make it durable

        # and we still get the old metadata (same as if no .data!)
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        self.assertEqual(metadata, df.read_metadata())

    def test_open_most_recent_missing_durable(self):
        policy = POLICIES.default
        df_mgr = self.df_router[policy]

        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)

        self.assertRaises(DiskFileNotExist, df.read_metadata)

        # now create a datafile missing durable
        ts = self.ts()
        with df.create() as writer:
            data = 'test data'
            writer.write(data)
            new_metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': ts.internal,
                'Content-Length': len(data),
                'X-Object-Sysmeta-Ec-Frag-Index': 3,
            }
            writer.put(new_metadata)
            # N.B. don't make it durable

        # add some .meta stuff
        extra_meta = {
            'X-Object-Meta-Foo': 'Bar',
            'X-Timestamp': self.ts().internal,
        }
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        df.write_metadata(extra_meta)

        # we still get the DiskFileNotExist (same as if no .data!)
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy,
                                 frag_index=3)
        self.assertRaises(DiskFileNotExist, df.read_metadata)

        # sanity, withtout the frag_index kwarg
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        self.assertRaises(DiskFileNotExist, df.read_metadata)


@patch_policies(with_ec_default=True)
class TestSuffixHashes(unittest.TestCase):
    """
    This tests all things related to hashing suffixes and therefore
    there's also few test methods for hash_cleanup_listdir as well
    (because it's used by hash_suffix).

    The public interface to suffix hashing is on the Manager::

         * hash_cleanup_listdir(hsh_path)
         * get_hashes(device, partition, suffixes, policy)
         * invalidate_hash(suffix_dir)

    The Manager.get_hashes method (used by the REPLICATION verb)
    calls Manager._get_hashes (which may be an alias to the module
    method get_hashes), which calls hash_suffix, which calls
    hash_cleanup_listdir.

    Outside of that, hash_cleanup_listdir and invalidate_hash are
    used mostly after writing new files via PUT or DELETE.

    Test methods are organized by::

        * hash_cleanup_listdir tests - behaviors
        * hash_cleanup_listdir tests - error handling
        * invalidate_hash tests - behavior
        * invalidate_hash tests - error handling
        * get_hashes tests - hash_suffix behaviors
        * get_hashes tests - hash_suffix error handling
        * get_hashes tests - behaviors
        * get_hashes tests - error handling

    """

    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.logger = debug_logger('suffix-hash-test')
        self.devices = os.path.join(self.testdir, 'node')
        os.mkdir(self.devices)
        self.existing_device = 'sda1'
        os.mkdir(os.path.join(self.devices, self.existing_device))
        self.conf = {
            'swift_dir': self.testdir,
            'devices': self.devices,
            'mount_check': False,
        }
        self.df_router = diskfile.DiskFileRouter(self.conf, self.logger)
        self._ts_iter = (Timestamp(t) for t in
                         itertools.count(int(time())))
        self.policy = None

    def ts(self):
        """
        Timestamps - forever.
        """
        return next(self._ts_iter)

    def fname_to_ts_hash(self, fname):
        """
        EC datafiles are only hashed by their timestamp
        """
        return md5(fname.split('#', 1)[0]).hexdigest()

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def iter_policies(self):
        for policy in POLICIES:
            self.policy = policy
            yield policy

    def assertEqual(self, *args):
        try:
            unittest.TestCase.assertEqual(self, *args)
        except AssertionError as err:
            if not self.policy:
                raise
            policy_trailer = '\n\n... for policy %r' % self.policy
            raise AssertionError(str(err) + policy_trailer)

    def _datafilename(self, timestamp, policy, frag_index=None):
        if frag_index is None:
            frag_index = randint(0, 9)
        filename = timestamp.internal
        if policy.policy_type == EC_POLICY:
            filename += '#%d' % frag_index
        filename += '.data'
        return filename

    def check_hash_cleanup_listdir(self, policy, input_files, output_files):
        orig_unlink = os.unlink
        file_list = list(input_files)

        def mock_listdir(path):
            return list(file_list)

        def mock_unlink(path):
            # timestamp 1 is a special tag to pretend a file disappeared
            # between the listdir and unlink.
            if '/0000000001.00000.' in path:
                # Using actual os.unlink for a non-existent name to reproduce
                # exactly what OSError it raises in order to prove that
                # common.utils.remove_file is squelching the error - but any
                # OSError would do.
                orig_unlink(uuid.uuid4().hex)
            file_list.remove(os.path.basename(path))

        df_mgr = self.df_router[policy]
        with unit_mock({'os.listdir': mock_listdir, 'os.unlink': mock_unlink}):
            if isinstance(output_files, Exception):
                path = os.path.join(self.testdir, 'does-not-matter')
                self.assertRaises(output_files.__class__,
                                  df_mgr.hash_cleanup_listdir, path)
                return
            files = df_mgr.hash_cleanup_listdir('/whatever')
            self.assertEquals(files, output_files)

    # hash_cleanup_listdir tests - behaviors

    def test_hash_cleanup_listdir_purge_data_newer_ts(self):
        for policy in self.iter_policies():
            # purge .data if there's a newer .ts
            file1 = self._datafilename(self.ts(), policy)
            file2 = self.ts().internal + '.ts'
            file_list = [file1, file2]
            self.check_hash_cleanup_listdir(policy, file_list, [file2])

    def test_hash_cleanup_listdir_purge_expired_ts(self):
        for policy in self.iter_policies():
            # purge older .ts files if there's a newer .data
            file1 = self.ts().internal + '.ts'
            file2 = self.ts().internal + '.ts'
            timestamp = self.ts()
            file3 = self._datafilename(timestamp, policy)
            file_list = [file1, file2, file3]
            expected = {
                # no durable datafile means you can't get rid of the
                # latest tombstone even if datafile is newer
                EC_POLICY: [file3, file2],
                REPL_POLICY: [file3],
            }[policy.policy_type]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_purge_ts_newer_data(self):
        for policy in self.iter_policies():
            # purge .ts if there's a newer .data
            file1 = self.ts().internal + '.ts'
            timestamp = self.ts()
            file2 = self._datafilename(timestamp, policy)
            file_list = [file1, file2]
            if policy.policy_type == EC_POLICY:
                durable_file = timestamp.internal + '.durable'
                file_list.append(durable_file)
            expected = {
                EC_POLICY: [durable_file, file2],
                REPL_POLICY: [file2],
            }[policy.policy_type]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_purge_older_ts(self):
        for policy in self.iter_policies():
            file1 = self.ts().internal + '.ts'
            file2 = self.ts().internal + '.ts'
            file3 = self._datafilename(self.ts(), policy)
            file4 = self.ts().internal + '.meta'
            expected = {
                # no durable means we can only throw out things before
                # the latest tombstone
                EC_POLICY: [file4, file3, file2],
                # keep .meta and .data and purge all .ts files
                REPL_POLICY: [file4, file3],
            }[policy.policy_type]
            file_list = [file1, file2, file3, file4]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_keep_meta_data_purge_ts(self):
        for policy in self.iter_policies():
            file1 = self.ts().internal + '.ts'
            file2 = self.ts().internal + '.ts'
            timestamp = self.ts()
            file3 = self._datafilename(timestamp, policy)
            file_list = [file1, file2, file3]
            if policy.policy_type == EC_POLICY:
                durable_filename = timestamp.internal + '.durable'
                file_list.append(durable_filename)
            file4 = self.ts().internal + '.meta'
            file_list.append(file4)
            # keep .meta and .data if meta newer than data and purge .ts
            expected = {
                EC_POLICY: [file4, durable_filename, file3],
                REPL_POLICY: [file4, file3],
            }[policy.policy_type]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_keep_one_ts(self):
        for policy in self.iter_policies():
            file1, file2, file3 = [self.ts().internal + '.ts'
                                   for i in range(3)]
            file_list = [file1, file2, file3]
            # keep only latest of multiple .ts files
            self.check_hash_cleanup_listdir(policy, file_list, [file3])

    def test_hash_cleanup_listdir_multi_data_file(self):
        for policy in self.iter_policies():
            file1 = self._datafilename(self.ts(), policy, 1)
            file2 = self._datafilename(self.ts(), policy, 2)
            file3 = self._datafilename(self.ts(), policy, 3)
            expected = {
                # keep all non-durable datafiles
                EC_POLICY: [file3, file2, file1],
                # keep only latest of multiple .data files
                REPL_POLICY: [file3]
            }[policy.policy_type]
            file_list = [file1, file2, file3]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_keeps_one_datafile(self):
        for policy in self.iter_policies():
            timestamps = [self.ts() for i in range(3)]
            file1 = self._datafilename(timestamps[0], policy, 1)
            file2 = self._datafilename(timestamps[1], policy, 2)
            file3 = self._datafilename(timestamps[2], policy, 3)
            file_list = [file1, file2, file3]
            if policy.policy_type == EC_POLICY:
                for t in timestamps:
                    file_list.append(t.internal + '.durable')
                latest_durable = file_list[-1]
            expected = {
                # keep latest durable and datafile
                EC_POLICY: [latest_durable, file3],
                # keep only latest of multiple .data files
                REPL_POLICY: [file3]
            }[policy.policy_type]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_keep_one_meta(self):
        for policy in self.iter_policies():
            # keep only latest of multiple .meta files
            t_data = self.ts()
            file1 = self._datafilename(t_data, policy)
            file2, file3 = [self.ts().internal + '.meta' for i in range(2)]
            file_list = [file1, file2, file3]
            if policy.policy_type == EC_POLICY:
                durable_file = t_data.internal + '.durable'
                file_list.append(durable_file)
            expected = {
                EC_POLICY: [file3, durable_file, file1],
                REPL_POLICY: [file3, file1]
            }[policy.policy_type]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_only_meta(self):
        for policy in self.iter_policies():
            file1, file2 = [self.ts().internal + '.meta' for i in range(2)]
            file_list = [file1, file2]
            if policy.policy_type == EC_POLICY:
                # EC policy does tolerate only .meta's in dir when cleaning up
                expected = [file2]
            else:
                # the get_ondisk_files contract validation doesn't allow a
                # directory with only .meta files
                expected = AssertionError()
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_ignore_orphaned_ts(self):
        for policy in self.iter_policies():
            # A more recent orphaned .meta file will prevent old .ts files
            # from being cleaned up otherwise
            file1, file2 = [self.ts().internal + '.ts' for i in range(2)]
            file3 = self.ts().internal + '.meta'
            file_list = [file1, file2, file3]
            self.check_hash_cleanup_listdir(policy, file_list, [file3, file2])

    def test_hash_cleanup_listdir_purge_old_data_only(self):
        for policy in self.iter_policies():
            # Oldest .data will be purge, .meta and .ts won't be touched
            file1 = self._datafilename(self.ts(), policy)
            file2 = self.ts().internal + '.ts'
            file3 = self.ts().internal + '.meta'
            file_list = [file1, file2, file3]
            self.check_hash_cleanup_listdir(policy, file_list, [file3, file2])

    def test_hash_cleanup_listdir_purge_old_ts(self):
        for policy in self.iter_policies():
            # A single old .ts file will be removed
            old_float = time() - (diskfile.ONE_WEEK + 1)
            file1 = Timestamp(old_float).internal + '.ts'
            file_list = [file1]
            self.check_hash_cleanup_listdir(policy, file_list, [])

    def test_hash_cleanup_listdir_meta_keeps_old_ts(self):
        for policy in self.iter_policies():
            old_float = time() - (diskfile.ONE_WEEK + 1)
            file1 = Timestamp(old_float).internal + '.ts'
            file2 = Timestamp(time() + 2).internal + '.meta'
            file_list = [file1, file2]
            if policy.policy_type == EC_POLICY:
                # EC will clean up old .ts despite a .meta
                expected = [file2]
            else:
                # An orphaned .meta will not clean up a very old .ts
                expected = [file2, file1]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_keep_single_old_data(self):
        for policy in self.iter_policies():
            old_float = time() - (diskfile.ONE_WEEK + 1)
            file1 = self._datafilename(Timestamp(old_float), policy)
            file_list = [file1]
            if policy.policy_type == EC_POLICY:
                # for EC an isolated old .data file is removed, its useless
                # without a .durable
                expected = []
            else:
                # A single old .data file will not be removed
                expected = file_list
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    def test_hash_cleanup_listdir_drops_isolated_durable(self):
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                file1 = Timestamp(time()).internal + '.durable'
                file_list = [file1]
                self.check_hash_cleanup_listdir(policy, file_list, [])

    def test_hash_cleanup_listdir_keep_single_old_meta(self):
        for policy in self.iter_policies():
            # A single old .meta file will not be removed
            old_float = time() - (diskfile.ONE_WEEK + 1)
            file1 = Timestamp(old_float).internal + '.meta'
            file_list = [file1]
            self.check_hash_cleanup_listdir(policy, file_list, [file1])

    # hash_cleanup_listdir tests - error handling

    def test_hash_cleanup_listdir_hsh_path_enoent(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # common.utils.listdir *completely* mutes ENOENT
            path = os.path.join(self.testdir, 'does-not-exist')
            self.assertEqual(df_mgr.hash_cleanup_listdir(path), [])

    def test_hash_cleanup_listdir_hsh_path_other_oserror(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            with mock.patch('os.listdir') as mock_listdir:
                mock_listdir.side_effect = OSError('kaboom!')
                # but it will raise other OSErrors
                path = os.path.join(self.testdir, 'does-not-matter')
                self.assertRaises(OSError, df_mgr.hash_cleanup_listdir,
                                  path)

    def test_hash_cleanup_listdir_reclaim_tombstone_remove_file_error(self):
        for policy in self.iter_policies():
            # Timestamp 1 makes the check routine pretend the file
            # disappeared after listdir before unlink.
            file1 = '0000000001.00000.ts'
            file_list = [file1]
            self.check_hash_cleanup_listdir(policy, file_list, [])

    def test_hash_cleanup_listdir_older_remove_file_error(self):
        for policy in self.iter_policies():
            # Timestamp 1 makes the check routine pretend the file
            # disappeared after listdir before unlink.
            file1 = self._datafilename(Timestamp(1), policy)
            file2 = '0000000002.00000.ts'
            file_list = [file1, file2]
            if policy.policy_type == EC_POLICY:
                # the .ts gets reclaimed up despite failed .data delete
                expected = []
            else:
                # the .ts isn't reclaimed because there were two files in dir
                expected = [file2]
            self.check_hash_cleanup_listdir(policy, file_list, expected)

    # invalidate_hash tests - behavior

    def test_invalidate_hash_file_does_not_exist(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            suffix_dir = os.path.dirname(df._datadir)
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path, diskfile.HASH_FILE)
            self.assertFalse(os.path.exists(hashes_file))  # sanity
            with mock.patch('swift.obj.diskfile.lock_path') as mock_lock:
                df_mgr.invalidate_hash(suffix_dir)
            self.assertFalse(mock_lock.called)
            # does not create file
            self.assertFalse(os.path.exists(hashes_file))

    def test_invalidate_hash_file_exists(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # create something to hash
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            df.delete(self.ts())
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertTrue(suffix in hashes)  # sanity
            # sanity check hashes file
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path, diskfile.HASH_FILE)
            with open(hashes_file, 'rb') as f:
                self.assertEqual(hashes, pickle.load(f))
            # invalidate the hash
            with mock.patch('swift.obj.diskfile.lock_path') as mock_lock:
                df_mgr.invalidate_hash(suffix_dir)
            self.assertTrue(mock_lock.called)
            with open(hashes_file, 'rb') as f:
                self.assertEqual({suffix: None}, pickle.load(f))

    # invalidate_hash tests - error handling

    def test_invalidate_hash_bad_pickle(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # make some valid data
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            df.delete(self.ts())
            # sanity check hashes file
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path, diskfile.HASH_FILE)
            self.assertFalse(os.path.exists(hashes_file))
            # write some garbage in hashes file
            with open(hashes_file, 'w') as f:
                f.write('asdf')
            # invalidate_hash silently *NOT* repair invalid data
            df_mgr.invalidate_hash(suffix_dir)
            with open(hashes_file) as f:
                self.assertEqual(f.read(), 'asdf')
            # ... but get_hashes will
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertTrue(suffix in hashes)

    # get_hashes tests - hash_suffix behaviors

    def test_hash_suffix_one_tombstone(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # write a tombstone
            timestamp = self.ts()
            df.delete(timestamp)
            tombstone_hash = md5(timestamp.internal + '.ts').hexdigest()
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            expected = {
                REPL_POLICY: {suffix: tombstone_hash},
                EC_POLICY: {suffix: {
                    # fi is None here because we have a tombstone
                    None: tombstone_hash}},
            }[policy.policy_type]
            self.assertEqual(hashes, expected)

    def test_hash_suffix_one_reclaim_tombstone(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # scale back this tests manager's reclaim age a bit
            df_mgr.reclaim_age = 1000
            # write a tombstone that's just a *little* older
            old_time = time() - 1001
            timestamp = Timestamp(old_time)
            df.delete(timestamp.internal)
            tombstone_hash = md5(timestamp.internal + '.ts').hexdigest()
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            expected = {
                # repl is broken, it doesn't use self.reclaim_age
                REPL_POLICY: tombstone_hash,
                EC_POLICY: {},
            }[policy.policy_type]
            self.assertEqual(hashes, {suffix: expected})

    def test_hash_suffix_one_datafile(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy, frag_index=7)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # write a datafile
            timestamp = self.ts()
            with df.create() as writer:
                test_data = 'test file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            datafile_hash = md5({
                EC_POLICY: timestamp.internal,
                REPL_POLICY: timestamp.internal + '.data',
            }[policy.policy_type]).hexdigest()
            expected = {
                REPL_POLICY: {suffix: datafile_hash},
                EC_POLICY: {suffix: {
                    # because there's no .durable file, we have no hash for
                    # the None key - only the frag index for the data file
                    7: datafile_hash}},
            }[policy.policy_type]
            msg = 'expected %r != %r for policy %r' % (
                expected, hashes, policy)
            self.assertEqual(hashes, expected, msg)

    def test_hash_suffix_multi_file_ends_in_tombstone(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o', policy=policy,
                                     frag_index=4)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            mkdirs(df._datadir)
            now = time()
            # go behind the scenes and setup a bunch of weird file names
            for tdiff in [500, 100, 10, 1]:
                for suff in ['.meta', '.data', '.ts']:
                    timestamp = Timestamp(now - tdiff)
                    filename = timestamp.internal
                    if policy.policy_type == EC_POLICY and suff == '.data':
                        filename += '#%s' % df._frag_index
                    filename += suff
                    open(os.path.join(df._datadir, filename), 'w').close()
            tombstone_hash = md5(filename).hexdigest()
            # call get_hashes and it should clean things up
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            expected = {
                REPL_POLICY: {suffix: tombstone_hash},
                EC_POLICY: {suffix: {
                    # fi is None here because we have a tombstone
                    None: tombstone_hash}},
            }[policy.policy_type]
            self.assertEqual(hashes, expected)
            # only the tombstone should be left
            found_files = os.listdir(df._datadir)
            self.assertEqual(found_files, [filename])

    def test_hash_suffix_multi_file_ends_in_datafile(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o', policy=policy,
                                     frag_index=4)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            mkdirs(df._datadir)
            now = time()
            timestamp = None
            # go behind the scenes and setup a bunch of weird file names
            for tdiff in [500, 100, 10, 1]:
                suffs = ['.meta', '.data']
                if tdiff > 50:
                    suffs.append('.ts')
                if policy.policy_type == EC_POLICY:
                    suffs.append('.durable')
                for suff in suffs:
                    timestamp = Timestamp(now - tdiff)
                    filename = timestamp.internal
                    if policy.policy_type == EC_POLICY and suff == '.data':
                        filename += '#%s' % df._frag_index
                    filename += suff
                    open(os.path.join(df._datadir, filename), 'w').close()
            # call get_hashes and it should clean things up
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            data_filename = timestamp.internal
            if policy.policy_type == EC_POLICY:
                data_filename += '#%s' % df._frag_index
            data_filename += '.data'
            metadata_filename = timestamp.internal + '.meta'
            durable_filename = timestamp.internal + '.durable'
            if policy.policy_type == EC_POLICY:
                hasher = md5()
                hasher.update(metadata_filename)
                hasher.update(durable_filename)
                expected = {
                    suffix: {
                        # metadata & durable updates are hashed separately
                        None: hasher.hexdigest(),
                        4: self.fname_to_ts_hash(data_filename),
                    }
                }
                expected_files = [data_filename, durable_filename,
                                  metadata_filename]
            elif policy.policy_type == REPL_POLICY:
                hasher = md5()
                hasher.update(metadata_filename)
                hasher.update(data_filename)
                expected = {suffix: hasher.hexdigest()}
                expected_files = [data_filename, metadata_filename]
            else:
                self.fail('unknown policy type %r' % policy.policy_type)
            msg = 'expected %r != %r for policy %r' % (
                expected, hashes, policy)
            self.assertEqual(hashes, expected, msg)
            # only the meta and data should be left
            self.assertEqual(sorted(os.listdir(df._datadir)),
                             sorted(expected_files))

    def test_hash_suffix_removes_empty_hashdir_and_suffix(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy, frag_index=2)
            os.makedirs(df._datadir)
            self.assertTrue(os.path.exists(df._datadir))  # sanity
            df_mgr.get_hashes('sda1', '0', [], policy)
            suffix_dir = os.path.dirname(df._datadir)
            self.assertFalse(os.path.exists(suffix_dir))

    def test_hash_suffix_removes_empty_hashdirs_in_valid_suffix(self):
        paths, suffix = find_paths_with_matching_suffixes(needed_matches=3,
                                                          needed_suffixes=0)
        matching_paths = paths.pop(suffix)
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', *matching_paths[0],
                                     policy=policy, frag_index=2)
            # create a real, valid hsh_path
            df.delete(Timestamp(time()))
            # and a couple of empty hsh_paths
            empty_hsh_paths = []
            for path in matching_paths[1:]:
                fake_df = df_mgr.get_diskfile('sda1', '0', *path,
                                              policy=policy)
                os.makedirs(fake_df._datadir)
                empty_hsh_paths.append(fake_df._datadir)
            for hsh_path in empty_hsh_paths:
                self.assertTrue(os.path.exists(hsh_path))  # sanity
            # get_hashes will cleanup empty hsh_path and leave valid one
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertTrue(suffix in hashes)
            self.assertTrue(os.path.exists(df._datadir))
            for hsh_path in empty_hsh_paths:
                self.assertFalse(os.path.exists(hsh_path))

    # get_hashes tests - hash_suffix error handling

    def test_hash_suffix_listdir_enotdir(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            suffix = '123'
            suffix_path = os.path.join(self.devices, 'sda1',
                                       diskfile.get_data_dir(policy), '0',
                                       suffix)
            os.makedirs(suffix_path)
            self.assertTrue(os.path.exists(suffix_path))  # sanity
            hashes = df_mgr.get_hashes('sda1', '0', [suffix], policy)
            # suffix dir cleaned up by get_hashes
            self.assertFalse(os.path.exists(suffix_path))
            expected = {
                EC_POLICY: {'123': {}},
                REPL_POLICY: {'123': EMPTY_ETAG},
            }[policy.policy_type]
            msg = 'expected %r != %r for policy %r' % (expected, hashes,
                                                       policy)
            self.assertEqual(hashes, expected, msg)

            # now make the suffix path a file
            open(suffix_path, 'w').close()
            hashes = df_mgr.get_hashes('sda1', '0', [suffix], policy)
            expected = {}
            msg = 'expected %r != %r for policy %r' % (expected, hashes,
                                                       policy)
            self.assertEqual(hashes, expected, msg)

    def test_hash_suffix_listdir_enoent(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            orig_listdir = os.listdir
            listdir_calls = []

            def mock_listdir(path):
                success = False
                try:
                    rv = orig_listdir(path)
                    success = True
                    return rv
                finally:
                    listdir_calls.append((path, success))

            with mock.patch('swift.obj.diskfile.os.listdir',
                            mock_listdir):
                # recalc always forces hash_suffix even if the suffix
                # does not exist!
                df_mgr.get_hashes('sda1', '0', ['123'], policy)

            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')

            self.assertEqual(listdir_calls, [
                # part path gets created automatically
                (part_path, True),
                # this one blows up
                (os.path.join(part_path, '123'), False),
            ])

    def test_hash_suffix_hash_cleanup_listdir_enotdir_quarantined(self):
        for policy in self.iter_policies():
            df = self.df_router[policy].get_diskfile(
                self.existing_device, '0', 'a', 'c', 'o', policy=policy)
            # make the suffix directory
            suffix_path = os.path.dirname(df._datadir)
            os.makedirs(suffix_path)
            suffix = os.path.basename(suffix_path)

            # make the df hash path a file
            open(df._datadir, 'wb').close()
            df_mgr = self.df_router[policy]
            hashes = df_mgr.get_hashes(self.existing_device, '0', [suffix],
                                       policy)
            expected = {
                REPL_POLICY: {suffix: EMPTY_ETAG},
                EC_POLICY: {suffix: {}},
            }[policy.policy_type]
            self.assertEqual(hashes, expected)
            # and hash path is quarantined
            self.assertFalse(os.path.exists(df._datadir))
            # each device a quarantined directory
            quarantine_base = os.path.join(self.devices,
                                           self.existing_device, 'quarantined')
            # the quarantine path is...
            quarantine_path = os.path.join(
                quarantine_base,  # quarantine root
                diskfile.get_data_dir(policy),  # per-policy data dir
                suffix,  # first dir from which quarantined file was removed
                os.path.basename(df._datadir)  # name of quarantined file
            )
            self.assertTrue(os.path.exists(quarantine_path))

    def test_hash_suffix_hash_cleanup_listdir_other_oserror(self):
        for policy in self.iter_policies():
            timestamp = self.ts()
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy,
                                     frag_index=7)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            with df.create() as writer:
                test_data = 'test_data'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)

            orig_os_listdir = os.listdir
            listdir_calls = []

            part_path = os.path.join(self.devices, self.existing_device,
                                     diskfile.get_data_dir(policy), '0')
            suffix_path = os.path.join(part_path, suffix)
            datadir_path = os.path.join(suffix_path, hash_path('a', 'c', 'o'))

            def mock_os_listdir(path):
                listdir_calls.append(path)
                if path == datadir_path:
                    # we want the part and suffix listdir calls to pass and
                    # make the hash_cleanup_listdir raise an exception
                    raise OSError(errno.EACCES, os.strerror(errno.EACCES))
                return orig_os_listdir(path)

            with mock.patch('os.listdir', mock_os_listdir):
                hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)

            self.assertEqual(listdir_calls, [
                part_path,
                suffix_path,
                datadir_path,
            ])
            expected = {suffix: None}
            msg = 'expected %r != %r for policy %r' % (
                expected, hashes, policy)
            self.assertEqual(hashes, expected, msg)

    def test_hash_suffix_rmdir_hsh_path_oserror(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # make an empty hsh_path to be removed
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy)
            os.makedirs(df._datadir)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            with mock.patch('os.rmdir', side_effect=OSError()):
                hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)
            expected = {
                EC_POLICY: {},
                REPL_POLICY: md5().hexdigest(),
            }[policy.policy_type]
            self.assertEqual(hashes, {suffix: expected})
            self.assertTrue(os.path.exists(df._datadir))

    def test_hash_suffix_rmdir_suffix_oserror(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # make an empty hsh_path to be removed
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy)
            os.makedirs(df._datadir)
            suffix_path = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_path)

            captured_paths = []

            def mock_rmdir(path):
                captured_paths.append(path)
                if path == suffix_path:
                    raise OSError('kaboom!')

            with mock.patch('os.rmdir', mock_rmdir):
                hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)
            expected = {
                EC_POLICY: {},
                REPL_POLICY: md5().hexdigest(),
            }[policy.policy_type]
            self.assertEqual(hashes, {suffix: expected})
            self.assertTrue(os.path.exists(suffix_path))
            self.assertEqual([
                df._datadir,
                suffix_path,
            ], captured_paths)

    # get_hashes tests - behaviors

    def test_get_hashes_creates_partition_and_pkl(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                       policy)
            self.assertEqual(hashes, {})
            part_path = os.path.join(
                self.devices, 'sda1', diskfile.get_data_dir(policy), '0')
            self.assertTrue(os.path.exists(part_path))
            hashes_file = os.path.join(part_path,
                                       diskfile.HASH_FILE)
            self.assertTrue(os.path.exists(hashes_file))

            # and double check the hashes
            new_hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)
            self.assertEqual(hashes, new_hashes)

    def test_get_hashes_new_pkl_finds_new_suffix_dirs(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(
                self.devices, self.existing_device,
                diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path,
                                       diskfile.HASH_FILE)
            # add something to find
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy, frag_index=4)
            timestamp = self.ts()
            df.delete(timestamp)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # get_hashes will find the untracked suffix dir
            self.assertFalse(os.path.exists(hashes_file))  # sanity
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            self.assertTrue(suffix in hashes)
            # ... and create a hashes pickle for it
            self.assertTrue(os.path.exists(hashes_file))

    def test_get_hashes_old_pickle_does_not_find_new_suffix_dirs(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # create a empty stale pickle
            part_path = os.path.join(
                self.devices, 'sda1', diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path,
                                       diskfile.HASH_FILE)
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            self.assertEqual(hashes, {})
            self.assertTrue(os.path.exists(hashes_file))  # sanity
            # add something to find
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                     policy=policy, frag_index=4)
            os.makedirs(df._datadir)
            filename = Timestamp(time()).internal + '.ts'
            open(os.path.join(df._datadir, filename), 'w').close()
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # but get_hashes has no reason to find it (because we didn't
            # call invalidate_hash)
            new_hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)
            self.assertEqual(new_hashes, hashes)
            # ... unless remote end asks for a recalc
            hashes = df_mgr.get_hashes(self.existing_device, '0', [suffix],
                                       policy)
            self.assertTrue(suffix in hashes)

    def test_get_hashes_does_not_rehash_known_suffix_dirs(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy, frag_index=4)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            timestamp = self.ts()
            df.delete(timestamp)
            # create the baseline hashes file
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            self.assertTrue(suffix in hashes)
            # now change the contents of the suffix w/o calling
            # invalidate_hash
            rmtree(df._datadir)
            suffix_path = os.path.dirname(df._datadir)
            self.assertTrue(os.path.exists(suffix_path))  # sanity
            new_hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)
            # ... and get_hashes is none the wiser
            self.assertEqual(new_hashes, hashes)

            # ... unless remote end asks for a recalc
            hashes = df_mgr.get_hashes(self.existing_device, '0', [suffix],
                                       policy)
            self.assertNotEqual(new_hashes, hashes)
            # and the empty suffix path is removed
            self.assertFalse(os.path.exists(suffix_path))
            # ... but is hashed as "empty"
            expected = {
                EC_POLICY: {},
                REPL_POLICY: md5().hexdigest(),
            }[policy.policy_type]
            self.assertEqual({suffix: expected}, hashes)

    def test_get_hashes_multi_file_multi_suffix(self):
        paths, suffix = find_paths_with_matching_suffixes(needed_matches=2,
                                                          needed_suffixes=3)
        matching_paths = paths.pop(suffix)
        matching_paths.sort(key=lambda path: hash_path(*path))
        other_paths = []
        for suffix, paths in paths.items():
            other_paths.append(paths[0])
            if len(other_paths) >= 2:
                break
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # first we'll make a tombstone
            df = df_mgr.get_diskfile(self.existing_device, '0',
                                     *other_paths[0], policy=policy,
                                     frag_index=4)
            timestamp = self.ts()
            df.delete(timestamp)
            tombstone_hash = md5(timestamp.internal + '.ts').hexdigest()
            tombstone_suffix = os.path.basename(os.path.dirname(df._datadir))
            # second file in another suffix has a .datafile
            df = df_mgr.get_diskfile(self.existing_device, '0',
                                     *other_paths[1], policy=policy,
                                     frag_index=5)
            timestamp = self.ts()
            with df.create() as writer:
                test_data = 'test_file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
                writer.commit(timestamp)
            datafile_name = timestamp.internal
            if policy.policy_type == EC_POLICY:
                datafile_name += '#%d' % df._frag_index
            datafile_name += '.data'
            durable_hash = md5(timestamp.internal + '.durable').hexdigest()
            datafile_suffix = os.path.basename(os.path.dirname(df._datadir))
            # in the *third* suffix - two datafiles for different hashes
            df = df_mgr.get_diskfile(self.existing_device, '0',
                                     *matching_paths[0], policy=policy,
                                     frag_index=6)
            matching_suffix = os.path.basename(os.path.dirname(df._datadir))
            timestamp = self.ts()
            with df.create() as writer:
                test_data = 'test_file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
                writer.commit(timestamp)
            # we'll keep track of file names for hash calculations
            filename = timestamp.internal
            if policy.policy_type == EC_POLICY:
                filename += '#%d' % df._frag_index
            filename += '.data'
            filenames = {
                'data': {
                    6: filename
                },
                'durable': [timestamp.internal + '.durable'],
            }
            df = df_mgr.get_diskfile(self.existing_device, '0',
                                     *matching_paths[1], policy=policy,
                                     frag_index=7)
            self.assertEqual(os.path.basename(os.path.dirname(df._datadir)),
                             matching_suffix)  # sanity
            timestamp = self.ts()
            with df.create() as writer:
                test_data = 'test_file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
                writer.commit(timestamp)
            filename = timestamp.internal
            if policy.policy_type == EC_POLICY:
                filename += '#%d' % df._frag_index
            filename += '.data'
            filenames['data'][7] = filename
            filenames['durable'].append(timestamp.internal + '.durable')
            # now make up the expected suffixes!
            if policy.policy_type == EC_POLICY:
                hasher = md5()
                for filename in filenames['durable']:
                    hasher.update(filename)
                expected = {
                    tombstone_suffix: {
                        None: tombstone_hash,
                    },
                    datafile_suffix: {
                        None: durable_hash,
                        5: self.fname_to_ts_hash(datafile_name),
                    },
                    matching_suffix: {
                        None: hasher.hexdigest(),
                        6: self.fname_to_ts_hash(filenames['data'][6]),
                        7: self.fname_to_ts_hash(filenames['data'][7]),
                    },
                }
            elif policy.policy_type == REPL_POLICY:
                hasher = md5()
                for filename in filenames['data'].values():
                    hasher.update(filename)
                expected = {
                    tombstone_suffix: tombstone_hash,
                    datafile_suffix: md5(datafile_name).hexdigest(),
                    matching_suffix: hasher.hexdigest(),
                }
            else:
                self.fail('unknown policy type %r' % policy.policy_type)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertEqual(hashes, expected)

    # get_hashes tests - error handling

    def test_get_hashes_bad_dev(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df_mgr.mount_check = True
            with mock.patch('swift.obj.diskfile.check_mount',
                            mock.MagicMock(side_effect=[False])):
                self.assertRaises(
                    DiskFileDeviceUnavailable,
                    df_mgr.get_hashes, self.existing_device, '0', ['123'],
                    policy)

    def test_get_hashes_zero_bytes_pickle(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(self.devices, self.existing_device,
                                     diskfile.get_data_dir(policy), '0')
            os.makedirs(part_path)
            # create a pre-existing zero-byte file
            open(os.path.join(part_path, diskfile.HASH_FILE), 'w').close()
            hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                       policy)
            self.assertEqual(hashes, {})

    def test_get_hashes_hash_suffix_enotdir(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # create a real suffix dir
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy, frag_index=3)
            df.delete(Timestamp(time()))
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # touch a bad suffix dir
            part_dir = os.path.join(self.devices, self.existing_device,
                                    diskfile.get_data_dir(policy), '0')
            open(os.path.join(part_dir, 'bad'), 'w').close()
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            self.assertTrue(suffix in hashes)
            self.assertFalse('bad' in hashes)

    def test_get_hashes_hash_suffix_other_oserror(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            suffix = '123'
            suffix_path = os.path.join(self.devices, self.existing_device,
                                       diskfile.get_data_dir(policy), '0',
                                       suffix)
            os.makedirs(suffix_path)
            self.assertTrue(os.path.exists(suffix_path))  # sanity
            hashes = df_mgr.get_hashes(self.existing_device, '0', [suffix],
                                       policy)
            expected = {
                EC_POLICY: {'123': {}},
                REPL_POLICY: {'123': EMPTY_ETAG},
            }[policy.policy_type]
            msg = 'expected %r != %r for policy %r' % (expected, hashes,
                                                       policy)
            self.assertEqual(hashes, expected, msg)

            # this OSError does *not* raise PathNotDir, and is allowed to leak
            # from hash_suffix into get_hashes
            mocked_os_listdir = mock.Mock(
                side_effect=OSError(errno.EACCES, os.strerror(errno.EACCES)))
            with mock.patch("os.listdir", mocked_os_listdir):
                with mock.patch('swift.obj.diskfile.logging') as mock_logging:
                    hashes = df_mgr.get_hashes('sda1', '0', [suffix], policy)
            self.assertEqual(mock_logging.method_calls,
                             [mock.call.exception('Error hashing suffix')])
            # recalc always causes a suffix to get reset to None; the listdir
            # error prevents the suffix from being rehashed
            expected = {'123': None}
            msg = 'expected %r != %r for policy %r' % (expected, hashes,
                                                       policy)
            self.assertEqual(hashes, expected, msg)

    def test_get_hashes_modified_recursive_retry(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # first create an empty pickle
            df_mgr.get_hashes(self.existing_device, '0', [], policy)
            hashes_file = os.path.join(
                self.devices, self.existing_device,
                diskfile.get_data_dir(policy), '0', diskfile.HASH_FILE)
            mtime = os.path.getmtime(hashes_file)
            non_local = {'mtime': mtime}

            calls = []

            def mock_getmtime(filename):
                t = non_local['mtime']
                if len(calls) <= 3:
                    # this will make the *next* call get a slightly
                    # newer mtime than the last
                    non_local['mtime'] += 1
                # track exactly the value for every return
                calls.append(t)
                return t
            with mock.patch('swift.obj.diskfile.getmtime',
                            mock_getmtime):
                df_mgr.get_hashes(self.existing_device, '0', ['123'],
                                  policy)

            self.assertEqual(calls, [
                mtime + 0,  # read
                mtime + 1,  # modified
                mtime + 2,  # read
                mtime + 3,  # modifed
                mtime + 4,  # read
                mtime + 4,  # not modifed
            ])


if __name__ == '__main__':
    unittest.main()
