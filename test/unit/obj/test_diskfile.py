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

import pickle
import binascii
import os
import errno
import itertools
from unittest import mock
import unittest
import email
import tempfile
import threading
import uuid
import xattr
import re
import sys
from collections import defaultdict
from random import shuffle, randint
from shutil import rmtree
from time import time
from tempfile import mkdtemp
from contextlib import closing, contextmanager
from gzip import GzipFile
import pyeclib.ec_iface

from eventlet import hubs, timeout, tpool
from swift.obj.diskfile import update_auditor_status, EUCLEAN
from test import BaseTestCase
from test.debug_logger import debug_logger
from test.unit import (mock as unit_mock, temptree, mock_check_drive,
                       patch_policies, make_timestamp_iter,
                       DEFAULT_TEST_EC_TYPE, requires_o_tmpfile_support_in_tmp,
                       encode_frag_archive_bodies, skip_if_no_xattrs)
from swift.obj import diskfile
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, Timestamp, \
    encode_timestamps, O_TMPFILE, md5 as _md5, MD5_OF_EMPTY_STRING
from swift.common import ring
from swift.common.splice import splice
from swift.common.exceptions import DiskFileNotExist, DiskFileQuarantined, \
    DiskFileDeviceUnavailable, DiskFileDeleted, DiskFileNotOpen, \
    DiskFileError, ReplicationLockTimeout, DiskFileCollision, \
    DiskFileExpired, SwiftException, DiskFileNoSpace, \
    DiskFileXattrNotSupported, PartitionLockTimeout
from swift.common.storage_policy import (
    POLICIES, get_policy_string, StoragePolicy, ECStoragePolicy, REPL_POLICY,
    EC_POLICY, PolicyError)
from test.unit.obj.common import write_diskfile


test_policies = [
    StoragePolicy(0, name='zero', is_default=True),
    ECStoragePolicy(1, name='one', is_default=False,
                    ec_type=DEFAULT_TEST_EC_TYPE,
                    ec_ndata=10, ec_nparity=4),
]


class md5(object):
    def __init__(self, s=b''):
        if not isinstance(s, bytes):
            s = s.encode('ascii')
        self.md = _md5(s, usedforsecurity=False)

    def update(self, s=b''):
        if not isinstance(s, bytes):
            s = s.encode('ascii')
        return self.md.update(s)

    @property
    def hexdigest(self):
        return self.md.hexdigest

    @property
    def digest(self):
        return self.md.digest


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
         'port': 6200},
        {'id': 1, 'device': 'sda1', 'zone': 1, 'ip': '127.0.0.1',
         'port': 6200},
        {'id': 2, 'device': 'sda1', 'zone': 2, 'ip': '127.0.0.2',
         'port': 6200},
        {'id': 3, 'device': 'sda1', 'zone': 4, 'ip': '127.0.0.3',
         'port': 6200},
        {'id': 4, 'device': 'sda1', 'zone': 5, 'ip': '127.0.0.4',
         'port': 6200},
        {'id': 5, 'device': 'sda1', 'zone': 6,
         'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6200},
        {'id': 6, 'device': 'sda1', 'zone': 7,
         'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
         'port': 6200}]
    intended_part_shift = 30
    intended_reload_time = 15
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id, intended_devs,
                          intended_part_shift),
            f)
    return ring.Ring(path, ring_name=ring_name,
                     reload_time=intended_reload_time)


def _make_datafilename(timestamp, policy, frag_index=None, durable=False):
    if frag_index is None:
        frag_index = randint(0, 9)
    filename = timestamp.internal
    if policy.policy_type == EC_POLICY:
        filename += '#%d' % int(frag_index)
        if durable:
            filename += '#d'
    filename += '.data'
    return filename


def _make_metafilename(meta_timestamp, ctype_timestamp=None):
    filename = meta_timestamp.internal
    if ctype_timestamp is not None:
        delta = meta_timestamp.raw - ctype_timestamp.raw
        filename = '%s-%x' % (filename, delta)
    filename += '.meta'
    return filename


@patch_policies
class TestDiskFileModuleMethods(unittest.TestCase):

    def setUp(self):
        skip_if_no_xattrs()
        utils.HASH_PATH_SUFFIX = b'endcap'
        utils.HASH_PATH_PREFIX = b''
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
        self.logger = debug_logger()
        self.df_mgr = diskfile.DiskFileManager(self.conf, logger=self.logger)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def _create_diskfile(self, policy):
        return self.df_mgr.get_diskfile(self.existing_device,
                                        '0', 'a', 'c', 'o',
                                        policy=policy)

    def test_relink_paths(self):
        target_dir = os.path.join(self.testdir, 'd1')
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 'f1')
        with open(target_path, 'w') as fd:
            fd.write('junk')
        new_target_path = os.path.join(self.testdir, 'd2', 'f1')
        created = diskfile.relink_paths(target_path, new_target_path)
        self.assertTrue(created)
        self.assertTrue(os.path.isfile(new_target_path))
        with open(new_target_path, 'r') as fd:
            self.assertEqual('junk', fd.read())

    def test_relink_paths_makedirs_error(self):
        target_dir = os.path.join(self.testdir, 'd1')
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 'f1')
        with open(target_path, 'w') as fd:
            fd.write('junk')
        new_target_path = os.path.join(self.testdir, 'd2', 'f1')
        with mock.patch('swift.obj.diskfile.os.makedirs',
                        side_effect=Exception('oops')):
            with self.assertRaises(Exception) as cm:
                diskfile.relink_paths(target_path, new_target_path)
            self.assertEqual('oops', str(cm.exception))
            with self.assertRaises(Exception) as cm:
                diskfile.relink_paths(target_path, new_target_path,
                                      ignore_missing=False)
            self.assertEqual('oops', str(cm.exception))

    def test_relink_paths_makedirs_race(self):
        # test two concurrent relinks of the same object hash dir with race
        # around makedirs
        target_dir = os.path.join(self.testdir, 'd1')
        # target dir exists
        os.mkdir(target_dir)
        target_path_1 = os.path.join(target_dir, 't1.data')
        target_path_2 = os.path.join(target_dir, 't2.data')
        # new target dir and files do not exist
        new_target_dir = os.path.join(self.testdir, 'd2')
        new_target_path_1 = os.path.join(new_target_dir, 't1.data')
        new_target_path_2 = os.path.join(new_target_dir, 't2.data')
        created = []

        def write_and_relink(target_path, new_target_path):
            with open(target_path, 'w') as fd:
                fd.write(target_path)
            created.append(diskfile.relink_paths(target_path, new_target_path))

        calls = []
        orig_makedirs = os.makedirs

        def mock_makedirs(path, *args):
            calls.append(path)
            if len(calls) == 1:
                # pretend another process jumps in here and relinks same dirs
                write_and_relink(target_path_2, new_target_path_2)
            return orig_makedirs(path, *args)

        with mock.patch('swift.obj.diskfile.os.makedirs', mock_makedirs):
            write_and_relink(target_path_1, new_target_path_1)

        self.assertEqual([new_target_dir, new_target_dir], calls)
        self.assertTrue(os.path.isfile(new_target_path_1))
        with open(new_target_path_1, 'r') as fd:
            self.assertEqual(target_path_1, fd.read())
        self.assertTrue(os.path.isfile(new_target_path_2))
        with open(new_target_path_2, 'r') as fd:
            self.assertEqual(target_path_2, fd.read())
        self.assertEqual([True, True], created)

    def test_relink_paths_object_dir_exists_but_not_dir(self):
        target_dir = os.path.join(self.testdir, 'd1')
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 't1.data')
        with open(target_path, 'w') as fd:
            fd.write(target_path)
        # make a file where the new object dir should be
        new_target_dir = os.path.join(self.testdir, 'd2')
        with open(new_target_dir, 'w') as fd:
            fd.write(new_target_dir)
        new_target_path = os.path.join(new_target_dir, 't1.data')

        with self.assertRaises(OSError) as cm:
            diskfile.relink_paths(target_path, new_target_path)
        self.assertEqual(errno.ENOTDIR, cm.exception.errno)

        # make a symlink to target where the new object dir should be
        os.unlink(new_target_dir)
        os.symlink(target_path, new_target_dir)
        with self.assertRaises(OSError) as cm:
            diskfile.relink_paths(target_path, new_target_path)
        self.assertEqual(errno.ENOTDIR, cm.exception.errno)

    def test_relink_paths_os_link_error(self):
        # check relink_paths raises exception from os.link
        target_dir = os.path.join(self.testdir, 'd1')
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 'f1')
        with open(target_path, 'w') as fd:
            fd.write('junk')
        new_target_path = os.path.join(self.testdir, 'd2', 'f1')
        with mock.patch('swift.obj.diskfile.os.link',
                        side_effect=OSError(errno.EPERM, 'nope')):
            with self.assertRaises(Exception) as cm:
                diskfile.relink_paths(target_path, new_target_path)
        self.assertEqual(errno.EPERM, cm.exception.errno)

    def test_relink_paths_target_path_does_not_exist(self):
        # check relink_paths does not raise exception
        target_dir = os.path.join(self.testdir, 'd1')
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 'f1')
        new_target_path = os.path.join(self.testdir, 'd2', 'f1')
        created = diskfile.relink_paths(target_path, new_target_path)
        self.assertFalse(os.path.exists(target_path))
        self.assertFalse(os.path.exists(new_target_path))
        self.assertFalse(created)
        with self.assertRaises(OSError) as cm:
            diskfile.relink_paths(target_path, new_target_path,
                                  ignore_missing=False)
        self.assertEqual(errno.ENOENT, cm.exception.errno)
        self.assertFalse(os.path.exists(target_path))
        self.assertFalse(os.path.exists(new_target_path))

    def test_relink_paths_os_link_race(self):
        # test two concurrent relinks of the same object hash dir with race
        # around os.link
        target_dir = os.path.join(self.testdir, 'd1')
        # target dir exists
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 't1.data')
        # new target dir and file do not exist
        new_target_dir = os.path.join(self.testdir, 'd2')
        new_target_path = os.path.join(new_target_dir, 't1.data')
        created = []

        def write_and_relink(target_path, new_target_path):
            with open(target_path, 'w') as fd:
                fd.write(target_path)
            created.append(diskfile.relink_paths(target_path, new_target_path))

        calls = []
        orig_link = os.link

        def mock_link(path, new_path):
            calls.append((path, new_path))
            if len(calls) == 1:
                # pretend another process jumps in here and links same files
                write_and_relink(target_path, new_target_path)
            return orig_link(path, new_path)

        with mock.patch('swift.obj.diskfile.os.link', mock_link):
            write_and_relink(target_path, new_target_path)

        self.assertEqual([(target_path, new_target_path)] * 2, calls)
        self.assertTrue(os.path.isfile(new_target_path))
        with open(new_target_path, 'r') as fd:
            self.assertEqual(target_path, fd.read())
        with open(target_path, 'r') as fd:
            self.assertEqual(target_path, fd.read())
        self.assertEqual([True, False], created)

    def test_relink_paths_different_file_exists(self):
        # check for an exception if a hard link cannot be made because a
        # different file already exists at new_target_path
        target_dir = os.path.join(self.testdir, 'd1')
        # target dir and file exists
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 't1.data')
        with open(target_path, 'w') as fd:
            fd.write(target_path)
        # new target dir and different file exist
        new_target_dir = os.path.join(self.testdir, 'd2')
        os.mkdir(new_target_dir)
        new_target_path = os.path.join(new_target_dir, 't1.data')
        with open(new_target_path, 'w') as fd:
            fd.write(new_target_path)

        with self.assertRaises(OSError) as cm:
            diskfile.relink_paths(target_path, new_target_path)

        self.assertEqual(errno.EEXIST, cm.exception.errno)
        # check nothing got deleted...
        self.assertTrue(os.path.isfile(target_path))
        with open(target_path, 'r') as fd:
            self.assertEqual(target_path, fd.read())
        self.assertTrue(os.path.isfile(new_target_path))
        with open(new_target_path, 'r') as fd:
            self.assertEqual(new_target_path, fd.read())

    def test_relink_paths_same_file_exists(self):
        # check for no exception if a hard link cannot be made because a link
        # to the same file already exists at the path
        target_dir = os.path.join(self.testdir, 'd1')
        # target dir and file exists
        os.mkdir(target_dir)
        target_path = os.path.join(target_dir, 't1.data')
        with open(target_path, 'w') as fd:
            fd.write(target_path)
        # new target dir and link to same file exist
        new_target_dir = os.path.join(self.testdir, 'd2')
        os.mkdir(new_target_dir)
        new_target_path = os.path.join(new_target_dir, 't1.data')
        os.link(target_path, new_target_path)
        with open(new_target_path, 'r') as fd:
            self.assertEqual(target_path, fd.read())  # sanity check

        # existing link checks ok
        created = diskfile.relink_paths(target_path, new_target_path)
        with open(new_target_path, 'r') as fd:
            self.assertEqual(target_path, fd.read())  # sanity check
        self.assertFalse(created)

        # now pretend there is an error when checking that the link already
        # exists - expect the EEXIST exception to be raised
        orig_stat = os.stat

        def mocked_stat(path):
            if path == new_target_path:
                raise OSError(errno.EPERM, 'cannot be sure link exists :(')
            return orig_stat(path)

        with mock.patch('swift.obj.diskfile.os.stat', mocked_stat):
            with self.assertRaises(OSError) as cm:
                diskfile.relink_paths(target_path, new_target_path)
        self.assertEqual(errno.EEXIST, cm.exception.errno, str(cm.exception))
        with open(new_target_path, 'r') as fd:
            self.assertEqual(target_path, fd.read())  # sanity check

        # ...unless while checking for an existing link the target file is
        # found to no longer exists, which is ok
        def mocked_stat(path):
            if path == target_path:
                raise OSError(errno.ENOENT, 'target longer here :)')
            return orig_stat(path)

        with mock.patch('swift.obj.diskfile.os.stat', mocked_stat):
            created = diskfile.relink_paths(target_path, new_target_path)
        with open(new_target_path, 'r') as fd:
            self.assertEqual(target_path, fd.read())  # sanity check
        self.assertFalse(created)

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
        self.assertIsNone(diskfile.extract_policy(pn))

        # malformed path
        self.assertIsNone(diskfile.extract_policy(''))
        bad_path = '/srv/node/sda1/objects-t/1/abc/def/1234.data'
        self.assertIsNone(diskfile.extract_policy(bad_path))
        pn = 'XXXX/0/606/1984527ed42b6ef6247c78606/1401379842.14643.data'
        self.assertIsNone(diskfile.extract_policy(pn))
        bad_path = '/srv/node/sda1/foo-1/1/abc/def/1234.data'
        self.assertIsNone(diskfile.extract_policy(bad_path))
        bad_path = '/srv/node/sda1/obj1/1/abc/def/1234.data'
        self.assertIsNone(diskfile.extract_policy(bad_path))

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
        self.assertEqual(diskfile.get_data_dir(POLICIES[0]),
                         diskfile.DATADIR_BASE)
        self.assertEqual(diskfile.get_data_dir(POLICIES[1]),
                         diskfile.DATADIR_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_data_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_data_dir, 99)

    def test_get_async_dir(self):
        self.assertEqual(diskfile.get_async_dir(POLICIES[0]),
                         diskfile.ASYNCDIR_BASE)
        self.assertEqual(diskfile.get_async_dir(POLICIES[1]),
                         diskfile.ASYNCDIR_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_async_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_async_dir, 99)

    def test_get_tmp_dir(self):
        self.assertEqual(diskfile.get_tmp_dir(POLICIES[0]),
                         diskfile.TMP_BASE)
        self.assertEqual(diskfile.get_tmp_dir(POLICIES[1]),
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
            os.makedirs(tmp_path)
            # now create a async update
            self.df_mgr.pickle_async_update(*pickle_args)
            # check tempdir
            self.assertTrue(os.path.isdir(tmp_path))

    def test_get_part_path(self):
        # partition passed as 'str'
        part_dir = diskfile.get_part_path('/srv/node/sda1', POLICIES[0], '123')
        exp_dir = '/srv/node/sda1/objects/123'
        self.assertEqual(part_dir, exp_dir)

        # partition passed as 'int'
        part_dir = diskfile.get_part_path('/srv/node/sdb5', POLICIES[1], 123)
        exp_dir = '/srv/node/sdb5/objects-1/123'
        self.assertEqual(part_dir, exp_dir)

    def test_can_read_old_meta(self):
        # outputs taken from `xattr -l <diskfile>`
        cases = {
            'python_2.7.18_swift_2.13_replicated': '''
0000   80 02 7D 71 01 28 55 0E 43 6F 6E 74 65 6E 74 2D    ..}q.(U.Content-
0010   4C 65 6E 67 74 68 71 02 55 02 31 33 55 04 6E 61    Lengthq.U.13U.na
0020   6D 65 71 03 55 12 2F 41 55 54 48 5F 74 65 73 74    meq.U./AUTH_test
0030   2F E2 98 83 2F E2 98 83 71 04 55 13 58 2D 4F 62    /.../...q.U.X-Ob
0040   6A 65 63 74 2D 4D 65 74 61 2D 4D 74 69 6D 65 55    ject-Meta-MtimeU
0050   11 31 36 38 32 39 35 39 38 37 34 2E 37 35 36 32    .1682959874.7562
0060   30 35 71 05 55 04 45 54 61 67 71 06 55 20 36 62    05q.U.ETagq.U 6b
0070   37 64 39 61 31 63 35 64 31 36 37 63 63 35 30 30    7d9a1c5d167cc500
0080   33 37 66 32 39 66 32 39 30 62 62 33 37 35 71 07    37f29f290bb375q.
0090   55 0B 58 2D 54 69 6D 65 73 74 61 6D 70 71 08 55    U.X-Timestampq.U
00A0   10 31 36 38 32 39 36 32 36 35 31 2E 39 37 34 39    .1682962651.9749
00B0   34 55 11 58 2D 4F 62 6A 65 63 74 2D 4D 65 74 61    4U.X-Object-Meta
00C0   2D E2 98 83 55 03 E2 98 83 71 09 55 0C 43 6F 6E    -...U....q.U.Con
00D0   74 65 6E 74 2D 54 79 70 65 71 0A 55 18 61 70 70    tent-Typeq.U.app
00E0   6C 69 63 61 74 69 6F 6E 2F 6F 63 74 65 74 2D 73    lication/octet-s
00F0   74 72 65 61 6D 71 0B 75 2E                         treamq.u.
            ''',
            'python_2.7.18_swift_2.13_ec': '''
0000   80 02 7D 71 01 28 55 0E 43 6F 6E 74 65 6E 74 2D    ..}q.(U.Content-
0010   4C 65 6E 67 74 68 71 02 55 02 38 34 55 04 6E 61    Lengthq.U.84U.na
0020   6D 65 71 03 55 12 2F 41 55 54 48 5F 74 65 73 74    meq.U./AUTH_test
0030   2F E2 98 83 2F E2 98 83 71 04 58 1E 00 00 00 58    /.../...q.X....X
0040   2D 4F 62 6A 65 63 74 2D 53 79 73 6D 65 74 61 2D    -Object-Sysmeta-
0050   45 63 2D 46 72 61 67 2D 49 6E 64 65 78 71 05 55    Ec-Frag-Indexq.U
0060   01 35 55 13 58 2D 4F 62 6A 65 63 74 2D 4D 65 74    .5U.X-Object-Met
0070   61 2D 4D 74 69 6D 65 55 11 31 36 38 32 39 35 39    a-MtimeU.1682959
0080   38 37 34 2E 37 35 36 32 30 35 71 06 58 22 00 00    874.756205q.X"..
0090   00 58 2D 4F 62 6A 65 63 74 2D 53 79 73 6D 65 74    .X-Object-Sysmet
00A0   61 2D 45 63 2D 43 6F 6E 74 65 6E 74 2D 4C 65 6E    a-Ec-Content-Len
00B0   67 74 68 71 07 55 02 31 33 71 08 58 18 00 00 00    gthq.U.13q.X....
00C0   58 2D 4F 62 6A 65 63 74 2D 53 79 73 6D 65 74 61    X-Object-Sysmeta
00D0   2D 45 63 2D 45 74 61 67 71 09 55 20 36 62 37 64    -Ec-Etagq.U 6b7d
00E0   39 61 31 63 35 64 31 36 37 63 63 35 30 30 33 37    9a1c5d167cc50037
00F0   66 32 39 66 32 39 30 62 62 33 37 35 71 0A 55 04    f29f290bb375q.U.
0100   45 54 61 67 71 0B 55 20 65 32 66 64 34 33 30 65    ETagq.U e2fd430e
0110   61 66 37 32 32 33 63 32 35 30 33 63 34 65 38 33    af7223c2503c4e83
0120   30 31 63 66 66 33 37 63 71 0C 55 0B 58 2D 54 69    01cff37cq.U.X-Ti
0130   6D 65 73 74 61 6D 70 71 0D 55 10 31 36 38 32 39    mestampq.U.16829
0140   36 32 32 36 32 2E 31 36 31 39 39 55 11 58 2D 4F    62262.16199U.X-O
0150   62 6A 65 63 74 2D 4D 65 74 61 2D E2 98 83 55 03    bject-Meta-...U.
0160   E2 98 83 71 0E 58 1A 00 00 00 58 2D 4F 62 6A 65    ...q.X....X-Obje
0170   63 74 2D 53 79 73 6D 65 74 61 2D 45 63 2D 53 63    ct-Sysmeta-Ec-Sc
0180   68 65 6D 65 71 0F 55 1A 6C 69 62 65 72 61 73 75    hemeq.U.liberasu
0190   72 65 63 6F 64 65 5F 72 73 5F 76 61 6E 64 20 34    recode_rs_vand 4
01A0   2B 32 71 10 55 0C 43 6F 6E 74 65 6E 74 2D 54 79    +2q.U.Content-Ty
01B0   70 65 71 11 55 18 61 70 70 6C 69 63 61 74 69 6F    peq.U.applicatio
01C0   6E 2F 6F 63 74 65 74 2D 73 74 72 65 61 6D 71 12    n/octet-streamq.
01D0   58 20 00 00 00 58 2D 4F 62 6A 65 63 74 2D 53 79    X ...X-Object-Sy
01E0   73 6D 65 74 61 2D 45 63 2D 53 65 67 6D 65 6E 74    smeta-Ec-Segment
01F0   2D 53 69 7A 65 71 13 55 07 31 30 34 38 35 37 36    -Sizeq.U.1048576
0200   71 14 75 2E                                        q.u.
            ''',
            'python_2.7.18_swift_2.23_replicated': '''
0000   80 02 7D 71 01 28 55 0E 43 6F 6E 74 65 6E 74 2D    ..}q.(U.Content-
0010   4C 65 6E 67 74 68 71 02 55 02 31 33 71 03 55 04    Lengthq.U.13q.U.
0020   6E 61 6D 65 71 04 55 12 2F 41 55 54 48 5F 74 65    nameq.U./AUTH_te
0030   73 74 2F E2 98 83 2F E2 98 83 71 05 55 0C 43 6F    st/.../...q.U.Co
0040   6E 74 65 6E 74 2D 54 79 70 65 71 06 55 18 61 70    ntent-Typeq.U.ap
0050   70 6C 69 63 61 74 69 6F 6E 2F 6F 63 74 65 74 2D    plication/octet-
0060   73 74 72 65 61 6D 71 07 55 04 45 54 61 67 71 08    streamq.U.ETagq.
0070   55 20 36 62 37 64 39 61 31 63 35 64 31 36 37 63    U 6b7d9a1c5d167c
0080   63 35 30 30 33 37 66 32 39 66 32 39 30 62 62 33    c50037f29f290bb3
0090   37 35 71 09 55 0B 58 2D 54 69 6D 65 73 74 61 6D    75q.U.X-Timestam
00A0   70 71 0A 55 10 31 36 38 32 39 36 33 32 30 39 2E    pq.U.1682963209.
00B0   38 32 32 37 32 71 0B 55 11 58 2D 4F 62 6A 65 63    82272q.U.X-Objec
00C0   74 2D 4D 65 74 61 2D E2 98 83 71 0C 55 03 E2 98    t-Meta-...q.U...
00D0   83 71 0D 55 13 58 2D 4F 62 6A 65 63 74 2D 4D 65    .q.U.X-Object-Me
00E0   74 61 2D 4D 74 69 6D 65 71 0E 55 11 31 36 38 32    ta-Mtimeq.U.1682
00F0   39 35 39 38 37 34 2E 37 35 36 32 30 35 71 0F 75    959874.756205q.u
0100   2E                                                 .
            ''',
            'python_3.10.6_swift_2.23_replicated': '''
0000   80 02 7D 71 00 28 63 5F 63 6F 64 65 63 73 0A 65    ..}q.(c_codecs.e
0010   6E 63 6F 64 65 0A 71 01 58 0B 00 00 00 58 2D 54    ncode.q.X....X-T
0020   69 6D 65 73 74 61 6D 70 71 02 58 06 00 00 00 6C    imestampq.X....l
0030   61 74 69 6E 31 71 03 86 71 04 52 71 05 68 01 58    atin1q..q.Rq.h.X
0040   10 00 00 00 31 36 38 32 39 36 33 30 31 37 2E 31    ....1682963017.1
0050   30 34 37 32 71 06 68 03 86 71 07 52 71 08 68 01    0472q.h..q.Rq.h.
0060   58 0C 00 00 00 43 6F 6E 74 65 6E 74 2D 54 79 70    X....Content-Typ
0070   65 71 09 68 03 86 71 0A 52 71 0B 68 01 58 18 00    eq.h..q.Rq.h.X..
0080   00 00 61 70 70 6C 69 63 61 74 69 6F 6E 2F 6F 63    ..application/oc
0090   74 65 74 2D 73 74 72 65 61 6D 71 0C 68 03 86 71    tet-streamq.h..q
00A0   0D 52 71 0E 68 01 58 0E 00 00 00 43 6F 6E 74 65    .Rq.h.X....Conte
00B0   6E 74 2D 4C 65 6E 67 74 68 71 0F 68 03 86 71 10    nt-Lengthq.h..q.
00C0   52 71 11 68 01 58 02 00 00 00 31 33 71 12 68 03    Rq.h.X....13q.h.
00D0   86 71 13 52 71 14 68 01 58 04 00 00 00 45 54 61    .q.Rq.h.X....ETa
00E0   67 71 15 68 03 86 71 16 52 71 17 68 01 58 20 00    gq.h..q.Rq.h.X .
00F0   00 00 36 62 37 64 39 61 31 63 35 64 31 36 37 63    ..6b7d9a1c5d167c
0100   63 35 30 30 33 37 66 32 39 66 32 39 30 62 62 33    c50037f29f290bb3
0110   37 35 71 18 68 03 86 71 19 52 71 1A 68 01 58 13    75q.h..q.Rq.h.X.
0120   00 00 00 58 2D 4F 62 6A 65 63 74 2D 4D 65 74 61    ...X-Object-Meta
0130   2D 4D 74 69 6D 65 71 1B 68 03 86 71 1C 52 71 1D    -Mtimeq.h..q.Rq.
0140   68 01 58 11 00 00 00 31 36 38 32 39 35 39 38 37    h.X....168295987
0150   34 2E 37 35 36 32 30 35 71 1E 68 03 86 71 1F 52    4.756205q.h..q.R
0160   71 20 68 01 58 1A 00 00 00 58 2D 4F 62 6A 65 63    q h.X....X-Objec
0170   74 2D 4D 65 74 61 2D C3 83 C2 A2 C3 82 C2 98 C3    t-Meta-.........
0180   82 C2 83 71 21 68 03 86 71 22 52 71 23 68 01 58    ...q!h..q"Rq#h.X
0190   0C 00 00 00 C3 83 C2 A2 C3 82 C2 98 C3 82 C2 83    ................
01A0   71 24 68 03 86 71 25 52 71 26 68 01 58 04 00 00    q$h..q%Rq&h.X...
01B0   00 6E 61 6D 65 71 27 68 03 86 71 28 52 71 29 68    .nameq'h..q(Rq)h
01C0   01 58 18 00 00 00 2F 41 55 54 48 5F 74 65 73 74    .X..../AUTH_test
01D0   2F C3 A2 C2 98 C2 83 2F C3 A2 C2 98 C2 83 71 2A    /....../......q*
01E0   68 03 86 71 2B 52 71 2C 75 2E                      h..q+Rq,u.
            ''',
            'python_2.7.18_swift_2.23_ec': '''
0000   80 02 7D 71 01 28 55 0E 43 6F 6E 74 65 6E 74 2D    ..}q.(U.Content-
0010   4C 65 6E 67 74 68 71 02 55 02 38 34 71 03 55 04    Lengthq.U.84q.U.
0020   6E 61 6D 65 71 04 55 12 2F 41 55 54 48 5F 74 65    nameq.U./AUTH_te
0030   73 74 2F E2 98 83 2F E2 98 83 71 05 55 1E 58 2D    st/.../...q.U.X-
0040   4F 62 6A 65 63 74 2D 53 79 73 6D 65 74 61 2D 45    Object-Sysmeta-E
0050   63 2D 46 72 61 67 2D 49 6E 64 65 78 55 01 35 55    c-Frag-IndexU.5U
0060   0C 43 6F 6E 74 65 6E 74 2D 54 79 70 65 71 06 55    .Content-Typeq.U
0070   18 61 70 70 6C 69 63 61 74 69 6F 6E 2F 6F 63 74    .application/oct
0080   65 74 2D 73 74 72 65 61 6D 71 07 55 22 58 2D 4F    et-streamq.U"X-O
0090   62 6A 65 63 74 2D 53 79 73 6D 65 74 61 2D 45 63    bject-Sysmeta-Ec
00A0   2D 43 6F 6E 74 65 6E 74 2D 4C 65 6E 67 74 68 55    -Content-LengthU
00B0   02 31 33 71 08 55 18 58 2D 4F 62 6A 65 63 74 2D    .13q.U.X-Object-
00C0   53 79 73 6D 65 74 61 2D 45 63 2D 45 74 61 67 55    Sysmeta-Ec-EtagU
00D0   20 36 62 37 64 39 61 31 63 35 64 31 36 37 63 63     6b7d9a1c5d167cc
00E0   35 30 30 33 37 66 32 39 66 32 39 30 62 62 33 37    50037f29f290bb37
00F0   35 71 09 55 04 45 54 61 67 71 0A 55 20 65 32 66    5q.U.ETagq.U e2f
0100   64 34 33 30 65 61 66 37 32 32 33 63 32 35 30 33    d430eaf7223c2503
0110   63 34 65 38 33 30 31 63 66 66 33 37 63 71 0B 55    c4e8301cff37cq.U
0120   0B 58 2D 54 69 6D 65 73 74 61 6D 70 71 0C 55 10    .X-Timestampq.U.
0130   31 36 38 32 39 36 33 31 33 30 2E 33 35 39 38 36    1682963130.35986
0140   71 0D 55 11 58 2D 4F 62 6A 65 63 74 2D 4D 65 74    q.U.X-Object-Met
0150   61 2D E2 98 83 71 0E 55 03 E2 98 83 71 0F 55 1A    a-...q.U....q.U.
0160   58 2D 4F 62 6A 65 63 74 2D 53 79 73 6D 65 74 61    X-Object-Sysmeta
0170   2D 45 63 2D 53 63 68 65 6D 65 55 1A 6C 69 62 65    -Ec-SchemeU.libe
0180   72 61 73 75 72 65 63 6F 64 65 5F 72 73 5F 76 61    rasurecode_rs_va
0190   6E 64 20 34 2B 32 71 10 55 13 58 2D 4F 62 6A 65    nd 4+2q.U.X-Obje
01A0   63 74 2D 4D 65 74 61 2D 4D 74 69 6D 65 71 11 55    ct-Meta-Mtimeq.U
01B0   11 31 36 38 32 39 35 39 38 37 34 2E 37 35 36 32    .1682959874.7562
01C0   30 35 71 12 55 20 58 2D 4F 62 6A 65 63 74 2D 53    05q.U X-Object-S
01D0   79 73 6D 65 74 61 2D 45 63 2D 53 65 67 6D 65 6E    ysmeta-Ec-Segmen
01E0   74 2D 53 69 7A 65 55 07 31 30 34 38 35 37 36 71    t-SizeU.1048576q
01F0   13 75 2E                                           .u.
            ''',
            'python_3.10.6_swift_2.23_ec': '''
0000   80 02 7D 71 00 28 63 5F 63 6F 64 65 63 73 0A 65    ..}q.(c_codecs.e
0010   6E 63 6F 64 65 0A 71 01 58 0B 00 00 00 58 2D 54    ncode.q.X....X-T
0020   69 6D 65 73 74 61 6D 70 71 02 58 06 00 00 00 6C    imestampq.X....l
0030   61 74 69 6E 31 71 03 86 71 04 52 71 05 68 01 58    atin1q..q.Rq.h.X
0040   10 00 00 00 31 36 38 32 39 36 32 39 35 35 2E 33    ....1682962955.3
0050   37 35 34 36 71 06 68 03 86 71 07 52 71 08 68 01    7546q.h..q.Rq.h.
0060   58 0C 00 00 00 43 6F 6E 74 65 6E 74 2D 54 79 70    X....Content-Typ
0070   65 71 09 68 03 86 71 0A 52 71 0B 68 01 58 18 00    eq.h..q.Rq.h.X..
0080   00 00 61 70 70 6C 69 63 61 74 69 6F 6E 2F 6F 63    ..application/oc
0090   74 65 74 2D 73 74 72 65 61 6D 71 0C 68 03 86 71    tet-streamq.h..q
00A0   0D 52 71 0E 68 01 58 0E 00 00 00 43 6F 6E 74 65    .Rq.h.X....Conte
00B0   6E 74 2D 4C 65 6E 67 74 68 71 0F 68 03 86 71 10    nt-Lengthq.h..q.
00C0   52 71 11 68 01 58 02 00 00 00 38 34 71 12 68 03    Rq.h.X....84q.h.
00D0   86 71 13 52 71 14 68 01 58 04 00 00 00 45 54 61    .q.Rq.h.X....ETa
00E0   67 71 15 68 03 86 71 16 52 71 17 68 01 58 20 00    gq.h..q.Rq.h.X .
00F0   00 00 65 32 66 64 34 33 30 65 61 66 37 32 32 33    ..e2fd430eaf7223
0100   63 32 35 30 33 63 34 65 38 33 30 31 63 66 66 33    c2503c4e8301cff3
0110   37 63 71 18 68 03 86 71 19 52 71 1A 68 01 58 13    7cq.h..q.Rq.h.X.
0120   00 00 00 58 2D 4F 62 6A 65 63 74 2D 4D 65 74 61    ...X-Object-Meta
0130   2D 4D 74 69 6D 65 71 1B 68 03 86 71 1C 52 71 1D    -Mtimeq.h..q.Rq.
0140   68 01 58 11 00 00 00 31 36 38 32 39 35 39 38 37    h.X....168295987
0150   34 2E 37 35 36 32 30 35 71 1E 68 03 86 71 1F 52    4.756205q.h..q.R
0160   71 20 68 01 58 1A 00 00 00 58 2D 4F 62 6A 65 63    q h.X....X-Objec
0170   74 2D 4D 65 74 61 2D C3 83 C2 A2 C3 82 C2 98 C3    t-Meta-.........
0180   82 C2 83 71 21 68 03 86 71 22 52 71 23 68 01 58    ...q!h..q"Rq#h.X
0190   0C 00 00 00 C3 83 C2 A2 C3 82 C2 98 C3 82 C2 83    ................
01A0   71 24 68 03 86 71 25 52 71 26 68 01 58 18 00 00    q$h..q%Rq&h.X...
01B0   00 58 2D 4F 62 6A 65 63 74 2D 53 79 73 6D 65 74    .X-Object-Sysmet
01C0   61 2D 45 63 2D 45 74 61 67 71 27 68 03 86 71 28    a-Ec-Etagq'h..q(
01D0   52 71 29 68 01 58 20 00 00 00 36 62 37 64 39 61    Rq)h.X ...6b7d9a
01E0   31 63 35 64 31 36 37 63 63 35 30 30 33 37 66 32    1c5d167cc50037f2
01F0   39 66 32 39 30 62 62 33 37 35 71 2A 68 03 86 71    9f290bb375q*h..q
0200   2B 52 71 2C 68 01 58 22 00 00 00 58 2D 4F 62 6A    +Rq,h.X"...X-Obj
0210   65 63 74 2D 53 79 73 6D 65 74 61 2D 45 63 2D 43    ect-Sysmeta-Ec-C
0220   6F 6E 74 65 6E 74 2D 4C 65 6E 67 74 68 71 2D 68    ontent-Lengthq-h
0230   03 86 71 2E 52 71 2F 68 01 58 02 00 00 00 31 33    ..q.Rq/h.X....13
0240   71 30 68 03 86 71 31 52 71 32 68 01 58 1E 00 00    q0h..q1Rq2h.X...
0250   00 58 2D 4F 62 6A 65 63 74 2D 53 79 73 6D 65 74    .X-Object-Sysmet
0260   61 2D 45 63 2D 46 72 61 67 2D 49 6E 64 65 78 71    a-Ec-Frag-Indexq
0270   33 68 03 86 71 34 52 71 35 68 01 58 01 00 00 00    3h..q4Rq5h.X....
0280   35 71 36 68 03 86 71 37 52 71 38 68 01 58 1A 00    5q6h..q7Rq8h.X..
0290   00 00 58 2D 4F 62 6A 65 63 74 2D 53 79 73 6D 65    ..X-Object-Sysme
02A0   74 61 2D 45 63 2D 53 63 68 65 6D 65 71 39 68 03    ta-Ec-Schemeq9h.
02B0   86 71 3A 52 71 3B 68 01 58 1A 00 00 00 6C 69 62    .q:Rq;h.X....lib
02C0   65 72 61 73 75 72 65 63 6F 64 65 5F 72 73 5F 76    erasurecode_rs_v
02D0   61 6E 64 20 34 2B 32 71 3C 68 03 86 71 3D 52 71    and 4+2q<h..q=Rq
02E0   3E 68 01 58 20 00 00 00 58 2D 4F 62 6A 65 63 74    >h.X ...X-Object
02F0   2D 53 79 73 6D 65 74 61 2D 45 63 2D 53 65 67 6D    -Sysmeta-Ec-Segm
0300   65 6E 74 2D 53 69 7A 65 71 3F 68 03 86 71 40 52    ent-Sizeq?h..q@R
0310   71 41 68 01 58 07 00 00 00 31 30 34 38 35 37 36    qAh.X....1048576
0320   71 42 68 03 86 71 43 52 71 44 68 01 58 04 00 00    qBh..qCRqDh.X...
0330   00 6E 61 6D 65 71 45 68 03 86 71 46 52 71 47 68    .nameqEh..qFRqGh
0340   01 58 18 00 00 00 2F 41 55 54 48 5F 74 65 73 74    .X..../AUTH_test
0350   2F C3 A2 C2 98 C2 83 2F C3 A2 C2 98 C2 83 71 48    /....../......qH
0360   68 03 86 71 49 52 71 4A 75 2E                      h..qIRqJu.
            ''',
            'python3.8.10_swift_2.31.1_replicated': '''
0000   80 02 7D 71 00 28 63 5F 63 6F 64 65 63 73 0A 65    ..}q.(c_codecs.e
0010   6E 63 6F 64 65 0A 71 01 58 0B 00 00 00 58 2D 54    ncode.q.X....X-T
0020   69 6D 65 73 74 61 6D 70 71 02 58 06 00 00 00 6C    imestampq.X....l
0030   61 74 69 6E 31 71 03 86 71 04 52 71 05 68 01 58    atin1q..q.Rq.h.X
0040   10 00 00 00 31 36 38 33 30 36 35 34 37 38 2E 32    ....1683065478.2
0050   35 30 30 34 71 06 68 03 86 71 07 52 71 08 68 01    5004q.h..q.Rq.h.
0060   58 0C 00 00 00 43 6F 6E 74 65 6E 74 2D 54 79 70    X....Content-Typ
0070   65 71 09 68 03 86 71 0A 52 71 0B 68 01 58 18 00    eq.h..q.Rq.h.X..
0080   00 00 61 70 70 6C 69 63 61 74 69 6F 6E 2F 6F 63    ..application/oc
0090   74 65 74 2D 73 74 72 65 61 6D 71 0C 68 03 86 71    tet-streamq.h..q
00A0   0D 52 71 0E 68 01 58 0E 00 00 00 43 6F 6E 74 65    .Rq.h.X....Conte
00B0   6E 74 2D 4C 65 6E 67 74 68 71 0F 68 03 86 71 10    nt-Lengthq.h..q.
00C0   52 71 11 68 01 58 01 00 00 00 38 71 12 68 03 86    Rq.h.X....8q.h..
00D0   71 13 52 71 14 68 01 58 04 00 00 00 45 54 61 67    q.Rq.h.X....ETag
00E0   71 15 68 03 86 71 16 52 71 17 68 01 58 20 00 00    q.h..q.Rq.h.X ..
00F0   00 37 30 63 31 64 62 35 36 66 33 30 31 63 39 65    .70c1db56f301c9e
0100   33 33 37 62 30 30 39 39 62 64 34 31 37 34 62 32    337b0099bd4174b2
0110   38 71 18 68 03 86 71 19 52 71 1A 68 01 58 13 00    8q.h..q.Rq.h.X..
0120   00 00 58 2D 4F 62 6A 65 63 74 2D 4D 65 74 61 2D    ..X-Object-Meta-
0130   4D 74 69 6D 65 71 1B 68 03 86 71 1C 52 71 1D 68    Mtimeq.h..q.Rq.h
0140   01 58 11 00 00 00 31 36 38 33 30 36 34 39 33 38    .X....1683064938
0150   2E 36 39 39 30 32 37 71 1E 68 03 86 71 1F 52 71    .699027q.h..q.Rq
0160   20 68 01 58 1A 00 00 00 58 2D 4F 62 6A 65 63 74     h.X....X-Object
0170   2D 4D 65 74 61 2D C3 83 C2 A2 C3 82 C2 98 C3 82    -Meta-..........
0180   C2 83 71 21 68 03 86 71 22 52 71 23 68 01 58 0C    ..q!h..q"Rq#h.X.
0190   00 00 00 C3 83 C2 A2 C3 82 C2 98 C3 82 C2 83 71    ...............q
01A0   24 68 03 86 71 25 52 71 26 68 01 58 04 00 00 00    $h..q%Rq&h.X....
01B0   6E 61 6D 65 71 27 68 03 86 71 28 52 71 29 68 01    nameq'h..q(Rq)h.
01C0   58 18 00 00 00 2F 41 55 54 48 5F 74 65 73 74 2F    X..../AUTH_test/
01D0   C3 A2 C2 98 C2 83 2F C3 A2 C2 98 C2 83 71 2A 68    ....../......q*h
01E0   03 86 71 2B 52 71 2C 75 2E                         ..q+Rq,u.
            ''',
        }

        def raw_xattr(output):
            return binascii.unhexlify(''.join(
                line[7:55] for line in output.split('\n')
            ).replace(' ', ''))

        path = os.path.join(self.testdir, str(uuid.uuid4()))
        for case, xattr_output in cases.items():
            try:
                to_write = raw_xattr(xattr_output)
                with open(path, 'wb') as fp:
                    xattr.setxattr(
                        fp.fileno(), 'user.swift.metadata', to_write)
                with open(path, 'rb') as fd:
                    actual = diskfile.read_metadata(fd)
                # name should come out as native strings
                self.assertEqual(actual['name'], '/AUTH_test/\u2603/\u2603')
                # other meta will be WSGI strings, though
                self.assertEqual(
                    actual['X-Object-Meta-\xe2\x98\x83'], '\xe2\x98\x83')
            except Exception:
                print('Failure in %s' % case, file=sys.stderr)
                raise

    def test_write_read_metadata(self):
        path = os.path.join(self.testdir, str(uuid.uuid4()))
        metadata = {'name': '/a/c/o',
                    'Content-Length': 99,
                    u'X-Object-Sysmeta-Ec-Frag-Index': 4,
                    u'X-Object-Meta-Strange': u'should be bytes',
                    b'X-Object-Meta-x\xff': b'not utf8 \xff',
                    u'X-Object-Meta-y\xe8': u'not ascii \xe8'}
        as_bytes = {b'name': b'/a/c/o',
                    b'Content-Length': 99,
                    b'X-Object-Sysmeta-Ec-Frag-Index': 4,
                    b'X-Object-Meta-Strange': b'should be bytes',
                    b'X-Object-Meta-x\xff': b'not utf8 \xff',
                    b'X-Object-Meta-y\xc3\xa8': b'not ascii \xc3\xa8'}
        as_native = dict((k.decode('utf-8', 'surrogateescape'),
                          v if isinstance(v, int) else
                          v.decode('utf-8', 'surrogateescape'))
                         for k, v in as_bytes.items())

        def check_metadata(expected, typ):
            with open(path, 'rb') as fd:
                actual = diskfile.read_metadata(fd)
            self.assertEqual(expected, actual)
            for k, v in actual.items():
                self.assertIsInstance(k, typ)
                self.assertIsInstance(v, (typ, int))

        # Check can write raw bytes
        with open(path, 'wb') as fd:
            diskfile.write_metadata(fd, as_bytes)
        check_metadata(as_native, str)
        # Check can write native (with surrogates on py3)
        with open(path, 'wb') as fd:
            diskfile.write_metadata(fd, as_native)
        check_metadata(as_native, str)
        # Check can write some crazy mix
        with open(path, 'wb') as fd:
            diskfile.write_metadata(fd, metadata)
        check_metadata(as_native, str)


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
            loc_generators = []
            datadirs = ["objects", "objects-1"]
            for datadir in datadirs:
                loc_generators.append(
                    diskfile.object_audit_location_generator(
                        devices=tmpdir, datadir=datadir, mount_check=False,
                        logger=logger))

            all_locs = itertools.chain(*loc_generators)
            locations = [(loc.path, loc.device, loc.partition, loc.policy) for
                         loc in all_locs]
            locations.sort()

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

            # Reset status file for next run
            for datadir in datadirs:
                diskfile.clear_auditor_status(tmpdir, datadir)

            # now without a logger
            for datadir in datadirs:
                loc_generators.append(
                    diskfile.object_audit_location_generator(
                        devices=tmpdir, datadir=datadir, mount_check=False,
                        logger=logger))

            all_locs = itertools.chain(*loc_generators)
            locations = [(loc.path, loc.device, loc.partition, loc.policy) for
                         loc in all_locs]
            locations.sort()
            self.assertEqual(locations, expected)

    def test_skipping_unmounted_devices(self):
        with temptree([]) as tmpdir, mock_check_drive() as mocks:
            mocks['ismount'].side_effect = lambda path: path.endswith('sdp')
            os.makedirs(os.path.join(tmpdir, "sdp", "objects",
                                     "2607", "df3",
                                     "ec2871fe724411f91787462f97d30df3"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects",
                                     "9785", "a10",
                                     "4993d582f41be9771505a8d4cb237a10"))

            locations = [
                (loc.path, loc.device, loc.partition, loc.policy)
                for loc in diskfile.object_audit_location_generator(
                    devices=tmpdir, datadir="objects", mount_check=True)]
            locations.sort()

            self.assertEqual(
                locations,
                [(os.path.join(tmpdir, "sdp", "objects",
                               "2607", "df3",
                               "ec2871fe724411f91787462f97d30df3"),
                  "sdp", "2607", POLICIES[0])])

            # Do it again, this time with a logger.
            logger = debug_logger()
            locations = [
                (loc.path, loc.device, loc.partition, loc.policy)
                for loc in diskfile.object_audit_location_generator(
                    devices=tmpdir, datadir="objects", mount_check=True,
                    logger=logger)]
            debug_lines = logger.get_lines_for_level('debug')
            self.assertEqual([
                'Skipping: %s/sdq is not mounted' % tmpdir,
            ], debug_lines)

    def test_skipping_files(self):
        with temptree([]) as tmpdir:
            os.makedirs(os.path.join(tmpdir, "sdp", "objects",
                                     "2607", "df3",
                                     "ec2871fe724411f91787462f97d30df3"))
            with open(os.path.join(tmpdir, "garbage"), "wb"):
                pass

            locations = [
                (loc.path, loc.device, loc.partition, loc.policy)
                for loc in diskfile.object_audit_location_generator(
                    devices=tmpdir, datadir="objects", mount_check=False)]

            self.assertEqual(
                locations,
                [(os.path.join(tmpdir, "sdp", "objects",
                               "2607", "df3",
                               "ec2871fe724411f91787462f97d30df3"),
                  "sdp", "2607", POLICIES[0])])

            # Do it again, this time with a logger.
            logger = debug_logger('test')
            locations = [
                (loc.path, loc.device, loc.partition, loc.policy)
                for loc in diskfile.object_audit_location_generator(
                    devices=tmpdir, datadir="objects", mount_check=False,
                    logger=logger)]
            debug_lines = logger.get_lines_for_level('debug')
            self.assertEqual([
                'Skipping: %s/garbage is not a directory' % tmpdir,
            ], debug_lines)
            logger.clear()

            with mock_check_drive() as mocks:
                mocks['ismount'].side_effect = lambda path: (
                    False if path.endswith('garbage') else True)
                locations = [
                    (loc.path, loc.device, loc.partition, loc.policy)
                    for loc in diskfile.object_audit_location_generator(
                        devices=tmpdir, datadir="objects", mount_check=True,
                        logger=logger)]
            debug_lines = logger.get_lines_for_level('debug')
            self.assertEqual([
                'Skipping: %s/garbage is not mounted' % tmpdir,
            ], debug_lines)

    def test_only_catch_expected_errors(self):
        # Crazy exceptions should still escape object_audit_location_generator
        # so that errors get logged and a human can see what's going wrong;
        # only normal FS corruption should be skipped over silently.

        def list_locations(dirname, datadir):
            return [(loc.path, loc.device, loc.partition, loc.policy)
                    for loc in diskfile.object_audit_location_generator(
                        devices=dirname, datadir=datadir, mount_check=False)]

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
                self.assertRaises(OSError, list_locations, tmpdir, "objects")
            with mock.patch('os.listdir', splode_if_endswith("2607")):
                self.assertRaises(OSError, list_locations, tmpdir, "objects")
            with mock.patch('os.listdir', splode_if_endswith("b54")):
                self.assertRaises(OSError, list_locations, tmpdir, "objects")

    def test_auditor_status(self):
        with temptree([]) as tmpdir:
            os.makedirs(os.path.join(tmpdir, "sdf", "objects", "1", "a", "b"))
            os.makedirs(os.path.join(tmpdir, "sdf", "objects", "2", "a", "b"))
            datadir = "objects"

            # Pretend that some time passed between each partition
            with mock.patch('os.stat') as mock_stat, \
                    mock_check_drive(isdir=True):
                mock_stat.return_value.st_mtime = time() - 60
                # Auditor starts, there are two partitions to check
                gen = diskfile.object_audit_location_generator(tmpdir,
                                                               datadir,
                                                               False)
                next(gen)
                next(gen)

            # Auditor stopped for some reason without raising StopIterator in
            # the generator and restarts There is now only one remaining
            # partition to check
            gen = diskfile.object_audit_location_generator(tmpdir, datadir,
                                                           False)
            with mock_check_drive(isdir=True):
                next(gen)

                # There are no more remaining partitions
                self.assertRaises(StopIteration, next, gen)

            # There are no partitions to check if the auditor restarts another
            # time and the status files have not been cleared
            gen = diskfile.object_audit_location_generator(tmpdir, datadir,
                                                           False)
            with mock_check_drive(isdir=True):
                self.assertRaises(StopIteration, next, gen)

            # Reset status file
            diskfile.clear_auditor_status(tmpdir, datadir)

            # If the auditor restarts another time, we expect to
            # check two partitions again, because the remaining
            # partitions were empty and a new listdir was executed
            gen = diskfile.object_audit_location_generator(tmpdir, datadir,
                                                           False)
            with mock_check_drive(isdir=True):
                next(gen)
                next(gen)

    def test_update_auditor_status_throttle(self):
        # If there are a lot of nearly empty partitions, the
        # update_auditor_status will write the status file many times a second,
        # creating some unexpected high write load. This test ensures that the
        # status file is only written once a minute.
        with temptree([]) as tmpdir:
            os.makedirs(os.path.join(tmpdir, "sdf", "objects", "1", "a", "b"))
            with mock.patch('swift.obj.diskfile.open') as mock_open:
                # File does not exist yet - write expected
                update_auditor_status(tmpdir, None, ['42'], "ALL")
                self.assertEqual(1, mock_open.call_count)

                mock_open.reset_mock()

                # File exists, updated just now - no write expected
                with mock.patch('os.stat') as mock_stat:
                    mock_stat.return_value.st_mtime = time()
                    update_auditor_status(tmpdir, None, ['42'], "ALL")
                    self.assertEqual(0, mock_open.call_count)

                mock_open.reset_mock()

                # File exists, updated just now, but empty partition list. This
                # is a finalizing call, write expected
                with mock.patch('os.stat') as mock_stat:
                    mock_stat.return_value.st_mtime = time()
                    update_auditor_status(tmpdir, None, [], "ALL")
                    self.assertEqual(1, mock_open.call_count)

                mock_open.reset_mock()

                # File updated more than 60 seconds ago - write expected
                with mock.patch('os.stat') as mock_stat:
                    mock_stat.return_value.st_mtime = time() - 61
                    update_auditor_status(tmpdir, None, ['42'], "ALL")
                    self.assertEqual(1, mock_open.call_count)


class TestDiskFileRouter(unittest.TestCase):

    @patch_policies(test_policies)
    def test_policy(self):
        conf = {}
        logger = debug_logger('test-' + self.__class__.__name__)
        df_router = diskfile.DiskFileRouter(conf, logger)
        manager_0 = df_router[POLICIES[0]]
        self.assertIsInstance(manager_0, diskfile.DiskFileManager)
        manager_1 = df_router[POLICIES[1]]
        self.assertIsInstance(manager_1, diskfile.ECDiskFileManager)

        # The DiskFileRouter should not have to load the policy again
        with mock.patch('swift.common.storage_policy.BaseStoragePolicy.' +
                        'get_diskfile_manager') as mock_load:
            manager_3 = df_router[POLICIES[0]]
            mock_load.assert_not_called()
            self.assertIs(manager_3, manager_0)
            self.assertIsInstance(manager_3, diskfile.DiskFileManager)

    def test_invalid_policy_config(self):
        # verify that invalid policy diskfile configs are detected when the
        # DiskfileRouter is created
        bad_policy = StoragePolicy(0, name='zero', is_default=True,
                                   diskfile_module='erasure_coding.fs')

        with patch_policies([bad_policy]):
            with self.assertRaises(PolicyError) as cm:
                diskfile.DiskFileRouter({}, debug_logger())
        self.assertIn('Invalid diskfile_module erasure_coding.fs',
                      str(cm.exception))

        bad_policy = ECStoragePolicy(0, name='one', is_default=True,
                                     ec_type=DEFAULT_TEST_EC_TYPE,
                                     ec_ndata=10, ec_nparity=4,
                                     diskfile_module='replication.fs')

        with patch_policies([bad_policy]):
            with self.assertRaises(PolicyError) as cm:
                diskfile.DiskFileRouter({}, debug_logger())
        self.assertIn('Invalid diskfile_module replication.fs',
                      str(cm.exception))

        bad_policy = StoragePolicy(0, name='zero', is_default=True,
                                   diskfile_module='thin_air.fs')

        with patch_policies([bad_policy]):
            with self.assertRaises(PolicyError) as cm:
                diskfile.DiskFileRouter({}, debug_logger())
        self.assertIn('Unable to load diskfile_module thin_air.fs',
                      str(cm.exception))


class BaseDiskFileTestMixin(object):
    """
    Bag of helpers that are useful in the per-policy DiskFile test classes,
    plus common setUp and tearDown methods.
    """

    # set mgr_cls on subclasses
    mgr_cls = None

    def setUp(self):
        skip_if_no_xattrs()
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(
            self.tmpdir, 'tmp_test_obj_server_DiskFile')
        self.existing_device = 'sda1'
        self.existing_device2 = 'sda2'
        for policy in POLICIES:
            mkdirs(os.path.join(self.testdir, self.existing_device,
                                diskfile.get_tmp_dir(policy)))
            mkdirs(os.path.join(self.testdir, self.existing_device2,
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

    def tearDown(self):
        rmtree(self.tmpdir, ignore_errors=True)
        tpool.execute = self._orig_tpool_exc

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
    def _get_diskfile(self, policy, frag_index=None, **kwargs):
        df_mgr = self.df_router[policy]
        return df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                   policy=policy, frag_index=frag_index,
                                   **kwargs)

    def test_init(self):
        for policy in POLICIES:
            df_router = diskfile.DiskFileRouter({}, self.logger)
            df_mgr = df_router[policy]
            self.assertEqual('/srv/node', df_mgr.devices)
            self.assertEqual(604800, df_mgr.reclaim_age)
            self.assertEqual(60.0, df_mgr.commit_window)
            self.assertTrue(df_mgr.mount_check)

        for policy in POLICIES:
            conf = dict(devices=self.testdir,
                        mount_check='false',
                        reclaim_age=1000,
                        commit_window=10.1)
            df_router = diskfile.DiskFileRouter(conf, self.logger)
            df_mgr = df_router[policy]
            self.assertEqual(self.testdir, df_mgr.devices)
            self.assertEqual(1000, df_mgr.reclaim_age)
            self.assertEqual(10.1, df_mgr.commit_window)
            self.assertFalse(df_mgr.mount_check)

    def test_init_commit_window(self):
        def assert_ok(value, expected):
            for policy in POLICIES:
                conf = {'commit_window': value}
                df_mgr = diskfile.DiskFileRouter(conf, self.logger)[policy]
                self.assertEqual(expected, df_mgr.commit_window)

        assert_ok(10.1, 10.1)
        assert_ok('10.1', 10.1)
        assert_ok(0, 0.0)

        def assert_invalid(value):
            for policy in POLICIES:
                conf = {'commit_window': value}
                with self.assertRaises(ValueError):
                    diskfile.DiskFileRouter(conf, self.logger)[policy]

        assert_invalid(-1.1)
        assert_invalid('-1.1')
        assert_invalid('auto')

    def test_cleanup_uses_configured_reclaim_age(self):
        # verify that the reclaim_age used when cleaning up tombstones is
        # either the default or the configured value
        def do_test(ts, expect_reclaim):
            for policy in POLICIES:
                self.df_router = diskfile.DiskFileRouter(
                    self.conf, self.logger)
                df = self._get_diskfile(policy)
                df.delete(ts.internal)
                tombstone_file = os.path.join(df._datadir, ts.internal + '.ts')
                # cleanup_ondisk_files always uses the configured value
                df._manager.cleanup_ondisk_files(
                    os.path.dirname(tombstone_file))
                self.assertNotEqual(
                    expect_reclaim, os.path.exists(tombstone_file))

        # reclaim_age not configured so default should be used
        do_test(Timestamp(time() - diskfile.DEFAULT_RECLAIM_AGE - 1), True)
        do_test(Timestamp(time() - diskfile.DEFAULT_RECLAIM_AGE + 100), False)

        # reclaim_age configured value should be used
        self.conf['reclaim_age'] = 1000
        do_test(Timestamp(time() - diskfile.DEFAULT_RECLAIM_AGE + 100), True)
        do_test(Timestamp(time() - 1001), True)
        do_test(Timestamp(time() + 100), False)

    def _test_get_ondisk_files(self, scenarios, policy,
                               frag_index=None, **kwargs):
        class_under_test = self._get_diskfile(
            policy, frag_index=frag_index, **kwargs)
        for test in scenarios:
            # test => [('filename.ext', '.ext'|False, ...), ...]
            expected = {
                ext[1:] + '_file': os.path.join(
                    class_under_test._datadir, filename)
                for (filename, ext) in [v[:2] for v in test]
                if ext in ('.data', '.meta', '.ts')}
            # list(zip(...)) for py3 compatibility (zip is lazy there)
            files = list(list(zip(*test))[0])

            for _order in ('ordered', 'shuffled', 'shuffled'):
                class_under_test = self._get_diskfile(
                    policy, frag_index=frag_index, **kwargs)
                try:
                    actual = class_under_test._get_ondisk_files(files)
                    self._assertDictContainsSubset(
                        expected, actual,
                        'Expected %s from %s but got %s'
                        % (expected, files, actual))
                except AssertionError as e:
                    self.fail('%s with files %s' % (str(e), files))
                shuffle(files)

    def _test_cleanup_ondisk_files(self, scenarios, policy,
                                   reclaim_age=None, commit_window=None):
        # check that expected files are left in hashdir after cleanup
        for test in scenarios:
            class_under_test = self.df_router[policy]
            # list(zip(...)) for py3 compatibility (zip is lazy there)
            files = list(list(zip(*test))[0])
            hashdir = os.path.join(self.testdir, str(uuid.uuid4()))
            os.mkdir(hashdir)
            for fname in files:
                open(os.path.join(hashdir, fname), 'w')
            expected_after_cleanup = set([f[0] for f in test
                                          if (f[2] if len(f) > 2 else f[1])])
            if commit_window is not None:
                class_under_test.commit_window = commit_window
            if reclaim_age:
                class_under_test.reclaim_age = reclaim_age
                class_under_test.cleanup_ondisk_files(hashdir)
            else:
                with mock.patch('swift.obj.diskfile.time') as mock_time:
                    # don't reclaim anything
                    mock_time.time.return_value = 0.0
                    class_under_test.cleanup_ondisk_files(hashdir)

            if expected_after_cleanup:
                after_cleanup = set(os.listdir(hashdir))
                errmsg = "expected %r, got %r for test %r" % (
                    sorted(expected_after_cleanup), sorted(after_cleanup), test
                )
                self.assertEqual(expected_after_cleanup, after_cleanup, errmsg)
            else:
                self.assertFalse(os.path.exists(hashdir))

    def _test_yield_hashes_cleanup(self, scenarios, policy):
        # opportunistic test to check that yield_hashes cleans up dir using
        # same scenarios as passed to _test_cleanup_ondisk_files_files
        for test in scenarios:
            class_under_test = self.df_router[policy]
            # list(zip(...)) for py3 compatibility (zip is lazy there)
            files = list(list(zip(*test))[0])
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
                mocked = 'swift.obj.diskfile.BaseDiskFileManager.get_dev_path'
                with mock.patch(mocked) as mock_path:
                    mock_path.return_value = dev_path
                    for _ in class_under_test.yield_hashes(
                            'ignored', '0', policy, suffixes=['abc']):
                        # return values are tested in test_yield_hashes_*
                        pass

            if expected_after_cleanup:
                after_cleanup = set(os.listdir(hashdir))
                errmsg = "expected %r, got %r for test %r" % (
                    sorted(expected_after_cleanup), sorted(after_cleanup), test
                )
                self.assertEqual(expected_after_cleanup, after_cleanup, errmsg)
            else:
                self.assertFalse(os.path.exists(hashdir))

    def test_get_ondisk_files_with_empty_dir(self):
        files = []
        expected = dict(
            data_file=None, meta_file=None, ctype_file=None, ts_file=None)
        for policy in POLICIES:
            for frag_index in (0, None, '13'):
                # check manager
                df_mgr = self.df_router[policy]
                datadir = os.path.join('/srv/node/sdb1/',
                                       diskfile.get_data_dir(policy))
                actual = df_mgr.get_ondisk_files(files, datadir)
                self._assertDictContainsSubset(expected, actual)
                # check diskfile under the hood
                df = self._get_diskfile(policy, frag_index=frag_index)
                actual = df._get_ondisk_files(files)
                self._assertDictContainsSubset(expected, actual)
                # check diskfile open
                self.assertRaises(DiskFileNotExist, df.open)

    def test_get_ondisk_files_with_unexpected_file(self):
        unexpected_files = ['junk', 'junk.data', '.junk']
        timestamp = next(make_timestamp_iter())
        tomb_file = timestamp.internal + '.ts'
        for policy in POLICIES:
            for unexpected in unexpected_files:
                self.logger.clear()
                files = [unexpected, tomb_file]
                df_mgr = self.df_router[policy]
                datadir = os.path.join('/srv/node/sdb1/',
                                       diskfile.get_data_dir(policy))

                results = df_mgr.get_ondisk_files(files, datadir)

                expected = {'ts_file': os.path.join(datadir, tomb_file)}
                self._assertDictContainsSubset(expected, results)
                log_lines = df_mgr.logger.get_lines_for_level('warning')
                self.assertTrue(
                    log_lines[0].startswith(
                        'Unexpected file %s'
                        % os.path.join(datadir, unexpected)))

    def test_get_ondisk_files_no_rsync_temp_file_warning(self):
        # get_ondisk_files logs no warnings for rsync temp files

        class_under_test = self._get_diskfile(POLICIES[0])
        files = [
            '.1472017820.44503.data.QBYCYU',  # rsync tempfile for a .data
            '.total-bs.abcdef',   # example of false positive
        ]
        paths = [os.path.join(class_under_test._datadir, f) for f in files]
        expected = {'unexpected': paths}
        results = class_under_test._get_ondisk_files(files)
        for k, v in expected.items():
            self.assertEqual(results[k], v)
        # no warnings
        self.assertFalse(self.logger.get_lines_for_level('warning'))
        # but we do get a debug!
        lines = self.logger.get_lines_for_level('debug')
        for path in paths:
            expected_msg = 'Rsync tempfile: %s' % path
            self.assertIn(expected_msg, lines)

    def test_cleanup_ondisk_files_reclaim_non_data_files(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set or 'survives' is True, the filename
        # should still be in the dir after cleanup.
        much_older = Timestamp(time() - 2000).internal
        older = Timestamp(time() - 1001).internal
        newer = Timestamp(time() - 900).internal
        scenarios = [
            [('%s.ts' % older, False, False)],

            # fresh tombstone is preserved
            [('%s.ts' % newer, '.ts', True)],

            # tombstone reclaimed despite junk file
            [('junk', False, True),
             ('%s.ts' % much_older, '.ts', False)],

            # fresh .meta not reclaimed even if isolated
            [('%s.meta' % newer, '.meta')],

            # fresh .meta not reclaimed when tombstone is reclaimed
            [('%s.meta' % newer, '.meta'),
             ('%s.ts' % older, False, False)],

            # stale isolated .meta is reclaimed
            [('%s.meta' % older, False, False)],

            # stale .meta is reclaimed along with tombstone
            [('%s.meta' % older, False, False),
             ('%s.ts' % older, False, False)]]

        self._test_cleanup_ondisk_files(scenarios, POLICIES.default,
                                        reclaim_age=1000, commit_window=0)

    def test_construct_dev_path(self):
        res_path = self.df_mgr.construct_dev_path('abc')
        self.assertEqual(os.path.join(self.df_mgr.devices, 'abc'), res_path)

    def test_pickle_async_update(self):
        self.df_mgr.logger.increment = mock.MagicMock()
        ts = Timestamp(10000.0).internal
        with mock.patch('swift.obj.diskfile.write_pickle') as wp:
            self.df_mgr.pickle_async_update(self.existing_device,
                                            'a', 'c', 'o',
                                            dict(a=1, b=2), ts, POLICIES[0])
            dp = self.df_mgr.construct_dev_path(self.existing_device)
            ohash = diskfile.hash_path('a', 'c', 'o')
            wp.assert_called_with({'a': 1, 'b': 2},
                                  os.path.join(
                                      dp, diskfile.get_async_dir(POLICIES[0]),
                                      ohash[-3:], ohash + '-' + ts),
                                  os.path.join(dp, 'tmp'))
        self.df_mgr.logger.increment.assert_called_with('async_pendings')

    def test_object_audit_location_generator(self):
        locations = list(
            self.df_mgr.object_audit_location_generator(POLICIES[0]))
        self.assertEqual(locations, [])

    def test_replication_one_per_device_deprecation(self):
        conf = dict(**self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 1)

        conf = dict(replication_concurrency_per_device='0', **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 0)

        conf = dict(replication_concurrency_per_device='2', **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 2)

        conf = dict(replication_concurrency_per_device=2, **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 2)

        # Check backward compatibility
        conf = dict(replication_one_per_device='true', **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 1)
        log_lines = mgr.logger.get_lines_for_level('warning')
        self.assertIn('replication_one_per_device is deprecated',
                      log_lines[-1])

        conf = dict(replication_one_per_device='false', **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 0)
        log_lines = mgr.logger.get_lines_for_level('warning')
        self.assertIn('replication_one_per_device is deprecated',
                      log_lines[-1])

        # If defined, new parameter has precedence
        conf = dict(replication_concurrency_per_device='2',
                    replication_one_per_device='true', **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 2)
        log_lines = mgr.logger.get_lines_for_level('warning')
        self.assertIn('replication_one_per_device ignored',
                      log_lines[-1])

        conf = dict(replication_concurrency_per_device='2',
                    replication_one_per_device='false', **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 2)
        log_lines = mgr.logger.get_lines_for_level('warning')
        self.assertIn('replication_one_per_device ignored',
                      log_lines[-1])

        conf = dict(replication_concurrency_per_device='0',
                    replication_one_per_device='true', **self.conf)
        mgr = diskfile.DiskFileManager(conf, self.logger)
        self.assertEqual(mgr.replication_concurrency_per_device, 0)
        log_lines = mgr.logger.get_lines_for_level('warning')
        self.assertIn('replication_one_per_device ignored',
                      log_lines[-1])

    def test_replication_lock_on(self):
        # Double check settings
        self.df_mgr.replication_concurrency_per_device = 1
        self.df_mgr.replication_lock_timeout = 0.1
        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            with self.assertRaises(ReplicationLockTimeout):
                with self.df_mgr.replication_lock(self.existing_device,
                                                  POLICIES.legacy, '2'):
                    success = True
        self.assertFalse(success)

    def test_replication_lock_off(self):
        # Double check settings
        self.df_mgr.replication_concurrency_per_device = 0
        self.df_mgr.replication_lock_timeout = 0.1

        # 2 locks must succeed
        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            try:
                with self.df_mgr.replication_lock(self.existing_device,
                                                  POLICIES.legacy, '2'):
                    success = True
            except ReplicationLockTimeout as err:
                self.fail('Unexpected exception: %s' % err)
        self.assertTrue(success)

        # 3 locks must succeed
        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            with self.df_mgr.replication_lock(self.existing_device,
                                              POLICIES.legacy, '2'):
                try:
                    with self.df_mgr.replication_lock(self.existing_device,
                                                      POLICIES.legacy, '3'):
                        success = True
                except ReplicationLockTimeout as err:
                    self.fail('Unexpected exception: %s' % err)
        self.assertTrue(success)

    def test_replication_lock_2(self):
        # Double check settings
        self.df_mgr.replication_concurrency_per_device = 2
        self.df_mgr.replication_lock_timeout = 0.1

        # 2 locks with replication_concurrency_per_device=2 must succeed
        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            try:
                with self.df_mgr.replication_lock(self.existing_device,
                                                  POLICIES.legacy, '2'):
                    success = True
            except ReplicationLockTimeout as err:
                self.fail('Unexpected exception: %s' % err)
        self.assertTrue(success)

        # 3 locks with replication_concurrency_per_device=2 must fail
        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            with self.df_mgr.replication_lock(self.existing_device,
                                              POLICIES.legacy, '2'):
                with self.assertRaises(ReplicationLockTimeout):
                    with self.df_mgr.replication_lock(self.existing_device,
                                                      POLICIES.legacy, '3'):
                        success = True
        self.assertFalse(success)

    def test_replication_lock_another_device_fine(self):
        # Double check settings
        self.df_mgr.replication_concurrency_per_device = 1
        self.df_mgr.replication_lock_timeout = 0.1
        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            try:
                with self.df_mgr.replication_lock(self.existing_device2,
                                                  POLICIES.legacy, '2'):
                    success = True
            except ReplicationLockTimeout as err:
                self.fail('Unexpected exception: %s' % err)
        self.assertTrue(success)

    def test_replication_lock_same_partition(self):
        # Double check settings
        self.df_mgr.replication_concurrency_per_device = 2
        self.df_mgr.replication_lock_timeout = 0.1
        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            with self.assertRaises(PartitionLockTimeout):
                with self.df_mgr.replication_lock(self.existing_device,
                                                  POLICIES.legacy, '1'):
                    success = True
        self.assertFalse(success)

    def test_partition_lock_same_partition(self):
        # Double check settings
        self.df_mgr.replication_lock_timeout = 0.1
        success = False
        with self.df_mgr.partition_lock(self.existing_device,
                                        POLICIES.legacy, '1', name='foo'):
            with self.assertRaises(PartitionLockTimeout):
                with self.df_mgr.partition_lock(self.existing_device,
                                                POLICIES.legacy, '1',
                                                name='foo'):
                    success = True
        self.assertFalse(success)

    def test_partition_lock_same_partition_different_name(self):
        # Double check settings
        self.df_mgr.replication_lock_timeout = 0.1
        success = False
        with self.df_mgr.partition_lock(self.existing_device,
                                        POLICIES.legacy, '1', name='foo'):
            with self.df_mgr.partition_lock(self.existing_device,
                                            POLICIES.legacy, '1',
                                            name='bar'):
                success = True
        self.assertTrue(success)

    def test_partition_lock_and_replication_lock_same_partition(self):
        # Double check settings
        self.df_mgr.replication_lock_timeout = 0.1
        success = False
        with self.df_mgr.partition_lock(self.existing_device,
                                        POLICIES.legacy, '1',
                                        name='replication'):
            with self.assertRaises(PartitionLockTimeout):
                with self.df_mgr.replication_lock(self.existing_device,
                                                  POLICIES.legacy, '1'):
                    success = True
        self.assertFalse(success)

        success = False
        with self.df_mgr.replication_lock(self.existing_device,
                                          POLICIES.legacy, '1'):
            with self.assertRaises(PartitionLockTimeout):
                with self.df_mgr.partition_lock(self.existing_device,
                                                POLICIES.legacy, '1',
                                                name='replication'):
                    success = True
        self.assertFalse(success)

    def test_missing_splice_warning(self):
        with mock.patch('swift.common.splice.splice._c_splice', None):
            self.conf['splice'] = 'yes'
            mgr = diskfile.DiskFileManager(self.conf, logger=self.logger)

        warnings = self.logger.get_lines_for_level('warning')
        self.assertGreater(len(warnings), 0)
        self.assertTrue('splice()' in warnings[-1])
        self.assertFalse(mgr.use_splice)

    def test_get_diskfile_from_hash_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            cleanup.return_value = {'files': ['1381679759.90941.data']}
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileDeviceUnavailable,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_not_dir(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta, \
                mock.patch(self._manager_mock(
                    'quarantine_renamer')) as quarantine_renamer:
            osexc = OSError()
            osexc.errno = errno.ENOTDIR
            cleanup.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])
            quarantine_renamer.assert_called_once_with(
                '/srv/dev/',
                ('/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900/' +
                 'made-up-filename'))

    def test_get_diskfile_from_hash_no_data(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta, \
                mock.patch(self._manager_mock(
                    'quarantine_renamer')) as quarantine_renamer:
            osexc = OSError()
            osexc.errno = errno.ENODATA
            cleanup.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])
            quarantine_renamer.assert_called_once_with(
                '/srv/dev/',
                ('/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900/' +
                 'made-up-filename'))

    def test_get_diskfile_from_hash_unclean(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta, \
                mock.patch(self._manager_mock(
                    'quarantine_renamer')) as quarantine_renamer:
            osexc = OSError()
            osexc.errno = EUCLEAN
            cleanup.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])
            quarantine_renamer.assert_called_once_with(
                '/srv/dev/',
                ('/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900/' +
                 'made-up-filename'))

    def test_get_diskfile_from_hash_no_dir(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            osexc = OSError()
            osexc.errno = errno.ENOENT
            cleanup.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_other_oserror(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            osexc = OSError()
            cleanup.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                OSError,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_no_actual_files(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            cleanup.return_value = {'files': []}
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_read_metadata_problem(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            cleanup.return_value = {'files': ['1381679759.90941.data']}
            readmeta.side_effect = EOFError()
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])

    def test_get_diskfile_from_hash_no_meta_name(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            cleanup.return_value = {'files': ['1381679759.90941.data']}
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
        with mock.patch(self._manager_mock('diskfile_cls')), \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            cleanup.return_value = {'files': ['1381679759.90941.data']}
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
        mock_return = object()
        with mock.patch(self._manager_mock('diskfile_cls'),
                        return_value=mock_return) as dfclass, \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            cleanup.return_value = {'files': ['1381679759.90941.data']}
            readmeta.return_value = {'name': '/a/c/o'}
            actual = self.df_mgr.get_diskfile_from_hash(
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])
            dfclass.assert_called_once_with(
                self.df_mgr, '/srv/dev/', '9',
                'a', 'c', 'o', policy=POLICIES[0])
            cleanup.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900')
            readmeta.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900/'
                '1381679759.90941.data')
            self.assertEqual(mock_return, actual)

    def test_get_diskfile_and_filenames_from_hash(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        mock_return = object()
        with mock.patch(self._manager_mock('diskfile_cls'),
                        return_value=mock_return) as dfclass, \
                mock.patch(self._manager_mock(
                    'cleanup_ondisk_files')) as cleanup, \
                mock.patch('swift.obj.diskfile.read_metadata') as readmeta:
            cleanup.return_value = {'files': ['1381679759.90941.data']}
            readmeta.return_value = {'name': '/a/c/o'}
            actual, names = self.df_mgr.get_diskfile_and_filenames_from_hash(
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', POLICIES[0])
            dfclass.assert_called_once_with(
                self.df_mgr, '/srv/dev/', '9',
                'a', 'c', 'o', policy=POLICIES[0])
            cleanup.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900')
            readmeta.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900/'
                '1381679759.90941.data')
            self.assertEqual(mock_return, actual)
            self.assertEqual(['1381679759.90941.data'], names)

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
            list(self.df_mgr.yield_suffixes(self.existing_device, '9', 0))
        except DiskFileDeviceUnavailable as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_yield_suffixes(self):
        self.df_mgr._listdir = mock.MagicMock(return_value=[
            'abc', 'def', 'ghi', 'abcd', '012'])
        dev = self.existing_device
        self.assertEqual(
            list(self.df_mgr.yield_suffixes(dev, '9', POLICIES[0])),
            [(self.testdir + '/' + dev + '/objects/9/abc', 'abc'),
             (self.testdir + '/' + dev + '/objects/9/def', 'def'),
             (self.testdir + '/' + dev + '/objects/9/012', '012')])

    def test_yield_hashes_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        exc = None
        try:
            list(self.df_mgr.yield_hashes(self.existing_device, '9',
                                          POLICIES[0]))
        except DiskFileDeviceUnavailable as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_yield_hashes_empty(self):
        def _listdir(path):
            return []

        with mock.patch('os.listdir', _listdir):
            self.assertEqual(list(self.df_mgr.yield_hashes(
                self.existing_device, '9', POLICIES[0])), [])

    def test_yield_hashes_cleans_up_everything(self):
        the_time = [1525354555.657585]

        def mock_time():
            return the_time[0]

        with mock.patch('time.time', mock_time):
            # Make a couple of (soon-to-be-)expired tombstones
            df1 = self.df_mgr.get_diskfile(
                self.existing_device, 0, 'a', 'c', 'o1', POLICIES[0])
            df1.delete(Timestamp(the_time[0]))
            df1_hash = utils.hash_path('a', 'c', 'o1')
            df1_suffix = df1_hash[-3:]

            df2 = self.df_mgr.get_diskfile(
                self.existing_device, 0, 'a', 'c', 'o2', POLICIES[0])
            df2.delete(Timestamp(the_time[0] + 1))
            df2_hash = utils.hash_path('a', 'c', 'o2')
            df2_suffix = df2_hash[-3:]

            # sanity checks
            self.assertTrue(os.path.exists(os.path.join(
                self.testdir, self.existing_device, 'objects', '0',
                df1_suffix, df1_hash,
                "1525354555.65758.ts")))
            self.assertTrue(os.path.exists(os.path.join(
                self.testdir, self.existing_device, 'objects', '0',
                df2_suffix, df2_hash,
                "1525354556.65758.ts")))

            # Cache the hashes and expire the tombstones
            self.df_mgr.get_hashes(self.existing_device, '0', [], POLICIES[0])
            the_time[0] += 2 * self.df_mgr.reclaim_age

            hashes = list(self.df_mgr.yield_hashes(
                self.existing_device, '0', POLICIES[0]))
        self.assertEqual(hashes, [])

        # The tombstones are gone
        self.assertFalse(os.path.exists(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df1_suffix, df1_hash,
            "1525354555.65758.ts")))
        self.assertFalse(os.path.exists(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df2_suffix, df2_hash,
            "1525354556.65758.ts")))

        # The empty hash dirs are gone
        self.assertFalse(os.path.exists(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df1_suffix, df1_hash)))
        self.assertFalse(os.path.exists(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df2_suffix, df2_hash)))

        # The empty suffix dirs, and partition are still there
        self.assertTrue(os.path.isdir(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df1_suffix)))
        self.assertTrue(os.path.isdir(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df2_suffix)))

        # but the suffixes is invalid
        part_dir = os.path.join(
            self.testdir, self.existing_device, 'objects', '0')
        invalidations_file = os.path.join(
            part_dir, diskfile.HASH_INVALIDATIONS_FILE)
        with open(invalidations_file) as f:
            invalids = f.read().splitlines()
            self.assertEqual(sorted((df1_suffix, df2_suffix)),
                             sorted(invalids))  # sanity

        # next time get hashes runs
        with mock.patch('time.time', mock_time):
            hashes = self.df_mgr.get_hashes(
                self.existing_device, '0', [], POLICIES[0])
        self.assertEqual(hashes, {})

        # ... suffixes will get cleanup
        self.assertFalse(os.path.exists(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df1_suffix)))
        self.assertFalse(os.path.exists(os.path.join(
            self.testdir, self.existing_device, 'objects', '0',
            df2_suffix)))

        # but really it's not diskfile's jobs to decide if a partition belongs
        # on a node or not
        self.assertTrue(os.path.isdir(os.path.join(
            self.testdir, self.existing_device, 'objects', '0')))

    def test_focused_yield_hashes_does_not_clean_up(self):
        the_time = [1525354555.657585]

        def mock_time():
            return the_time[0]

        with mock.patch('time.time', mock_time):
            df = self.df_mgr.get_diskfile(
                self.existing_device, 0, 'a', 'c', 'o', POLICIES[0])
            df.delete(Timestamp(the_time[0]))
            df_hash = utils.hash_path('a', 'c', 'o')
            df_suffix = df_hash[-3:]

            # sanity check
            self.assertTrue(os.path.exists(os.path.join(
                self.testdir, self.existing_device, 'objects', '0',
                df_suffix, df_hash,
                "1525354555.65758.ts")))

            # Expire the tombstone
            the_time[0] += 2 * self.df_mgr.reclaim_age

            hashes = list(self.df_mgr.yield_hashes(
                self.existing_device, '0', POLICIES[0],
                suffixes=[df_suffix]))
        self.assertEqual(hashes, [])

        # The partition dir is still there. Since we didn't visit all the
        # suffix dirs, we didn't learn whether or not the partition dir was
        # empty.
        self.assertTrue(os.path.exists(os.path.join(
            self.testdir, self.existing_device, 'objects', '0')))

    def test_yield_hashes_empty_suffixes(self):
        def _listdir(path):
            return []

        with mock.patch('os.listdir', _listdir):
            self.assertEqual(
                list(self.df_mgr.yield_hashes(self.existing_device, '9',
                                              POLICIES[0],
                                              suffixes=['456'])), [])

    def _check_yield_hashes(self, policy, suffix_map, expected, **kwargs):
        device = self.existing_device
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
            (hash_, timestamps)
            for hash_, timestamps in expected.items()]
        with mock.patch('os.listdir', _listdir), \
                mock.patch('os.unlink'), \
                mock.patch('os.rmdir'):
            df_mgr = self.df_router[policy]
            hash_items = list(df_mgr.yield_hashes(
                device, part, policy, **kwargs))
            expected = sorted(expected_items)
            actual = sorted(hash_items)
            # default list diff easiest to debug
            self.assertEqual(expected, actual)

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
            '1111111111111111111111111111127e': {'ts_data': ts1.internal},
            '2222222222222222222222222222227e': {'ts_data': ts2.internal},
            '3333333333333333333333333333300b': {'ts_data': ts3.internal},
        }
        for policy in POLICIES:
            self._check_yield_hashes(policy, suffix_map, expected,
                                     suffixes=['27e', '00b'])


@patch_policies
class TestDiskFileManager(DiskFileManagerMixin, BaseTestCase):

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
        self._test_cleanup_ondisk_files(scenarios, POLICIES[0])
        self._test_yield_hashes_cleanup(scenarios, POLICIES[0])

    def test_get_ondisk_files_with_stray_meta(self):
        # get_ondisk_files ignores a stray .meta file

        class_under_test = self._get_diskfile(POLICIES[0])
        files = ['0000000007.00000.meta']

        with mock.patch('swift.obj.diskfile.os.listdir', lambda *args: files):
            self.assertRaises(DiskFileNotExist, class_under_test.open)

    def test_verify_ondisk_files(self):
        # ._verify_ondisk_files should only return False if get_ondisk_files
        # has produced a bad set of files due to a bug, so to test it we need
        # to probe it directly.
        mgr = self.df_router[POLICIES.default]
        ok_scenarios = (
            {'ts_file': None, 'data_file': None, 'meta_file': None},
            {'ts_file': None, 'data_file': 'a_file', 'meta_file': None},
            {'ts_file': None, 'data_file': 'a_file', 'meta_file': 'a_file'},
            {'ts_file': 'a_file', 'data_file': None, 'meta_file': None},
        )

        for scenario in ok_scenarios:
            self.assertTrue(mgr._verify_ondisk_files(scenario),
                            'Unexpected result for scenario %s' % scenario)

        # construct every possible invalid combination of results
        vals = (None, 'a_file')
        for ts_file, data_file, meta_file in [
                (a, b, c) for a in vals for b in vals for c in vals]:
            scenario = {
                'ts_file': ts_file,
                'data_file': data_file,
                'meta_file': meta_file}
            if scenario in ok_scenarios:
                continue
            self.assertFalse(mgr._verify_ondisk_files(scenario),
                             'Unexpected result for scenario %s' % scenario)

    def test_parse_on_disk_filename(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            for ext in ('.meta', '.data', '.ts'):
                fname = '%s%s' % (ts.internal, ext)
                info = mgr.parse_on_disk_filename(fname, POLICIES.default)
                self.assertEqual(ts, info['timestamp'])
                self.assertEqual(ext, info['ext'])

    def test_parse_on_disk_filename_errors(self):
        mgr = self.df_router[POLICIES.default]
        with self.assertRaises(DiskFileError) as cm:
            mgr.parse_on_disk_filename('junk', POLICIES.default)
        self.assertEqual("Invalid Timestamp value in filename 'junk'",
                         str(cm.exception))

    def test_cleanup_ondisk_files_reclaim_with_data_files(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set or 'survives' is True, the filename
        # should still be in the dir after cleanup.
        much_older = Timestamp(time() - 2000).internal
        older = Timestamp(time() - 1001).internal
        newer = Timestamp(time() - 900).internal
        scenarios = [
            # .data files are not reclaimed, ever
            [('%s.data' % older, '.data', True)],
            [('%s.data' % newer, '.data', True)],

            # ... and we could have a mixture of fresh and stale .data
            [('%s.data' % newer, '.data', True),
             ('%s.data' % older, False, False)],

            # tombstone reclaimed despite newer data
            [('%s.data' % newer, '.data', True),
             ('%s.data' % older, False, False),
             ('%s.ts' % much_older, '.ts', False)],

            # .meta not reclaimed if there is a .data file
            [('%s.meta' % older, '.meta'),
             ('%s.data' % much_older, '.data')]]

        self._test_cleanup_ondisk_files(scenarios, POLICIES.default,
                                        reclaim_age=1000)

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
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': fresh_ts},
            '9373a92d072897b136b3fc06595b0456': {'ts_data': old_ts},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': fresher_ts},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

    def test_yield_hashes_yields_meta_timestamp(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        ts3 = next(ts_iter)
        suffix_map = {
            'abc': {
                # only tombstone is yield/sync -able
                '9333a92d072897b136b3fc06595b4abc': [
                    ts1.internal + '.ts',
                    ts2.internal + '.meta'],
                # dangling .meta is not yielded because it cannot be sync'd
                '9222a92d072897b136b3fc06595b4abc': [
                    ts3.internal + '.meta'],
            },
            '456': {
                # only latest metadata timestamp
                '9444a92d072897b136b3fc06595b0456': [
                    ts1.internal + '.data',
                    ts2.internal + '.meta',
                    ts3.internal + '.meta'],
                # exemplary datadir with .meta
                '9555a92d072897b136b3fc06595b7456': [
                    ts1.internal + '.data',
                    ts2.internal + '.meta'],
            },
        }
        expected = {
            '9333a92d072897b136b3fc06595b4abc':
            {'ts_data': ts1},
            '9444a92d072897b136b3fc06595b0456':
            {'ts_data': ts1, 'ts_meta': ts3},
            '9555a92d072897b136b3fc06595b7456':
            {'ts_data': ts1, 'ts_meta': ts2},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

    def test_yield_hashes_yields_content_type_timestamp(self):
        hash_ = '9373a92d072897b136b3fc06595b4abc'
        ts_iter = make_timestamp_iter()
        ts0, ts1, ts2, ts3, ts4 = (next(ts_iter) for _ in range(5))
        data_file = ts1.internal + '.data'

        # no content-type delta
        meta_file = ts2.internal + '.meta'
        suffix_map = {'abc': {hash_: [data_file, meta_file]}}
        expected = {hash_: {'ts_data': ts1,
                            'ts_meta': ts2}}
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # non-zero content-type delta
        delta = ts3.raw - ts2.raw
        meta_file = '%s-%x.meta' % (ts3.internal, delta)
        suffix_map = {'abc': {hash_: [data_file, meta_file]}}
        expected = {hash_: {'ts_data': ts1,
                            'ts_meta': ts3,
                            'ts_ctype': ts2}}
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # zero content-type delta
        meta_file = '%s+0.meta' % ts3.internal
        suffix_map = {'abc': {hash_: [data_file, meta_file]}}
        expected = {hash_: {'ts_data': ts1,
                            'ts_meta': ts3,
                            'ts_ctype': ts3}}
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # content-type in second meta file
        delta = ts3.raw - ts2.raw
        meta_file1 = '%s-%x.meta' % (ts3.internal, delta)
        meta_file2 = '%s.meta' % ts4.internal
        suffix_map = {'abc': {hash_: [data_file, meta_file1, meta_file2]}}
        expected = {hash_: {'ts_data': ts1,
                            'ts_meta': ts4,
                            'ts_ctype': ts2}}
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # obsolete content-type in second meta file, older than data file
        delta = ts3.raw - ts0.raw
        meta_file1 = '%s-%x.meta' % (ts3.internal, delta)
        meta_file2 = '%s.meta' % ts4.internal
        suffix_map = {'abc': {hash_: [data_file, meta_file1, meta_file2]}}
        expected = {hash_: {'ts_data': ts1,
                            'ts_meta': ts4}}
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # obsolete content-type in second meta file, same time as data file
        delta = ts3.raw - ts1.raw
        meta_file1 = '%s-%x.meta' % (ts3.internal, delta)
        meta_file2 = '%s.meta' % ts4.internal
        suffix_map = {'abc': {hash_: [data_file, meta_file1, meta_file2]}}
        expected = {hash_: {'ts_data': ts1,
                            'ts_meta': ts4}}
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
            '9373a92d072897b136b3fc06595b0456': {'ts_data': old_ts},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': fresher_ts},
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
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1},
        }
        try:
            self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                     frag_index=2)
            self.fail('Expected AssertionError')
        except AssertionError:
            pass

    def test_check_policy(self):
        mock_policy = mock.MagicMock()
        mock_policy.policy_type = REPL_POLICY
        # sanity, DiskFileManager is ok with REPL_POLICY
        diskfile.DiskFileManager.check_policy(mock_policy)
        # DiskFileManager raises ValueError with EC_POLICY
        mock_policy.policy_type = EC_POLICY
        with self.assertRaises(ValueError) as cm:
            diskfile.DiskFileManager.check_policy(mock_policy)
        self.assertEqual('Invalid policy_type: %s' % EC_POLICY,
                         str(cm.exception))


@patch_policies(with_ec_default=True)
class TestECDiskFileManager(DiskFileManagerMixin, BaseTestCase):

    mgr_cls = diskfile.ECDiskFileManager

    def test_get_ondisk_files_with_ec_policy_and_legacy_durable(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set then that filename should be returned by
        # the method under test for that extension type. If the optional
        # 'survives' is True, the filename should still be in the dir after
        # cleanup.
        scenarios = [
            # highest frag index is chosen by default
            [('0000000007.00000.durable', '.durable'),
             ('0000000007.00000#1.data', '.data'),
             ('0000000007.00000#0.data', False, True)],

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
             ('0000000006.00000#0.data', False)]
        ]

        # these scenarios have same outcome regardless of whether any
        # fragment preferences are specified
        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None)
        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None, frag_prefs=[])
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)
        self._test_yield_hashes_cleanup(scenarios, POLICIES.default)

        # next scenarios have different outcomes dependent on whether a
        # frag_prefs parameter is passed to diskfile constructor or not
        scenarios = [
            # data with no durable is ignored
            [('0000000007.00000#0.data', False, True)],

            # data newer than tombstone with no durable is ignored
            [('0000000007.00000#0.data', False, True),
             ('0000000006.00000.ts', '.ts', True)],

            # data newer than durable is ignored
            [('0000000009.00000#2.data', False, True),
             ('0000000009.00000#1.data', False, True),
             ('0000000008.00000#3.data', False, True),
             ('0000000007.00000.durable', '.durable'),
             ('0000000007.00000#1.data', '.data'),
             ('0000000007.00000#0.data', False, True)],

            # data newer than durable ignored, even if its only data
            [('0000000008.00000#1.data', False, True),
             ('0000000007.00000.durable', False, False)],

            # missing durable invalidates data, older meta deleted
            [('0000000007.00000.meta', False, True),
             ('0000000006.00000#0.data', False, True),
             ('0000000005.00000.meta', False, False),
             ('0000000004.00000#1.data', False, True)]]

        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None)
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

        scenarios = [
            # data with no durable is chosen
            [('0000000007.00000#0.data', '.data', True)],

            # data newer than tombstone with no durable is chosen
            [('0000000007.00000#0.data', '.data', True),
             ('0000000006.00000.ts', False, True)],

            # data newer than durable is chosen, older data preserved
            [('0000000009.00000#2.data', '.data', True),
             ('0000000009.00000#1.data', False, True),
             ('0000000008.00000#3.data', False, True),
             ('0000000007.00000.durable', False, True),
             ('0000000007.00000#1.data', False, True),
             ('0000000007.00000#0.data', False, True)],

            # data newer than durable chosen when its only data
            [('0000000008.00000#1.data', '.data', True),
             ('0000000007.00000.durable', False, False)],

            # data plus meta chosen without durable, older meta deleted
            [('0000000007.00000.meta', '.meta', True),
             ('0000000006.00000#0.data', '.data', True),
             ('0000000005.00000.meta', False, False),
             ('0000000004.00000#1.data', False, True)]]

        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None, frag_prefs=[])
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

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
                     [('0000000007.00000#1#d.data', '.data'),
                      ('0000000007.00000#0.data', False, True)],

                     # data older than durable is ignored
                     [('0000000007.00000#1#d.data', '.data'),
                      ('0000000006.00000#1.data', False),
                      ('0000000004.00000.ts', False)],

                     # newer meta trumps older meta
                     [('0000000009.00000.meta', '.meta'),
                      ('0000000008.00000.meta', False),
                      ('0000000007.00000#14#d.data', '.data'),
                      ('0000000004.00000.ts', False)],

                     # older meta is ignored
                     [('0000000007.00000#14#d.data', '.data'),
                      ('0000000006.00000.meta', False),
                      ('0000000004.00000.ts', False)],

                     # tombstone trumps meta and data at older timestamp
                     [('0000000006.00000.ts', '.ts'),
                      ('0000000005.00000.meta', False),
                      ('0000000004.00000#0#d.data', False)],

                     # tombstone trumps meta and data at same timestamp
                     [('0000000006.00000.meta', False),
                      ('0000000006.00000.ts', '.ts'),
                      ('0000000006.00000#0#d.data', False)],
                     ]

        # these scenarios have same outcome regardless of whether any
        # fragment preferences are specified
        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None)
        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None, frag_prefs=[])
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)
        self._test_yield_hashes_cleanup(scenarios, POLICIES.default)

        # next scenarios have different outcomes dependent on whether a
        # frag_prefs parameter is passed to diskfile constructor or not
        scenarios = [
            # non-durable is ignored
            [('0000000007.00000#0.data', False, True)],

            # non-durable data newer than tombstone is ignored
            [('0000000007.00000#0.data', False, True),
             ('0000000006.00000.ts', '.ts', True)],

            # data newer than durable data is ignored
            [('0000000009.00000#2.data', False, True),
             ('0000000009.00000#1.data', False, True),
             ('0000000008.00000#3.data', False, True),
             ('0000000007.00000#1#d.data', '.data'),
             ('0000000007.00000#0#d.data', False, True)],

            # non-durable data ignored, older meta deleted
            [('0000000007.00000.meta', False, True),
             ('0000000006.00000#0.data', False, True),
             ('0000000005.00000.meta', False, False),
             ('0000000004.00000#1.data', False, True)]]

        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None)
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

        scenarios = [
            # non-durable data is chosen
            [('0000000007.00000#0.data', '.data', True)],

            # non-durable data newer than tombstone is chosen
            [('0000000007.00000#0.data', '.data', True),
             ('0000000006.00000.ts', False, True)],

            # non-durable data newer than durable data is chosen, older data
            # preserved
            [('0000000009.00000#2.data', '.data', True),
             ('0000000009.00000#1.data', False, True),
             ('0000000008.00000#3.data', False, True),
             ('0000000007.00000#1#d.data', False, True),
             ('0000000007.00000#0#d.data', False, True)],

            # non-durable data plus meta chosen, older meta deleted
            [('0000000007.00000.meta', '.meta', True),
             ('0000000006.00000#0.data', '.data', True),
             ('0000000005.00000.meta', False, False),
             ('0000000004.00000#1.data', False, True)]]

        self._test_get_ondisk_files(scenarios, POLICIES.default,
                                    frag_index=None, frag_prefs=[])
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

    def test_get_ondisk_files_with_ec_policy_and_frag_index_legacy(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set then that filename should be returned by
        # the method under test for that extension type.
        scenarios = [[('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#1.data', '.data'),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000007.00000.durable', '.durable')],

                     # specific frag newer than durable is ignored
                     [('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#1.data', False, True),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000006.00000.durable', False)],

                     # specific frag older than durable is ignored
                     [('0000000007.00000#2.data', False),
                      ('0000000007.00000#1.data', False),
                      ('0000000007.00000#0.data', False),
                      ('0000000008.00000.durable', False)],

                     # specific frag older than newest durable is ignored
                     # even if is also has a durable
                     [('0000000007.00000#2.data', False),
                      ('0000000007.00000#1.data', False),
                      ('0000000007.00000.durable', False),
                      ('0000000008.00000#0.data', False, True),
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
                      ('0000000077.00000.durable', False, False)]
                     ]

        self._test_get_ondisk_files(scenarios, POLICIES.default, frag_index=1)
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

        # scenarios for empty frag_prefs, meaning durable not required
        scenarios = [
            # specific frag newer than durable is chosen
            [('0000000007.00000#2.data', False, True),
             ('0000000007.00000#1.data', '.data', True),
             ('0000000007.00000#0.data', False, True),
             ('0000000006.00000.durable', False, False)],
        ]
        self._test_get_ondisk_files(scenarios, POLICIES.default, frag_index=1,
                                    frag_prefs=[])
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

    def test_get_ondisk_files_with_ec_policy_and_frag_index(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set then that filename should be returned by
        # the method under test for that extension type.
        scenarios = [[('0000000007.00000#2#d.data', False, True),
                      ('0000000007.00000#1#d.data', '.data'),
                      ('0000000007.00000#0#d.data', False, True)],

                     # specific frag index 1 is returned as long as one durable
                     [('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#1.data', '.data', True),
                      ('0000000007.00000#0#d.data', False, True)],

                     # specific frag newer than durable data is ignored
                     [('0000000007.00000#2.data', False, True),
                      ('0000000007.00000#1.data', False, True),
                      ('0000000007.00000#0.data', False, True),
                      ('0000000006.00000#0#d.data', False, True)],

                     # specific frag older than durable data is ignored
                     [('0000000007.00000#2.data', False),
                      ('0000000007.00000#1.data', False),
                      ('0000000007.00000#0.data', False),
                      ('0000000008.00000#0#d.data', False, True)],

                     # specific frag older than newest durable data is ignored
                     # even if is durable
                     [('0000000007.00000#2#d.data', False),
                      ('0000000007.00000#1#d.data', False),
                      ('0000000008.00000#0#d.data', False, True)],

                     # meta included when frag index is specified
                     [('0000000009.00000.meta', '.meta'),
                      ('0000000007.00000#2#d.data', False, True),
                      ('0000000007.00000#1#d.data', '.data'),
                      ('0000000007.00000#0#d.data', False, True)],

                     # specific frag older than tombstone is ignored
                     [('0000000009.00000.ts', '.ts'),
                      ('0000000007.00000#2#d.data', False),
                      ('0000000007.00000#1#d.data', False),
                      ('0000000007.00000#0#d.data', False)],

                     # no data file returned if specific frag index missing
                     [('0000000007.00000#2#d.data', False, True),
                      ('0000000007.00000#14#d.data', False, True),
                      ('0000000007.00000#0#d.data', False, True)],

                     # meta ignored if specific frag index missing
                     [('0000000008.00000.meta', False, True),
                      ('0000000007.00000#14#d.data', False, True),
                      ('0000000007.00000#0#d.data', False, True)],

                     # meta ignored if no data files
                     # Note: this is anomalous, because we are specifying a
                     # frag_index, get_ondisk_files will tolerate .meta with
                     # no .data
                     [('0000000088.00000.meta', False, True)]
                     ]

        self._test_get_ondisk_files(scenarios, POLICIES.default, frag_index=1)
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

        # scenarios for empty frag_prefs, meaning durable not required
        scenarios = [
            # specific frag newer than durable is chosen
            [('0000000007.00000#2.data', False, True),
             ('0000000007.00000#1.data', '.data', True),
             ('0000000007.00000#0.data', False, True)],
        ]
        self._test_get_ondisk_files(scenarios, POLICIES.default, frag_index=1,
                                    frag_prefs=[])
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)

    def test_get_ondisk_files_with_ec_policy_some_legacy(self):
        # Test mixture of legacy durable files and durable data files that
        # might somehow end up in the same object dir.
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set then that filename should be returned by
        # the method under test for that extension type. If the optional
        # 'survives' is True, the filename should still be in the dir after
        # cleanup.
        scenarios = [
            # .durable at same timestamp is ok
            [('0000000007.00000#1#d.data', '.data', True),
             ('0000000007.00000#0#d.data', False, True),
             ('0000000007.00000.durable', False, True)],

            # .durable at same timestamp is ok with non durable wanted frag
            [('0000000007.00000#1.data', '.data', True),
             ('0000000007.00000#0#d.data', False, True),
             ('0000000007.00000.durable', False, True)],

            # older .durable file is cleaned up
            [('0000000007.00000#1#d.data', '.data', True),
             ('0000000007.00000#0#d.data', False, True),
             ('0000000006.00000.durable', False, False)],

            # older .durable does not interfere with non durable wanted frag
            [('0000000007.00000#1.data', '.data', True),
             ('0000000007.00000#0#d.data', False, True),
             ('0000000006.00000.durable', False, False)],

            # ...even if it has accompanying .data file
            [('0000000007.00000#1.data', '.data', True),
             ('0000000007.00000#0#d.data', False, True),
             ('0000000006.00000#0.data', False, False),
             ('0000000006.00000.durable', False, False)],

            # newer .durable file trumps older durable-data
            [('0000000007.00000#1#d.data', False, False),
             ('0000000007.00000#0#d.data', False, False),
             ('0000000008.00000#1.data', '.data', True),
             ('0000000008.00000.durable', False, True)],

            # newer .durable file with no .data trumps older durable-data
            [('0000000007.00000#1#d.data', False, False),
             ('0000000007.00000#0#d.data', False, False),
             ('0000000008.00000.durable', False, False)],
        ]

        self._test_get_ondisk_files(scenarios, POLICIES.default, frag_index=1)
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default)
        self._test_yield_hashes_cleanup(scenarios, POLICIES.default)

    def test_cleanup_ondisk_files_reclaim_with_data_files_legacy_durable(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set or 'survives' is True, the filename
        # should still be in the dir after cleanup.
        much_older = Timestamp(time() - 2000).internal
        older = Timestamp(time() - 1001).internal
        newer = Timestamp(time() - 900).internal
        scenarios = [
            # isolated legacy .durable is cleaned up immediately
            [('%s.durable' % newer, False, False)],

            # ...even when other older files are in dir
            [('%s.durable' % older, False, False),
             ('%s.ts' % much_older, False, False)],

            # isolated .data files are cleaned up when stale
            # ...even when there is an older legacy durable
            [('%s#2.data' % older, False, False),
             ('%s#4.data' % older, False, False),
             ('%s#2.data' % much_older, '.data', True),
             ('%s#4.data' % much_older, False, True),
             ('%s.durable' % much_older, '.durable', True)],

            # tombstone reclaimed despite much older legacy durable
            [('%s.ts' % older, '.ts', False),
             ('%s.durable' % much_older, False, False)],

            # .meta not reclaimed if there is legacy durable data
            [('%s.meta' % older, '.meta', True),
             ('%s#4.data' % much_older, False, True),
             ('%s.durable' % much_older, '.durable', True)],

            # stale .meta reclaimed along with stale legacy .durable
            [('%s.meta' % older, False, False),
             ('%s.durable' % much_older, False, False)]]

        self._test_cleanup_ondisk_files(scenarios, POLICIES.default,
                                        reclaim_age=1000, commit_window=0)

    def test_cleanup_ondisk_files_reclaim_with_data_files(self):
        # Each scenario specifies a list of (filename, extension, [survives])
        # tuples. If extension is set or 'survives' is True, the filename
        # should still be in the dir after cleanup.
        much_older = Timestamp(time() - 2000).internal
        older = Timestamp(time() - 1001).internal
        newer = Timestamp(time() - 900).internal
        scenarios = [
            # isolated .data files are cleaned up when stale
            [('%s#2.data' % older, False, False),
             ('%s#4.data' % older, False, False)],

            # ...even when there is an older durable fileset
            [('%s#2.data' % older, False, False),
             ('%s#4.data' % older, False, False),
             ('%s#2#d.data' % much_older, '.data', True),
             ('%s#4#d.data' % much_older, False, True)],

            # ... but preserved if still fresh
            [('%s#2.data' % newer, False, True),
             ('%s#4.data' % newer, False, True)],

            # ... and we could have a mixture of fresh and stale .data
            [('%s#2.data' % newer, False, True),
             ('%s#4.data' % older, False, False)],

            # tombstone reclaimed despite newer non-durable data
            [('%s#2.data' % newer, False, True),
             ('%s#4.data' % older, False, False),
             ('%s.ts' % much_older, '.ts', False)],

            # tombstone reclaimed despite much older durable
            [('%s.ts' % older, '.ts', False),
             ('%s#4#d.data' % much_older, False, False)],

            # .meta not reclaimed if there is durable data
            [('%s.meta' % older, '.meta', True),
             ('%s#4#d.data' % much_older, False, True)],

            # stale .meta reclaimed along with stale non-durable .data
            [('%s.meta' % older, False, False),
             ('%s#4.data' % much_older, False, False)]]

        self._test_cleanup_ondisk_files(scenarios, POLICIES.default,
                                        reclaim_age=1000, commit_window=0)

    def test_cleanup_ondisk_files_commit_window(self):
        # verify that non-durable files are not reclaimed regardless of
        # timestamp if written to disk within commit_window
        much_older = Timestamp(time() - 2000).internal
        older = Timestamp(time() - 1001).internal
        newer = Timestamp(time() - 900).internal
        scenarios = [
            # recently written nondurables not cleaned up
            [('%s#1.data' % older, True),
             ('%s#2.data' % newer, True),
             ('%s.meta' % much_older, False),
             ('%s.ts' % much_older, False)]]
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default,
                                        reclaim_age=1000, commit_window=60)

        # ... but if commit_window is reduced then recently written files are
        # cleaned up
        scenarios = [
            # older *timestamps* cleaned up
            [('%s#1.data' % older, False),
             ('%s#2.data' % newer, True),
             ('%s.meta' % much_older, False),
             ('%s.ts' % much_older, False)]]
        self._test_cleanup_ondisk_files(scenarios, POLICIES.default,
                                        reclaim_age=1000, commit_window=0)

    def test_get_ondisk_files_with_stray_meta(self):
        # get_ondisk_files ignores a stray .meta file
        class_under_test = self._get_diskfile(POLICIES.default)

        @contextmanager
        def create_files(df, files):
            os.makedirs(df._datadir)
            for fname in files:
                fpath = os.path.join(df._datadir, fname)
                with open(fpath, 'w') as f:
                    diskfile.write_metadata(f, {'name': df._name,
                                                'Content-Length': 0})
            yield
            rmtree(df._datadir, ignore_errors=True)

        # sanity
        good_files = [
            '0000000006.00000.meta',
            '0000000006.00000#1#d.data'
        ]
        with create_files(class_under_test, good_files):
            class_under_test.open()

        scenarios = [['0000000007.00000.meta'],

                     ['0000000007.00000.meta',
                      '0000000006.00000.durable'],  # legacy durable file

                     ['0000000007.00000.meta',
                      '0000000006.00000#1.data'],

                     ['0000000007.00000.meta',
                      '0000000006.00000.durable',  # legacy durable file
                      '0000000005.00000#1.data']
                     ]
        for files in scenarios:
            with create_files(class_under_test, files):
                try:
                    class_under_test.open()
                except DiskFileNotExist:
                    continue
            self.fail('expected DiskFileNotExist opening %s with %r' % (
                class_under_test.__class__.__name__, files))

        # Simulate another process deleting the data after we list contents
        # but before we actually open them
        orig_listdir = os.listdir

        def deleting_listdir(d):
            result = orig_listdir(d)
            for f in result:
                os.unlink(os.path.join(d, f))
            return result

        with create_files(class_under_test, good_files), \
                mock.patch('swift.obj.diskfile.os.listdir',
                           side_effect=deleting_listdir), \
                self.assertRaises(DiskFileNotExist):
            class_under_test.open()

    def test_verify_ondisk_files(self):
        # _verify_ondisk_files should only return False if get_ondisk_files
        # has produced a bad set of files due to a bug, so to test it we need
        # to probe it directly.
        mgr = self.df_router[POLICIES.default]
        ok_scenarios = (
            {'ts_file': None, 'data_file': None, 'meta_file': None,
             'durable_frag_set': None},
            {'ts_file': None, 'data_file': 'a_file', 'meta_file': None,
             'durable_frag_set': ['a_file']},
            {'ts_file': None, 'data_file': 'a_file', 'meta_file': 'a_file',
             'durable_frag_set': ['a_file']},
            {'ts_file': 'a_file', 'data_file': None, 'meta_file': None,
             'durable_frag_set': None},
        )

        for scenario in ok_scenarios:
            self.assertTrue(mgr._verify_ondisk_files(scenario),
                            'Unexpected result for scenario %s' % scenario)

        # construct every possible invalid combination of results
        vals = (None, 'a_file')
        for ts_file, data_file, meta_file, durable_frag in [
            (a, b, c, d)
                for a in vals for b in vals for c in vals for d in vals]:
            scenario = {
                'ts_file': ts_file,
                'data_file': data_file,
                'meta_file': meta_file,
                'durable_frag_set': [durable_frag] if durable_frag else None}
            if scenario in ok_scenarios:
                continue
            self.assertFalse(mgr._verify_ondisk_files(scenario),
                             'Unexpected result for scenario %s' % scenario)

    def test_parse_on_disk_filename(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            # non-durable data file
            for frag in (0, 2, 13):
                fname = '%s#%s.data' % (ts.internal, frag)
                info = mgr.parse_on_disk_filename(fname, POLICIES.default)
                self.assertEqual(ts, info['timestamp'])
                self.assertEqual('.data', info['ext'])
                self.assertEqual(frag, info['frag_index'])
                self.assertIs(False, info['durable'])
                self.assertEqual(mgr.make_on_disk_filename(**info), fname)

            # durable data file
            for frag in (0, 2, 13):
                fname = '%s#%s#d.data' % (ts.internal, frag)
                info = mgr.parse_on_disk_filename(fname, POLICIES.default)
                self.assertEqual(ts, info['timestamp'])
                self.assertEqual('.data', info['ext'])
                self.assertEqual(frag, info['frag_index'])
                self.assertIs(True, info['durable'])
                self.assertEqual(mgr.make_on_disk_filename(**info), fname)

            # data file with unexpected suffix marker, not an error in case
            # alternative marker suffixes added in future
            for frag in (0, 2, 13):
                fname = '%s#%s#junk.data' % (ts.internal, frag)
                info = mgr.parse_on_disk_filename(fname, POLICIES.default)
                self.assertEqual(ts, info['timestamp'])
                self.assertEqual('.data', info['ext'])
                self.assertEqual(frag, info['frag_index'])
                self.assertIs(False, info['durable'])
                expected = '%s#%s.data' % (ts.internal, frag)
                self.assertEqual(mgr.make_on_disk_filename(**info), expected)

            for ext in ('.meta', '.durable', '.ts'):
                fname = '%s%s' % (ts.internal, ext)
                info = mgr.parse_on_disk_filename(fname, POLICIES.default)
                self.assertEqual(ts, info['timestamp'])
                self.assertEqual(ext, info['ext'])
                self.assertIsNone(info['frag_index'])
                self.assertEqual(mgr.make_on_disk_filename(**info), fname)

    def test_parse_on_disk_filename_errors(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            fname = '%s.data' % ts.internal
            with self.assertRaises(DiskFileError) as cm:
                mgr.parse_on_disk_filename(fname, POLICIES.default)
            self.assertTrue(str(cm.exception).startswith("Bad fragment index"))

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

            # non-durable data file
            for frag, msg in expected.items():
                fname = '%s#%s.data' % (ts.internal, frag)
                with self.assertRaises(DiskFileError) as cm:
                    mgr.parse_on_disk_filename(fname, POLICIES.default)
                self.assertIn(msg, str(cm.exception).lower())

            # durable data file
            for frag, msg in expected.items():
                fname = '%s#%s#d.data' % (ts.internal, frag)
                with self.assertRaises(DiskFileError) as cm:
                    mgr.parse_on_disk_filename(fname, POLICIES.default)
                self.assertIn(msg, str(cm.exception).lower())

        with self.assertRaises(DiskFileError) as cm:
            mgr.parse_on_disk_filename('junk', POLICIES.default)
        self.assertEqual("Invalid Timestamp value in filename 'junk'",
                         str(cm.exception))

    def test_make_on_disk_filename(self):
        mgr = self.df_router[POLICIES.default]
        for ts in (Timestamp('1234567890.00001'),
                   Timestamp('1234567890.00001', offset=17)):
            for frag in (0, '0', 2, '2', 13, '13'):
                for durable in (True, False):
                    expected = _make_datafilename(
                        ts, POLICIES.default, frag_index=frag, durable=durable)
                    actual = mgr.make_on_disk_filename(
                        ts, '.data', frag_index=frag, durable=durable)
                    self.assertEqual(expected, actual)
                    parsed = mgr.parse_on_disk_filename(
                        actual, POLICIES.default)
                    self.assertEqual(parsed, {
                        'timestamp': ts,
                        'frag_index': int(frag),
                        'ext': '.data',
                        'ctype_timestamp': None,
                        'durable': durable
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
                        parsed = mgr.parse_on_disk_filename(
                            actual, POLICIES.default)
                        self.assertEqual(parsed, {
                            'timestamp': ts,
                            'frag_index': None,
                            'ext': ext,
                            'ctype_timestamp': None
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
        with self.assertRaises(DiskFileError):
            # .data requires a frag_index kwarg
            mgr.make_on_disk_filename(ts, '.data')

        for frag in (None, 'foo', '1.314', 1.314, -2, '-2'):
            with self.assertRaises(DiskFileError):
                mgr.make_on_disk_filename(ts, '.data', frag_index=frag)

            for ext in ('.meta', '.durable', '.ts'):
                expected = '%s%s' % (ts.internal, ext)
                # bad frag index should be ignored
                actual = mgr.make_on_disk_filename(ts, ext, frag_index=frag)
                self.assertEqual(expected, actual)

    def test_make_on_disk_filename_for_meta_with_content_type(self):
        # verify .meta filename encodes content-type timestamp
        mgr = self.df_router[POLICIES.default]
        time_ = 1234567890.00001
        for delta in (0, 1, 111111):
            t_meta = Timestamp(time_)
            t_type = Timestamp(time_ - delta / 100000.)
            sign = '-' if delta else '+'
            expected = '%s%s%x.meta' % (t_meta.short, sign, delta)
            actual = mgr.make_on_disk_filename(
                t_meta, '.meta', ctype_timestamp=t_type)
            self.assertEqual(expected, actual)
            parsed = mgr.parse_on_disk_filename(actual, POLICIES.default)
            self.assertEqual(parsed, {
                'timestamp': t_meta,
                'frag_index': None,
                'ext': '.meta',
                'ctype_timestamp': t_type
            })
            # these functions are inverse
            self.assertEqual(
                mgr.make_on_disk_filename(**parsed),
                expected)

    def test_yield_hashes_legacy_durable(self):
        old_ts = Timestamp('1383180000.12345')
        fresh_ts = Timestamp(time() - 10)
        fresher_ts = Timestamp(time() - 1)
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    fresh_ts.internal + '.ts'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    old_ts.internal + '#2.data',
                    old_ts.internal + '.durable'],
                '9373a92d072897b136b3fc06595b7456': [
                    fresh_ts.internal + '.ts',
                    fresher_ts.internal + '#2.data',
                    fresher_ts.internal + '.durable'],
            },
            'def': {},
        }
        expected = {
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': fresh_ts},
            '9373a92d072897b136b3fc06595b0456': {'ts_data': old_ts,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': fresher_ts,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes(self):
        old_ts = Timestamp('1383180000.12345')
        fresh_ts = Timestamp(time() - 10)
        fresher_ts = Timestamp(time() - 1)
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    fresh_ts.internal + '.ts'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    old_ts.internal + '#2#d.data'],
                '9373a92d072897b136b3fc06595b7456': [
                    fresh_ts.internal + '.ts',
                    fresher_ts.internal + '#2#d.data'],
            },
            'def': {},
        }
        expected = {
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': fresh_ts},
            '9373a92d072897b136b3fc06595b0456': {'ts_data': old_ts,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': fresher_ts,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_yields_meta_timestamp_legacy_durable(self):
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
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': ts1},
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'ts_meta': ts3,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': ts1,
                                                 'ts_meta': ts2,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # but meta timestamp is *not* returned if specified frag index
        # is not found
        expected = {
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': ts1},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=3)

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
                    ts1.internal + '#2#d.data',
                    ts2.internal + '.meta',
                    ts3.internal + '.meta'],
                '9373a92d072897b136b3fc06595b7456': [
                    ts1.internal + '#2#d.data',
                    ts2.internal + '.meta'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': ts1},
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'ts_meta': ts3,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': ts1,
                                                 'ts_meta': ts2,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected)

        # but meta timestamp is *not* returned if specified frag index
        # is not found
        expected = {
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': ts1},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=3)

    def test_yield_hashes_suffix_filter_legacy_durable(self):
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
            '9373a92d072897b136b3fc06595b0456': {'ts_data': old_ts,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': fresher_ts,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 suffixes=['456'], frag_index=2)

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
                    old_ts + '#2#d.data'],
                '9373a92d072897b136b3fc06595b7456': [
                    fresh_ts + '.ts',
                    fresher_ts + '#2#d.data'],
            },
            'def': {},
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': {'ts_data': old_ts,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': fresher_ts,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 suffixes=['456'], frag_index=2)

    def test_yield_hashes_skips_non_durable_data(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        suffix_map = {
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2#d.data'],
                '9373a92d072897b136b3fc06595b7456': [
                    ts1.internal + '#2.data'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

        # if we add a durable it shows up
        suffix_map['456']['9373a92d072897b136b3fc06595b7456'] = [
            ts1.internal + '#2#d.data']
        expected = {
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': ts1,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_optionally_yields_non_durable_data(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        suffix_map = {
            'abc': {
                '9373a92d072897b136b3fc06595b4abc': [
                    ts1.internal + '#2#d.data',
                    ts2.internal + '#2.data'],  # newer non-durable
                '9373a92d072897b136b3fc06595b0abc': [
                    ts1.internal + '#2.data',  # older non-durable
                    ts2.internal + '#2#d.data'],
            },
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2#d.data'],
                '9373a92d072897b136b3fc06595b7456': [
                    ts2.internal + '#2.data'],
            },
        }

        # sanity check non-durables not yielded
        expected = {
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': ts1,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b0abc': {'ts_data': ts2,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2, frag_prefs=None)

        # an empty frag_prefs list is sufficient to get non-durables yielded
        # (in preference over *older* durable)
        expected = {
            '9373a92d072897b136b3fc06595b4abc': {'ts_data': ts2,
                                                 'durable': False},
            '9373a92d072897b136b3fc06595b0abc': {'ts_data': ts2,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': ts2,
                                                 'durable': False},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2, frag_prefs=[])

    def test_yield_hashes_skips_missing_legacy_durable(self):
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
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

        # if we add a durable it shows up
        suffix_map['456']['9373a92d072897b136b3fc06595b7456'].append(
            ts1.internal + '.durable')
        expected = {
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
            '9373a92d072897b136b3fc06595b7456': {'ts_data': ts1,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_skips_newer_data_without_legacy_durable(self):
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
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=None)
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

        # if we add a durable then newer data shows up
        suffix_map['456']['9373a92d072897b136b3fc06595b0456'].append(
            ts2.internal + '.durable')
        expected = {
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts2,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=None)
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_skips_newer_non_durable_data(self):
        ts_iter = (Timestamp(t) for t in itertools.count(int(time())))
        ts1 = next(ts_iter)
        ts2 = next(ts_iter)
        ts3 = next(ts_iter)
        suffix_map = {
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2#d.data',
                    ts2.internal + '#2.data',
                    ts3.internal + '#2.data'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=None)
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

        # if we make it durable then newer data shows up
        suffix_map = {
            '456': {
                '9373a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2#d.data',
                    ts2.internal + '#2#d.data',
                    ts3.internal + '#2.data'],
            },
        }
        expected = {
            '9373a92d072897b136b3fc06595b0456': {'ts_data': ts2,
                                                 'durable': True},
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
                # this one is fine
                '9333a92d072897b136b3fc06595b0456': [
                    ts1.internal + '#2#d.data'],
                # this one is fine, legacy durable
                '9333a92d072897b136b3fc06595b1456': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable'],
                # missing frag index
                '9444a92d072897b136b3fc06595b7456': [
                    ts1.internal + '.data'],
                # junk
                '9555a92d072897b136b3fc06595b8456': [
                    'junk_file'],
                # not durable
                '9666a92d072897b136b3fc06595b9456': [
                    ts1.internal + '#2.data',
                    ts2.internal + '.meta'],
                # .meta files w/o .data files can't be opened, and are ignored
                '9777a92d072897b136b3fc06595ba456': [
                    ts1.internal + '.meta'],
                # multiple meta files with no data
                '9888a92d072897b136b3fc06595bb456': [
                    ts1.internal + '.meta',
                    ts2.internal + '.meta'],
                # this is good with meta
                '9999a92d072897b136b3fc06595bb456': [
                    ts1.internal + '#2#d.data',
                    ts2.internal + '.meta'],
                # this is good with meta, legacy durable
                '9999a92d072897b136b3fc06595bc456': [
                    ts1.internal + '#2.data',
                    ts1.internal + '.durable',
                    ts2.internal + '.meta'],
                # this one is wrong frag index
                '9aaaa92d072897b136b3fc06595b0456': [
                    ts1.internal + '#7#d.data'],
                # this one is wrong frag index, legacy durable
                '9aaaa92d072897b136b3fc06595b1456': [
                    ts1.internal + '#7.data',
                    ts1.internal + '.durable'],
            },
        }
        expected = {
            '9333a92d072897b136b3fc06595b0456': {'ts_data': ts1,
                                                 'durable': True},
            '9999a92d072897b136b3fc06595bb456': {'ts_data': ts1,
                                                 'ts_meta': ts2,
                                                 'durable': True},
            '9333a92d072897b136b3fc06595b1456': {'ts_data': ts1,
                                                 'durable': True},
            '9999a92d072897b136b3fc06595bc456': {'ts_data': ts1,
                                                 'ts_meta': ts2,
                                                 'durable': True},
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
                    ts1.internal + '#2#d.data',
                    ts1.internal + '#3#d.data',
                ],
                '2222222222222222222222222222227e': [
                    ts1.internal + '#2#d.data',
                    ts2.internal + '#2#d.data',
                ],
            },
            'd41': {
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaad41': [
                    ts1.internal + '#3#d.data',
                ],
            },
            '00b': {
                '3333333333333333333333333333300b': [
                    ts1.internal + '#2.data',
                    ts2.internal + '#2.data',
                    ts3.internal + '#2#d.data',
                ],
            },
        }
        expected = {
            '1111111111111111111111111111127e': {'ts_data': ts1,
                                                 'durable': True},
            '2222222222222222222222222222227e': {'ts_data': ts2,
                                                 'durable': True},
            '3333333333333333333333333333300b': {'ts_data': ts3,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def test_yield_hashes_filters_frag_index_legacy_durable(self):
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
            '1111111111111111111111111111127e': {'ts_data': ts1,
                                                 'durable': True},
            '2222222222222222222222222222227e': {'ts_data': ts2,
                                                 'durable': True},
            '3333333333333333333333333333300b': {'ts_data': ts3,
                                                 'durable': True},
        }
        self._check_yield_hashes(POLICIES.default, suffix_map, expected,
                                 frag_index=2)

    def _test_get_diskfile_from_hash_frag_index_filter(self, legacy_durable):
        df = self._get_diskfile(POLICIES.default)
        hash_ = os.path.basename(df._datadir)
        self.assertRaises(DiskFileNotExist,
                          self.df_mgr.get_diskfile_from_hash,
                          self.existing_device, '0', hash_,
                          POLICIES.default)  # sanity
        timestamp = Timestamp.now()
        for frag_index in (4, 7):
            write_diskfile(df, timestamp, frag_index=frag_index,
                           legacy_durable=legacy_durable)

        df4 = self.df_mgr.get_diskfile_from_hash(
            self.existing_device, '0', hash_, POLICIES.default, frag_index=4)
        self.assertEqual(df4._frag_index, 4)
        self.assertEqual(
            df4.read_metadata()['X-Object-Sysmeta-Ec-Frag-Index'], '4')
        df7 = self.df_mgr.get_diskfile_from_hash(
            self.existing_device, '0', hash_, POLICIES.default, frag_index=7)
        self.assertEqual(df7._frag_index, 7)
        self.assertEqual(
            df7.read_metadata()['X-Object-Sysmeta-Ec-Frag-Index'], '7')

    def test_get_diskfile_from_hash_frag_index_filter(self):
        self._test_get_diskfile_from_hash_frag_index_filter(False)

    def test_get_diskfile_from_hash_frag_index_filter_legacy_durable(self):
        self._test_get_diskfile_from_hash_frag_index_filter(True)

    def test_check_policy(self):
        mock_policy = mock.MagicMock()
        mock_policy.policy_type = EC_POLICY
        # sanity, ECDiskFileManager is ok with EC_POLICY
        diskfile.ECDiskFileManager.check_policy(mock_policy)
        # ECDiskFileManager raises ValueError with REPL_POLICY
        mock_policy.policy_type = REPL_POLICY
        with self.assertRaises(ValueError) as cm:
            diskfile.ECDiskFileManager.check_policy(mock_policy)
        self.assertEqual('Invalid policy_type: %s' % REPL_POLICY,
                         str(cm.exception))


class DiskFileMixin(BaseDiskFileTestMixin):

    def ts(self):
        """
        Timestamps - forever.
        """
        return next(self._ts_iter)

    def _create_ondisk_file(self, df, data, timestamp, metadata=None,
                            ctype_timestamp=None,
                            ext='.data', legacy_durable=False, commit=True):
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
        filename = timestamp.internal
        if ext == '.data' and df.policy.policy_type == EC_POLICY:
            if legacy_durable:
                filename = '%s#%s' % (timestamp.internal, df._frag_index)
                if commit:
                    durable_file = os.path.join(
                        df._datadir, '%s.durable' % timestamp.internal)
                    with open(durable_file, 'wb') as f:
                        pass
            elif commit:
                filename = '%s#%s#d' % (timestamp.internal, df._frag_index)
            else:
                filename = '%s#%s' % (timestamp.internal, df._frag_index)
        if ctype_timestamp:
            metadata.update(
                {'Content-Type-Timestamp':
                 Timestamp(ctype_timestamp).internal})
            filename = encode_timestamps(timestamp,
                                         Timestamp(ctype_timestamp),
                                         explicit=True)
        data_file = os.path.join(df._datadir, filename + ext)
        with open(data_file, 'wb') as f:
            f.write(data)
            xattr.setxattr(f.fileno(), diskfile.METADATA_KEY,
                           pickle.dumps(metadata, diskfile.PICKLE_PROTOCOL))

    def _simple_get_diskfile(self, partition='0', account='a', container='c',
                             obj='o', policy=None, frag_index=None, **kwargs):
        policy = policy or POLICIES.default
        df_mgr = self.df_router[policy]
        if policy.policy_type == EC_POLICY and frag_index is None:
            frag_index = 2
        return df_mgr.get_diskfile(self.existing_device, partition,
                                   account, container, obj,
                                   policy=policy, frag_index=frag_index,
                                   **kwargs)

    def _create_test_file(self, data, timestamp=None, metadata=None,
                          account='a', container='c', obj='o', **kwargs):
        if not isinstance(data, bytes):
            raise ValueError('data must be bytes')
        if metadata is None:
            metadata = {}
        metadata.setdefault('name', '/%s/%s/%s' % (account, container, obj))
        df = self._simple_get_diskfile(account=account, container=container,
                                       obj=obj, **kwargs)
        if timestamp is None:
            timestamp = time()
        timestamp = Timestamp(timestamp)

        # avoid getting O_TMPFILE warning in logs
        if not utils.o_tmpfile_in_tmpdir_supported():
            df.manager.use_linkat = False

        if df.policy.policy_type == EC_POLICY:
            data = encode_frag_archive_bodies(df.policy, data)[df._frag_index]

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
        return df, data

    def test_get_dev_path(self):
        self.df_mgr.devices = '/srv'
        device = 'sda1'
        dev_path = os.path.join(self.df_mgr.devices, device)

        mount_check = None
        self.df_mgr.mount_check = True
        with mock_check_drive(ismount=False):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             None)
        with mock_check_drive(ismount=True):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             dev_path)

        self.df_mgr.mount_check = False
        with mock_check_drive(isdir=False):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             None)
        with mock_check_drive(isdir=True):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             dev_path)

        mount_check = True
        with mock_check_drive(ismount=False):
            self.assertEqual(self.df_mgr.get_dev_path(device, mount_check),
                             None)
        with mock_check_drive(ismount=True):
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
                          b'1234567890', metadata={'X-Delete-At': '0'})

        try:
            self._create_test_file(b'1234567890', open_expired=True,
                                   metadata={'X-Delete-At': '0',
                                             'X-Object-Meta-Foo': 'bar'})
            df = self._simple_get_diskfile(open_expired=True)
            md = df.read_metadata()
            self.assertEqual(md['X-Object-Meta-Foo'], 'bar')
        except SwiftException as err:
            self.fail("Unexpected swift exception raised: %r" % err)

    def test_open_not_expired(self):
        try:
            self._create_test_file(
                b'1234567890', metadata={'X-Delete-At': str(2 * int(time()))})
        except SwiftException as err:
            self.fail("Unexpected swift exception raised: %r" % err)

    def test_get_metadata(self):
        timestamp = self.ts().internal
        df, df_data = self._create_test_file(b'1234567890',
                                             timestamp=timestamp)
        md = df.get_metadata()
        self.assertEqual(md['X-Timestamp'], timestamp)

    def test_read_metadata(self):
        timestamp = self.ts().internal
        self._create_test_file(b'1234567890', timestamp=timestamp)
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
        with self.assertRaises(DiskFileNotOpen):
            df.get_metadata()

    def test_get_datafile_metadata(self):
        ts_iter = make_timestamp_iter()
        body = b'1234567890'
        ts_data = next(ts_iter)
        metadata = {'X-Object-Meta-Test': 'test1',
                    'X-Object-Sysmeta-Test': 'test1'}
        df, df_data = self._create_test_file(body, timestamp=ts_data.internal,
                                             metadata=metadata)
        expected = df.get_metadata()
        ts_meta = next(ts_iter)
        df.write_metadata({'X-Timestamp': ts_meta.internal,
                           'X-Object-Meta-Test': 'changed',
                           'X-Object-Sysmeta-Test': 'ignored'})
        df.open()
        self.assertEqual(expected, df.get_datafile_metadata())
        expected.update({'X-Timestamp': ts_meta.internal,
                         'X-Object-Meta-Test': 'changed'})
        self.assertEqual(expected, df.get_metadata())

    def test_get_datafile_metadata_not_opened(self):
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotOpen):
            df.get_datafile_metadata()

    def test_get_metafile_metadata(self):
        ts_iter = make_timestamp_iter()
        body = b'1234567890'
        ts_data = next(ts_iter)
        metadata = {'X-Object-Meta-Test': 'test1',
                    'X-Object-Sysmeta-Test': 'test1'}
        df, df_data = self._create_test_file(body, timestamp=ts_data.internal,
                                             metadata=metadata)
        self.assertIsNone(df.get_metafile_metadata())

        # now create a meta file
        ts_meta = next(ts_iter)
        df.write_metadata({'X-Timestamp': ts_meta.internal,
                           'X-Object-Meta-Test': 'changed'})
        df.open()
        expected = {'X-Timestamp': ts_meta.internal,
                    'X-Object-Meta-Test': 'changed'}
        self.assertEqual(expected, df.get_metafile_metadata())

    def test_get_metafile_metadata_not_opened(self):
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotOpen):
            df.get_metafile_metadata()

    def test_not_opened(self):
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotOpen):
            with df:
                pass

    def test_disk_file_default_disallowed_metadata(self):
        # build an object with some meta (at t0+1s)
        orig_metadata = {'X-Object-Meta-Key1': 'Value1',
                         'X-Object-Transient-Sysmeta-KeyA': 'ValueA',
                         'Content-Type': 'text/garbage'}
        df = self._get_open_disk_file(ts=self.ts().internal,
                                      extra_metadata=orig_metadata)
        with df.open():
            if df.policy.policy_type == EC_POLICY:
                expected = df.policy.pyeclib_driver.get_segment_info(
                    1024, df.policy.ec_segment_size)['fragment_size']
            else:
                expected = 1024
            self.assertEqual(str(expected), df._metadata['Content-Length'])
        # write some new metadata (fast POST, don't send orig meta, at t0+1)
        df = self._simple_get_diskfile()
        df.write_metadata({'X-Timestamp': self.ts().internal,
                           'X-Object-Transient-Sysmeta-KeyB': 'ValueB',
                           'X-Object-Meta-Key2': 'Value2'})
        df = self._simple_get_diskfile()
        with df.open():
            # non-fast-post updateable keys are preserved
            self.assertEqual('text/garbage', df._metadata['Content-Type'])
            # original fast-post updateable keys are removed
            self.assertNotIn('X-Object-Meta-Key1', df._metadata)
            self.assertNotIn('X-Object-Transient-Sysmeta-KeyA', df._metadata)
            # new fast-post updateable keys are added
            self.assertEqual('Value2', df._metadata['X-Object-Meta-Key2'])
            self.assertEqual('ValueB',
                             df._metadata['X-Object-Transient-Sysmeta-KeyB'])

    def test_disk_file_preserves_sysmeta(self):
        # build an object with some meta (at t0)
        orig_metadata = {'X-Object-Sysmeta-Key1': 'Value1',
                         'Content-Type': 'text/garbage'}
        df = self._get_open_disk_file(ts=self.ts().internal,
                                      extra_metadata=orig_metadata)
        with df.open():
            if df.policy.policy_type == EC_POLICY:
                expected = df.policy.pyeclib_driver.get_segment_info(
                    1024, df.policy.ec_segment_size)['fragment_size']
            else:
                expected = 1024
            self.assertEqual(str(expected), df._metadata['Content-Length'])
        # write some new metadata (fast POST, don't send orig meta, at t0+1s)
        df = self._simple_get_diskfile()
        df.write_metadata({'X-Timestamp': self.ts().internal,
                           'X-Object-Sysmeta-Key1': 'Value2',
                           'X-Object-Meta-Key3': 'Value3'})
        df = self._simple_get_diskfile()
        with df.open():
            # non-fast-post updateable keys are preserved
            self.assertEqual('text/garbage', df._metadata['Content-Type'])
            # original sysmeta keys are preserved
            self.assertEqual('Value1', df._metadata['X-Object-Sysmeta-Key1'])

    def test_disk_file_preserves_slo(self):
        # build an object with some meta (at t0)
        orig_metadata = {'X-Static-Large-Object': 'True',
                         'Content-Type': 'text/garbage'}
        df = self._get_open_disk_file(ts=self.ts().internal,
                                      extra_metadata=orig_metadata)

        # sanity test
        with df.open():
            self.assertEqual('True', df._metadata['X-Static-Large-Object'])
            if df.policy.policy_type == EC_POLICY:
                expected = df.policy.pyeclib_driver.get_segment_info(
                    1024, df.policy.ec_segment_size)['fragment_size']
            else:
                expected = 1024
            self.assertEqual(str(expected), df._metadata['Content-Length'])

        # write some new metadata (fast POST, don't send orig meta, at t0+1s)
        df = self._simple_get_diskfile()
        df.write_metadata({'X-Timestamp': self.ts().internal})
        df = self._simple_get_diskfile()
        with df.open():
            # non-fast-post updateable keys are preserved
            self.assertEqual('text/garbage', df._metadata['Content-Type'])
            self.assertEqual('True', df._metadata['X-Static-Large-Object'])

    def test_disk_file_reader_iter(self):
        df, df_data = self._create_test_file(b'1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(b''.join(reader), df_data)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_reader_iter_w_quarantine(self):
        df, df_data = self._create_test_file(b'1234567890')

        def raise_dfq(m):
            raise DiskFileQuarantined(m)

        reader = df.reader(_quarantine_hook=raise_dfq)
        reader._obj_size += 1
        self.assertRaises(DiskFileQuarantined, b''.join, reader)

    def test_disk_file_reader_iter_w_io_error(self):
        df, df_data = self._create_test_file(b'1234567890')

        class FakeFp(object):
            def __init__(self, buf):
                self.pos = 0
                self.buf = buf

            def read(self, sz):
                if not self.buf:
                    raise IOError(5, 'Input/output error')
                chunk, self.buf = self.buf, b''
                self.pos += len(chunk)
                return chunk

            def close(self):
                pass

            def tell(self):
                return self.pos

        def raise_dfq(m):
            raise DiskFileQuarantined(m)

        reader = df.reader(_quarantine_hook=raise_dfq)
        reader._fp = FakeFp(b'1234')
        self.assertRaises(DiskFileQuarantined, b''.join, reader)

    def test_disk_file_app_iter_corners(self):
        df, df_data = self._create_test_file(b'1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(b''.join(reader.app_iter_range(0, None)),
                         df_data)
        self.assertEqual(quarantine_msgs, [])
        df = self._simple_get_diskfile()
        with df.open():
            reader = df.reader()
            self.assertEqual(b''.join(reader.app_iter_range(5, None)),
                             df_data[5:])

    def test_disk_file_app_iter_range_w_none(self):
        df, df_data = self._create_test_file(b'1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(b''.join(reader.app_iter_range(None, None)),
                         df_data)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_partial_closes(self):
        df, df_data = self._create_test_file(b'1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_range(0, 5)
        self.assertEqual(b''.join(it), df_data[:5])
        self.assertEqual(quarantine_msgs, [])
        self.assertTrue(reader._fp is None)

    def test_disk_file_app_iter_ranges(self):
        df, df_data = self._create_test_file(b'012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(0, 10), (10, 20), (20, 30)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', len(df_data))
        value = b''.join(it)
        self.assertIn(df_data[:10], value)
        self.assertIn(df_data[10:20], value)
        self.assertIn(df_data[20:30], value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_ranges_w_quarantine(self):
        df, df_data = self._create_test_file(b'012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(len(df_data), reader._obj_size)  # sanity check
        reader._obj_size += 1
        it = reader.app_iter_ranges([(0, len(df_data))],
                                    'plain/text',
                                    '\r\n--someheader\r\n', len(df_data))
        value = b''.join(it)
        self.assertIn(df_data, value)
        self.assertEqual(quarantine_msgs,
                         ["Bytes read: %s, does not match metadata: %s" %
                          (len(df_data), len(df_data) + 1)])

    def test_disk_file_app_iter_ranges_w_no_etag_quarantine(self):
        df, df_data = self._create_test_file(b'012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(0, 10)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', len(df_data))
        value = b''.join(it)
        self.assertIn(df_data[:10], value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_ranges_edges(self):
        df, df_data = self._create_test_file(b'012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(3, 10), (0, 2)], 'application/whatever',
                                    '\r\n--someheader\r\n', len(df_data))
        value = b''.join(it)
        self.assertIn(df_data[3:10], value)
        self.assertIn(df_data[:2], value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_large_app_iter_ranges(self):
        # This test case is to make sure that the disk file app_iter_ranges
        # method all the paths being tested.
        long_str = b'01234567890' * 65536
        df, df_data = self._create_test_file(long_str)
        target_strs = [df_data[3:10], df_data[0:65590]]
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(3, 10), (0, 65590)], 'plain/text',
                                    '5e816ff8b8b8e9a5d355497e5d9e0301',
                                    len(df_data))

        # The produced string actually missing the MIME headers
        # need to add these headers to make it as real MIME message.
        # The body of the message is produced by method app_iter_ranges
        # off of DiskFile object.
        header = b''.join([b'Content-Type: multipart/byteranges;',
                           b'boundary=',
                           b'5e816ff8b8b8e9a5d355497e5d9e0301\r\n'])

        value = header + b''.join(it)
        self.assertEqual(quarantine_msgs, [])

        message = email.message_from_bytes(value)
        parts = [p.get_payload(decode=True) for p in message.walk()][1:3]
        self.assertEqual(parts, target_strs)

    def test_disk_file_app_iter_ranges_empty(self):
        # This test case tests when empty value passed into app_iter_ranges
        # When ranges passed into the method is either empty array or None,
        # this method will yield empty string
        df, df_data = self._create_test_file(b'012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([], 'application/whatever',
                                    '\r\n--someheader\r\n', len(df_data))
        self.assertEqual(b''.join(it), b'')

        df = self._simple_get_diskfile()
        with df.open():
            reader = df.reader()
            it = reader.app_iter_ranges(None, 'app/something',
                                        '\r\n--someheader\r\n', 150)
            self.assertEqual(b''.join(it), b'')
            self.assertEqual(quarantine_msgs, [])

    def test_disk_file_mkstemp_creates_dir(self):
        for policy in POLICIES:
            tmpdir = os.path.join(self.testdir, self.existing_device,
                                  diskfile.get_tmp_dir(policy))
            os.rmdir(tmpdir)
            df = self._simple_get_diskfile(policy=policy)
            df.manager.use_linkat = False
            with df.create():
                self.assertTrue(os.path.exists(tmpdir))

    def test_disk_file_writer(self):
        df = self._simple_get_diskfile()
        with df.create() as writer:
            self.assertIsInstance(writer, diskfile.BaseDiskFileWriter)
            # create automatically opens for us
            self.assertIsNotNone(writer._fd)
            # can't re-open since we're already open
            with self.assertRaises(ValueError):
                writer.open()
            writer.write(b'asdf')
            writer.close()
            # can't write any more
            with self.assertRaises(ValueError):
                writer.write(b'asdf')
            # can close again
            writer.close()

    def test_disk_file_concurrent_writes(self):
        def threadA(df, events, errors):
            try:
                ts = self.ts()
                with df.create() as writer:
                    writer.write(b'dataA')
                    writer.put({
                        'X-Timestamp': ts.internal,
                        'Content-Length': 5,
                    })
                    events[0].set()
                    events[1].wait()
                    writer.commit(ts)
            except Exception as e:
                errors.append(e)
                raise

        def threadB(df, events, errors):
            try:
                events[0].wait()
                ts = self.ts()
                with df.create() as writer:
                    writer.write(b'dataB')
                    writer.put({
                        'X-Timestamp': ts.internal,
                        'Content-Length': 5,
                    })
                    writer.commit(ts)
                events[1].set()
            except Exception as e:
                errors.append(e)
                raise

        df = self._simple_get_diskfile()
        events = [threading.Event(), threading.Event()]
        errors = []

        threads = [threading.Thread(target=tgt, args=(df, events, errors))
                   for tgt in (threadA, threadB)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertFalse(errors)

        with df.open(), open(df._data_file, 'rb') as fp:
            self.assertEqual(b'dataB', fp.read())

    def test_disk_file_concurrent_marked_durable(self):
        ts = self.ts()

        def threadA(df, events, errors):
            try:
                with df.create() as writer:
                    writer.write(b'dataA')
                    writer.put({
                        'X-Timestamp': ts.internal,
                        'Content-Length': 5,
                    })
                    events[0].set()
                    events[1].wait()
                    writer.commit(ts)
            except Exception as e:
                errors.append(e)
                raise

        def threadB(df, events, errors):
            try:
                events[0].wait()
                # Mark it durable just like in ssync_receiver
                with df.create() as writer:
                    writer.commit(ts)
                events[1].set()
            except Exception as e:
                errors.append(e)
                raise

        df = self._simple_get_diskfile()
        events = [threading.Event(), threading.Event()]
        errors = []

        threads = [threading.Thread(target=tgt, args=(df, events, errors))
                   for tgt in (threadA, threadB)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertFalse(errors)

        with df.open(), open(df._data_file, 'rb') as fp:
            if df.policy.policy_type == EC_POLICY:
                # Confirm that it really *was* marked durable
                self.assertTrue(df._data_file.endswith('#d.data'))
            self.assertEqual(b'dataA', fp.read())

    def test_disk_file_concurrent_delete(self):
        def threadA(df, events, errors):
            try:
                ts = self.ts()
                with df.create() as writer:
                    writer.write(b'dataA')
                    writer.put({'X-Timestamp': ts.internal})
                    events[0].set()
                    events[1].wait()
                    writer.commit(ts)
            except Exception as e:
                errors.append(e)
                raise

        def threadB(df, events, errors):
            try:
                events[0].wait()
                df.delete(self.ts())
                events[1].set()
            except Exception as e:
                errors.append(e)
                raise

        df = self._simple_get_diskfile()
        events = [threading.Event(), threading.Event()]
        errors = []

        threads = [threading.Thread(target=tgt, args=(df, events, errors))
                   for tgt in (threadA, threadB)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertFalse(errors)

        self.assertRaises(DiskFileDeleted, df.open)

    def _get_open_disk_file(self, invalid_type=None, obj_name='o', fsize=1024,
                            csize=8, mark_deleted=False, prealloc=False,
                            ts=None, mount_check=False, extra_metadata=None,
                            policy=None, frag_index=None, data=None,
                            commit=True):
        '''returns a DiskFile'''
        policy = policy or POLICIES.legacy
        df = self._simple_get_diskfile(obj=obj_name, policy=policy,
                                       frag_index=frag_index)
        data = data or b'0' * fsize
        if not isinstance(data, bytes):
            raise ValueError('data must be bytes')
        if policy.policy_type == EC_POLICY:
            archives = encode_frag_archive_bodies(policy, data)
            try:
                data = archives[df._frag_index]
            except IndexError:
                data = archives[0]

        if ts:
            timestamp = Timestamp(ts)
        else:
            timestamp = Timestamp.now()
        if prealloc:
            prealloc_size = fsize
        else:
            prealloc_size = None

        with df.create(size=prealloc_size) as writer:
            writer.write(data)
            upload_size, etag = writer.chunks_finished()
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
            if commit:
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
            wrong_byte = b'X' if meta_xattr[:1] != b'X' else b'Y'
            xattr.setxattr(data_files[0], "user.swift.metadata",
                           wrong_byte + meta_xattr[1:])
        elif invalid_type == 'Subtly-Corrupt-Xattrs':
            # We have to go below read_metadata/write_metadata to get proper
            # corruption.
            meta_xattr = xattr.getxattr(data_files[0], "user.swift.metadata")
            wrong_checksum = md5(meta_xattr + b"some extra stuff").hexdigest()
            xattr.setxattr(data_files[0], "user.swift.metadata_checksum",
                           wrong_checksum.encode())
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

        df = self._get_open_disk_file(fsize=50 * 1024, csize=256)
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
        open_exc = invalid_type in ('Content-Length', 'Bad-Content-Length',
                                    'Subtly-Corrupt-Xattrs',
                                    'Corrupt-Xattrs', 'Truncated-Xattrs',
                                    'Missing-Name', 'Bad-X-Delete-At')
        open_collision = invalid_type == 'Bad-Name'

        def verify(*args, **kwargs):
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

    def test_quarantine_subtly_corrupt_xattrs(self):
        # xattrs that unpickle without error, but whose checksum does not
        # match
        self.run_quarantine_invalids('Subtly-Corrupt-Xattrs')

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
        with mock.patch('os.fstat', side_effect=OSError()):
            self.assertRaises(
                DiskFileQuarantined,
                self._get_open_disk_file)

    def test_quarantine_ioerror_enodata(self):
        df = self._get_open_disk_file()

        def my_open(filename, mode, *args, **kwargs):
            if mode == 'rb':
                raise IOError(errno.ENODATA, '-ENODATA fool!')
            return open(filename, mode, *args, **kwargs)

        with mock.patch('swift.obj.diskfile.open', my_open):
            with self.assertRaises(DiskFileQuarantined) as err:
                df.open()
            self.assertEqual(
                'Failed to open %s: [Errno %d] -ENODATA fool!'
                % (df._data_file, errno.ENODATA), str(err.exception))

    def test_quarantine_ioerror_euclean(self):
        df = self._get_open_disk_file()

        def my_open(filename, mode, *args, **kwargs):
            if mode == 'rb':
                raise IOError(EUCLEAN, '-EUCLEAN fool!')
            return open(filename, mode, *args, **kwargs)

        with mock.patch('swift.obj.diskfile.open', my_open):
            with self.assertRaises(DiskFileQuarantined) as err:
                df.open()
            self.assertEqual(
                'Failed to open %s: [Errno %d] -EUCLEAN fool!'
                % (df._data_file, EUCLEAN), str(err.exception))

    def test_quarantine_hashdir_not_a_directory(self):
        df, df_data = self._create_test_file(b'1234567890', account="abc",
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

    def test_quarantine_hashdir_not_listable(self):
        for eno in (errno.ENODATA, EUCLEAN):
            df, df_data = self._create_test_file(b'1234567890', account="abc",
                                                 container='123', obj='xyz')
            hashdir = df._datadir
            df = self.df_mgr.get_diskfile(
                self.existing_device, '0', 'abc', '123', 'xyz',
                policy=POLICIES.legacy)
            with mock.patch('os.listdir',
                            side_effect=OSError(eno, 'nope')):
                self.assertRaises(DiskFileQuarantined, df.open)

            # make sure the right thing got quarantined; the suffix dir should
            # not have moved, as that could have many objects in it
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
        df.manager.use_linkat = False
        for e in (errno.ENOSPC, errno.EDQUOT):
            with mock.patch("swift.obj.diskfile.mkstemp",
                            mock.MagicMock(side_effect=OSError(
                                e, os.strerror(e)))):
                with self.assertRaises(DiskFileNoSpace):
                    with df.create(size=200):
                        pass

        # Other OSErrors must not be raised as DiskFileNoSpace
        with mock.patch("swift.obj.diskfile.mkstemp",
                        mock.MagicMock(side_effect=OSError(
                            errno.EACCES, os.strerror(errno.EACCES)))):
            with self.assertRaises(OSError) as raised:
                with df.create(size=200):
                    pass
        self.assertEqual(raised.exception.errno, errno.EACCES)

    def test_create_close_oserror(self):
        # This is a horrible hack so you can run this test in isolation.
        # Some of the ctypes machinery calls os.close(), and that runs afoul
        # of our mock.
        with mock.patch.object(utils, '_sys_fallocate', None):
            utils.disable_fallocate()

            df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc',
                                          '123', 'xyz', policy=POLICIES.legacy)
            with mock.patch("swift.obj.diskfile.os.close",
                            mock.MagicMock(side_effect=OSError(
                                errno.EACCES, os.strerror(errno.EACCES)))):
                with df.create(size=200):
                    pass

    def test_write_metadata(self):
        df, df_data = self._create_test_file(b'1234567890')
        file_count = len(os.listdir(df._datadir))
        timestamp = Timestamp.now().internal
        metadata = {'X-Timestamp': timestamp, 'X-Object-Meta-test': 'data'}
        df.write_metadata(metadata)
        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), file_count + 1)
        exp_name = '%s.meta' % timestamp
        self.assertIn(exp_name, set(dl))

    def test_write_metadata_with_content_type(self):
        # if metadata has content-type then its time should be in file name
        df, df_data = self._create_test_file(b'1234567890')
        file_count = len(os.listdir(df._datadir))
        timestamp = Timestamp.now()
        metadata = {'X-Timestamp': timestamp.internal,
                    'X-Object-Meta-test': 'data',
                    'Content-Type': 'foo',
                    'Content-Type-Timestamp': timestamp.internal}
        df.write_metadata(metadata)
        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), file_count + 1)
        exp_name = '%s+0.meta' % timestamp.internal
        self.assertTrue(exp_name in set(dl),
                        'Expected file %s not found in %s' % (exp_name, dl))

    def test_write_metadata_with_older_content_type(self):
        # if metadata has content-type then its time should be in file name
        ts_iter = make_timestamp_iter()
        df, df_data = self._create_test_file(b'1234567890',
                                             timestamp=next(ts_iter))
        file_count = len(os.listdir(df._datadir))
        timestamp = next(ts_iter)
        timestamp2 = next(ts_iter)
        metadata = {'X-Timestamp': timestamp2.internal,
                    'X-Object-Meta-test': 'data',
                    'Content-Type': 'foo',
                    'Content-Type-Timestamp': timestamp.internal}
        df.write_metadata(metadata)
        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), file_count + 1, dl)
        exp_name = '%s-%x.meta' % (timestamp2.internal,
                                   timestamp2.raw - timestamp.raw)
        self.assertTrue(exp_name in set(dl),
                        'Expected file %s not found in %s' % (exp_name, dl))

    def test_write_metadata_with_content_type_removes_same_time_meta(self):
        # a meta file without content-type should be cleaned up in favour of
        # a meta file at same time with content-type
        ts_iter = make_timestamp_iter()
        df, df_data = self._create_test_file(b'1234567890',
                                             timestamp=next(ts_iter))
        file_count = len(os.listdir(df._datadir))
        timestamp = next(ts_iter)
        timestamp2 = next(ts_iter)
        metadata = {'X-Timestamp': timestamp2.internal,
                    'X-Object-Meta-test': 'data'}
        df.write_metadata(metadata)
        metadata = {'X-Timestamp': timestamp2.internal,
                    'X-Object-Meta-test': 'data',
                    'Content-Type': 'foo',
                    'Content-Type-Timestamp': timestamp.internal}
        df.write_metadata(metadata)

        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), file_count + 1, dl)
        exp_name = '%s-%x.meta' % (timestamp2.internal,
                                   timestamp2.raw - timestamp.raw)
        self.assertTrue(exp_name in set(dl),
                        'Expected file %s not found in %s' % (exp_name, dl))

    def test_write_metadata_with_content_type_removes_multiple_metas(self):
        # a combination of a meta file without content-type and an older meta
        # file with content-type should be cleaned up in favour of a meta file
        # at newer time with content-type
        ts_iter = make_timestamp_iter()
        df, df_data = self._create_test_file(b'1234567890',
                                             timestamp=next(ts_iter))
        file_count = len(os.listdir(df._datadir))
        timestamp = next(ts_iter)
        timestamp2 = next(ts_iter)
        metadata = {'X-Timestamp': timestamp2.internal,
                    'X-Object-Meta-test': 'data'}
        df.write_metadata(metadata)
        metadata = {'X-Timestamp': timestamp.internal,
                    'X-Object-Meta-test': 'data',
                    'Content-Type': 'foo',
                    'Content-Type-Timestamp': timestamp.internal}
        df.write_metadata(metadata)

        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), file_count + 2, dl)

        metadata = {'X-Timestamp': timestamp2.internal,
                    'X-Object-Meta-test': 'data',
                    'Content-Type': 'foo',
                    'Content-Type-Timestamp': timestamp.internal}
        df.write_metadata(metadata)

        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), file_count + 1, dl)
        exp_name = '%s-%x.meta' % (timestamp2.internal,
                                   timestamp2.raw - timestamp.raw)
        self.assertTrue(exp_name in set(dl),
                        'Expected file %s not found in %s' % (exp_name, dl))

    def test_write_metadata_no_xattr(self):
        timestamp = Timestamp.now().internal
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
        timestamp = Timestamp.now().internal
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

    def _create_diskfile_dir(self, timestamp, policy, legacy_durable=False,
                             partition=0, next_part_power=None,
                             expect_error=False):
        timestamp = Timestamp(timestamp)
        df = self._simple_get_diskfile(account='a', container='c',
                                       obj='o_%s' % policy,
                                       policy=policy,
                                       partition=partition,
                                       next_part_power=next_part_power)
        frag_index = None
        if policy.policy_type == EC_POLICY:
            frag_index = df._frag_index or 7
        if expect_error:
            with self.assertRaises(Exception):
                write_diskfile(df, timestamp, frag_index=frag_index,
                               legacy_durable=legacy_durable)
        else:
            write_diskfile(df, timestamp, frag_index=frag_index,
                           legacy_durable=legacy_durable)
        return df._datadir

    def test_commit(self):
        for policy in POLICIES:
            timestamp = Timestamp.now()
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o_%s' % policy,
                                           policy=policy)
            write_diskfile(df, timestamp, frag_index=2)
            dl = os.listdir(df._datadir)
            expected = [_make_datafilename(
                timestamp, policy, frag_index=2, durable=True)]
            self.assertEqual(len(dl), len(expected),
                             'Unexpected dir listing %s' % dl)
            self.assertEqual(expected, dl)
            if policy.policy_type == EC_POLICY:
                self.assertEqual(2, df._frag_index)

    def _do_test_write_cleanup(self, policy, legacy_durable=False):
        # create first fileset as starting state
        timestamp_1 = Timestamp.now()
        datadir_1 = self._create_diskfile_dir(
            timestamp_1, policy, legacy_durable)
        # second write should clean up first fileset
        timestamp_2 = Timestamp(time() + 1)
        datadir_2 = self._create_diskfile_dir(timestamp_2, policy)
        # sanity check
        self.assertEqual(datadir_1, datadir_2)
        dl = os.listdir(datadir_2)
        expected = [_make_datafilename(
            timestamp_2, policy, frag_index=2, durable=True)]
        self.assertEqual(len(dl), len(expected),
                         'Unexpected dir listing %s' % dl)
        self.assertEqual(expected, dl)

    def test_write_cleanup(self):
        for policy in POLICIES:
            self._do_test_write_cleanup(policy)

    def test_write_cleanup_legacy_durable(self):
        for policy in POLICIES:
            self._do_test_write_cleanup(policy, legacy_durable=True)

    @mock.patch("swift.obj.diskfile.BaseDiskFileManager.cleanup_ondisk_files")
    def test_write_cleanup_part_power_increase(self, mock_cleanup):
        # Without next_part_power set we expect only one cleanup per DiskFile
        # and no linking
        for policy in POLICIES:
            timestamp = Timestamp(time()).internal
            df_dir = self._create_diskfile_dir(timestamp, policy)
            self.assertEqual(1, mock_cleanup.call_count)
            mock_cleanup.assert_called_once_with(df_dir)
            mock_cleanup.reset_mock()

        # With next_part_power set to part_power + 1 we expect two cleanups per
        # DiskFile: first cleanup the current directory, but also cleanup the
        # future directory where hardlinks are created
        for policy in POLICIES:
            timestamp = Timestamp(time()).internal
            df_dir = self._create_diskfile_dir(
                timestamp, policy, next_part_power=11)

            self.assertEqual(2, mock_cleanup.call_count)
            mock_cleanup.assert_any_call(df_dir)

            # Make sure the translated path is also cleaned up
            expected_dir = utils.replace_partition_in_path(
                self.conf['devices'], df_dir, 11)
            mock_cleanup.assert_any_call(expected_dir)

            mock_cleanup.reset_mock()

        # With next_part_power set to part_power we expect two cleanups per
        # DiskFile: first cleanup the current directory, but also cleanup the
        # previous old directory
        for policy in POLICIES:
            hash_path = utils.hash_path('a', 'c', 'o_%s' % policy)
            partition = utils.get_partition_for_hash(hash_path, 10)
            timestamp = Timestamp(time()).internal
            df_dir = self._create_diskfile_dir(
                timestamp, policy, partition=partition, next_part_power=10)

            self.assertEqual(2, mock_cleanup.call_count)
            mock_cleanup.assert_any_call(df_dir)

            # Make sure the path using the old part power is also cleaned up
            expected_dir = utils.replace_partition_in_path(
                self.conf['devices'], df_dir, 9)
            mock_cleanup.assert_any_call(expected_dir)

            mock_cleanup.reset_mock()

    @mock.patch.object(diskfile.BaseDiskFileManager, 'cleanup_ondisk_files',
                       side_effect=Exception)
    def test_killed_before_cleanup(self, mock_cleanup):
        for policy in POLICIES:
            timestamp = Timestamp(time()).internal
            hash_path = utils.hash_path('a', 'c', 'o_%s' % policy)
            partition = utils.get_partition_for_hash(hash_path, 10)
            df_dir = self._create_diskfile_dir(timestamp, policy,
                                               partition=partition,
                                               next_part_power=11,
                                               expect_error=True)
            expected_dir = utils.replace_partition_in_path(
                self.conf['devices'], df_dir, 11)

            self.assertEqual(os.listdir(df_dir), os.listdir(expected_dir))

    def test_commit_fsync(self):
        for policy in POLICIES:
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o', policy=policy)

            timestamp = Timestamp.now()
            with df.create() as writer:
                metadata = {
                    'ETag': 'bogus_etag',
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': '0',
                }
                with mock.patch('swift.obj.diskfile.fsync') as mock_fsync:
                    writer.put(metadata)
                    self.assertEqual(1, mock_fsync.call_count)
                    writer.commit(timestamp)
                    self.assertEqual(1, mock_fsync.call_count)
            if policy.policy_type == EC_POLICY:
                self.assertIsInstance(mock_fsync.call_args[0][0], int)

    def test_commit_ignores_cleanup_ondisk_files_error(self):
        for policy in POLICIES:
            # Check OSError from cleanup_ondisk_files is caught and ignored
            mock_cleanup = mock.MagicMock(side_effect=OSError)
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o_error', policy=policy)

            timestamp = Timestamp.now()
            with df.create() as writer:
                metadata = {
                    'ETag': 'bogus_etag',
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': '0',
                }
                writer.put(metadata)
                with mock.patch(self._manager_mock(
                        'cleanup_ondisk_files', df), mock_cleanup):
                    writer.commit(timestamp)
            expected = {
                EC_POLICY: 1,
                REPL_POLICY: 0,
            }[policy.policy_type]
            self.assertEqual(expected, mock_cleanup.call_count)
            if expected:
                self.assertIn(
                    'Problem cleaning up',
                    df.manager.logger.get_lines_for_level('error')[0])

            expected = [_make_datafilename(
                timestamp, policy, frag_index=2, durable=True)]
            dl = os.listdir(df._datadir)
            self.assertEqual(len(dl), len(expected),
                             'Unexpected dir listing %s' % dl)
            self.assertEqual(expected, dl)

    def test_number_calls_to_cleanup_ondisk_files_during_create(self):
        # Check how many calls are made to cleanup_ondisk_files, and when,
        # during put(), commit() sequence
        for policy in POLICIES:
            expected = {
                EC_POLICY: (0, 1),
                REPL_POLICY: (1, 0),
            }[policy.policy_type]
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o_error', policy=policy)
            timestamp = Timestamp.now()
            with df.create() as writer:
                metadata = {
                    'ETag': 'bogus_etag',
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': '0',
                }
                with mock.patch(self._manager_mock(
                        'cleanup_ondisk_files', df)) as mock_cleanup:
                    writer.put(metadata)
                    self.assertEqual(expected[0], mock_cleanup.call_count)
                with mock.patch(self._manager_mock(
                        'cleanup_ondisk_files', df)) as mock_cleanup:
                    writer.commit(timestamp)
                    self.assertEqual(expected[1], mock_cleanup.call_count)

    def test_number_calls_to_cleanup_ondisk_files_during_delete(self):
        # Check how many calls are made to cleanup_ondisk_files, and when,
        # for delete() and necessary prerequisite steps
        for policy in POLICIES:
            expected = {
                EC_POLICY: (0, 1, 1),
                REPL_POLICY: (1, 0, 1),
            }[policy.policy_type]
            df = self._simple_get_diskfile(account='a', container='c',
                                           obj='o_error', policy=policy)
            timestamp = Timestamp.now()
            with df.create() as writer:
                metadata = {
                    'ETag': 'bogus_etag',
                    'X-Timestamp': timestamp.internal,
                    'Content-Length': '0',
                }
                with mock.patch(self._manager_mock(
                        'cleanup_ondisk_files', df)) as mock_cleanup:
                    writer.put(metadata)
                    self.assertEqual(expected[0], mock_cleanup.call_count)
                with mock.patch(self._manager_mock(
                        'cleanup_ondisk_files', df)) as mock_cleanup:
                    writer.commit(timestamp)
                    self.assertEqual(expected[1], mock_cleanup.call_count)
                with mock.patch(self._manager_mock(
                        'cleanup_ondisk_files', df)) as mock_cleanup:
                    timestamp = Timestamp.now()
                    df.delete(timestamp)
                    self.assertEqual(expected[2], mock_cleanup.call_count)

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

            ts = Timestamp.now()
            df.delete(ts)
            exp_name = '%s.ts' % ts.internal
            dl = os.listdir(df._datadir)
            self.assertEqual(len(dl), 1)
            self.assertIn(exp_name, set(dl))
            # cleanup before next policy
            os.unlink(os.path.join(df._datadir, exp_name))

    def test_open_deleted(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), 1)
        self.assertIn(exp_name, set(dl))
        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileDeleted, df.open)

    def test_open_deleted_with_corrupt_tombstone(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), 1)
        self.assertIn(exp_name, set(dl))
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
        df, df_data = self._create_test_file(
            b'blah blah',
            account='three', container='blind', obj='mice')
        hashdir = df._datadir
        df = self.df_mgr.get_diskfile_from_audit_location(
            diskfile.AuditLocation(hashdir, self.existing_device, '0',
                                   policy=POLICIES.default))
        df.open()
        self.assertEqual(df._name, '/three/blind/mice')

    def test_from_audit_location_with_mismatched_hash(self):
        df, df_data = self._create_test_file(
            b'blah blah',
            account='this', container='is', obj='right')
        hashdir = df._datadir
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
        self.assertIsNone(reader._fp)
        error_lines = df._logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        self.assertIn('close failure', error_lines[0])
        self.assertIn('Bad', error_lines[0])

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
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=10)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=9)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=8)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=7)
        self._create_ondisk_file(df, b'B', ext='.data', timestamp=6)
        self._create_ondisk_file(df, b'A', ext='.data', timestamp=5)
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileDeleted) as raised:
            df.open()
        self.assertEqual(raised.exception.timestamp, Timestamp(10).internal)

    def test_ondisk_search_loop_meta_ts_data(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=10)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=9)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, b'B', ext='.data', timestamp=6)
        self._create_ondisk_file(df, b'A', ext='.data', timestamp=5)
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileDeleted) as raised:
            df.open()
        self.assertEqual(raised.exception.timestamp, Timestamp(8).internal)

    def _test_ondisk_search_loop_meta_data_ts(self, legacy_durable=False):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=10)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=9)
        self._create_ondisk_file(
            df, b'B', ext='.data', legacy_durable=legacy_durable, timestamp=8)
        self._create_ondisk_file(
            df, b'A', ext='.data', legacy_durable=legacy_durable, timestamp=7)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=6)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertIn('X-Timestamp', df._metadata)
            self.assertEqual(df._metadata['X-Timestamp'],
                             Timestamp(10).internal)
            self.assertNotIn('deleted', df._metadata)

    def test_ondisk_search_loop_meta_data_ts(self):
        self._test_ondisk_search_loop_meta_data_ts()

    def test_ondisk_search_loop_meta_data_ts_legacy_durable(self):
        self._test_ondisk_search_loop_meta_data_ts(legacy_durable=True)

    def _test_ondisk_search_loop_multiple_meta_data(self,
                                                    legacy_durable=False):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=10,
                                 metadata={'X-Object-Meta-User': 'user-meta'})
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=9,
                                 ctype_timestamp=9,
                                 metadata={'Content-Type': 'newest',
                                           'X-Object-Meta-User': 'blah'})
        self._create_ondisk_file(
            df, b'B', ext='.data', legacy_durable=legacy_durable, timestamp=8,
            metadata={'Content-Type': 'newer'})
        self._create_ondisk_file(
            df, b'A', ext='.data', legacy_durable=legacy_durable, timestamp=7,
            metadata={'Content-Type': 'oldest'})
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEqual(df._metadata['X-Timestamp'],
                             Timestamp(10).internal)
            self.assertTrue('Content-Type' in df._metadata)
            self.assertEqual(df._metadata['Content-Type'], 'newest')
            self.assertTrue('X-Object-Meta-User' in df._metadata)
            self.assertEqual(df._metadata['X-Object-Meta-User'], 'user-meta')

    def test_ondisk_search_loop_multiple_meta_data(self):
        self._test_ondisk_search_loop_multiple_meta_data()

    def test_ondisk_search_loop_multiple_meta_data_legacy_durable(self):
        self._test_ondisk_search_loop_multiple_meta_data(legacy_durable=True)

    def _test_ondisk_search_loop_stale_meta_data(self, legacy_durable=False):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=10,
                                 metadata={'X-Object-Meta-User': 'user-meta'})
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=9,
                                 ctype_timestamp=7,
                                 metadata={'Content-Type': 'older',
                                           'X-Object-Meta-User': 'blah'})
        self._create_ondisk_file(
            df, b'B', ext='.data', legacy_durable=legacy_durable, timestamp=8,
            metadata={'Content-Type': 'newer'})
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEqual(df._metadata['X-Timestamp'],
                             Timestamp(10).internal)
            self.assertTrue('Content-Type' in df._metadata)
            self.assertEqual(df._metadata['Content-Type'], 'newer')
            self.assertTrue('X-Object-Meta-User' in df._metadata)
            self.assertEqual(df._metadata['X-Object-Meta-User'], 'user-meta')

    def test_ondisk_search_loop_stale_meta_data(self):
        self._test_ondisk_search_loop_stale_meta_data()

    def test_ondisk_search_loop_stale_meta_data_legacy_durable(self):
        self._test_ondisk_search_loop_stale_meta_data(legacy_durable=True)

    def _test_ondisk_search_loop_data_ts_meta(self, legacy_durable=False):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(
            df, b'B', ext='.data', legacy_durable=legacy_durable, timestamp=10)
        self._create_ondisk_file(
            df, b'A', ext='.data', legacy_durable=legacy_durable, timestamp=9)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=6)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertIn('X-Timestamp', df._metadata)
            self.assertEqual(df._metadata['X-Timestamp'],
                             Timestamp(10).internal)
            self.assertNotIn('deleted', df._metadata)

    def test_ondisk_search_loop_data_ts_meta(self):
        self._test_ondisk_search_loop_data_ts_meta()

    def test_ondisk_search_loop_data_ts_meta_legacy_durable(self):
        self._test_ondisk_search_loop_data_ts_meta(legacy_durable=True)

    def _test_ondisk_search_loop_wayward_files_ignored(self,
                                                       legacy_durable=False):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, b'X', ext='.bar', timestamp=11)
        self._create_ondisk_file(
            df, b'B', ext='.data', legacy_durable=legacy_durable, timestamp=10)
        self._create_ondisk_file(
            df, b'A', ext='.data', legacy_durable=legacy_durable, timestamp=9)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, b'', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=6)
        self._create_ondisk_file(df, b'', ext='.meta', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertIn('X-Timestamp', df._metadata)
            self.assertEqual(df._metadata['X-Timestamp'],
                             Timestamp(10).internal)
            self.assertNotIn('deleted', df._metadata)

    def test_ondisk_search_loop_wayward_files_ignored(self):
        self._test_ondisk_search_loop_wayward_files_ignored()

    def test_ondisk_search_loop_wayward_files_ignored_legacy_durable(self):
        self._test_ondisk_search_loop_wayward_files_ignored(
            legacy_durable=True)

    def _test_ondisk_search_loop_listdir_error(self, legacy_durable=False):
        df = self._simple_get_diskfile()

        def mock_listdir_exp(*args, **kwargs):
            raise OSError(errno.EACCES, os.strerror(errno.EACCES))

        with mock.patch("os.listdir", mock_listdir_exp):
            self._create_ondisk_file(df, b'X', ext='.bar', timestamp=11)
            self._create_ondisk_file(df, b'B', ext='.data', timestamp=10,
                                     legacy_durable=legacy_durable)
            self._create_ondisk_file(df, b'A', ext='.data', timestamp=9,
                                     legacy_durable=legacy_durable)
            self._create_ondisk_file(df, b'', ext='.ts', timestamp=8)
            self._create_ondisk_file(df, b'', ext='.ts', timestamp=7)
            self._create_ondisk_file(df, b'', ext='.meta', timestamp=6)
            self._create_ondisk_file(df, b'', ext='.meta', timestamp=5)
            df = self._simple_get_diskfile()
            self.assertRaises(DiskFileError, df.open)

    def test_ondisk_search_loop_listdir_error(self):
        self._test_ondisk_search_loop_listdir_error()

    def test_ondisk_search_loop_listdir_error_legacy_durable(self):
        self._test_ondisk_search_loop_listdir_error(legacy_durable=True)

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
        self.assertIn('a very special error', log_lines[-1])

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
        self.assertEqual(len(dl), 1)
        self.assertIn(exp_name, set(dl))
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
            if df.policy.policy_type == EC_POLICY:
                expected = df.policy.pyeclib_driver.get_segment_info(
                    1024, df.policy.ec_segment_size)['fragment_size']
            else:
                expected = 1024
            self.assertEqual(df.content_length, expected)

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
        self.assertEqual(len(dl), 1)
        self.assertIn(exp_name, set(dl))
        df = self._simple_get_diskfile()
        exc = None
        try:
            with df.open():
                df.timestamp
        except DiskFileDeleted as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_timestamp(self):
        ts_1 = self.ts()
        self._get_open_disk_file(ts=ts_1.internal)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertEqual(df.timestamp, ts_1.internal)
        ts_2 = self.ts()
        df.write_metadata({'X-Timestamp': ts_2.internal})
        with df.open():
            self.assertEqual(df.timestamp, ts_2.internal)

    def test_data_timestamp(self):
        ts_1 = self.ts()
        self._get_open_disk_file(ts=ts_1.internal)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertEqual(df.data_timestamp, ts_1.internal)
        ts_2 = self.ts()
        df.write_metadata({'X-Timestamp': ts_2.internal})
        with df.open():
            self.assertEqual(df.data_timestamp, ts_1.internal)

    def test_data_timestamp_not_open(self):
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotOpen):
            df.data_timestamp

    def test_content_type_and_timestamp(self):
        ts_1 = self.ts()
        self._get_open_disk_file(ts=ts_1.internal,
                                 extra_metadata={'Content-Type': 'image/jpeg'})
        df = self._simple_get_diskfile()
        with df.open():
            self.assertEqual(ts_1.internal, df.data_timestamp)
            self.assertEqual(ts_1.internal, df.timestamp)
            self.assertEqual(ts_1.internal, df.content_type_timestamp)
            self.assertEqual('image/jpeg', df.content_type)
        ts_2 = self.ts()
        ts_3 = self.ts()
        df.write_metadata({'X-Timestamp': ts_3.internal,
                           'Content-Type': 'image/gif',
                           'Content-Type-Timestamp': ts_2.internal})
        with df.open():
            self.assertEqual(ts_1.internal, df.data_timestamp)
            self.assertEqual(ts_3.internal, df.timestamp)
            self.assertEqual(ts_2.internal, df.content_type_timestamp)
            self.assertEqual('image/gif', df.content_type)

    def test_content_type_timestamp_not_open(self):
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotOpen):
            df.content_type_timestamp

    def test_content_type_not_open(self):
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotOpen):
            df.content_type

    def _do_test_durable_timestamp(self, legacy_durable):
        ts_1 = self.ts()
        df = self._simple_get_diskfile(frag_index=2)
        write_diskfile(df, ts_1, legacy_durable=legacy_durable)
        # get a new instance of the diskfile to ensure timestamp variable is
        # set by the open() and not just the write operations
        df = self._simple_get_diskfile(frag_index=2)
        with df.open():
            self.assertEqual(df.durable_timestamp, ts_1.internal)
        # verify durable timestamp does not change when metadata is written
        ts_2 = self.ts()
        df.write_metadata({'X-Timestamp': ts_2.internal})
        with df.open():
            self.assertEqual(df.durable_timestamp, ts_1.internal)

    def test_durable_timestamp(self):
        self._do_test_durable_timestamp(False)

    def test_durable_timestamp_not_open(self):
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotOpen):
            df.durable_timestamp

    def test_durable_timestamp_no_data_file(self):
        df = self._get_open_disk_file(self.ts().internal)
        for f in os.listdir(df._datadir):
            if f.endswith('.data'):
                os.unlink(os.path.join(df._datadir, f))
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotExist):
            df.open()
        # open() was attempted, but no data file so expect None
        self.assertIsNone(df.durable_timestamp)

    def test_error_in_cleanup_ondisk_files(self):

        def mock_cleanup(*args, **kwargs):
            raise OSError()

        df = self._get_open_disk_file()
        file_count = len(os.listdir(df._datadir))
        ts = time()
        with mock.patch(
                self._manager_mock('cleanup_ondisk_files'), mock_cleanup):
            # Expect to swallow the OSError
            df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEqual(len(dl), file_count + 1)
        self.assertIn(exp_name, set(dl))

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
            raise unittest.SkipTest("zero-copy support is missing")
        self.conf['splice'] = 'on'
        self.conf['keep_cache_size'] = 16384
        self.conf['disk_chunk_size'] = 4096

        df = self._get_open_disk_file(fsize=163840)
        reader = df.reader()
        self.assertTrue(reader.can_zero_copy_send())
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as dbc:
            with mock.patch("swift.obj.diskfile.DROP_CACHE_WINDOW", 4095):
                with open('/dev/null', 'w') as devnull:
                    reader.zero_copy_send(devnull.fileno())
                if df.policy.policy_type == EC_POLICY:
                    expected = 4 + 1
                else:
                    expected = (4 * 10) + 1
                self.assertEqual(len(dbc.mock_calls), expected)

    def test_zero_copy_turns_off_when_md5_sockets_not_supported(self):
        if not self._system_can_zero_copy():
            raise unittest.SkipTest("zero-copy support is missing")
        df_mgr = self.df_router[POLICIES.default]
        self.conf['splice'] = 'on'
        with mock.patch('swift.obj.diskfile.get_md5_socket') as mock_md5sock:
            mock_md5sock.side_effect = IOError(
                errno.EAFNOSUPPORT, "MD5 socket busted")
            df = self._get_open_disk_file(fsize=128)
            reader = df.reader()
            self.assertFalse(reader.can_zero_copy_send())

            log_lines = df_mgr.logger.get_lines_for_level('warning')
            self.assertIn('MD5 sockets', log_lines[-1])

    def test_tee_to_md5_pipe_length_mismatch(self):
        if not self._system_can_zero_copy():
            raise unittest.SkipTest("zero-copy support is missing")
        self.conf['splice'] = 'on'

        df = self._get_open_disk_file(fsize=16385)
        reader = df.reader()
        self.assertTrue(reader.can_zero_copy_send())

        with mock.patch('swift.obj.diskfile.tee') as mock_tee:
            mock_tee.side_effect = lambda _1, _2, _3, cnt: cnt - 1

            with open('/dev/null', 'w') as devnull:
                exc_re = (r'tee\(\) failed: tried to move \d+ bytes, but only '
                          r'moved -?\d+')
                try:
                    reader.zero_copy_send(devnull.fileno())
                except Exception as e:
                    self.assertTrue(re.match(exc_re, str(e)))
                else:
                    self.fail('Expected Exception was not raised')

    def test_splice_to_wsockfd_blocks(self):
        if not self._system_can_zero_copy():
            raise unittest.SkipTest("zero-copy support is missing")
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
            self.assertTrue(mock_close.called)
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
        df.manager.use_linkat = False
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
        self.assertNotIn('error', self.logger.all_log_lines())

    def test_create_unlink_cleanup_renamer_fails(self):
        # Test cleanup when renamer fails
        _m_renamer = mock.MagicMock(side_effect=OSError(errno.ENOENT,
                                    os.strerror(errno.ENOENT)))
        _m_unlink = mock.Mock()
        df = self._simple_get_diskfile()
        df.manager.use_linkat = False
        data = b'0' * 100
        metadata = {
            'ETag': md5(data).hexdigest(),
            'X-Timestamp': Timestamp.now().internal,
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
        self.assertFalse(writer._put_succeeded)
        self.assertTrue(_m_renamer.called)
        self.assertTrue(_m_unlink.called)
        self.assertNotIn('error', self.logger.all_log_lines())

    def test_create_unlink_cleanup_logging(self):
        # Test logging of os.unlink() failures.
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz', policy=POLICIES.legacy)
        df.manager.use_linkat = False
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

    @requires_o_tmpfile_support_in_tmp
    def test_get_tempfile_use_linkat_os_open_called(self):
        df = self._simple_get_diskfile()
        self.assertTrue(df.manager.use_linkat)
        _m_mkstemp = mock.MagicMock()
        _m_os_open = mock.Mock(return_value=12345)
        _m_mkc = mock.Mock()
        with mock.patch("swift.obj.diskfile.mkstemp", _m_mkstemp):
            with mock.patch("swift.obj.diskfile.os.open", _m_os_open):
                with mock.patch("swift.obj.diskfile.makedirs_count", _m_mkc):
                    writer = df.writer()
                    fd, tmppath = writer._get_tempfile()
        self.assertTrue(_m_mkc.called)
        flags = O_TMPFILE | os.O_WRONLY
        _m_os_open.assert_called_once_with(df._datadir, flags)
        self.assertIsNone(tmppath)
        self.assertEqual(fd, 12345)
        self.assertFalse(_m_mkstemp.called)

    @requires_o_tmpfile_support_in_tmp
    def test_get_tempfile_fallback_to_mkstemp(self):
        df = self._simple_get_diskfile()
        df._logger = debug_logger()
        self.assertTrue(df.manager.use_linkat)
        for err in (errno.EOPNOTSUPP, errno.EISDIR, errno.EINVAL):
            df.manager.use_linkat = True
            _m_open = mock.Mock(side_effect=OSError(err, os.strerror(err)))
            _m_mkstemp = mock.MagicMock(return_value=(0, "blah"))
            _m_mkc = mock.Mock()
            with mock.patch("swift.obj.diskfile.os.open", _m_open):
                with mock.patch("swift.obj.diskfile.mkstemp", _m_mkstemp):
                    with mock.patch("swift.obj.diskfile.makedirs_count",
                                    _m_mkc):
                        writer = df.writer()
                        fd, tmppath = writer._get_tempfile()
            self.assertTrue(_m_mkc.called)
            # Fallback should succeed and mkstemp() should be called.
            self.assertTrue(_m_mkstemp.called)
            self.assertEqual(tmppath, "blah")
            # Once opening file with O_TMPFILE has failed,
            # failure is cached to not try again
            self.assertFalse(df.manager.use_linkat)
            # Now that we try to use O_TMPFILE all the time, log at debug
            # instead of warning
            log = df.manager.logger.get_lines_for_level('warning')
            self.assertFalse(log)
            log = df.manager.logger.get_lines_for_level('debug')
            self.assertGreater(len(log), 0)
            self.assertTrue('O_TMPFILE' in log[-1])

    @requires_o_tmpfile_support_in_tmp
    def test_get_tmpfile_os_open_other_exceptions_are_raised(self):
        df = self._simple_get_diskfile()
        _m_open = mock.Mock(side_effect=OSError(errno.ENOSPC,
                            os.strerror(errno.ENOSPC)))
        _m_mkstemp = mock.MagicMock()
        _m_mkc = mock.Mock()
        with mock.patch("swift.obj.diskfile.os.open", _m_open):
            with mock.patch("swift.obj.diskfile.mkstemp", _m_mkstemp):
                with mock.patch("swift.obj.diskfile.makedirs_count", _m_mkc):
                    try:
                        writer = df.writer()
                        fd, tmppath = writer._get_tempfile()
                    except OSError as err:
                        self.assertEqual(err.errno, errno.ENOSPC)
                    else:
                        self.fail("Expecting ENOSPC")
        self.assertTrue(_m_mkc.called)
        # mkstemp() should not be invoked.
        self.assertFalse(_m_mkstemp.called)

    @requires_o_tmpfile_support_in_tmp
    def test_create_use_linkat_renamer_not_called(self):
        df = self._simple_get_diskfile()
        data = b'0' * 100
        metadata = {
            'ETag': md5(data).hexdigest(),
            'X-Timestamp': Timestamp.now().internal,
            'Content-Length': str(100),
        }
        _m_renamer = mock.Mock()
        with mock.patch("swift.obj.diskfile.renamer", _m_renamer):
            with df.create(size=100) as writer:
                writer.write(data)
                writer.put(metadata)
                self.assertTrue(writer._put_succeeded)

        self.assertFalse(_m_renamer.called)


@patch_policies(test_policies)
class TestDiskFile(DiskFileMixin, unittest.TestCase):

    mgr_cls = diskfile.DiskFileManager


@patch_policies(with_ec_default=True)
class TestECDiskFile(DiskFileMixin, unittest.TestCase):

    mgr_cls = diskfile.ECDiskFileManager

    def _test_commit_raises_DiskFileError_for_rename_error(self, fake_err):
        df = self._simple_get_diskfile(account='a', container='c',
                                       obj='o_rename_err',
                                       policy=POLICIES.default)
        timestamp = Timestamp.now()
        with df.create() as writer:
            metadata = {
                'ETag': 'bogus_etag',
                'X-Timestamp': timestamp.internal,
                'Content-Length': '0',
            }
            writer.put(metadata)
            with mock.patch('swift.obj.diskfile.os.rename',
                            side_effect=fake_err):
                with self.assertRaises(DiskFileError) as cm:
                    writer.commit(timestamp)
        dl = os.listdir(df._datadir)
        datafile = _make_datafilename(
            timestamp, POLICIES.default, frag_index=2, durable=False)
        self.assertEqual([datafile], dl)
        return df, cm.exception

    def test_commit_raises_DiskFileError_for_rename_ENOSPC_IOError(self):
        df, exc = self._test_commit_raises_DiskFileError_for_rename_error(
            IOError(errno.ENOSPC, 'ENOSPC'))
        self.assertIsInstance(exc, DiskFileNoSpace)
        self.assertIn('No space left on device', str(exc))
        self.assertIn('No space left on device',
                      df.manager.logger.get_lines_for_level('error')[0])
        self.assertFalse(df.manager.logger.get_lines_for_level('error')[1:])

    def test_commit_raises_DiskFileError_for_rename_EDQUOT_IOError(self):
        df, exc = self._test_commit_raises_DiskFileError_for_rename_error(
            IOError(errno.EDQUOT, 'EDQUOT'))
        self.assertIsInstance(exc, DiskFileNoSpace)
        self.assertIn('No space left on device', str(exc))
        self.assertIn('No space left on device',
                      df.manager.logger.get_lines_for_level('error')[0])
        self.assertFalse(df.manager.logger.get_lines_for_level('error')[1:])

    def test_commit_raises_DiskFileError_for_rename_other_IOError(self):
        df, exc = self._test_commit_raises_DiskFileError_for_rename_error(
            IOError(21, 'Some other IO Error'))
        self.assertIn('Problem making data file durable', str(exc))
        self.assertIn('Problem making data file durable',
                      df.manager.logger.get_lines_for_level('error')[0])
        self.assertFalse(df.manager.logger.get_lines_for_level('error')[1:])

    def test_commit_raises_DiskFileError_for_rename_OSError(self):
        df, exc = self._test_commit_raises_DiskFileError_for_rename_error(
            OSError(100, 'Some Error'))
        self.assertIn('Problem making data file durable', str(exc))
        self.assertIn('Problem making data file durable',
                      df.manager.logger.get_lines_for_level('error')[0])
        self.assertFalse(df.manager.logger.get_lines_for_level('error')[1:])

    def _test_commit_raises_DiskFileError_for_fsync_dir_errors(self, fake_err):
        df = self._simple_get_diskfile(account='a', container='c',
                                       obj='o_fsync_dir_err',
                                       policy=POLICIES.default)
        timestamp = Timestamp.now()
        with df.create() as writer:
            metadata = {
                'ETag': 'bogus_etag',
                'X-Timestamp': timestamp.internal,
                'Content-Length': '0',
            }
            writer.put(metadata)
            with mock.patch('swift.obj.diskfile.fsync_dir',
                            side_effect=fake_err):
                with self.assertRaises(DiskFileError) as cm:
                    writer.commit(timestamp)
        dl = os.listdir(df._datadir)
        datafile = _make_datafilename(
            timestamp, POLICIES.default, frag_index=2, durable=True)
        self.assertEqual([datafile], dl)
        self.assertIn('Problem making data file durable', str(cm.exception))
        self.assertIn('Problem making data file durable',
                      df.manager.logger.get_lines_for_level('error')[0])
        self.assertFalse(df.manager.logger.get_lines_for_level('error')[1:])

    def test_commit_raises_DiskFileError_for_fsync_dir_IOError(self):
        self._test_commit_raises_DiskFileError_for_fsync_dir_errors(
            IOError(21, 'Some IO Error'))

    def test_commit_raises_DiskFileError_for_fsync_dir_OSError(self):
        self._test_commit_raises_DiskFileError_for_fsync_dir_errors(
            OSError(100, 'Some Error'))

    def test_data_file_has_frag_index(self):
        policy = POLICIES.default
        for good_value in (0, '0', 2, '2', 13, '13'):
            # frag_index set by constructor arg
            ts = self.ts()
            expected = [_make_datafilename(
                ts, policy, good_value, durable=True)]
            df = self._get_open_disk_file(ts=ts, policy=policy,
                                          frag_index=good_value)
            self.assertEqual(expected, sorted(os.listdir(df._datadir)))
            # frag index should be added to object sysmeta
            actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
            self.assertEqual(int(good_value), int(actual))

            # metadata value overrides the constructor arg
            ts = self.ts()
            expected = [_make_datafilename(
                ts, policy, good_value, durable=True)]
            meta = {'X-Object-Sysmeta-Ec-Frag-Index': good_value}
            df = self._get_open_disk_file(ts=ts, policy=policy,
                                          frag_index='3',
                                          extra_metadata=meta)
            self.assertEqual(expected, sorted(os.listdir(df._datadir)))
            actual = df.get_metadata().get('X-Object-Sysmeta-Ec-Frag-Index')
            self.assertEqual(int(good_value), int(actual))

            # metadata value alone is sufficient
            ts = self.ts()
            expected = [_make_datafilename(
                ts, policy, good_value, durable=True)]
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
        orig_frag_index = 13
        # frag_index set by constructor arg
        ts = self.ts()
        expected = [_make_datafilename(
            ts, policy, frag_index=orig_frag_index, durable=True)]
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
        ts = self.ts()
        metadata = {'X-Timestamp': ts.internal,
                    'X-Object-Meta-Fruit': 'kiwi'}
        df.write_metadata(metadata)
        # sanity check we did write a meta file
        expected.append('%s.meta' % ts.internal)
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
        ts = self.ts()
        metadata = {'X-Timestamp': ts.internal,
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
        for bad_value in ('foo', '-2', -2, '3.14', 3.14, '14', 14, '999'):
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
                    'Etag': MD5_OF_EMPTY_STRING,
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
                    'Etag': MD5_OF_EMPTY_STRING,
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
            write_diskfile(df, ts)
        ts_meta = self.ts()
        df.write_metadata({
            'X-Timestamp': ts_meta.internal,
            'X-Object-Meta-Delete': 'me'
        })

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1#d.data',
            ts.internal + '#2#d.data',
            ts_meta.internal + '.meta',
        ])
        df.purge(ts, 2)
        # by default .meta file is not purged
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1#d.data',
            ts_meta.internal + '.meta',
        ])

    def test_purge_final_fragment_index_and_meta(self):
        ts = self.ts()
        df = self._simple_get_diskfile(frag_index=1)
        write_diskfile(df, ts)
        ts_meta = self.ts()
        df.write_metadata({
            'X-Timestamp': ts_meta.internal,
            'X-Object-Meta-Delete': 'me',
        })

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1#d.data',
            ts_meta.internal + '.meta',
        ])
        df.purge(ts, 1, meta_timestamp=ts_meta)
        self.assertFalse(os.path.exists(df._datadir))

    def test_purge_final_fragment_index_and_not_meta(self):
        ts = self.ts()
        df = self._simple_get_diskfile(frag_index=1)
        write_diskfile(df, ts)
        ts_meta = self.ts()
        df.write_metadata({
            'X-Timestamp': ts_meta.internal,
            'X-Object-Meta-Delete': 'me',
        })

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#1#d.data',
            ts_meta.internal + '.meta',
        ])
        df.purge(ts, 1, meta_timestamp=ts)
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts_meta.internal + '.meta',
        ])

    def test_purge_last_fragment_index(self):
        ts = self.ts()
        frag_index = 0
        df = self._simple_get_diskfile(frag_index=frag_index)
        write_diskfile(df, ts)
        # sanity
        self.assertEqual(os.listdir(df._datadir), [
            ts.internal + '#0#d.data',
        ])
        df.purge(ts, frag_index)
        self.assertFalse(os.path.exists(df._datadir))

    def test_purge_last_fragment_index_legacy_durable(self):
        # a legacy durable file doesn't get purged in case another fragment is
        # relying on it for durability
        ts = self.ts()
        frag_index = 0
        df = self._simple_get_diskfile(frag_index=frag_index)
        write_diskfile(df, ts, legacy_durable=True)
        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '#0.data',
            ts.internal + '.durable',
        ])
        df.purge(ts, frag_index)
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '.durable',
        ])

    def test_purge_non_existent_fragment_index(self):
        ts = self.ts()
        frag_index = 7
        df = self._simple_get_diskfile(frag_index=frag_index)
        write_diskfile(df, ts)

        # sanity
        self.assertEqual(os.listdir(df._datadir), [
            ts.internal + '#7#d.data',
        ])
        df.purge(ts, 3)
        # no effect
        self.assertEqual(os.listdir(df._datadir), [
            ts.internal + '#7#d.data',
        ])

    def test_purge_old_timestamp_frag_index(self):
        old_ts = self.ts()
        ts = self.ts()
        frag_index = 1
        df = self._simple_get_diskfile(frag_index=frag_index)
        write_diskfile(df, ts)

        # sanity
        self.assertEqual(os.listdir(df._datadir), [
            ts.internal + '#1#d.data',
        ])
        df.purge(old_ts, 1)
        # no effect
        self.assertEqual(os.listdir(df._datadir), [
            ts.internal + '#1#d.data',
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
        self.assertFalse(os.path.exists(df._datadir))

    def test_purge_without_frag(self):
        ts = self.ts()
        df = self._simple_get_diskfile()
        df.delete(ts)

        # sanity
        self.assertEqual(sorted(os.listdir(df._datadir)), [
            ts.internal + '.ts',
        ])
        df.purge(ts, None)
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
        # the directory was empty and has been removed
        self.assertFalse(os.path.exists(df._datadir))

    def _do_test_open_most_recent_durable(self, legacy_durable):
        policy = POLICIES.default
        df_mgr = self.df_router[policy]

        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)

        ts = self.ts()
        write_diskfile(df, ts, frag_index=3,
                       legacy_durable=legacy_durable)
        metadata = {
            'ETag': md5('test data').hexdigest(),
            'X-Timestamp': ts.internal,
            'Content-Length': str(len('test data')),
            'X-Object-Sysmeta-Ec-Etag': 'fake-etag',
            'X-Object-Sysmeta-Ec-Frag-Index': '3',
        }

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
        write_diskfile(df, ts, frag_index=3, commit=False,
                       legacy_durable=legacy_durable)
        # N.B. don't make it durable

        # and we still get the old metadata (same as if no .data!)
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        self.assertEqual(metadata, df.read_metadata())

    def test_open_most_recent_durable(self):
        self._do_test_open_most_recent_durable(False)

    def test_open_most_recent_durable_legacy(self):
        self._do_test_open_most_recent_durable(True)

    def test_open_most_recent_missing_durable(self):
        policy = POLICIES.default
        df_mgr = self.df_router[policy]

        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)

        self.assertRaises(DiskFileNotExist, df.read_metadata)

        # now create a datafile missing durable
        ts = self.ts()
        write_diskfile(df, ts, frag_index=3, commit=False)
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

        # sanity, without the frag_index kwarg
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        self.assertRaises(DiskFileNotExist, df.read_metadata)

    def test_fragments(self):
        ts_1 = self.ts()
        self._get_open_disk_file(ts=ts_1.internal, frag_index=0)
        df = self._get_open_disk_file(ts=ts_1.internal, frag_index=2)
        self.assertEqual(df.fragments, {ts_1: [0, 2]})

        # now add a newer datafile for frag index 3 but don't write a
        # durable with it (so ignore the error when we try to open)
        ts_2 = self.ts()
        try:
            df = self._get_open_disk_file(ts=ts_2.internal, frag_index=3,
                                          commit=False)
        except DiskFileNotExist:
            pass

        # sanity check: should have 3* .data
        files = os.listdir(df._datadir)
        self.assertEqual(3, len(files))
        with df.open():
            self.assertEqual(df.fragments, {ts_1: [0, 2], ts_2: [3]})

    def test_fragments_available_when_not_durable(self):
        # verify frags available even if open fails e.g. if none are durable
        ts_1 = self.ts()
        ts_2 = self.ts()
        for ts, fi in ((ts_1, 0), (ts_1, 2), (ts_2, 3)):
            try:
                df = self._get_open_disk_file(
                    ts=ts, frag_index=fi, commit=False)
            except DiskFileNotExist:
                pass
        df = self._simple_get_diskfile()

        # sanity check: should have 3* .data
        files = os.listdir(df._datadir)
        self.assertEqual(3, len(files))
        self.assertRaises(DiskFileNotExist, df.open)
        self.assertEqual(df.fragments, {ts_1: [0, 2], ts_2: [3]})

    def test_fragments_not_open(self):
        df = self._simple_get_diskfile()
        self.assertIsNone(df.fragments)

    def test_durable_timestamp_when_not_durable(self):
        try:
            self._get_open_disk_file(self.ts().internal, commit=False)
        except DiskFileNotExist:
            pass
        df = self._simple_get_diskfile()
        with self.assertRaises(DiskFileNotExist):
            df.open()
        # open() was attempted, but no durable file so expect None
        self.assertIsNone(df.durable_timestamp)

    def test_durable_timestamp_missing_frag_index(self):
        ts1 = self.ts()
        self._get_open_disk_file(ts=ts1.internal, frag_index=1)
        df = self._simple_get_diskfile(frag_index=2)
        with self.assertRaises(DiskFileNotExist):
            df.open()
        # open() was attempted, but no data file for frag index so expect None
        self.assertIsNone(df.durable_timestamp)

    def test_durable_timestamp_newer_non_durable_data_file(self):
        ts1 = self.ts()
        self._get_open_disk_file(ts=ts1.internal)
        ts2 = self.ts()
        try:
            self._get_open_disk_file(ts=ts2.internal, commit=False)
        except DiskFileNotExist:
            pass
        df = self._simple_get_diskfile()
        # sanity check - two .data files
        self.assertEqual(2, len(os.listdir(df._datadir)))
        df.open()
        self.assertEqual(ts1, df.durable_timestamp)

    def test_durable_timestamp_legacy_durable(self):
        self._do_test_durable_timestamp(True)

    def _test_open_with_fragment_preferences(self, legacy_durable=False):
        policy = POLICIES.default
        df_mgr = self.df_router[policy]

        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)

        ts_1, ts_2, ts_3, ts_4 = (self.ts() for _ in range(4))

        # create two durable frags, first with index 0
        frag_0_metadata = write_diskfile(df, ts_1, frag_index=0,
                                         legacy_durable=legacy_durable)

        # second with index 3
        frag_3_metadata = write_diskfile(df, ts_1, frag_index=3,
                                         legacy_durable=legacy_durable)

        # sanity check: should have 2 * .data plus possibly a .durable
        self.assertEqual(3 if legacy_durable else 2,
                         len(os.listdir(df._datadir)))

        # add some .meta stuff
        meta_1_metadata = {
            'X-Object-Meta-Foo': 'Bar',
            'X-Timestamp': ts_2.internal,
        }
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        df.write_metadata(meta_1_metadata)
        # sanity check: should have 2 * .data, possibly .durable, .meta
        self.assertEqual(4 if legacy_durable else 3,
                         len(os.listdir(df._datadir)))

        # sanity: should get frag index 3
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        expected = dict(frag_3_metadata)
        expected.update(meta_1_metadata)
        self.assertEqual(expected, df.read_metadata())

        # add a newer datafile for frag index 2
        # N.B. don't make it durable - skip call to commit()
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        frag_2_metadata = write_diskfile(df, ts_3, frag_index=2, commit=False,
                                         data=b'new test data',
                                         legacy_durable=legacy_durable)
        # sanity check: should have 2* .data, possibly .durable, .meta, .data
        self.assertEqual(5 if legacy_durable else 4,
                         len(os.listdir(df._datadir)))

        # sanity check: with no frag preferences we get old metadata
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy)
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_2.internal, df.timestamp)
        self.assertEqual(ts_1.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # with empty frag preferences we get metadata from newer non-durable
        # data file
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy, frag_prefs=[])
        self.assertEqual(frag_2_metadata, df.read_metadata())
        self.assertEqual(ts_3.internal, df.timestamp)
        self.assertEqual(ts_3.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # check we didn't destroy any potentially valid data by opening the
        # non-durable data file
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy)
        self.assertEqual(expected, df.read_metadata())

        # now add some newer .meta stuff which should replace older .meta
        meta_2_metadata = {
            'X-Object-Meta-Foo': 'BarBarBarAnne',
            'X-Timestamp': ts_4.internal,
        }
        df = df_mgr.get_diskfile(self.existing_device, '0',
                                 'a', 'c', 'o', policy=policy)
        df.write_metadata(meta_2_metadata)
        # sanity check: should have 2 * .data, possibly .durable, .data, .meta
        self.assertEqual(5 if legacy_durable else 4,
                         len(os.listdir(df._datadir)))

        # sanity check: with no frag preferences we get newer metadata applied
        # to durable data file
        expected = dict(frag_3_metadata)
        expected.update(meta_2_metadata)
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy)
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_4.internal, df.timestamp)
        self.assertEqual(ts_1.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # with empty frag preferences we still get metadata from newer .meta
        # but applied to non-durable data file
        expected = dict(frag_2_metadata)
        expected.update(meta_2_metadata)
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy, frag_prefs=[])
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_4.internal, df.timestamp)
        self.assertEqual(ts_3.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # check we didn't destroy any potentially valid data by opening the
        # non-durable data file
        expected = dict(frag_3_metadata)
        expected.update(meta_2_metadata)
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy)
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_4.internal, df.timestamp)
        self.assertEqual(ts_1.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # prefer frags at ts_1, exclude no indexes, expect highest frag index
        prefs = [{'timestamp': ts_1.internal, 'exclude': []},
                 {'timestamp': ts_2.internal, 'exclude': []},
                 {'timestamp': ts_3.internal, 'exclude': []}]
        expected = dict(frag_3_metadata)
        expected.update(meta_2_metadata)
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy, frag_prefs=prefs)
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_4.internal, df.timestamp)
        self.assertEqual(ts_1.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # prefer frags at ts_1, exclude frag index 3 so expect frag index 0
        prefs = [{'timestamp': ts_1.internal, 'exclude': [3]},
                 {'timestamp': ts_2.internal, 'exclude': []},
                 {'timestamp': ts_3.internal, 'exclude': []}]
        expected = dict(frag_0_metadata)
        expected.update(meta_2_metadata)
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy, frag_prefs=prefs)
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_4.internal, df.timestamp)
        self.assertEqual(ts_1.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # now make ts_3 the preferred timestamp, excluded indexes don't exist
        prefs = [{'timestamp': ts_3.internal, 'exclude': [4, 5, 6]},
                 {'timestamp': ts_2.internal, 'exclude': []},
                 {'timestamp': ts_1.internal, 'exclude': []}]
        expected = dict(frag_2_metadata)
        expected.update(meta_2_metadata)
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy, frag_prefs=prefs)
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_4.internal, df.timestamp)
        self.assertEqual(ts_3.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

        # now make ts_2 the preferred timestamp - there are no frags at ts_2,
        # next preference is ts_3 but index 2 is excluded, then at ts_1 index 3
        # is excluded so we get frag 0 at ts_1
        prefs = [{'timestamp': ts_2.internal, 'exclude': [1]},
                 {'timestamp': ts_3.internal, 'exclude': [2]},
                 {'timestamp': ts_1.internal, 'exclude': [3]}]

        expected = dict(frag_0_metadata)
        expected.update(meta_2_metadata)
        df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                 policy=policy, frag_prefs=prefs)
        self.assertEqual(expected, df.read_metadata())
        self.assertEqual(ts_4.internal, df.timestamp)
        self.assertEqual(ts_1.internal, df.data_timestamp)
        self.assertEqual(ts_1.internal, df.durable_timestamp)
        self.assertEqual({ts_1: [0, 3], ts_3: [2]}, df.fragments)

    def test_open_with_fragment_preferences_legacy_durable(self):
        self._test_open_with_fragment_preferences(legacy_durable=True)

    def test_open_with_fragment_preferences(self):
        self._test_open_with_fragment_preferences(legacy_durable=False)

    def test_open_with_bad_fragment_preferences(self):
        policy = POLICIES.default
        df_mgr = self.df_router[policy]

        for bad in (
            'ouch',
            2,
            [{'timestamp': '1234.5678', 'excludes': [1]}, {}],
            [{'timestamp': 'not a timestamp', 'excludes': [1, 2]}],
            [{'timestamp': '1234.5678', 'excludes': [1, -1]}],
            [{'timestamp': '1234.5678', 'excludes': 1}],
            [{'timestamp': '1234.5678'}],
            [{'excludes': [1, 2]}]

        ):
            try:
                df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                    policy=policy, frag_prefs=bad)
                self.fail('Expected DiskFileError for bad frag_prefs: %r'
                          % bad)
            except DiskFileError as e:
                self.assertIn('frag_prefs', str(e))

    def test_disk_file_app_iter_ranges_checks_only_aligned_frag_data(self):
        policy = POLICIES.default
        frag_size = policy.fragment_size
        # make sure there are two fragment size worth of data on disk
        data = b'ab' * policy.ec_segment_size
        df, df_data = self._create_test_file(data)
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        # each range uses a fresh reader app_iter_range which triggers a disk
        # read at the range offset - make sure each of those disk reads will
        # fetch an amount of data from disk that is greater than but not equal
        # to a fragment size
        reader._disk_chunk_size = int(frag_size * 1.5)
        with mock.patch.object(
                reader._diskfile.policy.pyeclib_driver, 'get_metadata')\
                as mock_get_metadata:
            it = reader.app_iter_ranges(
                [(0, 10), (10, 20),
                 (frag_size + 20, frag_size + 30)],
                'plain/text', '\r\n--someheader\r\n', len(df_data))
            value = b''.join(it)
        # check that only first range which starts at 0 triggers a frag check
        self.assertEqual(1, mock_get_metadata.call_count)
        self.assertIn(df_data[:10], value)
        self.assertIn(df_data[10:20], value)
        self.assertIn(df_data[frag_size + 20:frag_size + 30], value)
        self.assertEqual(quarantine_msgs, [])

    def test_reader_quarantines_corrupted_ec_archive(self):
        # This has same purpose as
        # TestAuditor.test_object_audit_checks_EC_fragments just making
        # sure that checks happen in DiskFileReader layer.
        policy = POLICIES.default
        df, df_data = self._create_test_file(b'x' * policy.ec_segment_size,
                                             timestamp=self.ts())

        def do_test(corrupted_frag_body, expected_offset, expected_read):
            # expected_offset is offset at which corruption should be reported
            # expected_read is number of bytes that should be read before the
            # exception is raised
            ts = self.ts()
            write_diskfile(df, ts, corrupted_frag_body)

            # at the open for the diskfile, no error occurred
            # reading first corrupt frag is sufficient to detect the corruption
            df.open()
            with self.assertRaises(DiskFileQuarantined) as cm:
                reader = df.reader()
                reader._disk_chunk_size = int(policy.fragment_size)
                bytes_read = 0
                for chunk in reader:
                    bytes_read += len(chunk)

            with self.assertRaises(DiskFileNotExist):
                df.open()

            self.assertEqual(expected_read, bytes_read)
            self.assertEqual('Invalid EC metadata at offset 0x%x' %
                             expected_offset, cm.exception.args[0])

        # TODO with liberasurecode < 1.2.0 the EC metadata verification checks
        # only the magic number at offset 59 bytes into the frag so we'll
        # corrupt up to and including that. Once liberasurecode >= 1.2.0 is
        # required we should be able to reduce the corruption length.
        corruption_length = 64
        # corrupted first frag can be detected
        corrupted_frag_body = (b' ' * corruption_length +
                               df_data[corruption_length:])
        do_test(corrupted_frag_body, 0, 0)

        # corrupted the second frag can be also detected
        corrupted_frag_body = (df_data + b' ' * corruption_length +
                               df_data[corruption_length:])
        do_test(corrupted_frag_body, len(df_data), len(df_data))

        # if the second frag is shorter than frag size then corruption is
        # detected when the reader is closed
        corrupted_frag_body = (df_data + b' ' * corruption_length +
                               df_data[corruption_length:-10])
        do_test(corrupted_frag_body, len(df_data), len(corrupted_frag_body))

    def test_reader_ec_exception_causes_quarantine(self):
        policy = POLICIES.default

        def do_test(exception):
            df, df_data = self._create_test_file(b'x' * policy.ec_segment_size,
                                                 timestamp=self.ts())
            df.manager.logger.clear()

            with mock.patch.object(df.policy.pyeclib_driver, 'get_metadata',
                                   side_effect=exception):
                df.open()
                with self.assertRaises(DiskFileQuarantined) as cm:
                    for chunk in df.reader():
                        pass

            with self.assertRaises(DiskFileNotExist):
                df.open()

            self.assertEqual('Invalid EC metadata at offset 0x0',
                             cm.exception.args[0])
            log_lines = df.manager.logger.get_lines_for_level('warning')
            self.assertIn('Quarantined object', log_lines[0])
            self.assertIn('Invalid EC metadata at offset 0x0', log_lines[0])

        do_test(pyeclib.ec_iface.ECInvalidFragmentMetadata('testing'))
        do_test(pyeclib.ec_iface.ECBadFragmentChecksum('testing'))
        do_test(pyeclib.ec_iface.ECInvalidParameter('testing'))

    def test_reader_ec_exception_does_not_cause_quarantine(self):
        # ECDriverError should not cause quarantine, only certain subclasses
        policy = POLICIES.default

        df, df_data = self._create_test_file(b'x' * policy.ec_segment_size,
                                             timestamp=self.ts())

        with mock.patch.object(
                df.policy.pyeclib_driver, 'get_metadata',
                side_effect=pyeclib.ec_iface.ECDriverError('testing')):
            df.open()
            read_data = b''.join([d for d in df.reader()])
        self.assertEqual(df_data, read_data)
        log_lines = df.manager.logger.get_lines_for_level('warning')
        self.assertIn('Problem checking EC fragment', log_lines[0])

        df.open()  # not quarantined

    def test_reader_frag_check_does_not_quarantine_if_its_not_binary(self):
        # This may look weird but for super-safety, check the
        # ECDiskFileReader._frag_check doesn't quarantine when non-binary
        # type chunk incomming (that would occurre only from coding bug)
        policy = POLICIES.default

        df, df_data = self._create_test_file(b'x' * policy.ec_segment_size,
                                             timestamp=self.ts())
        df.open()
        for invalid_type_chunk in (None, [], [[]], 1):
            reader = df.reader()
            reader._check_frag(invalid_type_chunk)

        # None and [] are just skipped and [[]] and 1 are detected as invalid
        # chunks
        log_lines = df.manager.logger.get_lines_for_level('warning')
        self.assertEqual(2, len(log_lines))
        for log_line in log_lines:
            self.assertIn(
                'Unexpected fragment data type (not quarantined)', log_line)

        df.open()  # not quarantined

    def test_ondisk_data_info_has_durable_key(self):
        # non-durable; use frag_prefs=[] to allow it to be opened
        df = self._simple_get_diskfile(obj='o1', frag_prefs=[])
        self._create_ondisk_file(df, b'', ext='.data', timestamp=10,
                                 metadata={'name': '/a/c/o1'}, commit=False)
        with df.open():
            self.assertIn('durable', df._ondisk_info['data_info'])
            self.assertFalse(df._ondisk_info['data_info']['durable'])

        # durable
        df = self._simple_get_diskfile(obj='o2')
        self._create_ondisk_file(df, b'', ext='.data', timestamp=10,
                                 metadata={'name': '/a/c/o2'})
        with df.open():
            self.assertIn('durable', df._ondisk_info['data_info'])
            self.assertTrue(df._ondisk_info['data_info']['durable'])

        # legacy durable
        df = self._simple_get_diskfile(obj='o3')
        self._create_ondisk_file(df, b'', ext='.data', timestamp=10,
                                 metadata={'name': '/a/c/o3'},
                                 legacy_durable=True)
        with df.open():
            data_info = df._ondisk_info['data_info']
            # sanity check it is legacy with no #d part in filename
            self.assertEqual(data_info['filename'], '0000000010.00000#2.data')
            self.assertIn('durable', data_info)
            self.assertTrue(data_info['durable'])


@patch_policies(with_ec_default=True)
class TestSuffixHashes(unittest.TestCase):
    """
    This tests all things related to hashing suffixes and therefore
    there's also few test methods for cleanup_ondisk_files as well
    (because it's used by hash_suffix).

    The public interface to suffix hashing is on the Manager::

         * cleanup_ondisk_files(hsh_path)
         * get_hashes(device, partition, suffixes, policy)
         * invalidate_hash(suffix_dir)

    The Manager.get_hashes method (used by the REPLICATE verb)
    calls Manager._get_hashes (which may be an alias to the module
    method get_hashes), which calls hash_suffix, which calls
    cleanup_ondisk_files.

    Outside of that, cleanup_ondisk_files and invalidate_hash are
    used mostly after writing new files via PUT or DELETE.

    Test methods are organized by::

        * cleanup_ondisk_files tests - behaviors
        * cleanup_ondisk_files tests - error handling
        * invalidate_hash tests - behavior
        * invalidate_hash tests - error handling
        * get_hashes tests - hash_suffix behaviors
        * get_hashes tests - hash_suffix error handling
        * get_hashes tests - behaviors
        * get_hashes tests - error handling

    """

    def setUp(self):
        skip_if_no_xattrs()
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

    @contextmanager
    def policy_in_message(self):
        try:
            yield
        except AssertionError as err:
            if not self.policy:
                raise
            policy_trailer = '\n\n... for policy %r' % self.policy
            raise AssertionError(str(err) + policy_trailer)

    def assertEqual(self, *args):
        with self.policy_in_message():
            unittest.TestCase.assertEqual(self, *args)

    def get_different_suffix_df(self, df, **kwargs):
        # returns diskfile in the same partition with different suffix
        suffix_dir = os.path.dirname(df._datadir)
        for i in itertools.count():
            df2 = df._manager.get_diskfile(
                os.path.basename(df._device_path),
                df._datadir.split('/')[-3],
                df._account,
                df._container,
                'o%d' % i,
                policy=df.policy,
                **kwargs)
            suffix_dir2 = os.path.dirname(df2._datadir)
            if suffix_dir != suffix_dir2:
                return df2

    def test_valid_suffix(self):
        self.assertTrue(diskfile.valid_suffix(u'000'))
        self.assertTrue(diskfile.valid_suffix('000'))
        self.assertTrue(diskfile.valid_suffix('123'))
        self.assertTrue(diskfile.valid_suffix('fff'))
        self.assertFalse(diskfile.valid_suffix(list('123')))
        self.assertFalse(diskfile.valid_suffix(123))
        self.assertFalse(diskfile.valid_suffix(' 12'))
        self.assertFalse(diskfile.valid_suffix('-00'))
        self.assertFalse(diskfile.valid_suffix(u'-00'))
        self.assertFalse(diskfile.valid_suffix('1234'))

    def check_cleanup_ondisk_files(self, policy, input_files, output_files):
        orig_unlink = os.unlink
        file_list = list(input_files)
        rmdirs = []

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
        with unit_mock({'os.listdir': mock_listdir, 'os.unlink': mock_unlink,
                        'os.rmdir': rmdirs.append}):
            if isinstance(output_files, Exception):
                path = os.path.join(self.testdir, 'does-not-matter')
                self.assertRaises(output_files.__class__,
                                  df_mgr.cleanup_ondisk_files, path)
                return
            df_mgr.commit_window = 0
            files = df_mgr.cleanup_ondisk_files('/whatever')['files']
            self.assertEqual(files, output_files)
            if files:
                self.assertEqual(rmdirs, [])
            else:
                self.assertEqual(rmdirs, ['/whatever'])

    # cleanup_ondisk_files tests - behaviors

    def test_cleanup_ondisk_files_purge_data_newer_ts(self):
        for policy in self.iter_policies():
            # purge .data if there's a newer .ts
            file1 = _make_datafilename(self.ts(), policy)
            file2 = self.ts().internal + '.ts'
            file_list = [file1, file2]
            self.check_cleanup_ondisk_files(policy, file_list, [file2])

    def test_cleanup_ondisk_files_purge_expired_ts(self):
        for policy in self.iter_policies():
            # purge older .ts files if there's a newer .data
            file1 = self.ts().internal + '.ts'
            file2 = self.ts().internal + '.ts'
            timestamp = self.ts()
            file3 = _make_datafilename(timestamp, policy, durable=False)
            file_list = [file1, file2, file3]
            expected = {
                # no durable datafile means you can't get rid of the
                # latest tombstone even if datafile is newer
                EC_POLICY: [file3, file2],
                REPL_POLICY: [file3],
            }[policy.policy_type]
            self.check_cleanup_ondisk_files(policy, file_list, expected)

    def _do_test_cleanup_ondisk_files_purge_ts_newer_data(
            self, policy, legacy_durable=False):
        # purge .ts if there's a newer .data
        file1 = self.ts().internal + '.ts'
        timestamp = self.ts()
        file2 = _make_datafilename(
            timestamp, policy, durable=not legacy_durable)
        file_list = [file1, file2]
        expected = [file2]
        if policy.policy_type == EC_POLICY and legacy_durable:
            durable_file = timestamp.internal + '.durable'
            file_list.append(durable_file)
            expected.insert(0, durable_file)
        self.check_cleanup_ondisk_files(policy, file_list, expected)

    def test_cleanup_ondisk_files_purge_ts_newer_data(self):
        for policy in self.iter_policies():
            self._do_test_cleanup_ondisk_files_purge_ts_newer_data(policy)

    def test_cleanup_ondisk_files_purge_ts_newer_data_and_legacy_durable(self):
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                self._do_test_cleanup_ondisk_files_purge_ts_newer_data(
                    policy, legacy_durable=True)

    def test_cleanup_ondisk_files_purge_older_ts(self):
        for policy in self.iter_policies():
            file1 = self.ts().internal + '.ts'
            file2 = self.ts().internal + '.ts'
            file3 = _make_datafilename(self.ts(), policy, durable=False)
            file4 = self.ts().internal + '.meta'
            expected = {
                # no durable means we can only throw out things before
                # the latest tombstone
                EC_POLICY: [file4, file3, file2],
                # keep .meta and .data and purge all .ts files
                REPL_POLICY: [file4, file3],
            }[policy.policy_type]
            file_list = [file1, file2, file3, file4]
            self.check_cleanup_ondisk_files(policy, file_list, expected)

    def _do_test_cleanup_ondisk_files_keep_meta_data_purge_ts(
            self, policy, legacy_durable=False):
        file1 = self.ts().internal + '.ts'
        file2 = self.ts().internal + '.ts'
        timestamp = self.ts()
        file3 = _make_datafilename(
            timestamp, policy, durable=not legacy_durable)
        file_list = [file1, file2, file3]
        expected = [file3]
        if policy.policy_type == EC_POLICY and legacy_durable:
            durable_filename = timestamp.internal + '.durable'
            file_list.append(durable_filename)
            expected.insert(0, durable_filename)
        file4 = self.ts().internal + '.meta'
        file_list.append(file4)
        expected.insert(0, file4)
        # keep .meta and .data if meta newer than data and purge .ts
        self.check_cleanup_ondisk_files(policy, file_list, expected)

    def test_cleanup_ondisk_files_keep_meta_data_purge_ts(self):
        for policy in self.iter_policies():
            self._do_test_cleanup_ondisk_files_keep_meta_data_purge_ts(policy)

    def test_cleanup_ondisk_files_keep_meta_data_purge_ts_legacy_durable(self):
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                self._do_test_cleanup_ondisk_files_keep_meta_data_purge_ts(
                    policy, legacy_durable=True)

    def test_cleanup_ondisk_files_keep_one_ts(self):
        for policy in self.iter_policies():
            file1, file2, file3 = [self.ts().internal + '.ts'
                                   for i in range(3)]
            file_list = [file1, file2, file3]
            # keep only latest of multiple .ts files
            self.check_cleanup_ondisk_files(policy, file_list, [file3])

    def test_cleanup_ondisk_files_multi_data_file(self):
        for policy in self.iter_policies():
            file1 = _make_datafilename(self.ts(), policy, 1, durable=False)
            file2 = _make_datafilename(self.ts(), policy, 2, durable=False)
            file3 = _make_datafilename(self.ts(), policy, 3, durable=False)
            expected = {
                # keep all non-durable datafiles
                EC_POLICY: [file3, file2, file1],
                # keep only latest of multiple .data files
                REPL_POLICY: [file3]
            }[policy.policy_type]
            file_list = [file1, file2, file3]
            self.check_cleanup_ondisk_files(policy, file_list, expected)

    def _do_test_cleanup_ondisk_files_keeps_one_datafile(self, policy,
                                                         legacy_durable=False):
        timestamps = [self.ts() for i in range(3)]
        file1 = _make_datafilename(timestamps[0], policy, 1,
                                   durable=not legacy_durable)
        file2 = _make_datafilename(timestamps[1], policy, 2,
                                   durable=not legacy_durable)
        file3 = _make_datafilename(timestamps[2], policy, 3,
                                   durable=not legacy_durable)
        file_list = [file1, file2, file3]
        expected = [file3]
        if policy.policy_type == EC_POLICY and legacy_durable:
            for t in timestamps:
                file_list.append(t.internal + '.durable')
            expected.insert(0, file_list[-1])
        self.check_cleanup_ondisk_files(policy, file_list, expected)

    def test_cleanup_ondisk_files_keeps_one_datafile(self):
        for policy in self.iter_policies():
            self._do_test_cleanup_ondisk_files_keeps_one_datafile(policy)

    def test_cleanup_ondisk_files_keeps_one_datafile_and_legacy_durable(self):
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                self._do_test_cleanup_ondisk_files_keeps_one_datafile(
                    policy, legacy_durable=True)

    def _do_test_cleanup_ondisk_files_keep_one_meta(self, policy,
                                                    legacy_durable=False):
        # keep only latest of multiple .meta files
        t_data = self.ts()
        file1 = _make_datafilename(t_data, policy, durable=not legacy_durable)
        file2, file3 = [self.ts().internal + '.meta' for i in range(2)]
        file_list = [file1, file2, file3]
        expected = [file3, file1]
        if policy.policy_type == EC_POLICY and legacy_durable:
            durable_file = t_data.internal + '.durable'
            file_list.append(durable_file)
            expected.insert(1, durable_file)
        self.check_cleanup_ondisk_files(policy, file_list, expected)

    def test_cleanup_ondisk_files_keep_one_meta(self):
        for policy in self.iter_policies():
            self._do_test_cleanup_ondisk_files_keep_one_meta(policy)

    def test_cleanup_ondisk_files_keep_one_meta_legacy_durable(self):
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                self._do_test_cleanup_ondisk_files_keep_one_meta(
                    policy, legacy_durable=True)

    def test_cleanup_ondisk_files_only_meta(self):
        for policy in self.iter_policies():
            file1, file2 = [self.ts().internal + '.meta' for i in range(2)]
            file_list = [file1, file2]
            self.check_cleanup_ondisk_files(policy, file_list, [file2])

    def test_cleanup_ondisk_files_ignore_orphaned_ts(self):
        for policy in self.iter_policies():
            # A more recent orphaned .meta file will prevent old .ts files
            # from being cleaned up otherwise
            file1, file2 = [self.ts().internal + '.ts' for i in range(2)]
            file3 = self.ts().internal + '.meta'
            file_list = [file1, file2, file3]
            self.check_cleanup_ondisk_files(policy, file_list, [file3, file2])

    def test_cleanup_ondisk_files_purge_old_data_only(self):
        for policy in self.iter_policies():
            # Oldest .data will be purge, .meta and .ts won't be touched
            file1 = _make_datafilename(self.ts(), policy)
            file2 = self.ts().internal + '.ts'
            file3 = self.ts().internal + '.meta'
            file_list = [file1, file2, file3]
            self.check_cleanup_ondisk_files(policy, file_list, [file3, file2])

    def test_cleanup_ondisk_files_purge_old_ts(self):
        for policy in self.iter_policies():
            # A single old .ts file will be removed
            old_float = time() - (diskfile.DEFAULT_RECLAIM_AGE + 1)
            file1 = Timestamp(old_float).internal + '.ts'
            file_list = [file1]
            self.check_cleanup_ondisk_files(policy, file_list, [])

    def test_cleanup_ondisk_files_keep_isolated_meta_purge_old_ts(self):
        for policy in self.iter_policies():
            # A single old .ts file will be removed despite presence of a .meta
            old_float = time() - (diskfile.DEFAULT_RECLAIM_AGE + 1)
            file1 = Timestamp(old_float).internal + '.ts'
            file2 = Timestamp(time() + 2).internal + '.meta'
            file_list = [file1, file2]
            self.check_cleanup_ondisk_files(policy, file_list, [file2])

    def test_cleanup_ondisk_files_keep_single_old_data(self):
        for policy in self.iter_policies():
            old_float = time() - (diskfile.DEFAULT_RECLAIM_AGE + 1)
            file1 = _make_datafilename(
                Timestamp(old_float), policy, durable=True)
            file_list = [file1]
            self.check_cleanup_ondisk_files(policy, file_list, file_list)

    def test_cleanup_ondisk_drops_old_non_durable_data(self):
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                old_float = time() - (diskfile.DEFAULT_RECLAIM_AGE + 1)
                file1 = _make_datafilename(
                    Timestamp(old_float), policy, durable=False)
                file_list = [file1]
                # for EC an isolated old non-durable .data file is removed
                expected = []
                self.check_cleanup_ondisk_files(policy, file_list, expected)

    def test_cleanup_ondisk_files_drops_isolated_durable(self):
        # check behaviour for legacy durable files
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                file1 = Timestamp.now().internal + '.durable'
                file_list = [file1]
                self.check_cleanup_ondisk_files(policy, file_list, [])

    def test_cleanup_ondisk_files_purges_single_old_meta(self):
        for policy in self.iter_policies():
            # A single old .meta file will be removed
            old_float = time() - (diskfile.DEFAULT_RECLAIM_AGE + 1)
            file1 = Timestamp(old_float).internal + '.meta'
            file_list = [file1]
            self.check_cleanup_ondisk_files(policy, file_list, [])

    # cleanup_ondisk_files tests - error handling

    def test_cleanup_ondisk_files_hsh_path_enoent(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # common.utils.listdir *completely* mutes ENOENT
            path = os.path.join(self.testdir, 'does-not-exist')
            self.assertEqual(df_mgr.cleanup_ondisk_files(path)['files'], [])

    def test_cleanup_ondisk_files_hsh_path_other_oserror(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            with mock.patch('os.listdir') as mock_listdir:
                mock_listdir.side_effect = OSError('kaboom!')
                # but it will raise other OSErrors
                path = os.path.join(self.testdir, 'does-not-matter')
                self.assertRaises(OSError, df_mgr.cleanup_ondisk_files,
                                  path)

    def test_cleanup_ondisk_files_reclaim_tombstone_remove_file_error(self):
        for policy in self.iter_policies():
            # Timestamp 1 makes the check routine pretend the file
            # disappeared after listdir before unlink.
            file1 = '0000000001.00000.ts'
            file_list = [file1]
            self.check_cleanup_ondisk_files(policy, file_list, [])

    def test_cleanup_ondisk_files_older_remove_file_error(self):
        for policy in self.iter_policies():
            # Timestamp 1 makes the check routine pretend the file
            # disappeared after listdir before unlink.
            file1 = _make_datafilename(Timestamp(1), policy)
            file2 = '0000000002.00000.ts'
            file_list = [file1, file2]
            self.check_cleanup_ondisk_files(policy, file_list, [])

    # invalidate_hash tests - behavior

    def test_invalidate_hash_file_does_not_exist(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path, diskfile.HASH_FILE)
            inv_file = os.path.join(
                part_path, diskfile.HASH_INVALIDATIONS_FILE)
            # sanity, new partition has no suffix hashing artifacts
            self.assertFalse(os.path.exists(hashes_file))
            self.assertFalse(os.path.exists(inv_file))
            # invalidating a hash does not create the hashes_file
            with mock.patch(
                    'swift.obj.diskfile.BaseDiskFileManager.invalidate_hash',
                    side_effect=diskfile.invalidate_hash) \
                    as mock_invalidate_hash:
                df.delete(self.ts())
            self.assertFalse(os.path.exists(hashes_file))
            # ... but does invalidate the suffix
            self.assertEqual([mock.call(suffix_dir)],
                             mock_invalidate_hash.call_args_list)
            with open(inv_file) as f:
                self.assertEqual(suffix, f.read().strip('\n'))
            # ... and hashing suffixes finds (and hashes) the new suffix
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertIn(suffix, hashes)
            self.assertTrue(os.path.exists(hashes_file))
            self.assertIn(os.path.basename(suffix_dir), hashes)
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                found_hashes.pop('updated')
                self.assertTrue(found_hashes.pop('valid'))
                self.assertEqual(hashes, found_hashes)
            # ... and truncates the invalidations file
            with open(inv_file) as f:
                self.assertEqual('', f.read().strip('\n'))

    def test_invalidate_hash_empty_file_exists(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            mkdirs(part_path)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            pkl_path = os.path.join(part_path, diskfile.HASH_FILE)
            self.assertTrue(os.path.exists(pkl_path))
            self.assertEqual(hashes, {})
            # create something to hash
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            df.delete(self.ts())
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertIn(suffix, hashes)  # sanity

    def test_invalidate_hash_file_not_truncated_when_empty(self):
        orig_open = open

        def watch_open(*args, **kargs):
            name = os.path.basename(args[0])
            open_log[name].append(args[1])
            return orig_open(*args, **kargs)

        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            mkdirs(part_path)
            inv_file = os.path.join(
                part_path, diskfile.HASH_INVALIDATIONS_FILE)
            hash_file = os.path.join(
                part_path, diskfile.HASH_FILE)

            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertEqual(hashes, {})
            self.assertTrue(os.path.exists(hash_file))
            # create something to hash
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            df.delete(self.ts())
            self.assertTrue(os.path.exists(inv_file))
            # invalidation file created, lets consolidate it
            df_mgr.get_hashes('sda1', '0', [], policy)

            open_log = defaultdict(list)
            with mock.patch('builtins.open', watch_open):
                self.assertTrue(os.path.exists(inv_file))
                # no new suffixes get invalidated... so no write iop
                df_mgr.get_hashes('sda1', '0', [], policy)
            # each file is opened once to read
            expected = {
                'hashes.pkl': ['rb'],
                'hashes.invalid': ['r'],
            }
            self.assertEqual(open_log, expected)

    def _test_invalidate_hash_racing_get_hashes_diff_suffix(self, existing):
        # a suffix can be changed or created by second process while new pkl is
        # being calculated - verify that suffix is correct after next
        # get_hashes call
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            if existing:
                mkdirs(part_path)
                # force hashes.pkl to exist
                df_mgr.get_hashes('sda1', '0', [], policy)
                self.assertTrue(os.path.exists(os.path.join(
                    part_path, diskfile.HASH_FILE)))
            orig_listdir = os.listdir
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            df2 = self.get_different_suffix_df(df)
            suffix2 = os.path.basename(os.path.dirname(df2._datadir))
            non_local = {'df2touched': False}
            df.delete(self.ts())

            def mock_listdir(*args, **kwargs):
                # simulating an invalidation occurring in another process while
                # get_hashes is executing
                result = orig_listdir(*args, **kwargs)
                if not non_local['df2touched']:
                    non_local['df2touched'] = True
                    # other process creates new suffix
                    df2.delete(self.ts())
                return result

            if not existing:
                self.assertFalse(os.path.exists(os.path.join(
                    part_path, diskfile.HASH_FILE)))
            with mock.patch('swift.obj.diskfile.os.listdir',
                            mock_listdir):
                # creates pkl file if not already there
                hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertTrue(os.path.exists(os.path.join(
                part_path, diskfile.HASH_FILE)))

            # second suffix added after directory listing, it's added later
            self.assertIn(suffix, hashes)
            self.assertNotIn(suffix2, hashes)
            # updates pkl file
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertIn(suffix, hashes)
            self.assertIn(suffix2, hashes)

    def test_invalidate_hash_racing_get_hashes_diff_suffix_new_part(self):
        self._test_invalidate_hash_racing_get_hashes_diff_suffix(False)

    def test_invalidate_hash_racing_get_hashes_diff_suffix_existing_part(self):
        self._test_invalidate_hash_racing_get_hashes_diff_suffix(True)

    def _check_hash_invalidations_race_get_hashes_same_suffix(self, existing):
        # verify that when two processes concurrently call get_hashes, then any
        # concurrent hash invalidation will survive and be consolidated on a
        # subsequent call to get_hashes (i.e. ensure first get_hashes process
        # does not ignore the concurrent hash invalidation that second
        # get_hashes might have consolidated to hashes.pkl)
        non_local = {}

        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            orig_hash_suffix = df_mgr._hash_suffix
            if existing:
                # create hashes.pkl
                part_path = os.path.join(self.devices, 'sda1',
                                         diskfile.get_data_dir(policy), '0')
                mkdirs(part_path)
                df_mgr.get_hashes('sda1', '0', [], policy)
                self.assertTrue(os.path.exists(os.path.join(
                    part_path, diskfile.HASH_FILE)))

            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            part_dir = os.path.dirname(suffix_dir)
            invalidations_file = os.path.join(
                part_dir, diskfile.HASH_INVALIDATIONS_FILE)

            non_local['hash'] = None
            non_local['called'] = False

            # delete will append suffix to hashes.invalid
            df.delete(self.ts())
            with open(invalidations_file) as f:
                self.assertEqual(suffix, f.read().strip('\n'))  # sanity
            hash1 = df_mgr._hash_suffix(suffix_dir)

            def mock_hash_suffix(*args, **kwargs):
                # after first get_hashes has called _hash_suffix, simulate a
                # second process invalidating the same suffix, followed by a
                # third process calling get_hashes and failing (or yielding)
                # after consolidate_hashes has completed
                result = orig_hash_suffix(*args, **kwargs)
                if not non_local['called']:
                    non_local['called'] = True
                    # appends suffix to hashes.invalid
                    df.delete(self.ts())
                    # simulate another process calling get_hashes but failing
                    # after hash invalidation have been consolidated
                    hashes = df_mgr.consolidate_hashes(part_dir)
                    if existing:
                        self.assertTrue(hashes['valid'])
                    else:
                        self.assertFalse(hashes['valid'])
                    # get the updated suffix hash...
                    non_local['hash'] = orig_hash_suffix(suffix_dir)
                return result

            with mock.patch.object(df_mgr, '_hash_suffix', mock_hash_suffix):
                # repeats listing when pkl modified
                hashes = df_mgr.get_hashes('sda1', '0', [], policy)

            # first get_hashes should complete with suffix1 state
            self.assertIn(suffix, hashes)
            # sanity check - the suffix hash has changed...
            self.assertNotEqual(hash1, non_local['hash'])
            # the invalidation file has been truncated...
            with open(invalidations_file, 'r') as f:
                self.assertEqual('', f.read())
            # so hashes should have the latest suffix hash...
            self.assertEqual(hashes[suffix], non_local['hash'])

            non_local['called'] = False
            with mock.patch.object(df_mgr, '_hash_suffix', mock_hash_suffix):
                df_mgr.get_hashes('sda1', '0', [suffix], policy,
                                  skip_rehash=True)
            self.assertFalse(non_local['called'])
            with open(invalidations_file) as f:
                self.assertEqual(suffix, f.read().strip('\n'))  # sanity

    def test_hash_invalidations_race_get_hashes_same_suffix_new(self):
        self._check_hash_invalidations_race_get_hashes_same_suffix(False)

    def test_hash_invalidations_race_get_hashes_same_suffix_existing(self):
        self._check_hash_invalidations_race_get_hashes_same_suffix(True)

    def _check_unpickle_error_and_get_hashes_failure(self, existing):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            suffix = os.path.basename(os.path.dirname(df._datadir))

            # avoid getting O_TMPFILE warning in logs
            if not utils.o_tmpfile_in_tmpdir_supported():
                df.manager.use_linkat = False
            if existing:
                df.delete(self.ts())
                hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            df.delete(self.ts())
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path, diskfile.HASH_FILE)
            # write a corrupt hashes.pkl
            open(hashes_file, 'w')
            # simulate first call to get_hashes failing after attempting to
            # consolidate hashes
            with mock.patch('swift.obj.diskfile.os.listdir',
                            side_effect=Exception()):
                self.assertRaises(
                    Exception, df_mgr.get_hashes, 'sda1', '0', [], policy)
            # sanity on-disk state is invalid
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                found_hashes.pop('updated')
                self.assertEqual(False, found_hashes.pop('valid'))
            # verify subsequent call to get_hashes reaches correct outcome
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertIn(suffix, hashes)
            self.assertEqual([], df_mgr.logger.get_lines_for_level('warning'))

    def test_unpickle_error_and_get_hashes_failure_new_part(self):
        self._check_unpickle_error_and_get_hashes_failure(False)

    def test_unpickle_error_and_get_hashes_failure_existing_part(self):
        self._check_unpickle_error_and_get_hashes_failure(True)

    def test_invalidate_hash_consolidation(self):
        def assert_consolidation(suffixes):
            # verify that suffixes are invalidated after consolidation
            with mock.patch('swift.obj.diskfile.lock_path') as mock_lock:
                hashes = df_mgr.consolidate_hashes(part_path)
            self.assertTrue(mock_lock.called)
            for suffix in suffixes:
                self.assertIn(suffix, hashes)
                self.assertIsNone(hashes[suffix])
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                self.assertTrue(hashes['valid'])
                self.assertEqual(hashes, found_hashes)
            with open(invalidations_file, 'r') as f:
                self.assertEqual("", f.read())
            return hashes

        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # create something to hash
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            df.delete(self.ts())
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            original_hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertIn(suffix, original_hashes)  # sanity
            self.assertIsNotNone(original_hashes[suffix])

            # sanity check hashes file
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path, diskfile.HASH_FILE)
            invalidations_file = os.path.join(
                part_path, diskfile.HASH_INVALIDATIONS_FILE)
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                found_hashes.pop('updated')
                self.assertTrue(found_hashes.pop('valid'))
                self.assertEqual(original_hashes, found_hashes)

            # invalidate the hash
            with mock.patch('swift.obj.diskfile.lock_path') as mock_lock:
                df_mgr.invalidate_hash(suffix_dir)
            self.assertTrue(mock_lock.called)
            # suffix should be in invalidations file
            with open(invalidations_file, 'r') as f:
                self.assertEqual(suffix + "\n", f.read())
            # hashes file is unchanged
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                found_hashes.pop('updated')
                self.assertTrue(found_hashes.pop('valid'))
                self.assertEqual(original_hashes, found_hashes)

            # consolidate the hash and the invalidations
            hashes = assert_consolidation([suffix])

            # invalidate a different suffix hash in same partition but not in
            # existing hashes.pkl
            df2 = self.get_different_suffix_df(df)
            df2.delete(self.ts())
            suffix_dir2 = os.path.dirname(df2._datadir)
            suffix2 = os.path.basename(suffix_dir2)
            # suffix2 should be in invalidations file
            with open(invalidations_file, 'r') as f:
                self.assertEqual(suffix2 + "\n", f.read())
            # hashes file is not yet changed
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                self.assertTrue(hashes['valid'])
                self.assertEqual(hashes, found_hashes)

            # consolidate hashes
            hashes = assert_consolidation([suffix, suffix2])

            # invalidating suffix2 multiple times is ok
            df2.delete(self.ts())
            df2.delete(self.ts())
            # suffix2 should be in invalidations file
            with open(invalidations_file, 'r') as f:
                invalids = f.read().splitlines()
                self.assertEqual(sorted((suffix2, suffix2)),
                                 sorted(invalids))  # sanity
            # hashes file is not yet changed
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                self.assertTrue(hashes['valid'])
                self.assertEqual(hashes, found_hashes)
            # consolidate hashes
            assert_consolidation([suffix, suffix2])

            # Might get some garbage in your invalidations file
            part_dir = os.path.dirname(suffix_dir)
            bad_suffix = "~" + suffix
            df_mgr.invalidate_hash(os.path.join(part_dir, bad_suffix))
            # Sure enough, it's in there
            with open(invalidations_file, 'r') as f:
                self.assertEqual(bad_suffix + "\n", f.read())
            # ... but it won't taint your pickle
            hashes = df_mgr.consolidate_hashes(part_path)
            self.assertNotIn(bad_suffix, hashes)
            with open(invalidations_file, 'r') as f:
                self.assertEqual("", f.read())

            # Old code doesn't have that protection
            df_mgr.invalidate_hash(os.path.join(part_dir, bad_suffix))
            with open(invalidations_file, 'r') as f:
                self.assertEqual(bad_suffix + "\n", f.read())
            with mock.patch.object(diskfile, 'valid_suffix', lambda s: True):
                # ... so it would propogate to the pickle when we consolidate,
                # and we ought to be able to deal with that
                hashes = assert_consolidation([bad_suffix])
            self.assertIn(bad_suffix, hashes)
            self.assertIsNone(hashes[bad_suffix])
            # When we go to really get hashes, we see the bad,
            # throw out the existing pickle, and rehash everything
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertNotIn(bad_suffix, hashes)

    def test_get_hashes_consolidates_suffix_rehash_once(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)
            df.delete(self.ts())
            suffix_dir = os.path.dirname(df._datadir)

            with mock.patch.object(df_mgr, 'consolidate_hashes',
                                   side_effect=df_mgr.consolidate_hashes
                                   ) as mock_consolidate_hashes, \
                    mock.patch.object(df_mgr, '_hash_suffix',
                                      side_effect=df_mgr._hash_suffix
                                      ) as mock_hash_suffix:
                # creates pkl file
                df_mgr.get_hashes('sda1', '0', [], policy)
                mock_consolidate_hashes.assert_called_once()
                self.assertEqual([mock.call(suffix_dir, policy=policy)],
                                 mock_hash_suffix.call_args_list)
                # second object in path
                df2 = self.get_different_suffix_df(df)
                df2.delete(self.ts())
                suffix_dir2 = os.path.dirname(df2._datadir)
                mock_consolidate_hashes.reset_mock()
                mock_hash_suffix.reset_mock()
                # updates pkl file
                df_mgr.get_hashes('sda1', '0', [], policy)
                mock_consolidate_hashes.assert_called_once()
                self.assertEqual([mock.call(suffix_dir2, policy=policy)],
                                 mock_hash_suffix.call_args_list)

    def test_consolidate_hashes_raises_exception(self):
        # verify that if consolidate_hashes raises an exception then suffixes
        # are rehashed and a hashes.pkl is written
        for policy in self.iter_policies():
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path, diskfile.HASH_FILE)
            invalidations_file = os.path.join(
                part_path, diskfile.HASH_INVALIDATIONS_FILE)

            self.logger.clear()
            df_mgr = self.df_router[policy]
            # create something to hash
            df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                     policy=policy)

            # avoid getting O_TMPFILE warning in logs
            if not utils.o_tmpfile_in_tmpdir_supported():
                df.manager.use_linkat = False

            self.assertFalse(os.path.exists(part_path))
            df.delete(self.ts())
            self.assertTrue(os.path.exists(invalidations_file))
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            # no pre-existing hashes.pkl
            self.assertFalse(os.path.exists(hashes_file))
            with mock.patch.object(df_mgr, '_hash_suffix',
                                   return_value='fake hash'):
                with mock.patch.object(df_mgr, 'consolidate_hashes',
                                       side_effect=Exception()):
                    hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertEqual({suffix: 'fake hash'}, hashes)

            # sanity check hashes file
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                found_hashes.pop('updated')
                self.assertTrue(found_hashes.pop('valid'))
                self.assertEqual(hashes, found_hashes)

            # sanity check log warning
            warnings = self.logger.get_lines_for_level('warning')
            self.assertEqual(warnings, ["Unable to read %r" % hashes_file])

            # repeat with pre-existing hashes.pkl
            self.logger.clear()
            with mock.patch.object(df_mgr, '_hash_suffix',
                                   return_value='new fake hash'):
                with mock.patch.object(df_mgr, 'consolidate_hashes',
                                       side_effect=Exception()):
                    hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertEqual({suffix: 'new fake hash'}, hashes)

            # sanity check hashes file
            with open(hashes_file, 'rb') as f:
                found_hashes = pickle.load(f)
                found_hashes.pop('updated')
                self.assertTrue(found_hashes.pop('valid'))
                self.assertEqual(hashes, found_hashes)

            # sanity check log warning
            warnings = self.logger.get_lines_for_level('warning')
            self.assertEqual(warnings, ["Unable to read %r" % hashes_file])

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
            self.assertIn(suffix, hashes)

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

    def test_hash_suffix_one_tombstone_and_one_meta(self):
        # A tombstone plus a newer meta file can happen if a tombstone is
        # replicated to a node with a newer meta file but older data file. The
        # meta file will be ignored when the diskfile is opened so the
        # effective state of the disk files is equivalent to only having the
        # tombstone. Replication cannot remove the meta file, and the meta file
        # cannot be ssync replicated to a node with only the tombstone, so
        # we want the get_hashes result to be the same as if the meta file was
        # not there.
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # write a tombstone
            timestamp = self.ts()
            df.delete(timestamp)
            # write a meta file
            df.write_metadata({'X-Timestamp': self.ts().internal})
            # sanity check
            self.assertEqual(2, len(os.listdir(df._datadir)))
            tombstone_hash = md5(timestamp.internal + '.ts').hexdigest()
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            expected = {
                REPL_POLICY: {suffix: tombstone_hash},
                EC_POLICY: {suffix: {
                    # fi is None here because we have a tombstone
                    None: tombstone_hash}},
            }[policy.policy_type]
            self.assertEqual(hashes, expected)

    def test_hash_suffix_one_reclaim_tombstone_and_one_meta(self):
        # An isolated meta file can happen if a tombstone is replicated to a
        # node with a newer meta file but older data file, and the tombstone is
        # subsequently reclaimed. The meta file will be ignored when the
        # diskfile is opened so the effective state of the disk files is
        # equivalent to having no files.
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                continue
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            now = time()
            # write a tombstone that's just a *little* older than reclaim time
            df.delete(Timestamp(now - 1001))
            # write a meta file that's not quite so old
            ts_meta = Timestamp(now - 501)
            df.write_metadata({'X-Timestamp': ts_meta.internal})
            # sanity check
            self.assertEqual(2, len(os.listdir(df._datadir)))
            # scale back the df manager's reclaim age a bit to make the
            # tombstone reclaimable
            df_mgr.reclaim_age = 1000

            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            # the tombstone is reclaimed, the meta file remains, the suffix
            # hash is not updated BUT the suffix dir cannot be deleted so
            # a suffix hash equal to hash of empty string is reported.
            # TODO: this is not same result as if the meta file did not exist!
            self.assertEqual([ts_meta.internal + '.meta'],
                             os.listdir(df._datadir))
            self.assertEqual(hashes, {suffix: MD5_OF_EMPTY_STRING})

            # scale back the df manager's reclaim age even more - call to
            # get_hashes does not trigger reclaim because the suffix has
            # MD5_OF_EMPTY_STRING in hashes.pkl
            df_mgr.reclaim_age = 500
            df_mgr.commit_window = 0
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertEqual([ts_meta.internal + '.meta'],
                             os.listdir(df._datadir))
            self.assertEqual(hashes, {suffix: MD5_OF_EMPTY_STRING})

            # call get_hashes with recalculate = [suffix] and the suffix dir
            # gets re-hashed so the .meta if finally reclaimed.
            hashes = df_mgr.get_hashes('sda1', '0', [suffix], policy)
            self.assertFalse(os.path.exists(os.path.dirname(df._datadir)))
            self.assertEqual(hashes, {})

    def test_hash_suffix_one_reclaim_tombstone(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy)
            # scale back this tests manager's reclaim age a bit
            df_mgr.reclaim_age = 1000
            # write a tombstone that's just a *little* older
            old_time = time() - 1001
            timestamp = Timestamp(old_time)
            df.delete(timestamp.internal)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertEqual(hashes, {})

    def test_hash_suffix_ts_cleanup_after_recalc(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy)
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)

            # scale back reclaim age a bit
            df_mgr.reclaim_age = 1000
            # write a valid tombstone
            old_time = time() - 500
            timestamp = Timestamp(old_time)
            df.delete(timestamp.internal)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertIn(suffix, hashes)
            self.assertIsNotNone(hashes[suffix])

            # we have tombstone entry
            tombstone = '%s.ts' % timestamp.internal
            self.assertTrue(os.path.exists(df._datadir))
            self.assertIn(tombstone, os.listdir(df._datadir))

            # lower reclaim age to force tombstone reclaiming
            df_mgr.reclaim_age = 200

            # not cleaning up because suffix not invalidated
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertTrue(os.path.exists(df._datadir))
            self.assertIn(tombstone, os.listdir(df._datadir))
            self.assertIn(suffix, hashes)
            self.assertIsNotNone(hashes[suffix])

            # recalculating suffix hash cause cleanup
            hashes = df_mgr.get_hashes('sda1', '0', [suffix], policy)

            self.assertEqual(hashes, {})
            self.assertFalse(os.path.exists(df._datadir))

    def test_hash_suffix_ts_cleanup_after_invalidate_hash(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy)
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)

            # scale back reclaim age a bit
            df_mgr.reclaim_age = 1000
            # write a valid tombstone
            old_time = time() - 500
            timestamp = Timestamp(old_time)
            df.delete(timestamp.internal)
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertIn(suffix, hashes)
            self.assertIsNotNone(hashes[suffix])

            # we have tombstone entry
            tombstone = '%s.ts' % timestamp.internal
            self.assertTrue(os.path.exists(df._datadir))
            self.assertIn(tombstone, os.listdir(df._datadir))

            # lower reclaim age to force tombstone reclaiming
            df_mgr.reclaim_age = 200

            # not cleaning up because suffix not invalidated
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            self.assertTrue(os.path.exists(df._datadir))
            self.assertIn(tombstone, os.listdir(df._datadir))
            self.assertIn(suffix, hashes)
            self.assertIsNotNone(hashes[suffix])

            # However if we call invalidate_hash for the suffix dir,
            # get_hashes can reclaim the tombstone
            with mock.patch('swift.obj.diskfile.lock_path'):
                df_mgr.invalidate_hash(suffix_dir)

            # updating invalidated hashes cause cleanup
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)

            self.assertEqual(hashes, {})
            self.assertFalse(os.path.exists(df._datadir))

    def test_hash_suffix_one_reclaim_and_one_valid_tombstone(self):
        paths, suffix = find_paths_with_matching_suffixes(2, 1)
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            a, c, o = paths[suffix][0]
            df1 = df_mgr.get_diskfile(
                'sda1', '0', a, c, o, policy=policy)
            # scale back this tests manager's reclaim age a bit
            df_mgr.reclaim_age = 1000
            # write one tombstone that's just a *little* older
            df1.delete(Timestamp(time() - 1001))
            # create another tombstone in same suffix dir that's newer
            a, c, o = paths[suffix][1]
            df2 = df_mgr.get_diskfile(
                'sda1', '0', a, c, o, policy=policy)
            t_df2 = Timestamp(time() - 900)
            df2.delete(t_df2)

            hashes = df_mgr.get_hashes('sda1', '0', [], policy)

            suffix = os.path.basename(os.path.dirname(df1._datadir))
            df2_tombstone_hash = md5(t_df2.internal + '.ts').hexdigest()
            expected = {
                REPL_POLICY: {suffix: df2_tombstone_hash},
                EC_POLICY: {suffix: {
                    # fi is None here because we have a tombstone
                    None: df2_tombstone_hash}},
            }[policy.policy_type]

            self.assertEqual(hashes, expected)

    def test_hash_suffix_one_datafile(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(
                'sda1', '0', 'a', 'c', 'o', policy=policy, frag_index=7)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # write a datafile
            timestamp = self.ts()
            with df.create() as writer:
                test_data = b'test file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
                # note - no commit so data is non-durable
            hashes = df_mgr.get_hashes('sda1', '0', [], policy)
            datafile_hash = md5({
                EC_POLICY: timestamp.internal,
                REPL_POLICY: timestamp.internal + '.data',
            }[policy.policy_type]).hexdigest()
            expected = {
                REPL_POLICY: {suffix: datafile_hash},
                EC_POLICY: {suffix: {
                    # because there's no durable state, we have no hash for
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

    def _do_hash_suffix_multi_file_ends_in_datafile(self, policy,
                                                    legacy_durable):
        # if legacy_durable is True then synthesize legacy durable files
        # instead of having a durable marker in the data file name
        frag_index = 4
        df_mgr = self.df_router[policy]
        df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o', policy=policy,
                                 frag_index=frag_index)
        suffix = os.path.basename(os.path.dirname(df._datadir))
        mkdirs(df._datadir)
        now = time()
        timestamp = None
        # go behind the scenes and setup a bunch of weird file names
        for tdiff in [500, 100, 10, 1]:
            suffs = ['.meta', '.data']
            if tdiff > 50:
                suffs.append('.ts')
            if policy.policy_type == EC_POLICY and legacy_durable:
                suffs.append('.durable')
            for suff in suffs:
                timestamp = Timestamp(now - tdiff)
                if suff == '.data':
                    filename = _make_datafilename(
                        timestamp, policy, frag_index,
                        durable=not legacy_durable)
                else:
                    filename = timestamp.internal + suff
                open(os.path.join(df._datadir, filename), 'w').close()
        meta_timestamp = Timestamp(now)
        metadata_filename = meta_timestamp.internal + '.meta'
        open(os.path.join(df._datadir, metadata_filename), 'w').close()

        # call get_hashes and it should clean up all but the most recent files
        hashes = df_mgr.get_hashes('sda1', '0', [], policy)

        # calculate expected outcome
        data_filename = _make_datafilename(
            timestamp, policy, frag_index, durable=not legacy_durable)
        expected_files = [data_filename, metadata_filename]
        if policy.policy_type == EC_POLICY:
            # note: expected hashes is same with or without legacy durable file
            hasher = md5()
            hasher.update(metadata_filename)
            hasher.update(timestamp.internal + '.durable')
            expected = {
                suffix: {
                    # metadata & durable updates are hashed separately
                    None: hasher.hexdigest(),
                    4: self.fname_to_ts_hash(data_filename),
                }
            }
            if legacy_durable:
                expected_files.append(timestamp.internal + '.durable')
        elif policy.policy_type == REPL_POLICY:
            hasher = md5()
            hasher.update(metadata_filename)
            hasher.update(data_filename)
            expected = {suffix: hasher.hexdigest()}
        else:
            self.fail('unknown policy type %r' % policy.policy_type)
        self.assertEqual(hashes, expected)
        # only the meta and data should be left
        self.assertEqual(sorted(os.listdir(df._datadir)),
                         sorted(expected_files))

    def test_hash_suffix_multifile_ends_in_datafile(self):
        for policy in self.iter_policies():
            self._do_hash_suffix_multi_file_ends_in_datafile(
                policy, legacy_durable=False)

    def test_hash_suffix_multifile_ends_in_datafile_legacy_durable(self):
        for policy in self.iter_policies():
            if policy.policy_type == EC_POLICY:
                self._do_hash_suffix_multi_file_ends_in_datafile(
                    policy, legacy_durable=True)

    def _verify_get_hashes(self, filenames, ts_data, ts_meta, ts_ctype,
                           policy):
        """
        Helper method to create a set of ondisk files and verify suffix_hashes.

        :param filenames: list of filenames to create in an object hash dir
        :param ts_data: newest data timestamp, used for expected result
        :param ts_meta: newest meta timestamp, used for expected result
        :param ts_ctype: newest content-type timestamp, used for expected
                         result
        :param policy: storage policy to use for test
        """
        df_mgr = self.df_router[policy]
        df = df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o',
                                 policy=policy, frag_index=4)
        suffix = os.path.basename(os.path.dirname(df._datadir))
        partition_dir = os.path.dirname(os.path.dirname(df._datadir))
        rmtree(partition_dir, ignore_errors=True)  # clean dir for each test
        mkdirs(df._datadir)

        # calculate expected result
        hasher = md5()
        if policy.policy_type == EC_POLICY:
            hasher.update(ts_meta.internal + '.meta')
            hasher.update(ts_data.internal + '.durable')
            if ts_ctype:
                hasher.update(ts_ctype.internal + '_ctype')
            expected = {
                suffix: {
                    None: hasher.hexdigest(),
                    4: md5(ts_data.internal).hexdigest(),
                }
            }
        elif policy.policy_type == REPL_POLICY:
            hasher.update(ts_meta.internal + '.meta')
            hasher.update(ts_data.internal + '.data')
            if ts_ctype:
                hasher.update(ts_ctype.internal + '_ctype')
            expected = {suffix: hasher.hexdigest()}
        else:
            self.fail('unknown policy type %r' % policy.policy_type)

        for fname in filenames:
            open(os.path.join(df._datadir, fname), 'w').close()

        hashes = df_mgr.get_hashes('sda1', '0', [], policy)

        msg = 'expected %r != %r for policy %r' % (
            expected, hashes, policy)
        self.assertEqual(hashes, expected, msg)

    def test_hash_suffix_with_older_content_type_in_meta(self):
        # single meta file having older content-type
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_data, ts_ctype, ts_meta = (
                    self.ts(), self.ts(), self.ts())

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_meta, ts_ctype)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_meta, ts_ctype, policy)

        do_test(False)
        do_test(True)

    def test_hash_suffix_with_same_age_content_type_in_meta(self):
        # single meta file having same age content-type
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_data, ts_meta = (self.ts(), self.ts())

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_meta, ts_meta)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_meta, ts_meta, policy)

        do_test(False)
        do_test(True)

    def test_hash_suffix_with_obsolete_content_type_in_meta(self):
        # After rsync replication we could have a single meta file having
        # content-type older than a replicated data file
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_ctype, ts_data, ts_meta = (self.ts(), self.ts(), self.ts())

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_meta, ts_ctype)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_meta, None, policy)

        do_test(False)
        do_test(True)

    def test_hash_suffix_with_older_content_type_in_newer_meta(self):
        # After rsync replication we could have two meta files: newest
        # content-type is in newer meta file, older than newer meta file
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_data, ts_older_meta, ts_ctype, ts_newer_meta = (
                    self.ts() for _ in range(4))

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_older_meta),
                             _make_metafilename(ts_newer_meta, ts_ctype)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_newer_meta, ts_ctype, policy)

        do_test(False)
        do_test(True)

    def test_hash_suffix_with_same_age_content_type_in_newer_meta(self):
        # After rsync replication we could have two meta files: newest
        # content-type is in newer meta file, at same age as newer meta file
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_data, ts_older_meta, ts_newer_meta = (
                    self.ts() for _ in range(3))

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_newer_meta, ts_newer_meta)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_newer_meta, ts_newer_meta, policy)

        do_test(False)
        do_test(True)

    def test_hash_suffix_with_older_content_type_in_older_meta(self):
        # After rsync replication we could have two meta files: newest
        # content-type is in older meta file, older than older meta file
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_data, ts_ctype, ts_older_meta, ts_newer_meta = (
                    self.ts() for _ in range(4))

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_newer_meta),
                             _make_metafilename(ts_older_meta, ts_ctype)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_newer_meta, ts_ctype, policy)

        do_test(False)
        do_test(True)

    def test_hash_suffix_with_same_age_content_type_in_older_meta(self):
        # After rsync replication we could have two meta files: newest
        # content-type is in older meta file, at same age as older meta file
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_data, ts_older_meta, ts_newer_meta = (
                    self.ts() for _ in range(3))

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_newer_meta),
                             _make_metafilename(ts_older_meta, ts_older_meta)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_newer_meta, ts_older_meta, policy)

        do_test(False)
        do_test(True)

    def test_hash_suffix_with_obsolete_content_type_in_older_meta(self):
        # After rsync replication we could have two meta files: newest
        # content-type is in older meta file, but older than data file
        def do_test(legacy_durable):
            for policy in self.iter_policies():
                ts_ctype, ts_data, ts_older_meta, ts_newer_meta = (
                    self.ts() for _ in range(4))

                filenames = [_make_datafilename(ts_data, policy, frag_index=4,
                                                durable=not legacy_durable),
                             _make_metafilename(ts_newer_meta),
                             _make_metafilename(ts_older_meta, ts_ctype)]
                if policy.policy_type == EC_POLICY and legacy_durable:
                    filenames.append(ts_data.internal + '.durable')

                self._verify_get_hashes(
                    filenames, ts_data, ts_newer_meta, None, policy)

        do_test(False)
        do_test(True)

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
            df.delete(Timestamp.now())
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
            self.assertIn(suffix, hashes)
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
            expected = {}
            msg = 'expected %r != %r for policy %r' % (
                expected, hashes, policy)
            self.assertEqual(hashes, expected, msg)

            # now make the suffix path a file
            open(suffix_path, 'w').close()
            hashes = df_mgr.get_hashes('sda1', '0', [suffix], policy)
            expected = {}
            msg = 'expected %r != %r for policy %r' % (
                expected, hashes, policy)
            self.assertEqual(hashes, expected, msg)

    def test_hash_suffix_listdir_enoent(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(self.devices, 'sda1',
                                     diskfile.get_data_dir(policy), '0')
            mkdirs(part_path)  # ensure we'll bother writing a pkl at all
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

            self.assertEqual(listdir_calls, [
                # part path gets created automatically
                (part_path, True),
                # this one blows up
                (os.path.join(part_path, '123'), False),
            ])

    def test_hash_suffix_cleanup_ondisk_files_enotdir_quarantined(self):
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
            self.assertEqual(hashes, {})
            # and hash path is quarantined
            self.assertFalse(os.path.exists(df._datadir))
            # each device a quarantined directory
            quarantine_base = os.path.join(self.devices,
                                           self.existing_device, 'quarantined')
            # the quarantine path is...
            quarantine_path = os.path.join(
                quarantine_base,  # quarantine root
                diskfile.get_data_dir(policy),  # per-policy data dir
                os.path.basename(df._datadir)  # name of quarantined file
            )
            self.assertTrue(os.path.exists(quarantine_path))

    def test_auditor_hashdir_not_listable(self):
        def list_locations(dirname, datadir):
            return [(loc.path, loc.device, loc.partition, loc.policy)
                    for loc in diskfile.object_audit_location_generator(
                    devices=dirname, datadir=datadir, mount_check=False)]

        real_listdir = os.listdir

        def splode_if_endswith(suffix, err):
            def sploder(path):
                if path.endswith(suffix):
                    raise OSError(err, os.strerror(err))
                else:
                    return real_listdir(path)

            return sploder

        with temptree([]) as tmpdir:
            hashdir1 = os.path.join(tmpdir, "sdf", "objects", "2607", "b54",
                                    "fe450ec990a88cc4b252b181bab04b54")
            os.makedirs(hashdir1)
            with open(os.path.join(hashdir1, '1656032666.98003.ts'), 'w'):
                pass
            hashdir2 = os.path.join(tmpdir, "sdf", "objects", "2809", "afd",
                                    "7089ab48d955ab0851fc51cc17a34afd")
            os.makedirs(hashdir2)
            with open(os.path.join(hashdir2, '1656080624.31899.ts'), 'w'):
                pass

            expected = [(hashdir2, 'sdf', '2809', POLICIES[0])]

            # Parts that look like files are just skipped
            with mock.patch('os.listdir', splode_if_endswith(
                    "2607", errno.ENOTDIR)):
                self.assertEqual(expected, list_locations(tmpdir, 'objects'))
            diskfile.clear_auditor_status(tmpdir, 'objects')
            # ENODATA on a suffix is ok
            with mock.patch('os.listdir', splode_if_endswith(
                    "b54", errno.ENODATA)):
                self.assertEqual(expected, list_locations(tmpdir, 'objects'))
            diskfile.clear_auditor_status(tmpdir, 'objects')
            # EUCLEAN too
            with mock.patch('os.listdir', splode_if_endswith(
                    "b54", EUCLEAN)):
                self.assertEqual(expected, list_locations(tmpdir, 'objects'))
            diskfile.clear_auditor_status(tmpdir, 'objects')

            # sanity the other way
            expected = [(hashdir1, 'sdf', '2607', POLICIES[0])]
            with mock.patch('os.listdir', splode_if_endswith(
                    "2809", errno.ENODATA)):
                self.assertEqual(expected, list_locations(tmpdir, 'objects'))
            diskfile.clear_auditor_status(tmpdir, 'objects')
            with mock.patch('os.listdir', splode_if_endswith(
                    "2809", EUCLEAN)):
                self.assertEqual(expected, list_locations(tmpdir, 'objects'))
            diskfile.clear_auditor_status(tmpdir, 'objects')
            with mock.patch('os.listdir', splode_if_endswith(
                    "afd", errno.ENOTDIR)):
                self.assertEqual(expected, list_locations(tmpdir, 'objects'))
            diskfile.clear_auditor_status(tmpdir, 'objects')

    def test_hash_suffix_cleanup_ondisk_files_enodata_quarantined(self):
        for policy in self.iter_policies():
            df = self.df_router[policy].get_diskfile(
                self.existing_device, '0', 'a', 'c', 'o', policy=policy)
            # make everything down to the hash directory
            os.makedirs(df._datadir)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            orig_listdir = os.listdir

            def fake_listdir(path):
                if path == df._datadir:
                    raise OSError(errno.ENODATA, 'nope')
                return orig_listdir(path)

            df_mgr = self.df_router[policy]
            with mock.patch('os.listdir', side_effect=fake_listdir):
                hashes = df_mgr.get_hashes(self.existing_device, '0', [suffix],
                                           policy)
            self.assertEqual(hashes, {})
            # and hash path is quarantined
            self.assertFalse(os.path.exists(df._datadir))
            # each device a quarantined directory
            quarantine_base = os.path.join(self.devices,
                                           self.existing_device, 'quarantined')
            # the quarantine path is...
            quarantine_path = os.path.join(
                quarantine_base,  # quarantine root
                diskfile.get_data_dir(policy),  # per-policy data dir
                os.path.basename(df._datadir)  # name of quarantined file
            )
            self.assertTrue(os.path.exists(quarantine_path))

    def test_hash_suffix_cleanup_ondisk_files_euclean_quarantined(self):
        for policy in self.iter_policies():
            df = self.df_router[policy].get_diskfile(
                self.existing_device, '0', 'a', 'c', 'o', policy=policy)
            # make everything down to the hash directory
            os.makedirs(df._datadir)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            orig_listdir = os.listdir

            def fake_listdir(path):
                if path == df._datadir:
                    raise OSError(EUCLEAN, 'nope')
                return orig_listdir(path)

            df_mgr = self.df_router[policy]
            with mock.patch('os.listdir', side_effect=fake_listdir):
                hashes = df_mgr.get_hashes(self.existing_device, '0', [suffix],
                                           policy)
            self.assertEqual(hashes, {})
            # and hash path is quarantined
            self.assertFalse(os.path.exists(df._datadir))
            # each device a quarantined directory
            quarantine_base = os.path.join(self.devices,
                                           self.existing_device, 'quarantined')
            # the quarantine path is...
            quarantine_path = os.path.join(
                quarantine_base,  # quarantine root
                diskfile.get_data_dir(policy),  # per-policy data dir
                os.path.basename(df._datadir)  # name of quarantined file
            )
            self.assertTrue(os.path.exists(quarantine_path))

    def test_hash_suffix_cleanup_ondisk_files_other_oserror(self):
        for policy in self.iter_policies():
            timestamp = self.ts()
            df_mgr = self.df_router[policy]
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy,
                                     frag_index=7)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            with df.create() as writer:
                test_data = b'test_data'
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
                    # make the cleanup_ondisk_files raise an exception
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

    def test_get_hashes_does_not_create_partition(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                       policy)
            self.assertEqual(hashes, {})
            part_path = os.path.join(
                self.devices, 'sda1', diskfile.get_data_dir(policy), '0')
            self.assertFalse(os.path.exists(part_path))

    def test_get_hashes_creates_pkl(self):
        # like above, but -- if the partition already exists, make the pickle
        for policy in self.iter_policies():
            part_path = os.path.join(
                self.devices, 'sda1', diskfile.get_data_dir(policy), '0')
            mkdirs(part_path)
            df_mgr = self.df_router[policy]
            hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                       policy)
            self.assertEqual(hashes, {})
            self.assertTrue(os.path.exists(part_path))
            hashes_file = os.path.join(part_path,
                                       diskfile.HASH_FILE)
            self.assertTrue(os.path.exists(hashes_file))

            # and double check the hashes
            new_hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)
            self.assertEqual(hashes, new_hashes)

    def _do_test_get_hashes_new_pkl_finds_new_suffix_dirs(self, device):
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
            suffix_dir = os.path.dirname(df._datadir)
            suffix = os.path.basename(suffix_dir)
            # get_hashes will find the untracked suffix dir
            self.assertFalse(os.path.exists(hashes_file))  # sanity
            hashes = df_mgr.get_hashes(device, '0', [], policy)
            self.assertIn(suffix, hashes)
            # ... and create a hashes pickle for it
            self.assertTrue(os.path.exists(hashes_file))
            # repeat and check there is no rehashing
            with mock.patch.object(df_mgr, '_hash_suffix',
                                   return_value=hashes[suffix]) as mocked:
                repeat_hashes = df_mgr.get_hashes(device, '0', [], policy)
            self.assertEqual(hashes, repeat_hashes)
            mocked.assert_not_called()

    def test_get_hashes_new_pkl_finds_new_suffix_dirs_unicode(self):
        self._do_test_get_hashes_new_pkl_finds_new_suffix_dirs(u'sda1')

    def test_get_hashes_new_pkl_finds_new_suffix_dirs(self):
        self._do_test_get_hashes_new_pkl_finds_new_suffix_dirs('sda1')

    def test_get_hashes_new_pkl_missing_invalid_finds_new_suffix_dirs(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(
                self.devices, self.existing_device,
                diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path,
                                       diskfile.HASH_FILE)
            invalidations_file = os.path.join(
                part_path, diskfile.HASH_INVALIDATIONS_FILE)
            # add something to find
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy, frag_index=4)
            timestamp = self.ts()
            df.delete(timestamp)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            with open(invalidations_file) as f:
                self.assertEqual('%s\n' % suffix, f.read())
            # even if invalidations_file is missing ...
            os.unlink(invalidations_file)
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            # get_hashes will *still* find the untracked suffix dir
            self.assertIn(suffix, hashes)
            # ... and create a hashes pickle for it
            self.assertTrue(os.path.exists(hashes_file))

    def test_get_hashes_new_pkl_lying_invalid_finds_new_suffix_dirs(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            part_path = os.path.join(
                self.devices, self.existing_device,
                diskfile.get_data_dir(policy), '0')
            hashes_file = os.path.join(part_path,
                                       diskfile.HASH_FILE)
            invalidations_file = os.path.join(
                part_path, diskfile.HASH_INVALIDATIONS_FILE)
            # add something to find
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy, frag_index=4)
            timestamp = self.ts()
            df.delete(timestamp)
            suffix = os.path.basename(os.path.dirname(df._datadir))
            with open(invalidations_file) as f:
                self.assertEqual('%s\n' % suffix, f.read())
            # even if invalidations_file is lying ...
            with open(invalidations_file, 'w') as f:
                f.write('%x\n' % (int(suffix, 16) + 1))
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            # get_hashes will *still* find the untracked suffix dir
            self.assertIn(suffix, hashes)
            # ... and create a hashes pickle for it
            self.assertTrue(os.path.exists(hashes_file))

    def test_get_hashes_old_pickle_does_not_find_new_suffix_dirs(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # create an empty stale pickle
            part_path = os.path.join(
                self.devices, 'sda1', diskfile.get_data_dir(policy), '0')
            mkdirs(part_path)
            hashes_file = os.path.join(part_path,
                                       diskfile.HASH_FILE)
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            self.assertEqual(hashes, {})
            self.assertTrue(os.path.exists(hashes_file))  # sanity
            # add something to find
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c', 'o',
                                     policy=policy, frag_index=4)
            os.makedirs(df._datadir)
            filename = Timestamp.now().internal + '.ts'
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
            self.assertIn(suffix, hashes)

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
            self.assertIn(suffix, hashes)
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
            # ... and the suffix key is removed
            expected = {}
            self.assertEqual(expected, hashes)

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
                test_data = b'test_file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
                writer.commit(timestamp)
            datafile_name = _make_datafilename(
                timestamp, policy, frag_index=5)
            durable_hash = md5(timestamp.internal + '.durable').hexdigest()
            datafile_suffix = os.path.basename(os.path.dirname(df._datadir))
            # in the *third* suffix - two datafiles for different hashes
            df = df_mgr.get_diskfile(self.existing_device, '0',
                                     *matching_paths[0], policy=policy,
                                     frag_index=6)
            matching_suffix = os.path.basename(os.path.dirname(df._datadir))
            timestamp = self.ts()
            with df.create() as writer:
                test_data = b'test_file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
                writer.commit(timestamp)
            # we'll keep track of file names for hash calculations
            filename = _make_datafilename(
                timestamp, policy, frag_index=6)
            data_filenames = {
                6: filename
            }
            df = df_mgr.get_diskfile(self.existing_device, '0',
                                     *matching_paths[1], policy=policy,
                                     frag_index=7)
            self.assertEqual(os.path.basename(os.path.dirname(df._datadir)),
                             matching_suffix)  # sanity
            timestamp = self.ts()
            with df.create() as writer:
                test_data = b'test_file'
                writer.write(test_data)
                metadata = {
                    'X-Timestamp': timestamp.internal,
                    'ETag': md5(test_data).hexdigest(),
                    'Content-Length': len(test_data),
                }
                writer.put(metadata)
                writer.commit(timestamp)
            filename = _make_datafilename(
                timestamp, policy, frag_index=7)
            data_filenames[7] = filename
            # now make up the expected suffixes!
            if policy.policy_type == EC_POLICY:
                hasher = md5()
                for filename in data_filenames.values():
                    # each data file updates the hasher with durable timestamp
                    hasher.update(filename.split('#', 1)[0] + '.durable')
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
                        6: self.fname_to_ts_hash(data_filenames[6]),
                        7: self.fname_to_ts_hash(data_filenames[7]),
                    },
                }
            elif policy.policy_type == REPL_POLICY:
                hasher = md5()
                for filename in data_filenames.values():
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
            with mock_check_drive(ismount=False):
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

    def _test_get_hashes_race(self, hash_breaking_function):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]

            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy, frag_index=3)
            suffix = os.path.basename(os.path.dirname(df._datadir))

            df2 = self.get_different_suffix_df(df, frag_index=5)
            suffix2 = os.path.basename(os.path.dirname(df2._datadir))
            part_path = os.path.dirname(os.path.dirname(
                os.path.join(df._datadir)))
            mkdirs(part_path)
            hashfile_path = os.path.join(part_path, diskfile.HASH_FILE)
            # create hashes.pkl
            hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                       policy)
            self.assertEqual(hashes, {})  # sanity
            self.assertTrue(os.path.exists(hashfile_path))
            # and optionally tamper with the hashes.pkl...
            hash_breaking_function(hashfile_path)
            non_local = {'called': False}
            orig_hash_suffix = df_mgr._hash_suffix

            # then create a suffix
            df.delete(self.ts())

            def mock_hash_suffix(*args, **kwargs):
                # capture first call to mock_hash
                if not non_local['called']:
                    non_local['called'] = True
                    df2.delete(self.ts())
                    non_local['other_hashes'] = df_mgr.get_hashes(
                        self.existing_device, '0', [], policy)
                return orig_hash_suffix(*args, **kwargs)

            with mock.patch.object(df_mgr, '_hash_suffix', mock_hash_suffix):
                hashes = df_mgr.get_hashes(self.existing_device, '0', [],
                                           policy)

            self.assertTrue(non_local['called'])
            self.assertIn(suffix, hashes)
            self.assertIn(suffix2, hashes)

    def test_get_hashes_race_invalid_pickle(self):
        def hash_breaking_function(hashfile_path):
            # create a garbage invalid zero-byte file which can not unpickle
            open(hashfile_path, 'w').close()
        self._test_get_hashes_race(hash_breaking_function)

    def test_get_hashes_race_new_partition(self):
        def hash_breaking_function(hashfile_path):
            # simulate rebalanced part doing post-rsync REPLICATE
            os.unlink(hashfile_path)
            part_dir = os.path.dirname(hashfile_path)
            os.unlink(os.path.join(part_dir, '.lock'))
            # sanity
            self.assertEqual([], os.listdir(os.path.dirname(hashfile_path)))
        self._test_get_hashes_race(hash_breaking_function)

    def test_get_hashes_race_existing_partition(self):
        def hash_breaking_function(hashfile_path):
            # no-op - simulate ok existing partition
            self.assertTrue(os.path.exists(hashfile_path))
        self._test_get_hashes_race(hash_breaking_function)

    def test_get_hashes_hash_suffix_enotdir(self):
        for policy in self.iter_policies():
            df_mgr = self.df_router[policy]
            # create a real suffix dir
            df = df_mgr.get_diskfile(self.existing_device, '0', 'a', 'c',
                                     'o', policy=policy, frag_index=3)
            df.delete(Timestamp.now())
            suffix = os.path.basename(os.path.dirname(df._datadir))
            # touch a bad suffix dir
            part_dir = os.path.join(self.devices, self.existing_device,
                                    diskfile.get_data_dir(policy), '0')
            open(os.path.join(part_dir, 'bad'), 'w').close()
            hashes = df_mgr.get_hashes(self.existing_device, '0', [], policy)
            self.assertIn(suffix, hashes)
            self.assertNotIn('bad', hashes)

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
            expected = {}
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
            part_path = os.path.join(self.devices, self.existing_device,
                                     diskfile.get_data_dir(policy), '0')
            mkdirs(part_path)
            # first create an empty pickle
            df_mgr.get_hashes(self.existing_device, '0', [], policy)
            self.assertTrue(os.path.exists(os.path.join(
                part_path, diskfile.HASH_FILE)))
            non_local = {'suffix_count': 1}
            calls = []

            def mock_read_hashes(filename):
                rv = {'%03x' % i: 'fake'
                      for i in range(non_local['suffix_count'])}
                if len(calls) <= 3:
                    # this will make the *next* call get slightly
                    # different content
                    non_local['suffix_count'] += 1
                # track exactly the value for every return
                calls.append(dict(rv))
                rv['valid'] = True
                return rv
            with mock.patch('swift.obj.diskfile.read_hashes',
                            mock_read_hashes):
                df_mgr.get_hashes(self.existing_device, '0', ['123'],
                                  policy)

            self.assertEqual(calls, [
                {'000': 'fake'},  # read
                {'000': 'fake', '001': 'fake'},  # modification
                {'000': 'fake', '001': 'fake', '002': 'fake'},  # read
                {'000': 'fake', '001': 'fake', '002': 'fake',
                 '003': 'fake'},  # modifed
                {'000': 'fake', '001': 'fake', '002': 'fake',
                 '003': 'fake', '004': 'fake'},  # read
                {'000': 'fake', '001': 'fake', '002': 'fake',
                 '003': 'fake', '004': 'fake'},  # not modifed
            ])


class TestHashesHelpers(unittest.TestCase):

    def setUp(self):
        self.testdir = tempfile.mkdtemp()

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_read_legacy_hashes(self):
        hashes = {'fff': 'fake'}
        hashes_file = os.path.join(self.testdir, diskfile.HASH_FILE)
        with open(hashes_file, 'wb') as f:
            pickle.dump(hashes, f)
        expected = {
            'fff': 'fake',
            'updated': -1,
            'valid': True,
        }
        self.assertEqual(expected, diskfile.read_hashes(self.testdir))

    def test_write_hashes_valid_updated(self):
        hashes = {'888': 'fake', 'valid': True}
        now = time()
        with mock.patch('swift.obj.diskfile.time.time', return_value=now):
            diskfile.write_hashes(self.testdir, hashes)
        hashes_file = os.path.join(self.testdir, diskfile.HASH_FILE)
        with open(hashes_file, 'rb') as f:
            data = pickle.load(f)
        expected = {
            '888': 'fake',
            'updated': now,
            'valid': True,
        }
        self.assertEqual(expected, data)

    def test_write_hashes_invalid_updated(self):
        hashes = {'valid': False}
        now = time()
        with mock.patch('swift.obj.diskfile.time.time', return_value=now):
            diskfile.write_hashes(self.testdir, hashes)
        hashes_file = os.path.join(self.testdir, diskfile.HASH_FILE)
        with open(hashes_file, 'rb') as f:
            data = pickle.load(f)
        expected = {
            'updated': now,
            'valid': False,
        }
        self.assertEqual(expected, data)

    def test_write_hashes_safe_default(self):
        hashes = {}
        now = time()
        with mock.patch('swift.obj.diskfile.time.time', return_value=now):
            diskfile.write_hashes(self.testdir, hashes)
        hashes_file = os.path.join(self.testdir, diskfile.HASH_FILE)
        with open(hashes_file, 'rb') as f:
            data = pickle.load(f)
        expected = {
            'updated': now,
            'valid': False,
        }
        self.assertEqual(expected, data)

    def test_read_write_valid_hashes_mutation_and_transative_equality(self):
        hashes = {'000': 'fake', 'valid': True}
        diskfile.write_hashes(self.testdir, hashes)
        # write_hashes mutates the passed in hashes, it adds the updated key
        self.assertIn('updated', hashes)
        self.assertTrue(hashes['valid'])
        result = diskfile.read_hashes(self.testdir)
        # unpickling result in a new object
        self.assertNotEqual(id(hashes), id(result))
        # with the exactly the same value mutation from write_hashes
        self.assertEqual(hashes, result)

    def test_read_write_invalid_hashes_mutation_and_transative_equality(self):
        hashes = {'valid': False}
        diskfile.write_hashes(self.testdir, hashes)
        # write_hashes mutates the passed in hashes, it adds the updated key
        self.assertIn('updated', hashes)
        self.assertFalse(hashes['valid'])
        result = diskfile.read_hashes(self.testdir)
        # unpickling result in a new object
        self.assertNotEqual(id(hashes), id(result))
        # with the exactly the same value mutation from write_hashes
        self.assertEqual(hashes, result)

    def test_ignore_corrupted_hashes(self):
        corrupted_hashes = {u'\x00\x00\x00': False, 'valid': True}
        diskfile.write_hashes(self.testdir, corrupted_hashes)
        result = diskfile.read_hashes(self.testdir)
        self.assertFalse(result['valid'])


if __name__ == '__main__':
    unittest.main()
