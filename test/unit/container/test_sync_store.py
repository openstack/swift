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

import os
import errno
from unittest import mock
import random
import logging
import unittest
import tempfile
from shutil import rmtree
from test.debug_logger import debug_logger

from swift.container.backend import DATADIR
from swift.container import sync_store


class FakeContainerBroker(object):

    def __init__(self, path):
        self.db_file = path
        self.db_dir = os.path.dirname(path)
        self.metadata = dict()
        self._is_deleted = False

    def is_deleted(self):
        return self._is_deleted


class TestContainerSyncStore(unittest.TestCase):

    def setUp(self):
        self.logger = debug_logger('test-container-sync-store')
        self.logger.level = logging.DEBUG
        self.test_dir_prefix = tempfile.mkdtemp()
        self.devices_dir = os.path.join(self.test_dir_prefix, 'srv/node/')
        os.makedirs(self.devices_dir)
        # Create dummy container dbs
        self.devices = ['sdax', 'sdb', 'sdc']
        self.partitions = ['21765', '38965', '13234']
        self.suffixes = ['312', '435']
        self.hashes = ['f19ed', '53ef', '0ab5', '9c3a']
        for device in self.devices:
            data_dir_path = os.path.join(self.devices_dir,
                                         device,
                                         DATADIR)
            os.makedirs(data_dir_path)
            for part in self.partitions:
                for suffix in self.suffixes:
                    for hsh in self.hashes:
                        db_dir = os.path.join(data_dir_path,
                                              part,
                                              suffix,
                                              hsh)
                        os.makedirs(db_dir)
                        db_file = os.path.join(db_dir, '%s.db' % hsh)
                        with open(db_file, 'w') as outfile:
                            outfile.write('%s' % db_file)

    def tearDown(self):
        rmtree(self.test_dir_prefix)

    def pick_dbfile(self):
        hsh = random.choice(self.hashes)
        return os.path.join(self.devices_dir,
                            random.choice(self.devices),
                            DATADIR,
                            random.choice(self.partitions),
                            random.choice(self.suffixes),
                            hsh,
                            '%s.db' % hsh)

    # Path conversion tests
    # container path is of the form:
    # /srv/node/sdb/containers/part/.../*.db
    # or more generally:
    # devices/device/DATADIR/part/.../*.db
    # synced container path is assumed to be of the form:
    # /srv/node/sdb/sync_containers/part/.../*.db
    # or more generally:
    # devices/device/SYNC_DATADIR/part/.../*.db
    # Indeed the ONLY DIFFERENCE is DATADIR <-> SYNC_DATADIR
    # Since, however, the strings represented by the constants
    # DATADIR or SYNC_DATADIR
    # can appear in the devices or the device part, the conversion
    # function between the two is a bit more subtle then a mere replacement.

    # This function tests the conversion between a container path
    # and a synced container path
    def test_container_to_synced_container_path_conversion(self):
        # The conversion functions are oblivious to the suffix
        # so we just pick up a constant one.
        db_path_suffix = self._db_path_suffix()

        # We build various container path putting in both
        # DATADIR and SYNC_DATADIR strings in the
        # device and devices parts.
        for devices, device in self._container_path_elements_generator():
            path = os.path.join(devices, device, DATADIR, db_path_suffix)
            # Call the conversion function
            sds = sync_store.ContainerSyncStore(devices, self.logger, False)
            path = sds._container_to_synced_container_path(path)
            # Validate that ONLY the DATADIR part was replaced with
            # sync_store.SYNC_DATADIR
            self._validate_container_path_parts(path, devices, device,
                                                sync_store.SYNC_DATADIR,
                                                db_path_suffix)

    # This function tests the conversion between a synced container path
    # and a container path
    def test_synced_container_to_container_path_conversion(self):
        # The conversion functions are oblivious to the suffix
        # so we just pick up a constant one.
        db_path_suffix = ('133791/625/82a7f5a2c43281b0eab3597e35bb9625/'
                          '82a7f5a2c43281b0eab3597e35bb9625.db')

        # We build various synced container path putting in both
        # DATADIR and SYNC_DATADIR strings in the
        # device and devices parts.
        for devices, device in self._container_path_elements_generator():
            path = os.path.join(devices, device,
                                sync_store.SYNC_DATADIR, db_path_suffix)
            # Call the conversion function
            sds = sync_store.ContainerSyncStore(devices, self.logger, False)
            path = sds._synced_container_to_container_path(path)
            # Validate that ONLY the SYNC_DATADIR part was replaced with
            # DATADIR
            self._validate_container_path_parts(path, devices, device,
                                                DATADIR,
                                                db_path_suffix)

    # Constructs a db path suffix of the form:
    # 133791/625/82...25/82...25.db
    def _db_path_suffix(self):
        def random_hexa_string(length):
            '%0x' % random.randrange(16 ** length)

        db = random_hexa_string(32)
        return '%s/%s/%s/%s.db' % (random_hexa_string(5),
                                   random_hexa_string(3),
                                   db, db)

    def _container_path_elements_generator(self):
        # We build various container path elements putting in both
        # DATADIR and SYNC_DATADIR strings in the
        # device and devices parts.
        for devices in ['/srv/node', '/srv/node/',
                        '/srv/node/dev',
                        '/srv/node/%s' % DATADIR,
                        '/srv/node/%s' % sync_store.SYNC_DATADIR]:
            for device in ['sdf1', 'sdf1/sdf2',
                           'sdf1/%s' % DATADIR,
                           'sdf1/%s' % sync_store.SYNC_DATADIR,
                           '%s/sda' % DATADIR,
                           '%s/sda' % sync_store.SYNC_DATADIR]:
                yield devices, device

    def _validate_container_path_parts(self, path, devices,
                                       device, target, suffix):
        # Recall that the path is of the form:
        # devices/device/target/suffix
        # where each of the sub path elements (e.g. devices)
        # has a path structure containing path elements separated by '/'
        # We thus validate by splitting the path according to '/'
        # traversing all of its path elements making sure that the
        # first elements are those of devices,
        # the second are those of device
        # etc.
        spath = path.split('/')
        spath.reverse()
        self.assertEqual(spath.pop(), '')
        # Validate path against 'devices'
        for p in [p for p in devices.split('/') if p]:
            self.assertEqual(spath.pop(), p)
        # Validate path against 'device'
        for p in [p for p in device.split('/') if p]:
            self.assertEqual(spath.pop(), p)
        # Validate path against target
        self.assertEqual(spath.pop(), target)
        # Validate path against suffix
        for p in [p for p in suffix.split('/') if p]:
            self.assertEqual(spath.pop(), p)

    def test_add_synced_container(self):
        # Add non-existing and existing synced containers
        sds = sync_store.ContainerSyncStore(self.devices_dir,
                                            self.logger,
                                            False)
        cfile = self.pick_dbfile()
        broker = FakeContainerBroker(cfile)
        for i in range(2):
            sds.add_synced_container(broker)
            scpath = sds._container_to_synced_container_path(cfile)
            with open(scpath, 'r') as infile:
                self.assertEqual(infile.read(), cfile)

        iterated_synced_containers = list()
        for db_path in sds.synced_containers_generator():
            iterated_synced_containers.append(db_path)

        self.assertEqual(len(iterated_synced_containers), 1)

    def test_remove_synced_container(self):
        # Add a synced container to remove
        sds = sync_store.ContainerSyncStore(self.devices_dir,
                                            self.logger,
                                            False)
        cfile = self.pick_dbfile()
        # We keep here the link file so as to validate its deletion later
        lfile = sds._container_to_synced_container_path(cfile)
        broker = FakeContainerBroker(cfile)
        sds.add_synced_container(broker)

        # Remove existing and non-existing synced containers
        for i in range(2):
            sds.remove_synced_container(broker)

        iterated_synced_containers = list()
        for db_path in sds.synced_containers_generator():
            iterated_synced_containers.append(db_path)

        self.assertEqual(len(iterated_synced_containers), 0)

        # Make sure the whole link path gets deleted
        # recall that the path has the following suffix:
        # <hexa string of length 6>/<hexa string of length 3>/
        # <hexa string of length 32>/<same 32 hexa string>.db
        # and we expect the .db as well as all path elements
        # to get deleted
        self.assertFalse(os.path.exists(lfile))
        lfile = os.path.dirname(lfile)
        for i in range(3):
            self.assertFalse(os.path.exists(os.path.dirname(lfile)))
            lfile = os.path.dirname(lfile)

    def test_iterate_synced_containers(self):
        # populate sync container db
        sds = sync_store.ContainerSyncStore(self.devices_dir,
                                            self.logger,
                                            False)
        containers = list()
        for i in range(10):
            cfile = self.pick_dbfile()
            broker = FakeContainerBroker(cfile)
            sds.add_synced_container(broker)
            containers.append(cfile)

        iterated_synced_containers = list()
        for db_path in sds.synced_containers_generator():
            iterated_synced_containers.append(db_path)

        self.assertEqual(
            set(containers), set(iterated_synced_containers))

    def test_unhandled_exceptions_in_add_remove(self):
        sds = sync_store.ContainerSyncStore(self.devices_dir,
                                            self.logger,
                                            False)
        cfile = self.pick_dbfile()
        broker = FakeContainerBroker(cfile)

        with mock.patch(
                'swift.container.sync_store.os.stat',
                side_effect=OSError(errno.EPERM, 'permission denied')):
            with self.assertRaises(OSError) as cm:
                sds.add_synced_container(broker)
        self.assertEqual(errno.EPERM, cm.exception.errno)

        with mock.patch(
                'swift.container.sync_store.os.makedirs',
                side_effect=OSError(errno.EPERM, 'permission denied')):
            with self.assertRaises(OSError) as cm:
                sds.add_synced_container(broker)
        self.assertEqual(errno.EPERM, cm.exception.errno)

        with mock.patch(
                'swift.container.sync_store.os.symlink',
                side_effect=OSError(errno.EPERM, 'permission denied')):
            with self.assertRaises(OSError) as cm:
                sds.add_synced_container(broker)
        self.assertEqual(errno.EPERM, cm.exception.errno)

        with mock.patch(
                'swift.container.sync_store.os.unlink',
                side_effect=OSError(errno.EPERM, 'permission denied')):
            with self.assertRaises(OSError) as cm:
                sds.remove_synced_container(broker)
        self.assertEqual(errno.EPERM, cm.exception.errno)

    def test_update_sync_store_according_to_metadata_and_deleted(self):
        # This function tests the update_sync_store 'logics'
        # with respect to various combinations of the
        # sync-to and sync-key metadata items and whether
        # the database is marked for delete.
        # The table below summarizes the expected result
        # for the various combinations, e.g.:
        # If metadata items exist and the database
        # is not marked for delete then add should be called.

        results_list = [
            [False, 'a', 'b', 'add'],
            [False, 'a', '', 'remove'],
            [False, 'a', None, 'remove'],
            [False, '', 'b', 'remove'],
            [False, '', '', 'remove'],
            [False, '', None, 'remove'],
            [False, None, 'b', 'remove'],
            [False, None, '', 'remove'],
            [False, None, None, 'none'],
            [True, 'a', 'b', 'remove'],
            [True, 'a', '', 'remove'],
            [True, 'a', None, 'remove'],
            [True, '', 'b', 'remove'],
            [True, '', '', 'remove'],
            [True, '', None, 'remove'],
            [True, None, 'b', 'remove'],
            [True, None, '', 'remove'],
            [True, None, None, 'none'],
        ]

        store = 'swift.container.sync_store.ContainerSyncStore'
        with mock.patch(store + '.add_synced_container') as add_container:
            with mock.patch(
                    store + '.remove_synced_container') as remove_container:
                sds = sync_store.ContainerSyncStore(self.devices_dir,
                                                    self.logger,
                                                    False)
                add_calls = 0
                remove_calls = 0
                # We now iterate over the list of combinations
                # Validating that add and removed are called as
                # expected
                for deleted, sync_to, sync_key, expected_op in results_list:
                    cfile = self.pick_dbfile()
                    broker = FakeContainerBroker(cfile)
                    broker._is_deleted = deleted
                    if sync_to is not None:
                        broker.metadata['X-Container-Sync-To'] = [
                            sync_to, 1]
                    if sync_key is not None:
                        broker.metadata['X-Container-Sync-Key'] = [
                            sync_key, 1]
                    sds.update_sync_store(broker)
                    if expected_op == 'add':
                        add_calls += 1
                    if expected_op == 'remove':
                        remove_calls += 1
                    self.assertEqual(add_container.call_count,
                                     add_calls)
                    self.assertEqual(remove_container.call_count,
                                     remove_calls)


if __name__ == '__main__':
    unittest.main()
