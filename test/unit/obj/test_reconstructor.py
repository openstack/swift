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
import unittest
import os
import cPickle as pickle
import tempfile

from contextlib import closing
from gzip import GzipFile
from shutil import rmtree
from test.unit import FakeLogger, patch_policies
from swift.common import utils
from swift.obj import diskfile, reconstructor as object_reconstructor
from swift.common import ring
from swift.common.storage_policy import StoragePolicy, POLICIES, \
    REPL_POLICY


def _create_test_rings(path):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2, 3, 4, 5, 6],
        [1, 2, 3, 0, 5, 6, 4],
        [2, 3, 0, 1, 6, 4, 5]
    ]

    intended_devs = [
        {'id': 0, 'device': 'sda', 'zone': 0, 'ip': '127.0.0.0', 'port': 6000},
        {'id': 1, 'device': 'sda', 'zone': 1, 'ip': '127.0.0.1', 'port': 6000},
        {'id': 2, 'device': 'sda', 'zone': 2, 'ip': '127.0.0.2', 'port': 6000},
        {'id': 3, 'device': 'sda', 'zone': 4, 'ip': '127.0.0.3', 'port': 6000},
        {'id': 4, 'device': 'sda', 'zone': 5, 'ip': '127.0.0.4', 'port': 6000},
        {'id': 5, 'device': 'sda', 'zone': 6,
         'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6000},
        {'id': 6, 'device': 'sda', 'zone': 7,
         'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'port': 6000},
    ]
    intended_part_shift = 30
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift),
            f)

    testgz = os.path.join(path, 'object-1.ring.gz')
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift),
            f)
    return


@patch_policies([
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 0, 'name': 'zero', 'is_default': False}),
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 1, 'name': 'one', 'is_default': True})
])
class TestObjectReconstructor(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        os.mkdir(os.path.join(self.devices, 'sda'))
        self.objects = os.path.join(self.devices, 'sda',
                                    diskfile.get_data_dir(0))
        self.objects_1 = os.path.join(self.devices, 'sda',
                                      diskfile.get_data_dir(1))
        os.mkdir(self.objects)
        os.mkdir(self.objects_1)
        self.parts = {}
        self.parts_1 = {}
        self.part_nums = ['0', '1', '2', '3']
        for part in self.part_nums:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(self.parts[part])
            self.parts_1[part] = os.path.join(self.objects_1, part)
            os.mkdir(self.parts_1[part])
        _create_test_rings(self.testdir)
        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')
        self.reconstructor = \
            object_reconstructor.ObjectReconstructor(self.conf)
        self.reconstructor.logger = FakeLogger()
        self.df_mgr = diskfile.DiskFileManager(self.conf,
                                               self.reconstructor.logger)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_get_partners(self):

        # both of the expected values below are exhaustive possible results
        # from get_part nodes given the polices and dev lists defined in
        # our custom test ring.  We confirm every combination returns
        # the expected values

        # format: [(devid in question), (part_nodes for the given part), ...]
        expected_handoffs = \
            [(0, [1, 2, 3]), (1, [2, 3, 0]), (2, [3, 0, 1]),
             (3, [0, 1, 2]), (4, [0, 1, 2]), (4, [1, 2, 3]), (4, [2, 3, 0]),
             (4, [3, 0, 1]), (5, [0, 1, 2]), (5, [1, 2, 3]), (5, [2, 3, 0]),
             (5, [3, 0, 1]), (6, [0, 1, 2]), (6, [1, 2, 3]), (6, [2, 3, 0]),
             (6, [3, 0, 1]), (0, [1, 2, 3]), (1, [2, 3, 0]), (2, [3, 0, 1]),
             (3, [0, 1, 2]), (4, [0, 1, 2]), (4, [1, 2, 3]), (4, [2, 3, 0]),
             (4, [3, 0, 1]), (5, [0, 1, 2]), (5, [1, 2, 3]), (5, [2, 3, 0]),
             (5, [3, 0, 1]), (6, [0, 1, 2]), (6, [1, 2, 3]), (6, [2, 3, 0]),
             (6, [3, 0, 1])]

        # format: [(devid in question), (part_nodes for the given part),
        #          left id, right id...]
        expected_partners = \
            [(0, [0, 1, 2], 2, 1), (0, [2, 3, 0], 3, 2),
             (0, [3, 0, 1], 3, 1), (1, [0, 1, 2], 0, 2), (1, [1, 2, 3], 3, 2),
             (1, [3, 0, 1], 0, 3), (2, [0, 1, 2], 1, 0), (2, [1, 2, 3], 1, 3),
             (2, [2, 3, 0], 0, 3), (3, [1, 2, 3], 2, 1), (3, [2, 3, 0], 2, 0),
             (3, [3, 0, 1], 1, 0), (0, [0, 1, 2], 2, 1), (0, [2, 3, 0], 3, 2),
             (0, [3, 0, 1], 3, 1), (1, [0, 1, 2], 0, 2), (1, [1, 2, 3], 3, 2),
             (1, [3, 0, 1], 0, 3), (2, [0, 1, 2], 1, 0), (2, [1, 2, 3], 1, 3),
             (2, [2, 3, 0], 0, 3), (3, [1, 2, 3], 2, 1), (3, [2, 3, 0], 2, 0),
             (3, [3, 0, 1], 1, 0)]

        got_handoffs = []
        got_partners = []
        for pol in POLICIES:
            obj_ring = self.reconstructor.get_object_ring(pol.idx)
            for local_dev in obj_ring.devs:
                for part_num in self.part_nums:
                    part_nodes = obj_ring.get_part_nodes(int(part_num))
                    ids = []
                    for node in part_nodes:
                        ids.append(node['id'])
                    handoff, partners = self.reconstructor._get_partners(
                        local_dev['id'], obj_ring, part_num)
                    if handoff is False:
                        left = partners[0]['id']
                        right = partners[1]['id']
                        got_partners.append((local_dev['id'], ids, left,
                                             right))
                    else:
                        got_handoffs.append((local_dev['id'], ids))

        self.assertEquals(expected_handoffs, got_handoffs)
        self.assertEquals(expected_partners, got_partners)


if __name__ == '__main__':
    unittest.main()
