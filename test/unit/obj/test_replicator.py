# Copyright (c) 2010 OpenStack, LLC.
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

from __future__ import with_statement

import unittest
import os
from gzip import GzipFile
from shutil import rmtree
import cPickle as pickle
import logging
import fcntl
from contextlib import contextmanager

from eventlet.green import subprocess

from swift.obj import replicator as object_replicator
from swift.common import ring

def _ips():
    return ['127.0.0.0',]
object_replicator.whataremyips = _ips

class NullHandler(logging.Handler):
    def emit(self, record):
        pass
null_logger = logging.getLogger("testing")
null_logger.addHandler(NullHandler())

class MockProcess(object):
    ret_code = None
    ret_log = None

    class Stream(object):
        def read(self):
            return MockProcess.ret_log.next()

    def __init__(self, *args, **kwargs):
        self.stdout = self.Stream()

    def wait(self):
        return self.ret_code.next()

@contextmanager
def _mock_process(ret):
    orig_process = subprocess.Popen
    MockProcess.ret_code = (i[0] for i in ret)
    MockProcess.ret_log = (i[1] for i in ret)
    object_replicator.subprocess.Popen = MockProcess
    yield
    object_replicator.subprocess.Popen = orig_process

def _create_test_ring(path):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2, 3, 4, 5, 6],
        [1, 2, 3, 0, 5, 6, 4],
        [2, 3, 0, 1, 6, 4, 5],
        ]
    intended_devs = [
        {'id': 0, 'device': 'sda', 'zone': 0, 'ip': '127.0.0.0', 'port': 6000},
        {'id': 1, 'device': 'sda', 'zone': 1, 'ip': '127.0.0.1', 'port': 6000},
        {'id': 2, 'device': 'sda', 'zone': 2, 'ip': '127.0.0.2', 'port': 6000},
        {'id': 3, 'device': 'sda', 'zone': 4, 'ip': '127.0.0.3', 'port': 6000},
        {'id': 4, 'device': 'sda', 'zone': 5, 'ip': '127.0.0.4', 'port': 6000},
        {'id': 5, 'device': 'sda', 'zone': 6, 'ip': '127.0.0.5', 'port': 6000},
        {'id': 6, 'device': 'sda', 'zone': 7, 'ip': '127.0.0.6', 'port': 6000},
        ]
    intended_part_shift = 30
    intended_reload_time = 15
    pickle.dump(ring.RingData(intended_replica2part2dev_id,
        intended_devs, intended_part_shift),
        GzipFile(testgz, 'wb'))
    return ring.Ring(testgz, reload_time=intended_reload_time)


class TestObjectReplicator(unittest.TestCase):

    def setUp(self):
        # Setup a test ring (stolen from common/test_ring.py)
        self.testdir = os.path.join('/dev/shm', 'test_replicator')
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        os.mkdir(os.path.join(self.devices, 'sda'))
        self.objects = os.path.join(self.devices, 'sda', 'objects')
        os.mkdir(self.objects)
        for part in ['0','1','2', '3']:
            os.mkdir(os.path.join(self.objects, part))
        self.ring = _create_test_ring(self.testdir)
        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')
        self.replicator = object_replicator.ObjectReplicator(
            self.conf, null_logger)

#    def test_check_ring(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        self.assertTrue(self.replicator.check_ring())
#        orig_check = self.replicator.next_check
#        self.replicator.next_check = orig_check - 30
#        self.assertTrue(self.replicator.check_ring())
#        self.replicator.next_check = orig_check
#        orig_ring_time = self.replicator.object_ring._mtime
#        self.replicator.object_ring._mtime = orig_ring_time - 30
#        self.assertTrue(self.replicator.check_ring())
#        self.replicator.next_check = orig_check - 30
#        self.assertFalse(self.replicator.check_ring())
#
#    def test_collect_jobs(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        self.assertTrue('1' in self.replicator.parts_to_delete)
#        self.assertEquals(
#            [node['id'] for node in self.replicator.partitions['0']['nodes']],
#            [1,2])
#        self.assertEquals(
#            [node['id'] for node in self.replicator.partitions['1']['nodes']],
#            [1,2,3])
#        self.assertEquals(
#            [node['id'] for node in self.replicator.partitions['2']['nodes']],
#            [2,3])
#        self.assertEquals(
#            [node['id'] for node in self.replicator.partitions['3']['nodes']],
#            [3,1])
#        for part in ['0', '1', '2', '3']:
#            self.assertEquals(self.replicator.partitions[part]['device'], 'sda')
#            self.assertEquals(self.replicator.partitions[part]['path'],
#                self.objects)
#
#    def test_delete_partition(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        part_path = os.path.join(self.objects, '1')
#        self.assertTrue(os.access(part_path, os.F_OK))
#        self.replicator.delete_partition('1')
#        self.assertFalse(os.access(part_path, os.F_OK))
#
#    def test_rsync(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        with _mock_process([(0,''), (0,''), (0,'')]):
#            self.replicator.rsync('0')
#
#    def test_rsync_delete_no(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        with _mock_process([(-1, "stuff in log"), (-1, "stuff in log"),
#                (0,''), (0,'')]):
#            self.replicator.rsync('1')
#            self.assertEquals(self.replicator.parts_to_delete['1'],
#                [False, True, True])
#
#    def test_rsync_delete_yes(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        with _mock_process([(0,''), (0,''), (0,'')]):
#            self.replicator.rsync('1')
#            self.assertEquals(self.replicator.parts_to_delete['1'],
#                [True, True, True])
#
#    def test_rsync_delete_yes_with_failure(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        with _mock_process([(-1, "stuff in log"), (0, ''), (0,''), (0,'')]):
#            self.replicator.rsync('1')
#            self.assertEquals(self.replicator.parts_to_delete['1'],
#                [True, True, True])
#
#    def test_rsync_failed_drive(self):
#        self.replicator.collect_jobs('sda', 0, self.ring)
#        with _mock_process([(12,'There was an error in file IO'),
#            (0,''), (0,''), (0,'')]):
#            self.replicator.rsync('1')
#            self.assertEquals(self.replicator.parts_to_delete['1'],
#                [True, True, True])

    def test_run(self):
        with _mock_process([(0,'')]*100):
            self.replicator.run()

    def test_run_withlog(self):
        with _mock_process([(0,"stuff in log")]*100):
            self.replicator.run()

if __name__ == '__main__':
    unittest.main()
