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
import collections
import json
import unittest
import os
import mock
from gzip import GzipFile
from shutil import rmtree
import six.moves.cPickle as pickle
import time
import tempfile
from contextlib import contextmanager, closing
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY, ENOTDIR

from eventlet.green import subprocess
from eventlet import Timeout, sleep

from test.unit import (debug_logger, patch_policies, make_timestamp_iter,
                       mocked_http_conn, mock_check_drive, skip_if_no_xattrs)
from swift.common import utils
from swift.common.utils import (hash_path, mkdirs, normalize_timestamp,
                                storage_directory)
from swift.common import ring
from swift.obj import diskfile, replicator as object_replicator
from swift.common.storage_policy import StoragePolicy, POLICIES


def _ips(*args, **kwargs):
    return ['127.0.0.0']


def mock_http_connect(status):

    class FakeConn(object):

        def __init__(self, status, *args, **kwargs):
            self.status = status
            self.reason = 'Fake'
            self.host = args[0]
            self.port = args[1]
            self.method = args[4]
            self.path = args[5]
            self.with_exc = False
            self.headers = kwargs.get('headers', {})

        def getresponse(self):
            if self.with_exc:
                raise Exception('test')
            return self

        def getheader(self, header):
            return self.headers[header]

        def read(self, amt=None):
            return pickle.dumps({})

        def close(self):
            return
    return lambda *args, **kwargs: FakeConn(status, *args, **kwargs)

process_errors = []


class MockProcess(object):
    ret_code = None
    ret_log = None
    check_args = None
    captured_log = None

    class Stream(object):

        def read(self):
            return next(MockProcess.ret_log)

    def __init__(self, *args, **kwargs):
        targs = next(MockProcess.check_args)
        for targ in targs:
            # Allow more than 2 candidate targs
            # (e.g. a case that either node is fine when nodes shuffled)
            if isinstance(targ, tuple):
                allowed = False
                for target in targ:
                    if target in args[0]:
                        allowed = True
                if not allowed:
                    process_errors.append("Invalid: %s not in %s" % (targ,
                                                                     args))
            else:
                if targ not in args[0]:
                    process_errors.append("Invalid: %s not in %s" % (targ,
                                                                     args))
        self.captured_info = {
            'rsync_args': args[0],
        }
        self.stdout = self.Stream()

    def wait(self):
        # the _mock_process context manager assures this class attribute is a
        # mutable list and takes care of resetting it
        rv = next(self.ret_code)
        if self.captured_log is not None:
            self.captured_info['ret_code'] = rv
            self.captured_log.append(self.captured_info)
        return rv


@contextmanager
def _mock_process(ret):
    captured_log = []
    MockProcess.captured_log = captured_log
    orig_process = subprocess.Popen
    MockProcess.ret_code = (i[0] for i in ret)
    MockProcess.ret_log = (i[1] for i in ret)
    MockProcess.check_args = (i[2] for i in ret)
    object_replicator.subprocess.Popen = MockProcess
    yield captured_log
    MockProcess.captured_log = None
    object_replicator.subprocess.Popen = orig_process


class MockHungProcess(object):
    def __init__(self, polls_needed=0, *args, **kwargs):
        class MockStdout(object):
            def read(self):
                pass
        self.stdout = MockStdout()
        self._state = 'running'
        self._calls = []
        self._polls = 0
        self._polls_needed = polls_needed

    def wait(self, timeout=None):
        self._calls.append(('wait', self._state))
        if self._state == 'running':
            # Sleep so we trip the rsync timeout
            sleep(1)
            raise BaseException('You need to mock out some timeouts')
        if not self._polls_needed:
            self._state = 'os-reaped'
            return 137
        if timeout is not None:
            raise subprocess.TimeoutExpired('some cmd', timeout)
        raise BaseException("You're waiting indefinitely on something "
                            "we've established is hung")

    def poll(self):
        self._calls.append(('poll', self._state))
        self._polls += 1
        if self._polls >= self._polls_needed:
            self._state = 'os-reaped'
            return 137
        else:
            return None

    def terminate(self):
        self._calls.append(('terminate', self._state))
        if self._state == 'running':
            self._state = 'terminating'

    def kill(self):
        self._calls.append(('kill', self._state))
        self._state = 'killed'


def _create_test_rings(path, devs=None, next_part_power=None):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2, 3, 4, 5, 6],
        [1, 2, 3, 0, 5, 6, 4],
        [2, 3, 0, 1, 6, 4, 5],
    ]
    intended_devs = devs or [
        {'id': 0, 'device': 'sda', 'zone': 0,
         'region': 1, 'ip': '127.0.0.0', 'port': 6200},
        {'id': 1, 'device': 'sda', 'zone': 1,
         'region': 2, 'ip': '127.0.0.1', 'port': 6200},
        {'id': 2, 'device': 'sda', 'zone': 2,
         'region': 3, 'ip': '127.0.0.2', 'port': 6200},
        {'id': 3, 'device': 'sda', 'zone': 4,
         'region': 2, 'ip': '127.0.0.3', 'port': 6200},
        {'id': 4, 'device': 'sda', 'zone': 5,
         'region': 1, 'ip': '127.0.0.4', 'port': 6200,
         'replication_ip': '127.0.1.4'},
        {'id': 5, 'device': 'sda', 'zone': 6,
         'region': 3, 'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6200},
        {'id': 6, 'device': 'sda', 'zone': 7, 'region': 1,
         'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'port': 6200},
    ]
    intended_part_shift = 30
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift, next_part_power),
            f)

    testgz = os.path.join(path, 'object-1.ring.gz')
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift, next_part_power),
            f)
    for policy in POLICIES:
        policy.object_ring = None  # force reload
    return


@patch_policies([StoragePolicy(0, 'zero', False),
                StoragePolicy(1, 'one', True)])
class TestObjectReplicator(unittest.TestCase):

    def setUp(self):
        skip_if_no_xattrs()
        utils.HASH_PATH_SUFFIX = b'endcap'
        utils.HASH_PATH_PREFIX = b''
        # recon cache path
        self.recon_cache = tempfile.mkdtemp()
        rmtree(self.recon_cache, ignore_errors=1)
        os.mkdir(self.recon_cache)
        # Setup a test ring (stolen from common/test_ring.py)
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)

        self.objects, self.objects_1, self.parts, self.parts_1 = \
            self._write_disk_data('sda')
        _create_test_rings(self.testdir)
        self.logger = debug_logger('test-replicator')
        self.conf = dict(
            bind_ip=_ips()[0], bind_port=6200,
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1', sync_method='rsync',
            recon_cache_path=self.recon_cache)
        self._create_replicator()
        self.ts = make_timestamp_iter()

    def tearDown(self):
        self.assertFalse(process_errors)
        rmtree(self.testdir, ignore_errors=1)
        rmtree(self.recon_cache, ignore_errors=1)

    def test_handoff_replication_setting_warnings(self):
        conf_tests = [
            # (config, expected_warning)
            ({}, False),
            ({'handoff_delete': 'auto'}, False),
            ({'handoffs_first': 'no'}, False),
            ({'handoff_delete': '2'}, True),
            ({'handoffs_first': 'yes'}, True),
            ({'handoff_delete': '1', 'handoffs_first': 'yes'}, True),
        ]
        log_message = 'Handoff only mode is not intended for normal ' \
            'operation, please disable handoffs_first and ' \
            'handoff_delete before the next normal rebalance'
        for config, expected_warning in conf_tests:
            self.logger.clear()
            object_replicator.ObjectReplicator(config, logger=self.logger)
            warning_log_lines = self.logger.get_lines_for_level('warning')
            if expected_warning:
                expected_log_lines = [log_message]
            else:
                expected_log_lines = []
            self.assertEqual(expected_log_lines, warning_log_lines,
                             'expected %s != %s for config %r' % (
                                 expected_log_lines,
                                 warning_log_lines,
                                 config,
                             ))

    def _write_disk_data(self, disk_name, with_json=False):
        os.mkdir(os.path.join(self.devices, disk_name))
        objects = os.path.join(self.devices, disk_name,
                               diskfile.get_data_dir(POLICIES[0]))
        objects_1 = os.path.join(self.devices, disk_name,
                                 diskfile.get_data_dir(POLICIES[1]))
        os.mkdir(objects)
        os.mkdir(objects_1)
        parts = {}
        parts_1 = {}
        for part in ['0', '1', '2', '3']:
            parts[part] = os.path.join(objects, part)
            os.mkdir(parts[part])
            parts_1[part] = os.path.join(objects_1, part)
            os.mkdir(parts_1[part])

        if with_json:
            for json_file in ['auditor_status_ZBF.json',
                              'auditor_status_ALL.json']:
                for obj_dir in [objects, objects_1]:
                    with open(os.path.join(obj_dir, json_file), 'w'):
                        pass

        return objects, objects_1, parts, parts_1

    def _create_replicator(self):
        self.replicator = object_replicator.ObjectReplicator(self.conf)
        self.replicator.logger = self.logger
        self.replicator._zero_stats()
        self.replicator.all_devs_info = set()
        self.df_mgr = diskfile.DiskFileManager(self.conf, self.logger)

    def test_run_once_no_local_device_in_ring(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    bind_ip='1.1.1.1', recon_cache_path=self.recon_cache,
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf,
                                                        logger=self.logger)
        replicator.run_once()
        expected = [
            "Can't find itself in policy with index 0 with ips 1.1.1.1 and"
            " with port 6200 in ring file, not replicating",
            "Can't find itself in policy with index 1 with ips 1.1.1.1 and"
            " with port 6200 in ring file, not replicating",
        ]
        self.assertEqual(expected, self.logger.get_lines_for_level('error'))

    def test_run_once(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    bind_ip=_ips()[0], recon_cache_path=self.recon_cache,
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf,
                                                        logger=self.logger)
        was_connector = object_replicator.http_connect
        object_replicator.http_connect = mock_http_connect(200)
        cur_part = '0'
        df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o',
                                      policy=POLICIES[0])
        mkdirs(df._datadir)
        f = open(os.path.join(df._datadir,
                              normalize_timestamp(time.time()) + '.data'),
                 'wb')
        f.write(b'1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, cur_part, data_dir)
        process_arg_checker = []
        ring = replicator.load_object_ring(POLICIES[0])
        nodes = [node for node in
                 ring.get_part_nodes(int(cur_part))
                 if node['ip'] not in _ips()]
        rsync_mods = tuple(['%s::object/sda/objects/%s' %
                            (node['ip'], cur_part) for node in nodes])
        for node in nodes:
            process_arg_checker.append(
                (0, '', ['rsync', whole_path_from, rsync_mods]))
        start = replicator.replication_cycle
        self.assertGreaterEqual(start, 0)
        self.assertLessEqual(start, 9)
        with _mock_process(process_arg_checker):
            replicator.run_once()
        self.assertEqual((start + 1) % 10, replicator.replication_cycle)
        self.assertFalse(process_errors)
        self.assertFalse(self.logger.get_lines_for_level('error'))

        # Returns 0 at first, and 60 on all following .next() calls
        def _infinite_gen():
            yield 0
            while True:
                yield 60

        for cycle in range(1, 10):
            with _mock_process(process_arg_checker):
                with mock.patch('time.time', side_effect=_infinite_gen()):
                    replicator.run_once()
                    self.assertEqual((start + 1 + cycle) % 10,
                                     replicator.replication_cycle)

        recon_fname = os.path.join(self.recon_cache, "object.recon")
        with open(recon_fname) as cachefile:
            recon = json.loads(cachefile.read())
            self.assertEqual(1, recon.get('replication_time'))
            self.assertIn('replication_stats', recon)
            self.assertIn('replication_last', recon)
        expected = 'Object replication complete (once). (1.00 minutes)'
        self.assertIn(expected, self.logger.get_lines_for_level('info'))
        self.assertFalse(self.logger.get_lines_for_level('error'))
        object_replicator.http_connect = was_connector

    # policy 1
    def test_run_once_1(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    recon_cache_path=self.recon_cache,
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf,
                                                        logger=self.logger)
        was_connector = object_replicator.http_connect
        object_replicator.http_connect = mock_http_connect(200)
        cur_part = '0'
        df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o',
                                      policy=POLICIES[1])
        mkdirs(df._datadir)
        f = open(os.path.join(df._datadir,
                              normalize_timestamp(time.time()) + '.data'),
                 'wb')
        f.write(b'1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects_1, cur_part, data_dir)
        process_arg_checker = []
        ring = replicator.load_object_ring(POLICIES[1])
        nodes = [node for node in
                 ring.get_part_nodes(int(cur_part))
                 if node['ip'] not in _ips()]
        rsync_mods = tuple(['%s::object/sda/objects-1/%s' %
                            (node['ip'], cur_part) for node in nodes])
        for node in nodes:
            process_arg_checker.append(
                (0, '', ['rsync', whole_path_from, rsync_mods]))
        with _mock_process(process_arg_checker):
            with mock.patch('swift.obj.replicator.whataremyips',
                            side_effect=_ips):
                replicator.run_once()
        self.assertFalse(process_errors)
        self.assertFalse(self.logger.get_lines_for_level('error'))
        object_replicator.http_connect = was_connector

    def test_check_ring(self):
        for pol in POLICIES:
            obj_ring = self.replicator.load_object_ring(pol)
            self.assertTrue(self.replicator.check_ring(obj_ring))
            orig_check = self.replicator.next_check
            self.replicator.next_check = orig_check - 30
            self.assertTrue(self.replicator.check_ring(obj_ring))
            self.replicator.next_check = orig_check
            orig_ring_time = obj_ring._mtime
            obj_ring._mtime = orig_ring_time - 30
            self.assertTrue(self.replicator.check_ring(obj_ring))
            self.replicator.next_check = orig_check - 30
            self.assertFalse(self.replicator.check_ring(obj_ring))

    def test_collect_jobs_mkdirs_error(self):

        non_local = {}

        def blowup_mkdirs(path):
            non_local['path'] = path
            raise OSError('Ow!')

        with mock.patch.object(object_replicator, 'mkdirs', blowup_mkdirs):
            rmtree(self.objects, ignore_errors=1)
            object_replicator.mkdirs = blowup_mkdirs
            self.replicator.collect_jobs()
            self.assertEqual(self.logger.get_lines_for_level('error'), [
                'ERROR creating %s: ' % non_local['path']])
            log_args, log_kwargs = self.logger.log_dict['error'][0]
            self.assertEqual(str(log_kwargs['exc_info'][1]), 'Ow!')

    def test_collect_jobs(self):
        jobs = self.replicator.collect_jobs()
        jobs_to_delete = [j for j in jobs if j['delete']]
        jobs_by_pol_part = {}
        for job in jobs:
            jobs_by_pol_part[str(int(job['policy'])) + job['partition']] = job
        self.assertEqual(len(jobs_to_delete), 2)
        self.assertTrue('1', jobs_to_delete[0]['partition'])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['00']['nodes']], [1, 2])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['01']['nodes']],
            [1, 2, 3])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['02']['nodes']], [2, 3])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['03']['nodes']], [3, 1])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['10']['nodes']], [1, 2])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['11']['nodes']],
            [1, 2, 3])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['12']['nodes']], [2, 3])
        self.assertEqual(
            [node['id'] for node in jobs_by_pol_part['13']['nodes']], [3, 1])
        for part in ['00', '01', '02', '03']:
            for node in jobs_by_pol_part[part]['nodes']:
                self.assertEqual(node['device'], 'sda')
            self.assertEqual(jobs_by_pol_part[part]['path'],
                             os.path.join(self.objects, part[1:]))
        for part in ['10', '11', '12', '13']:
            for node in jobs_by_pol_part[part]['nodes']:
                self.assertEqual(node['device'], 'sda')
            self.assertEqual(jobs_by_pol_part[part]['path'],
                             os.path.join(self.objects_1, part[1:]))

    def test_collect_jobs_unmounted(self):
        with mock_check_drive() as mocks:
            jobs = self.replicator.collect_jobs()
        self.assertEqual(jobs, [])
        self.assertEqual(mocks['ismount'].mock_calls, [])
        self.assertEqual(len(mocks['isdir'].mock_calls), 2)

        self.replicator.mount_check = True
        with mock_check_drive() as mocks:
            jobs = self.replicator.collect_jobs()
        self.assertEqual(jobs, [])
        self.assertEqual(mocks['isdir'].mock_calls, [])
        self.assertEqual(len(mocks['ismount'].mock_calls), 2)

    def test_collect_jobs_failure_report_with_auditor_stats_json(self):
        devs = [
            {'id': 0, 'device': 'sda', 'zone': 0,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            {'id': 1, 'device': 'sdb', 'zone': 1,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            {'id': 2, 'device': 'sdc', 'zone': 2,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.1', 'replication_port': 6200},
            {'id': 3, 'device': 'sdd', 'zone': 3,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.1', 'replication_port': 6200},
        ]
        objects_sdb, objects_1_sdb, _, _ = \
            self._write_disk_data('sdb', with_json=True)
        objects_sdc, objects_1_sdc, _, _ = \
            self._write_disk_data('sdc', with_json=True)
        objects_sdd, objects_1_sdd, _, _ = \
            self._write_disk_data('sdd', with_json=True)
        _create_test_rings(self.testdir, devs)

        self.replicator.collect_jobs()
        self.assertEqual(self.replicator.total_stats.failure, 0)

    @mock.patch('swift.obj.replicator.random.shuffle', side_effect=lambda l: l)
    def test_collect_jobs_multi_disk(self, mock_shuffle):
        devs = [
            # Two disks on same IP/port
            {'id': 0, 'device': 'sda', 'zone': 0,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            {'id': 1, 'device': 'sdb', 'zone': 1,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            # Two disks on same server, different ports
            {'id': 2, 'device': 'sdc', 'zone': 2,
             'region': 2, 'ip': '1.1.1.2', 'port': 1112,
             'replication_ip': '127.0.0.1', 'replication_port': 6200},
            {'id': 3, 'device': 'sdd', 'zone': 4,
             'region': 2, 'ip': '1.1.1.2', 'port': 1112,
             'replication_ip': '127.0.0.1', 'replication_port': 6201},
        ]
        objects_sdb, objects_1_sdb, _, _ = self._write_disk_data('sdb')
        objects_sdc, objects_1_sdc, _, _ = self._write_disk_data('sdc')
        objects_sdd, objects_1_sdd, _, _ = self._write_disk_data('sdd')
        _create_test_rings(self.testdir, devs)

        jobs = self.replicator.collect_jobs()

        self.assertEqual([mock.call(jobs)], mock_shuffle.mock_calls)

        jobs_to_delete = [j for j in jobs if j['delete']]
        self.assertEqual(len(jobs_to_delete), 4)
        self.assertEqual([
            '1', '2',  # policy 0; 1 not on sda, 2 not on sdb
            '1', '2',  # policy 1; 1 not on sda, 2 not on sdb
        ], [j['partition'] for j in jobs_to_delete])

        jobs_by_pol_part_dev = {}
        for job in jobs:
            # There should be no jobs with a device not in just sda & sdb
            self.assertTrue(job['device'] in ('sda', 'sdb'))
            jobs_by_pol_part_dev[
                str(int(job['policy'])) + job['partition'] + job['device']
            ] = job

        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['00sda']['nodes']],
                         [1, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['00sdb']['nodes']],
                         [0, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['01sda']['nodes']],
                         [1, 2, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['01sdb']['nodes']],
                         [2, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['02sda']['nodes']],
                         [2, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['02sdb']['nodes']],
                         [2, 3, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['03sda']['nodes']],
                         [3, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['03sdb']['nodes']],
                         [3, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['10sda']['nodes']],
                         [1, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['10sdb']['nodes']],
                         [0, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['11sda']['nodes']],
                         [1, 2, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['11sdb']['nodes']],
                         [2, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['12sda']['nodes']],
                         [2, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['12sdb']['nodes']],
                         [2, 3, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['13sda']['nodes']],
                         [3, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['13sdb']['nodes']],
                         [3, 0])
        for part in ['00', '01', '02', '03']:
            self.assertEqual(jobs_by_pol_part_dev[part + 'sda']['path'],
                             os.path.join(self.objects, part[1:]))
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdb']['path'],
                             os.path.join(objects_sdb, part[1:]))
        for part in ['10', '11', '12', '13']:
            self.assertEqual(jobs_by_pol_part_dev[part + 'sda']['path'],
                             os.path.join(self.objects_1, part[1:]))
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdb']['path'],
                             os.path.join(objects_1_sdb, part[1:]))

    @mock.patch('swift.obj.replicator.random.shuffle', side_effect=lambda l: l)
    def test_collect_jobs_multi_disk_diff_ports_normal(self, mock_shuffle):
        # Normally (servers_per_port=0), replication_ip AND replication_port
        # are used to determine local ring device entries.  Here we show that
        # with bind_ip='127.0.0.1', bind_port=6200, only "sdc" is local.
        devs = [
            # Two disks on same IP/port
            {'id': 0, 'device': 'sda', 'zone': 0,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            {'id': 1, 'device': 'sdb', 'zone': 1,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            # Two disks on same server, different ports
            {'id': 2, 'device': 'sdc', 'zone': 2,
             'region': 2, 'ip': '1.1.1.2', 'port': 1112,
             'replication_ip': '127.0.0.1', 'replication_port': 6200},
            {'id': 3, 'device': 'sdd', 'zone': 4,
             'region': 2, 'ip': '1.1.1.2', 'port': 1112,
             'replication_ip': '127.0.0.1', 'replication_port': 6201},
        ]
        objects_sdb, objects_1_sdb, _, _ = self._write_disk_data('sdb')
        objects_sdc, objects_1_sdc, _, _ = self._write_disk_data('sdc')
        objects_sdd, objects_1_sdd, _, _ = self._write_disk_data('sdd')
        _create_test_rings(self.testdir, devs)

        self.conf['bind_ip'] = '127.0.0.1'
        self._create_replicator()

        jobs = self.replicator.collect_jobs()

        self.assertEqual([mock.call(jobs)], mock_shuffle.mock_calls)

        jobs_to_delete = [j for j in jobs if j['delete']]
        self.assertEqual(len(jobs_to_delete), 2)
        self.assertEqual([
            '3',  # policy 0; 3 not on sdc
            '3',  # policy 1; 3 not on sdc
        ], [j['partition'] for j in jobs_to_delete])

        jobs_by_pol_part_dev = {}
        for job in jobs:
            # There should be no jobs with a device not sdc
            self.assertEqual(job['device'], 'sdc')
            jobs_by_pol_part_dev[
                str(int(job['policy'])) + job['partition'] + job['device']
            ] = job

        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['00sdc']['nodes']],
                         [0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['01sdc']['nodes']],
                         [1, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['02sdc']['nodes']],
                         [3, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['03sdc']['nodes']],
                         [3, 0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['10sdc']['nodes']],
                         [0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['11sdc']['nodes']],
                         [1, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['12sdc']['nodes']],
                         [3, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['13sdc']['nodes']],
                         [3, 0, 1])
        for part in ['00', '01', '02', '03']:
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdc']['path'],
                             os.path.join(objects_sdc, part[1:]))
        for part in ['10', '11', '12', '13']:
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdc']['path'],
                             os.path.join(objects_1_sdc, part[1:]))

    @mock.patch('swift.obj.replicator.random.shuffle', side_effect=lambda l: l)
    def test_collect_jobs_multi_disk_servers_per_port(self, mock_shuffle):
        # Normally (servers_per_port=0), replication_ip AND replication_port
        # are used to determine local ring device entries.  Here we show that
        # with servers_per_port > 0 and bind_ip='127.0.0.1', bind_port=6200,
        # then both "sdc" and "sdd" are local.
        devs = [
            # Two disks on same IP/port
            {'id': 0, 'device': 'sda', 'zone': 0,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            {'id': 1, 'device': 'sdb', 'zone': 1,
             'region': 1, 'ip': '1.1.1.1', 'port': 1111,
             'replication_ip': '127.0.0.0', 'replication_port': 6200},
            # Two disks on same server, different ports
            {'id': 2, 'device': 'sdc', 'zone': 2,
             'region': 2, 'ip': '1.1.1.2', 'port': 1112,
             'replication_ip': '127.0.0.1', 'replication_port': 6200},
            {'id': 3, 'device': 'sdd', 'zone': 4,
             'region': 2, 'ip': '1.1.1.2', 'port': 1112,
             'replication_ip': '127.0.0.1', 'replication_port': 6201},
        ]
        objects_sdb, objects_1_sdb, _, _ = self._write_disk_data('sdb')
        objects_sdc, objects_1_sdc, _, _ = self._write_disk_data('sdc')
        objects_sdd, objects_1_sdd, _, _ = self._write_disk_data('sdd')
        _create_test_rings(self.testdir, devs)

        self.conf['bind_ip'] = '127.0.0.1'
        self.conf['servers_per_port'] = 1  # diff port ok
        self._create_replicator()

        jobs = self.replicator.collect_jobs()

        self.assertEqual([mock.call(jobs)], mock_shuffle.mock_calls)

        jobs_to_delete = [j for j in jobs if j['delete']]
        self.assertEqual(len(jobs_to_delete), 4)
        self.assertEqual([
            '3', '0',  # policy 0; 3 not on sdc, 0 not on sdd
            '3', '0',  # policy 1; 3 not on sdc, 0 not on sdd
        ], [j['partition'] for j in jobs_to_delete])

        jobs_by_pol_part_dev = {}
        for job in jobs:
            # There should be no jobs with a device not in just sdc & sdd
            self.assertTrue(job['device'] in ('sdc', 'sdd'))
            jobs_by_pol_part_dev[
                str(int(job['policy'])) + job['partition'] + job['device']
            ] = job

        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['00sdc']['nodes']],
                         [0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['00sdd']['nodes']],
                         [0, 1, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['01sdc']['nodes']],
                         [1, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['01sdd']['nodes']],
                         [1, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['02sdc']['nodes']],
                         [3, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['02sdd']['nodes']],
                         [2, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['03sdc']['nodes']],
                         [3, 0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['03sdd']['nodes']],
                         [0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['10sdc']['nodes']],
                         [0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['10sdd']['nodes']],
                         [0, 1, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['11sdc']['nodes']],
                         [1, 3])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['11sdd']['nodes']],
                         [1, 2])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['12sdc']['nodes']],
                         [3, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['12sdd']['nodes']],
                         [2, 0])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['13sdc']['nodes']],
                         [3, 0, 1])
        self.assertEqual([node['id']
                          for node in jobs_by_pol_part_dev['13sdd']['nodes']],
                         [0, 1])
        for part in ['00', '01', '02', '03']:
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdc']['path'],
                             os.path.join(objects_sdc, part[1:]))
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdd']['path'],
                             os.path.join(objects_sdd, part[1:]))
        for part in ['10', '11', '12', '13']:
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdc']['path'],
                             os.path.join(objects_1_sdc, part[1:]))
            self.assertEqual(jobs_by_pol_part_dev[part + 'sdd']['path'],
                             os.path.join(objects_1_sdd, part[1:]))

    def test_collect_jobs_handoffs_first(self):
        self.replicator.handoffs_first = True
        jobs = self.replicator.collect_jobs()
        self.assertTrue(jobs[0]['delete'])
        self.assertEqual('1', jobs[0]['partition'])

    def test_handoffs_first_mode_will_process_all_jobs_after_handoffs(self):
        # make an object in the handoff & primary partition
        expected_suffix_paths = []
        for policy in POLICIES:
            # primary
            ts = next(self.ts)
            df = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o', policy)
            with df.create() as w:
                w.write(b'asdf')
                w.put({'X-Timestamp': ts.internal})
                w.commit(ts)
            expected_suffix_paths.append(os.path.dirname(df._datadir))
            # handoff
            ts = next(self.ts)
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o', policy)
            with df.create() as w:
                w.write(b'asdf')
                w.put({'X-Timestamp': ts.internal})
                w.commit(ts)
            expected_suffix_paths.append(os.path.dirname(df._datadir))

        # rsync will be called for all parts we created objects in
        process_arg_checker = [
            # (return_code, stdout, <each in capture rsync args>)
            (0, '', []),
            (0, '', []),
            (0, '', []),  # handoff job "first" policy
            (0, '', []),
            (0, '', []),
            (0, '', []),  # handoff job "second" policy
            (0, '', []),
            (0, '', []),  # update job "first" policy
            (0, '', []),
            (0, '', []),  # update job "second" policy
        ]
        # each handoff partition node gets one replicate request for after
        # rsync (2 * 3), each primary partition with objects gets two
        # replicate requests (pre-flight and post sync) to each of each
        # partners (2 * 2 * 2), the 2 remaining empty parts (2 & 3) get a
        # pre-flight replicate request per node for each storage policy
        # (2 * 2 * 2) - so 6 + 8 + 8 == 22
        replicate_responses = [200] * 22
        stub_body = pickle.dumps({})
        with _mock_process(process_arg_checker) as rsync_log, \
            mock.patch('swift.obj.replicator.whataremyips',
                       side_effect=_ips), \
                mocked_http_conn(*replicate_responses,
                                 body=stub_body) as conn_log:
            self.replicator.handoffs_first = True
            self.replicator.replicate()
        # all jobs processed!
        self.assertEqual(self.replicator.job_count,
                         self.replicator.total_stats.attempted)
        self.assertFalse(self.replicator.handoffs_remaining)

        # sanity, all the handoffs suffixes we filled in were rsync'd
        found_rsync_suffix_paths = set()
        for subprocess_info in rsync_log:
            local_path, remote_path = subprocess_info['rsync_args'][-2:]
            found_rsync_suffix_paths.add(local_path)
        self.assertEqual(set(expected_suffix_paths), found_rsync_suffix_paths)
        # sanity, all nodes got replicated
        found_replicate_calls = defaultdict(int)
        for req in conn_log.requests:
            self.assertEqual(req['method'], 'REPLICATE')
            found_replicate_key = (
                int(req['headers']['X-Backend-Storage-Policy-Index']),
                req['path'])
            found_replicate_calls[found_replicate_key] += 1
        expected_replicate_calls = {
            (0, '/sda/1/a83'): 3,
            (1, '/sda/1/a83'): 3,
            (0, '/sda/0'): 2,
            (0, '/sda/0/a83'): 2,
            (1, '/sda/0'): 2,
            (1, '/sda/0/a83'): 2,
            (0, '/sda/2'): 2,
            (1, '/sda/2'): 2,
            (0, '/sda/3'): 2,
            (1, '/sda/3'): 2,
        }
        self.assertEqual(dict(found_replicate_calls),
                         expected_replicate_calls)

    def test_handoffs_first_mode_will_abort_if_handoffs_remaining(self):
        # make an object in the handoff partition
        handoff_suffix_paths = []
        for policy in POLICIES:
            ts = next(self.ts)
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o', policy)
            with df.create() as w:
                w.write(b'asdf')
                w.put({'X-Timestamp': ts.internal})
                w.commit(ts)
            handoff_suffix_paths.append(os.path.dirname(df._datadir))
        process_arg_checker = [
            # (return_code, stdout, <each in capture rsync args>)
            (0, '', []),
            (1, '', []),
            (0, '', []),
            (0, '', []),
            (0, '', []),
            (0, '', []),
        ]
        stub_body = pickle.dumps({})
        with _mock_process(process_arg_checker) as rsync_log, \
            mock.patch('swift.obj.replicator.whataremyips',
                       side_effect=_ips), \
                mocked_http_conn(*[200] * 5, body=stub_body) as conn_log:
            self.replicator.handoffs_first = True
            self.replicator.replicate()
        # stopped after handoffs!
        self.assertEqual(1, self.replicator.handoffs_remaining)
        self.assertEqual(8, self.replicator.job_count)
        # in addition to the two update_deleted jobs as many as "concurrency"
        # jobs may have been spawned into the pool before the failed
        # update_deleted job incremented handoffs_remaining and caused the
        # handoffs_first check to abort the current pass
        self.assertLessEqual(self.replicator.total_stats.attempted,
                             2 + self.replicator.concurrency)

        # sanity, all the handoffs suffixes we filled in were rsync'd
        found_rsync_suffix_paths = set()
        expected_replicate_requests = set()
        for subprocess_info in rsync_log:
            local_path, remote_path = subprocess_info['rsync_args'][-2:]
            found_rsync_suffix_paths.add(local_path)
            if subprocess_info['ret_code'] == 0:
                node_ip = remote_path.split(':', 1)[0]
                expected_replicate_requests.add(node_ip)
        self.assertEqual(set(handoff_suffix_paths), found_rsync_suffix_paths)
        # sanity, all successful rsync nodes got REPLICATE requests
        found_replicate_requests = set()
        self.assertEqual(5, len(conn_log.requests))
        for req in conn_log.requests:
            self.assertEqual(req['method'], 'REPLICATE')
            found_replicate_requests.add(req['ip'])
        self.assertEqual(expected_replicate_requests,
                         found_replicate_requests)

        # and at least one partition got removed!
        remaining_policies = []
        for path in handoff_suffix_paths:
            if os.path.exists(path):
                policy = diskfile.extract_policy(path)
                remaining_policies.append(policy)
        self.assertEqual(len(remaining_policies), 1)
        remaining_policy = remaining_policies[0]

        # try again but with handoff_delete allowing for a single failure
        with _mock_process(process_arg_checker) as rsync_log, \
            mock.patch('swift.obj.replicator.whataremyips',
                       side_effect=_ips), \
                mocked_http_conn(*[200] * 14, body=stub_body) as conn_log:
            self.replicator.handoff_delete = 2
            self.replicator._zero_stats()
            self.replicator.replicate()
        # all jobs processed!
        self.assertEqual(self.replicator.job_count,
                         self.replicator.total_stats.attempted)
        self.assertFalse(self.replicator.handoffs_remaining)
        # sanity, all parts got replicated
        found_replicate_calls = defaultdict(int)
        for req in conn_log.requests:
            self.assertEqual(req['method'], 'REPLICATE')
            found_replicate_key = (
                int(req['headers']['X-Backend-Storage-Policy-Index']),
                req['path'])
            found_replicate_calls[found_replicate_key] += 1
        expected_replicate_calls = {
            (int(remaining_policy), '/sda/1/a83'): 2,
            (0, '/sda/0'): 2,
            (1, '/sda/0'): 2,
            (0, '/sda/2'): 2,
            (1, '/sda/2'): 2,
            (0, '/sda/3'): 2,
            (1, '/sda/3'): 2,
        }
        self.assertEqual(dict(found_replicate_calls),
                         expected_replicate_calls)

        # and now all handoff partitions have been rebalanced away!
        removed_paths = set()
        for path in handoff_suffix_paths:
            if not os.path.exists(path):
                removed_paths.add(path)
        self.assertEqual(removed_paths, set(handoff_suffix_paths))

    def test_replicator_skips_bogus_partition_dirs(self):
        # A directory in the wrong place shouldn't crash the replicator
        rmtree(self.objects)
        rmtree(self.objects_1)
        os.mkdir(self.objects)
        os.mkdir(self.objects_1)

        os.mkdir(os.path.join(self.objects, "burrito"))
        jobs = self.replicator.collect_jobs()
        self.assertEqual(len(jobs), 0)

    def test_replicator_skips_rsync_temp_files(self):
        # the empty pre-setup dirs aren't that useful to us
        device_path = os.path.join(self.devices, 'sda')
        rmtree(device_path, ignore_errors=1)
        os.mkdir(device_path)
        # create a real data file to trigger rsync
        df = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o',
                                      policy=POLICIES.legacy)
        ts = next(self.ts)
        with df.create() as w:
            w.write(b'asdf')
            w.put({'X-Timestamp': ts.internal})
            w.commit(ts)
        # pre-flight and post sync request for both other primaries
        expected_replicate_requests = 4
        process_arg_checker = [
            # (return_code, stdout, <each in capture rsync args>)
            (0, '', []),
            (0, '', []),
        ]
        stub_body = pickle.dumps({})
        with _mock_process(process_arg_checker) as rsync_log, \
            mock.patch('swift.obj.replicator.whataremyips',
                       side_effect=_ips), \
                mocked_http_conn(*[200] * expected_replicate_requests,
                                 body=stub_body) as conn_log:
            self.replicator.replicate()
        self.assertEqual(['REPLICATE'] * expected_replicate_requests,
                         [r['method'] for r in conn_log.requests])
        # expect one rsync to each other primary node
        self.assertEqual(2, len(rsync_log))
        expected = '--exclude=.*.[0-9a-zA-Z][0-9a-zA-Z][0-9a-zA-Z]' \
            '[0-9a-zA-Z][0-9a-zA-Z][0-9a-zA-Z]'
        for subprocess_info in rsync_log:
            rsync_args = subprocess_info['rsync_args']
            for arg in rsync_args:
                if arg.startswith('--exclude'):
                    self.assertEqual(arg, expected)
                    break
            else:
                self.fail('Did not find --exclude argument in %r' %
                          rsync_args)

    def test_replicator_removes_zbf(self):
        # After running xfs_repair, a partition directory could become a
        # zero-byte file. If this happens, the replicator should clean it
        # up, log something, and move on to the next partition.

        # Surprise! Partition dir 1 is actually a zero-byte file.
        pol_0_part_1_path = os.path.join(self.objects, '1')
        rmtree(pol_0_part_1_path)
        with open(pol_0_part_1_path, 'w'):
            pass
        self.assertTrue(os.path.isfile(pol_0_part_1_path))  # sanity check

        # Policy 1's partition dir 1 is also a zero-byte file.
        pol_1_part_1_path = os.path.join(self.objects_1, '1')
        rmtree(pol_1_part_1_path)
        with open(pol_1_part_1_path, 'w'):
            pass
        self.assertTrue(os.path.isfile(pol_1_part_1_path))  # sanity check

        # Don't delete things in collect_jobs(); all the stat() calls would
        # make replicator startup really slow.
        self.replicator.collect_jobs()
        self.assertTrue(os.path.exists(pol_0_part_1_path))
        self.assertTrue(os.path.exists(pol_1_part_1_path))

        # After a replication pass, the files should be gone
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            self.replicator.run_once()

        self.assertFalse(os.path.exists(pol_0_part_1_path))
        self.assertFalse(os.path.exists(pol_1_part_1_path))
        self.assertEqual(
            sorted(self.logger.get_lines_for_level('warning')), [
                ('Removing partition directory which was a file: %s'
                 % pol_1_part_1_path),
                ('Removing partition directory which was a file: %s'
                 % pol_0_part_1_path),
            ])

    def test_delete_partition(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.load_object_ring(POLICIES[0])
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for node in nodes:
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_default_sync_method(self):
        self.replicator.conf.pop('sync_method')
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.load_object_ring(POLICIES[0])
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for node in nodes:
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_ssync_single_region(self):
        devs = [
            {'id': 0, 'device': 'sda', 'zone': 0,
             'region': 1, 'ip': '127.0.0.0', 'port': 6200},
            {'id': 1, 'device': 'sda', 'zone': 1,
             'region': 1, 'ip': '127.0.0.1', 'port': 6200},
            {'id': 2, 'device': 'sda', 'zone': 2,
             'region': 1, 'ip': '127.0.0.2', 'port': 6200},
            {'id': 3, 'device': 'sda', 'zone': 4,
             'region': 1, 'ip': '127.0.0.3', 'port': 6200},
            {'id': 4, 'device': 'sda', 'zone': 5,
             'region': 1, 'ip': '127.0.0.4', 'port': 6200},
            {'id': 5, 'device': 'sda', 'zone': 6,
             'region': 1, 'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6200},
            {'id': 6, 'device': 'sda', 'zone': 7, 'region': 1,
             'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'port': 6200},
        ]
        _create_test_rings(self.testdir, devs=devs)
        self.conf['sync_method'] = 'ssync'
        self.replicator = object_replicator.ObjectReplicator(self.conf)
        self.replicator.logger = debug_logger()
        self.replicator._zero_stats()

        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            ts = normalize_timestamp(time.time())
            f = open(os.path.join(df._datadir, ts + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            whole_path_from = storage_directory(self.objects, 1, ohash)
            suffix_dir_path = os.path.dirname(whole_path_from)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))

            def _fake_ssync(node, job, suffixes, **kwargs):
                return True, {ohash: ts}

            self.replicator.sync_method = _fake_ssync
            self.replicator.replicate()
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertFalse(os.access(suffix_dir_path, os.F_OK))
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_1(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES[1])
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects_1, '1', data_dir)
            part_path = os.path.join(self.objects_1, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.load_object_ring(POLICIES[1])
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for node in nodes:
                rsync_mod = '%s::object/sda/objects-1/%s' % (node['ip'], 1)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_with_failures(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.load_object_ring(POLICIES[0])
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for i, node in enumerate(nodes):
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                if i == 0:
                    # force one of the rsync calls to fail
                    ret_code = 1
                else:
                    ret_code = 0
                process_arg_checker.append(
                    (ret_code, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            # The path should still exist
            self.assertTrue(os.access(part_path, os.F_OK))

    def test_delete_partition_with_handoff_delete(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            self.replicator.handoff_delete = 2
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.load_object_ring(POLICIES[0])
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for i, node in enumerate(nodes):
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                if i == 0:
                    # force one of the rsync calls to fail
                    ret_code = 1
                else:
                    ret_code = 0
                process_arg_checker.append(
                    (ret_code, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_with_handoff_delete_failures(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            self.replicator.handoff_delete = 2
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.load_object_ring(POLICIES[0])
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for i, node in enumerate(nodes):
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                if i in (0, 1):
                    # force two of the rsync calls to fail
                    ret_code = 1
                else:
                    ret_code = 0
                process_arg_checker.append(
                    (ret_code, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(part_path, os.F_OK))

    def test_delete_partition_with_handoff_delete_fail_in_other_region(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.load_object_ring(POLICIES[0])
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for node in nodes:
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                if node['region'] != 1:
                    #  the rsync calls for other region to fail
                    ret_code = 1
                else:
                    ret_code = 0
                process_arg_checker.append(
                    (ret_code, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(part_path, os.F_OK))

    def test_delete_partition_override_params(self):
        df = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o',
                                      policy=POLICIES.legacy)
        mkdirs(df._datadir)
        part_path = os.path.join(self.objects, '1')
        self.assertTrue(os.access(part_path, os.F_OK))
        self.replicator.replicate(override_devices=['sdb'])
        self.assertTrue(os.access(part_path, os.F_OK))
        self.replicator.replicate(override_partitions=['9'])
        self.assertTrue(os.access(part_path, os.F_OK))
        self.replicator.replicate(override_devices=['sda'],
                                  override_partitions=['1'])
        self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_policy_override_params(self):
        df0 = self.df_mgr.get_diskfile('sda', '99', 'a', 'c', 'o',
                                       policy=POLICIES.legacy)
        df1 = self.df_mgr.get_diskfile('sda', '99', 'a', 'c', 'o',
                                       policy=POLICIES[1])
        mkdirs(df0._datadir)
        mkdirs(df1._datadir)

        pol0_part_path = os.path.join(self.objects, '99')
        pol1_part_path = os.path.join(self.objects_1, '99')

        # sanity checks
        self.assertTrue(os.access(pol0_part_path, os.F_OK))
        self.assertTrue(os.access(pol1_part_path, os.F_OK))

        # a bogus policy index doesn't bother the replicator any more than a
        # bogus device or partition does
        self.replicator.run_once(policies='1,2,5')

        self.assertFalse(os.access(pol1_part_path, os.F_OK))
        self.assertTrue(os.access(pol0_part_path, os.F_OK))

        # since we weren't operating on everything, but only a subset of
        # storage policies, we didn't dump any recon stats.
        self.assertFalse(os.path.exists(
            os.path.join(self.recon_cache, 'object.recon')))

    def test_delete_partition_ssync(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            ts = normalize_timestamp(time.time())
            f = open(os.path.join(df._datadir, ts + '.data'),
                     'wb')
            f.write(b'0')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            whole_path_from = storage_directory(self.objects, 1, ohash)
            suffix_dir_path = os.path.dirname(whole_path_from)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))

            self.call_nums = 0
            self.conf['sync_method'] = 'ssync'

            def _fake_ssync(node, job, suffixes, **kwargs):
                success = True
                ret_val = {ohash: ts}
                if self.call_nums == 2:
                    # ssync should return (True, []) only when the second
                    # candidate node has not get the replica yet.
                    success = False
                    ret_val = {}
                self.call_nums += 1
                return success, ret_val

            self.replicator.sync_method = _fake_ssync
            self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))
            self.replicator.replicate()
            # The file should be deleted at the second replicate call
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertFalse(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))
            self.replicator.replicate()
            # The partition should be deleted at the third replicate call
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertFalse(os.access(suffix_dir_path, os.F_OK))
            self.assertFalse(os.access(part_path, os.F_OK))
            del self.call_nums

    def test_delete_partition_ssync_with_sync_failure(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            ts = normalize_timestamp(time.time())
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir, ts + '.data'), 'wb')
            f.write(b'0')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            whole_path_from = storage_directory(self.objects, 1, ohash)
            suffix_dir_path = os.path.dirname(whole_path_from)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            self.call_nums = 0
            self.conf['sync_method'] = 'ssync'

            def _fake_ssync(node, job, suffixes, **kwags):
                success = False
                ret_val = {}
                if self.call_nums == 2:
                    # ssync should return (True, []) only when the second
                    # candidate node has not get the replica yet.
                    success = True
                    ret_val = {ohash: ts}
                self.call_nums += 1
                return success, ret_val

            self.replicator.sync_method = _fake_ssync
            self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))
            self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))
            self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))
            del self.call_nums

    def test_delete_objs_ssync_only_when_in_sync(self):
        self.replicator.logger = debug_logger('test-replicator')
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            ts = normalize_timestamp(time.time())
            f = open(os.path.join(df._datadir, ts + '.data'), 'wb')
            f.write(b'0')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            whole_path_from = storage_directory(self.objects, 1, ohash)
            suffix_dir_path = os.path.dirname(whole_path_from)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            self.call_nums = 0
            self.conf['sync_method'] = 'ssync'

            in_sync_objs = {}

            def _fake_ssync(node, job, suffixes, remote_check_objs=None):
                self.call_nums += 1
                if remote_check_objs is None:
                    # sync job
                    ret_val = {ohash: ts}
                else:
                    ret_val = in_sync_objs
                return True, ret_val

            self.replicator.sync_method = _fake_ssync
            self.replicator.replicate()
            self.assertEqual(3, self.call_nums)
            # The file should still exist
            self.assertTrue(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))

            del self.call_nums

    def test_delete_partition_ssync_with_cleanup_failure(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            self.replicator.logger = mock_logger = \
                debug_logger('test-replicator')
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            ts = normalize_timestamp(time.time())
            f = open(os.path.join(df._datadir, ts + '.data'), 'wb')
            f.write(b'0')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            whole_path_from = storage_directory(self.objects, 1, ohash)
            suffix_dir_path = os.path.dirname(whole_path_from)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))

            self.call_nums = 0
            self.conf['sync_method'] = 'ssync'

            def _fake_ssync(node, job, suffixes, **kwargs):
                success = True
                ret_val = {ohash: ts}
                if self.call_nums == 2:
                    # ssync should return (True, []) only when the second
                    # candidate node has not get the replica yet.
                    success = False
                    ret_val = {}
                self.call_nums += 1
                return success, ret_val

            rmdir_func = os.rmdir

            def raise_exception_rmdir(exception_class, error_no):
                instance = exception_class()
                instance.errno = error_no
                instance.strerror = os.strerror(error_no)

                def func(directory):
                    if directory == suffix_dir_path:
                        raise instance
                    else:
                        rmdir_func(directory)

                return func

            self.replicator.sync_method = _fake_ssync
            self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))

            # Fail with ENOENT
            with mock.patch('os.rmdir',
                            raise_exception_rmdir(OSError, ENOENT)):
                self.replicator.replicate()
            self.assertFalse(mock_logger.get_lines_for_level('error'))
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))

            # Fail with ENOTEMPTY
            with mock.patch('os.rmdir',
                            raise_exception_rmdir(OSError, ENOTEMPTY)):
                self.replicator.replicate()
            self.assertFalse(mock_logger.get_lines_for_level('error'))
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))

            # Fail with ENOTDIR
            with mock.patch('os.rmdir',
                            raise_exception_rmdir(OSError, ENOTDIR)):
                self.replicator.replicate()
            self.assertEqual(mock_logger.get_lines_for_level('error'), [
                'Unexpected error trying to cleanup suffix dir:%r: ' %
                os.path.dirname(df._datadir),
            ])
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertTrue(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))

            # Finally we can cleanup everything
            self.replicator.replicate()
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertFalse(os.access(suffix_dir_path, os.F_OK))
            self.assertTrue(os.access(part_path, os.F_OK))
            self.replicator.replicate()
            self.assertFalse(os.access(whole_path_from, os.F_OK))
            self.assertFalse(os.access(suffix_dir_path, os.F_OK))
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_run_once_recover_from_failure(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    bind_ip=_ips()[0],
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf)
        was_connector = object_replicator.http_connect
        try:
            object_replicator.http_connect = mock_http_connect(200)
            # Write some files into '1' and run replicate- they should be moved
            # to the other partitions and then node should get deleted.
            cur_part = '1'
            df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o',
                                          policy=POLICIES.legacy)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write(b'1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, cur_part, data_dir)
            ring = replicator.load_object_ring(POLICIES[0])
            process_arg_checker = []
            nodes = [node for node in
                     ring.get_part_nodes(int(cur_part))
                     if node['ip'] not in _ips()]
            for node in nodes:
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'],
                                                           cur_part)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            self.assertTrue(os.access(os.path.join(self.objects,
                                                   '1', data_dir, ohash),
                                      os.F_OK))
            with _mock_process(process_arg_checker):
                replicator.run_once()
            self.assertFalse(process_errors)
            for i, result in [('0', True), ('1', False),
                              ('2', True), ('3', True)]:
                self.assertEqual(os.access(
                    os.path.join(self.objects,
                                 i, diskfile.HASH_FILE),
                    os.F_OK), result)
        finally:
            object_replicator.http_connect = was_connector

    def test_run_once_recover_from_timeout(self):
        # verify that replicator will pass over all policies' partitions even
        # if a timeout occurs while replicating one partition to one node.
        timeouts = [Timeout()]

        def fake_get_hashes(df_mgr, device, partition, policy, **kwargs):
            self.get_hash_count += 1
            dev_path = df_mgr.get_dev_path(device)
            part_path = os.path.join(dev_path, diskfile.get_data_dir(policy),
                                     str(partition))
            # Simulate a REPLICATE timeout by raising Timeout for second call
            # to get_hashes (with recalculate suffixes) for a specific
            # partition
            if (timeouts and '/objects/' in part_path and
                    part_path.endswith('0') and 'recalculate' in kwargs):
                raise timeouts.pop(0)
            return 1, {'abc': 'def'}

        # map partition_path -> [nodes]
        sync_paths = collections.defaultdict(list)

        def fake_sync(node, job, suffixes, *args, **kwargs):
            sync_paths[job['path']].append(node)
            return True, {}

        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    bind_ip=_ips()[0],  # local dev has id=0
                    mount_check='false', timeout='300', stats_interval='1')
        with mock.patch('swift.obj.diskfile.DiskFileManager._get_hashes',
                        fake_get_hashes):
            with mock.patch('swift.obj.replicator.http_connect',
                            mock_http_connect(200)):
                with mock.patch('swift.obj.replicator.dump_recon_cache'):
                    replicator = object_replicator.ObjectReplicator(
                        conf, logger=self.logger)

                    self.get_hash_count = 0
                    with mock.patch.object(replicator, 'sync', fake_sync):
                        replicator.run_once()

        log_lines = replicator.logger.logger.get_lines_for_level('error')
        self.assertIn("Error syncing with node:", log_lines[0])
        self.assertFalse(log_lines[1:])
        # setup creates 4 partitions; partition 1 does not map to local dev id
        # 0 so will be handled by update_delete(); partitions 0, 2, 3 are
        # handled by update() for each of two policies, so expect 6 paths to be
        # sync'd
        self.assertEqual(6, len(sync_paths))
        # partition 3 has 2 nodes in remote region, only first node is sync'd.
        # partition 0 in policy 0 has fake_get_hashes timeout before first
        # sync, so only second node is sync'd.
        # other partitions are sync'd to 2 nodes in same region.
        expected_node_count = {  # map path_end -> expected sync node count
            '/objects/0': 1,
            '/objects/1': 2,
            '/objects/2': 2,
            '/objects/3': 1,
            '/objects-1/0': 2,
            '/objects-1/1': 2,
            '/objects-1/2': 2,
            '/objects-1/3': 1
        }
        for path, nodes in sync_paths.items():
            path_end = path[path.index('/objects'):]
            self.assertEqual(expected_node_count[path_end], len(nodes),
                             'Expected %s but got %s for path %s' %
                             (expected_node_count[path_end], len(nodes), path))
        # partitions 0 and 2 attempt 3 calls each per policy to get_hashes = 12
        # partitions 3 attempts 2 calls per policy to get_hashes = 4
        # partitions 1 dosn't get_hashes because of update_deleted
        self.assertEqual(16, self.get_hash_count)

        # attempt to 16 times but succeeded only 15 times due to Timeout
        suffix_hashes = sum(
            count for (metric, count), _junk in
            replicator.logger.logger.log_dict['update_stats']
            if metric == 'suffix.hashes')
        self.assertEqual(15, suffix_hashes)

    def test_run(self):
        with _mock_process([(0, '')] * 100):
            with mock.patch('swift.obj.replicator.http_connect',
                            mock_http_connect(200)):
                self.replicator.replicate()

    def test_run_withlog(self):
        with _mock_process([(0, "stuff in log")] * 100):
            with mock.patch('swift.obj.replicator.http_connect',
                            mock_http_connect(200)):
                self.replicator.replicate()

    def test_sync_just_calls_sync_method(self):
        self.replicator.sync_method = mock.MagicMock()
        self.replicator.sync('node', 'job', 'suffixes')
        self.replicator.sync_method.assert_called_once_with(
            'node', 'job', 'suffixes')

    @mock.patch('swift.obj.replicator.tpool.execute')
    @mock.patch('swift.obj.replicator.http_connect', autospec=True)
    @mock.patch('swift.obj.replicator._do_listdir')
    def test_update(self, mock_do_listdir, mock_http, mock_tpool_execute):

        def set_default(self):
            self.replicator.suffix_count = 0
            self.replicator.suffix_sync = 0
            self.replicator.suffix_hash = 0
            self.replicator.last_replication_count = 0
            self.replicator._zero_stats()
            self.replicator.partition_times = []

        self.headers = {'Content-Length': '0',
                        'user-agent': 'object-replicator %s' % os.getpid()}
        mock_tpool_execute.return_value = (0, {})

        all_jobs = self.replicator.collect_jobs()
        jobs = [job for job in all_jobs if not job['delete']]

        mock_http.return_value = answer = mock.MagicMock()
        answer.getresponse.return_value = resp = mock.MagicMock()
        # Check incorrect http_connect with status 507 and
        # count of attempts and call args
        resp.status = 507
        error = '%(replication_ip)s/%(device)s responded as unmounted'
        expected_listdir_calls = [
            mock.call(int(job['partition']),
                      self.replicator.replication_cycle)
            for job in jobs]
        do_listdir_results = [False, False, True, False, True, False]
        mock_do_listdir.side_effect = do_listdir_results
        expected_tpool_calls = [
            mock.call(self.replicator._df_router[job['policy']]._get_hashes,
                      job['device'], job['partition'], job['policy'],
                      do_listdir=do_listdir)
            for job, do_listdir in zip(jobs, do_listdir_results)
        ]
        for job in jobs:
            set_default(self)
            ring = job['policy'].object_ring
            self.headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
            self.replicator.update(job)
            error_lines = self.logger.get_lines_for_level('error')
            expected = []
            # ... first the primaries
            for node in job['nodes']:
                expected.append(error % node)
            # ... then it will get handoffs
            for node in job['policy'].object_ring.get_more_nodes(
                    int(job['partition'])):
                expected.append(error % node)
            # ... and finally we get an error about running out of nodes
            expected.append('Ran out of handoffs while replicating '
                            'partition %s of policy %d' %
                            (job['partition'], job['policy']))
            self.assertEqual(expected, error_lines)
            self.assertEqual(len(self.replicator.partition_times), 1)
            self.assertEqual(mock_http.call_count, len(ring._devs) - 1)
            reqs = []
            for node in job['nodes']:
                reqs.append(mock.call(node['ip'], node['port'], node['device'],
                                      job['partition'], 'REPLICATE', '',
                                      headers=self.headers))
            if job['partition'] == '0':
                self.assertEqual(self.replicator.suffix_hash, 0)
            mock_http.assert_has_calls(reqs, any_order=True)
            mock_http.reset_mock()
            self.logger.clear()
        mock_do_listdir.assert_has_calls(expected_listdir_calls)
        mock_tpool_execute.assert_has_calls(expected_tpool_calls)
        mock_do_listdir.side_effect = None
        mock_do_listdir.return_value = False
        # Check incorrect http_connect with status 400 != HTTP_OK
        resp.status = 400
        error = 'Invalid response %(resp)s from %(ip)s'
        for job in jobs:
            set_default(self)
            self.replicator.update(job)
            # ... only the primaries
            expected = [error % {'resp': 400, 'ip': node['replication_ip']}
                        for node in job['nodes']]
            self.assertEqual(expected,
                             self.logger.get_lines_for_level('error'))
            self.assertEqual(len(self.replicator.partition_times), 1)
            self.logger.clear()

        # Check successful http_connection and exception with
        # incorrect pickle.loads(resp.read())
        resp.status = 200
        resp.read.return_value = 'garbage'
        expect = 'Error syncing with node: %r: '
        for job in jobs:
            set_default(self)
            self.replicator.update(job)
            # ... only the primaries
            expected = [expect % node for node in job['nodes']]
            error_lines = self.logger.get_lines_for_level('error')
            self.assertEqual(expected, error_lines)
            self.assertEqual(len(self.replicator.partition_times), 1)
            self.logger.clear()

        # Check successful http_connection and correct
        # pickle.loads(resp.read()) for non local node
        resp.status = 200
        local_job = None
        resp.read.return_value = pickle.dumps({})
        for job in jobs:
            set_default(self)
            # limit local job to policy 0 for simplicity
            if job['partition'] == '0' and int(job['policy']) == 0:
                local_job = job.copy()
                continue
            self.replicator.update(job)
            self.assertEqual([], self.logger.get_lines_for_level('error'))
            self.assertEqual(len(self.replicator.partition_times), 1)
            self.assertEqual(self.replicator.suffix_hash, 0)
            self.assertEqual(self.replicator.suffix_sync, 0)
            self.assertEqual(self.replicator.suffix_count, 0)
            self.logger.clear()

        # Check successful http_connect and sync for local node
        mock_tpool_execute.return_value = (1, {'a83': 'ba47fd314242ec8c'
                                                      '7efb91f5d57336e4'})
        resp.read.return_value = pickle.dumps({'a83': 'c130a2c17ed45102a'
                                                      'ada0f4eee69494ff'})
        set_default(self)
        self.replicator.sync = fake_func = \
            mock.MagicMock(return_value=(True, []))
        self.replicator.update(local_job)
        reqs = []
        for node in local_job['nodes']:
            reqs.append(mock.call(node, local_job, ['a83']))
        fake_func.assert_has_calls(reqs, any_order=True)
        self.assertEqual(fake_func.call_count, 2)
        stats = self.replicator.total_stats
        self.assertEqual(stats.attempted, 1)
        self.assertEqual(stats.suffix_sync, 2)
        self.assertEqual(stats.suffix_hash, 1)
        self.assertEqual(stats.suffix_count, 1)

        # Efficient Replication Case
        set_default(self)
        self.replicator.sync = fake_func = \
            mock.MagicMock(return_value=(True, []))
        all_jobs = self.replicator.collect_jobs()
        job = None
        for tmp in all_jobs:
            if tmp['partition'] == '3':
                job = tmp
                break
        # The candidate nodes to replicate (i.e. dev1 and dev3)
        # belong to another region
        self.replicator.update(job)
        self.assertEqual(fake_func.call_count, 1)
        stats = self.replicator.total_stats
        self.assertEqual(stats.attempted, 1)
        self.assertEqual(stats.suffix_sync, 1)
        self.assertEqual(stats.suffix_hash, 1)
        self.assertEqual(stats.suffix_count, 1)

        mock_http.reset_mock()
        self.logger.clear()

        # test for replication params on policy 0 only
        repl_job = local_job.copy()
        for node in repl_job['nodes']:
            node['replication_ip'] = '127.0.0.11'
            node['replication_port'] = '6011'
        set_default(self)
        # with only one set of headers make sure we specify index 0 here
        # as otherwise it may be different from earlier tests
        self.headers['X-Backend-Storage-Policy-Index'] = 0
        self.replicator.update(repl_job)
        reqs = []
        for node in repl_job['nodes']:
            reqs.append(mock.call(node['replication_ip'],
                                  node['replication_port'], node['device'],
                                  repl_job['partition'], 'REPLICATE',
                                  '', headers=self.headers))
            reqs.append(mock.call(node['replication_ip'],
                                  node['replication_port'], node['device'],
                                  repl_job['partition'], 'REPLICATE',
                                  '/a83', headers=self.headers))
        mock_http.assert_has_calls(reqs, any_order=True)

    def test_rsync_compress_different_region(self):
        self.assertEqual(self.replicator.sync_method, self.replicator.rsync)
        jobs = self.replicator.collect_jobs()
        _m_rsync = mock.Mock(return_value=0)
        _m_os_path_exists = mock.Mock(return_value=True)
        with mock.patch.object(self.replicator, '_rsync', _m_rsync), \
                mock.patch('os.path.exists', _m_os_path_exists):
            for job in jobs:
                self.assertTrue('region' in job)
                for node in job['nodes']:
                    for rsync_compress in (True, False):
                        self.replicator.rsync_compress = rsync_compress
                        ret = self.replicator.sync(node, job,
                                                   ['fake_suffix'])
                        self.assertTrue(ret)
                        if node['region'] != job['region']:
                            if rsync_compress:
                                # --compress arg should be passed to rsync
                                # binary only when rsync_compress option is
                                # enabled AND destination node is in a
                                # different region
                                self.assertTrue('--compress' in
                                                _m_rsync.call_args[0][0])
                            else:
                                self.assertFalse('--compress' in
                                                 _m_rsync.call_args[0][0])
                        else:
                            self.assertFalse('--compress' in
                                             _m_rsync.call_args[0][0])
                        self.assertEqual(
                            _m_os_path_exists.call_args_list[-1][0][0],
                            os.path.join(job['path'], 'fake_suffix'))
                        self.assertEqual(
                            _m_os_path_exists.call_args_list[-2][0][0],
                            os.path.join(job['path']))

    def test_do_listdir(self):
        # Test if do_listdir is enabled for every 10th partition to rehash
        # First number is the number of partitions in the job, list entries
        # are the expected partition numbers per run
        test_data = {
            9: [1, 0, 1, 1, 1, 1, 1, 1, 1, 1],
            29: [3, 2, 3, 3, 3, 3, 3, 3, 3, 3],
            111: [12, 11, 11, 11, 11, 11, 11, 11, 11, 11]}

        for partitions, expected in test_data.items():
            seen = []
            for phase in range(10):
                invalidated = 0
                for partition in range(partitions):
                    if object_replicator._do_listdir(partition, phase):
                        seen.append(partition)
                        invalidated += 1
                # Every 10th partition is seen after each phase
                self.assertEqual(expected[phase], invalidated)

            # After 10 cycles every partition is seen exactly once
            self.assertEqual(sorted(range(partitions)), sorted(seen))

    def test_replicate_skipped_partpower_increase(self):
        _create_test_rings(self.testdir, next_part_power=4)
        self.replicator.replicate()
        self.assertEqual(0, self.replicator.job_count)
        self.assertEqual(0, self.replicator.total_stats.attempted)
        warnings = self.logger.get_lines_for_level('warning')
        self.assertIn(
            "next_part_power set in policy 'one'. Skipping", warnings)

    def test_replicate_rsync_timeout(self):
        cur_part = '0'
        df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o',
                                      policy=POLICIES[0])
        mkdirs(df._datadir)
        f = open(os.path.join(df._datadir,
                              normalize_timestamp(time.time()) + '.data'),
                 'wb')
        f.write(b'1234567890')
        f.close()

        mock_procs = []

        def new_mock(*a, **kw):
            proc = MockHungProcess()
            mock_procs.append(proc)
            return proc

        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)), \
                mock.patch.object(self.replicator, 'rsync_timeout', 0.01), \
                mock.patch('eventlet.green.subprocess.Popen', new_mock):
            self.replicator.rsync_error_log_line_length = 20
            self.replicator.run_once()
        for proc in mock_procs:
            self.assertEqual(proc._calls, [
                ('wait', 'running'),
                ('kill', 'running'),
                ('wait', 'killed'),
            ])
        self.assertEqual(len(mock_procs), 2)
        error_lines = self.replicator.logger.get_lines_for_level('error')
        # verify logs are truncated to rsync_error_log_line_length
        self.assertEqual('Killing long-running', error_lines[0])
        self.assertEqual('Killing long-running', error_lines[1])

    def test_replicate_rsync_timeout_wedged(self):
        cur_part = '0'
        df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o',
                                      policy=POLICIES[0])
        mkdirs(df._datadir)
        f = open(os.path.join(df._datadir,
                              normalize_timestamp(time.time()) + '.data'),
                 'wb')
        f.write(b'1234567890')
        f.close()

        mock_procs = []

        def new_mock(*a, **kw):
            proc = MockHungProcess(polls_needed=2)
            mock_procs.append(proc)
            return proc

        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)), \
                mock.patch.object(self.replicator, 'rsync_timeout', 0.01), \
                mock.patch('eventlet.green.subprocess.Popen', new_mock):
            self.replicator.run_once()
        for proc in mock_procs:
            self.assertEqual(proc._calls, [
                ('wait', 'running'),
                ('kill', 'running'),
                ('wait', 'killed'),
                ('poll', 'killed'),
                ('poll', 'killed'),
            ])
        self.assertEqual(len(mock_procs), 2)

    def test_limit_rsync_log(self):
        def do_test(length_limit, log_line, expected):
            self.replicator.rsync_error_log_line_length = length_limit
            result = self.replicator._limit_rsync_log(log_line)
            self.assertEqual(result, expected)

        tests = [{'length_limit': 20,
                  'log_line': 'a' * 20,
                  'expected': 'a' * 20},
                 {'length_limit': 20,
                  'log_line': 'a' * 19,
                  'expected': 'a' * 19},
                 {'length_limit': 20,
                  'log_line': 'a' * 21,
                  'expected': 'a' * 20},
                 {'length_limit': None,
                  'log_line': 'a' * 50,
                  'expected': 'a' * 50},
                 {'length_limit': 0,
                  'log_line': 'a' * 50,
                  'expected': 'a' * 50}]

        for params in tests:
            do_test(**params)


@patch_policies([StoragePolicy(0, 'zero', False),
                 StoragePolicy(1, 'one', True)])
class TestMultiProcessReplicator(unittest.TestCase):
    def setUp(self):
        # recon cache path
        self.recon_cache = tempfile.mkdtemp()
        rmtree(self.recon_cache, ignore_errors=1)
        os.mkdir(self.recon_cache)
        self.recon_file = os.path.join(self.recon_cache, 'object.recon')

        bind_port = 6200

        # Set up some rings
        self.testdir = tempfile.mkdtemp()
        _create_test_rings(self.testdir, devs=[
            {'id': 0, 'device': 'sda', 'zone': 0,
             'region': 1, 'ip': '127.0.0.1', 'port': bind_port},
            {'id': 1, 'device': 'sdb', 'zone': 0,
             'region': 1, 'ip': '127.0.0.1', 'port': bind_port},
            {'id': 2, 'device': 'sdc', 'zone': 0,
             'region': 1, 'ip': '127.0.0.1', 'port': bind_port},
            {'id': 3, 'device': 'sdd', 'zone': 0,
             'region': 1, 'ip': '127.0.0.1', 'port': bind_port},
            {'id': 4, 'device': 'sde', 'zone': 0,
             'region': 1, 'ip': '127.0.0.1', 'port': bind_port},
            {'id': 100, 'device': 'notme0', 'zone': 0,
             'region': 1, 'ip': '127.99.99.99', 'port': bind_port}])

        self.logger = debug_logger('test-replicator')
        self.conf = dict(
            bind_ip='127.0.0.1', bind_port=bind_port,
            swift_dir=self.testdir,
            mount_check='false', recon_cache_path=self.recon_cache,
            timeout='300', stats_interval='1', sync_method='rsync')

        self.replicator = object_replicator.ObjectReplicator(
            self.conf, logger=self.logger)

    def tearDown(self):
        self.assertFalse(process_errors)
        rmtree(self.testdir, ignore_errors=1)
        rmtree(self.recon_cache, ignore_errors=1)

    def fake_replicate(self, override_devices, **kw):
        # Faked-out replicate() method. Just updates the stats, but doesn't
        # do any work.
        for device in override_devices:
            stats = self.replicator.stats_for_dev[device]
            if device == 'sda':
                stats.attempted = 1
                stats.success = 10
                stats.failure = 100
                stats.hashmatch = 1000
                stats.rsync = 10000
                stats.remove = 100000
                stats.suffix_count = 1000000
                stats.suffix_hash = 10000000
                stats.suffix_sync = 100000000
                stats.failure_nodes = {
                    '10.1.1.1': {'d11': 1}}
            elif device == 'sdb':
                stats.attempted = 2
                stats.success = 20
                stats.failure = 200
                stats.hashmatch = 2000
                stats.rsync = 20000
                stats.remove = 200000
                stats.suffix_count = 2000000
                stats.suffix_hash = 20000000
                stats.suffix_sync = 200000000
                stats.failure_nodes = {
                    '10.2.2.2': {'d22': 2}}
            elif device == 'sdc':
                stats.attempted = 3
                stats.success = 30
                stats.failure = 300
                stats.hashmatch = 3000
                stats.rsync = 30000
                stats.remove = 300000
                stats.suffix_count = 3000000
                stats.suffix_hash = 30000000
                stats.suffix_sync = 300000000
                stats.failure_nodes = {
                    '10.3.3.3': {'d33': 3}}
            elif device == 'sdd':
                stats.attempted = 4
                stats.success = 40
                stats.failure = 400
                stats.hashmatch = 4000
                stats.rsync = 40000
                stats.remove = 400000
                stats.suffix_count = 4000000
                stats.suffix_hash = 40000000
                stats.suffix_sync = 400000000
                stats.failure_nodes = {
                    '10.4.4.4': {'d44': 4}}
            elif device == 'sde':
                stats.attempted = 5
                stats.success = 50
                stats.failure = 500
                stats.hashmatch = 5000
                stats.rsync = 50000
                stats.remove = 500000
                stats.suffix_count = 5000000
                stats.suffix_hash = 50000000
                stats.suffix_sync = 500000000
                stats.failure_nodes = {
                    '10.5.5.5': {'d55': 5}}
            else:
                raise Exception("mock can't handle %r" % device)

    def test_no_multiprocessing(self):
        self.replicator.replicator_workers = 0
        self.assertEqual(self.replicator.get_worker_args(), [])

    def test_device_distribution(self):
        self.replicator.replicator_workers = 2
        self.assertEqual(self.replicator.get_worker_args(), [{
            'override_devices': ['sda', 'sdc', 'sde'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 0,
        }, {
            'override_devices': ['sdb', 'sdd'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 1,
        }])

    def test_override_policies(self):
        self.replicator.replicator_workers = 2
        args = self.replicator.get_worker_args(policies="3,5,7", once=True)
        self.assertEqual(args, [{
            'override_devices': ['sda', 'sdc', 'sde'],
            'override_partitions': [],
            'override_policies': [3, 5, 7],
            'have_overrides': True,
            'multiprocess_worker_index': 0,
        }, {
            'override_devices': ['sdb', 'sdd'],
            'override_partitions': [],
            'override_policies': [3, 5, 7],
            'have_overrides': True,
            'multiprocess_worker_index': 1,
        }])

        # override policies don't apply in run-forever mode
        args = self.replicator.get_worker_args(policies="3,5,7", once=False)
        self.assertEqual(args, [{
            'override_devices': ['sda', 'sdc', 'sde'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 0,
        }, {
            'override_devices': ['sdb', 'sdd'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 1,
        }])

    def test_more_workers_than_disks(self):
        self.replicator.replicator_workers = 999
        self.assertEqual(self.replicator.get_worker_args(), [{
            'override_devices': ['sda'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 0,
        }, {
            'override_devices': ['sdb'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 1,
        }, {
            'override_devices': ['sdc'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 2,
        }, {
            'override_devices': ['sdd'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 3,
        }, {
            'override_devices': ['sde'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 4,
        }])

        # Remember how many workers we actually have so that the log-line
        # prefixes are reasonable. Otherwise, we'd have five workers, each
        # logging lines starting with things like "[worker X/999 pid=P]"
        # despite there being only five.
        self.assertEqual(self.replicator.replicator_workers, 5)

    def test_command_line_overrides(self):
        self.replicator.replicator_workers = 2

        args = self.replicator.get_worker_args(
            devices="sda,sdc,sdd", partitions="12,34,56", once=True)
        self.assertEqual(args, [{
            'override_devices': ['sda', 'sdd'],
            'override_partitions': [12, 34, 56],
            'override_policies': [],
            'have_overrides': True,
            'multiprocess_worker_index': 0,
        }, {
            'override_devices': ['sdc'],
            'override_partitions': [12, 34, 56],
            'override_policies': [],
            'have_overrides': True,
            'multiprocess_worker_index': 1,
        }])

        args = self.replicator.get_worker_args(
            devices="sda,sdc,sdd", once=True)
        self.assertEqual(args, [{
            'override_devices': ['sda', 'sdd'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': True,
            'multiprocess_worker_index': 0,
        }, {
            'override_devices': ['sdc'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': True,
            'multiprocess_worker_index': 1,
        }])

        # no overrides apply in run-forever mode
        args = self.replicator.get_worker_args(
            devices="sda,sdc,sdd", partitions="12,34,56", once=False)
        self.assertEqual(args, [{
            'override_devices': ['sda', 'sdc', 'sde'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 0,
        }, {
            'override_devices': ['sdb', 'sdd'],
            'override_partitions': [],
            'override_policies': [],
            'have_overrides': False,
            'multiprocess_worker_index': 1,
        }])

    def test_worker_logging(self):
        self.replicator.replicator_workers = 3

        def log_some_stuff(*a, **kw):
            self.replicator.logger.debug("debug message")
            self.replicator.logger.info("info message")
            self.replicator.logger.warning("warning message")
            self.replicator.logger.error("error message")

        with mock.patch.object(self.replicator, 'replicate', log_some_stuff), \
                mock.patch("os.getpid", lambda: 8804):
            self.replicator.get_worker_args()
            self.replicator.run_once(multiprocess_worker_index=0,
                                     override_devices=['sda', 'sdb'])

        prefix = "[worker 1/3 pid=8804] "
        for level, lines in self.logger.logger.all_log_lines().items():
            for line in lines:
                self.assertTrue(
                    line.startswith(prefix),
                    "%r doesn't start with %r (level %s)" % (
                        line, prefix, level))

    def test_recon_run_once(self):
        self.replicator.replicator_workers = 3

        the_time = [1521680000]

        def mock_time():
            rv = the_time[0]
            the_time[0] += 120
            return rv

        # Simulate a couple child processes
        with mock.patch.object(self.replicator, 'replicate',
                               self.fake_replicate), \
                mock.patch('time.time', mock_time):
            self.replicator.get_worker_args()
            self.replicator.run_once(multiprocess_worker_index=0,
                                     override_devices=['sda', 'sdb'])
            self.replicator.run_once(multiprocess_worker_index=1,
                                     override_devices=['sdc'])
            self.replicator.run_once(multiprocess_worker_index=2,
                                     override_devices=['sdd', 'sde'])

        with open(self.recon_file) as fh:
            recon_data = json.load(fh)
        self.assertIn('object_replication_per_disk', recon_data)
        self.assertIn('sda', recon_data['object_replication_per_disk'])
        self.assertIn('sdb', recon_data['object_replication_per_disk'])
        self.assertIn('sdc', recon_data['object_replication_per_disk'])
        self.assertIn('sdd', recon_data['object_replication_per_disk'])
        self.assertIn('sde', recon_data['object_replication_per_disk'])
        sda = recon_data['object_replication_per_disk']['sda']

        # Spot-check a couple of fields
        self.assertEqual(sda['replication_stats']['attempted'], 1)
        self.assertEqual(sda['replication_stats']['success'], 10)
        self.assertEqual(sda['object_replication_time'], 2)  # minutes
        self.assertEqual(sda['object_replication_last'], 1521680120)

        # Aggregate the workers' recon updates
        self.replicator.post_multiprocess_run()
        with open(self.recon_file) as fh:
            recon_data = json.load(fh)
        self.assertEqual(recon_data['replication_stats']['attempted'], 15)
        self.assertEqual(recon_data['replication_stats']['failure'], 1500)
        self.assertEqual(recon_data['replication_stats']['hashmatch'], 15000)
        self.assertEqual(recon_data['replication_stats']['remove'], 1500000)
        self.assertEqual(recon_data['replication_stats']['rsync'], 150000)
        self.assertEqual(recon_data['replication_stats']['success'], 150)
        self.assertEqual(recon_data['replication_stats']['suffix_count'],
                         15000000)
        self.assertEqual(recon_data['replication_stats']['suffix_hash'],
                         150000000)
        self.assertEqual(recon_data['replication_stats']['suffix_sync'],
                         1500000000)
        self.assertEqual(recon_data['replication_stats']['failure_nodes'], {
            '10.1.1.1': {'d11': 1},
            '10.2.2.2': {'d22': 2},
            '10.3.3.3': {'d33': 3},
            '10.4.4.4': {'d44': 4},
            '10.5.5.5': {'d55': 5},
        })
        self.assertEqual(recon_data['object_replication_time'], 2)  # minutes
        self.assertEqual(recon_data['object_replication_last'], 1521680120)

    def test_recon_skipped_with_overrides(self):
        self.replicator.replicator_workers = 3

        the_time = [1521680000]

        def mock_time():
            rv = the_time[0]
            the_time[0] += 120
            return rv

        with mock.patch.object(self.replicator, 'replicate',
                               self.fake_replicate), \
                mock.patch('time.time', mock_time):
            self.replicator.get_worker_args()
            self.replicator.run_once(multiprocess_worker_index=0,
                                     have_overrides=True,
                                     override_devices=['sda', 'sdb'])
        self.assertFalse(os.path.exists(self.recon_file))

        # have_overrides=False makes us get recon stats
        with mock.patch.object(self.replicator, 'replicate',
                               self.fake_replicate), \
                mock.patch('time.time', mock_time):
            self.replicator.get_worker_args()
            self.replicator.run_once(multiprocess_worker_index=0,
                                     have_overrides=False,
                                     override_devices=['sda', 'sdb'])
        with open(self.recon_file) as fh:
            recon_data = json.load(fh)
        self.assertIn('sda', recon_data['object_replication_per_disk'])

    def test_recon_run_forever(self):
        the_time = [1521521521.52152]

        def mock_time():
            rv = the_time[0]
            the_time[0] += 120
            return rv

        self.replicator.replicator_workers = 2
        self.replicator._next_rcache_update = the_time[0]

        # One worker has finished a pass, the other hasn't.
        with mock.patch.object(self.replicator, 'replicate',
                               self.fake_replicate), \
                mock.patch('time.time', mock_time):
            self.replicator.get_worker_args()
            # Yes, this says run_once, but this is only to populate
            # object.recon with some stats. The real test is for the
            # aggregation.
            self.replicator.run_once(multiprocess_worker_index=0,
                                     override_devices=['sda', 'sdb', 'sdc'])

        # This will not produce aggregate stats since not every device has
        # finished a pass.
        the_time[0] += self.replicator.stats_interval
        with mock.patch('time.time', mock_time):
            rv = self.replicator.is_healthy()
        self.assertTrue(rv)
        with open(self.recon_file) as fh:
            recon_data = json.load(fh)
        self.assertNotIn('replication_stats', recon_data)

        # Now all the local devices have completed a replication pass, so we
        # will produce aggregate stats.
        with mock.patch.object(self.replicator, 'replicate',
                               self.fake_replicate), \
                mock.patch('time.time', mock_time):
            self.replicator.get_worker_args()
            self.replicator.run_once(multiprocess_worker_index=1,
                                     override_devices=['sdd', 'sde'])
        the_time[0] += self.replicator.stats_interval
        with mock.patch('time.time', mock_time):
            rv = self.replicator.is_healthy()
        self.assertTrue(rv)
        with open(self.recon_file) as fh:
            recon_data = json.load(fh)
        self.assertIn('replication_stats', recon_data)

        # no need to exhaustively check every sum
        self.assertEqual(recon_data['replication_stats']['attempted'], 15)
        self.assertEqual(recon_data['replication_stats']['success'], 150)

        self.assertEqual(
            recon_data['replication_last'],
            min(pd['replication_last']
                for pd in recon_data['object_replication_per_disk'].values()))


class TestReplicatorStats(unittest.TestCase):
    def test_to_recon(self):
        st = object_replicator.Stats(
            attempted=1, failure=2, hashmatch=3, remove=4,
            rsync=5, success=7,
            suffix_count=8, suffix_hash=9, suffix_sync=10,
            failure_nodes={'10.1.2.3': {'sda': 100, 'sdb': 200}})
        # This is what appears in the recon dump
        self.assertEqual(st.to_recon(), {
            'attempted': 1,
            'failure': 2,
            'hashmatch': 3,
            'remove': 4,
            'rsync': 5,
            'success': 7,
            'suffix_count': 8,
            'suffix_hash': 9,
            'suffix_sync': 10,
            'failure_nodes': {'10.1.2.3': {'sda': 100, 'sdb': 200}},
        })

    def test_recon_roundtrip(self):
        before = object_replicator.Stats(
            attempted=1, failure=2, hashmatch=3, remove=4,
            rsync=5, success=7,
            suffix_count=8, suffix_hash=9, suffix_sync=10,
            failure_nodes={'10.1.2.3': {'sda': 100, 'sdb': 200}})
        after = object_replicator.Stats.from_recon(before.to_recon())
        self.assertEqual(after.attempted, before.attempted)
        self.assertEqual(after.failure, before.failure)
        self.assertEqual(after.hashmatch, before.hashmatch)
        self.assertEqual(after.remove, before.remove)
        self.assertEqual(after.rsync, before.rsync)
        self.assertEqual(after.success, before.success)
        self.assertEqual(after.suffix_count, before.suffix_count)
        self.assertEqual(after.suffix_hash, before.suffix_hash)
        self.assertEqual(after.suffix_sync, before.suffix_sync)
        self.assertEqual(after.failure_nodes, before.failure_nodes)

    def test_from_recon_skips_extra_fields(self):
        # If another attribute ever sneaks its way in, we should ignore it.
        # This will make aborted upgrades a little less painful for
        # operators.
        recon_dict = {'attempted': 1, 'failure': 2, 'hashmatch': 3,
                      'spices': 5, 'treasures': 8}
        stats = object_replicator.Stats.from_recon(recon_dict)
        self.assertEqual(stats.attempted, 1)
        self.assertEqual(stats.failure, 2)
        self.assertEqual(stats.hashmatch, 3)
        # We don't gain attributes just because they're in object.recon.
        self.assertFalse(hasattr(stats, 'spices'))
        self.assertFalse(hasattr(stats, 'treasures'))

    def test_add_failure_stats(self):
        st = object_replicator.Stats()
        st.add_failure_stats([('10.1.1.1', 'd10'), ('10.1.1.1', 'd11')])
        st.add_failure_stats([('10.1.1.1', 'd10')])
        st.add_failure_stats([('10.1.1.1', 'd12'), ('10.2.2.2', 'd20'),
                              ('10.2.2.2', 'd21'), ('10.2.2.2', 'd21'),
                              ('10.2.2.2', 'd21')])
        self.assertEqual(st.failure, 8)

        as_dict = st.to_recon()
        self.assertEqual(as_dict['failure_nodes'], {
            '10.1.1.1': {
                'd10': 2,
                'd11': 1,
                'd12': 1,
            },
            '10.2.2.2': {
                'd20': 1,
                'd21': 3,
            },
        })

    def test_add(self):
        st1 = object_replicator.Stats(
            attempted=1, failure=2, hashmatch=3, remove=4, rsync=5,
            success=6, suffix_count=7, suffix_hash=8, suffix_sync=9,
            failure_nodes={
                '10.1.1.1': {'sda': 10, 'sdb': 20},
                '10.1.1.2': {'sda': 10, 'sdb': 20}})
        st2 = object_replicator.Stats(
            attempted=2, failure=4, hashmatch=6, remove=8, rsync=10,
            success=12, suffix_count=14, suffix_hash=16, suffix_sync=18,
            failure_nodes={
                '10.1.1.2': {'sda': 10, 'sdb': 20},
                '10.1.1.3': {'sda': 10, 'sdb': 20}})
        total = st1 + st2
        self.assertEqual(total.attempted, 3)
        self.assertEqual(total.failure, 6)
        self.assertEqual(total.hashmatch, 9)
        self.assertEqual(total.remove, 12)
        self.assertEqual(total.rsync, 15)
        self.assertEqual(total.success, 18)
        self.assertEqual(total.suffix_count, 21)
        self.assertEqual(total.suffix_hash, 24)
        self.assertEqual(total.suffix_sync, 27)
        self.assertEqual(total.failure_nodes, {
            '10.1.1.1': {'sda': 10, 'sdb': 20},
            '10.1.1.2': {'sda': 20, 'sdb': 40},
            '10.1.1.3': {'sda': 10, 'sdb': 20},
        })


if __name__ == '__main__':
    unittest.main()
