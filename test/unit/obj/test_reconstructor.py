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
import itertools
import json
import unittest
import os
from hashlib import md5
import mock
import six.moves.cPickle as pickle
import tempfile
import time
import shutil
import re
import random
import struct
import collections
from eventlet import Timeout, sleep, spawn

from contextlib import closing, contextmanager
from gzip import GzipFile
from shutil import rmtree
from six.moves.urllib.parse import unquote
from swift.common import utils
from swift.common.exceptions import DiskFileError
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import dump_recon_cache
from swift.obj import diskfile, reconstructor as object_reconstructor
from swift.common import ring
from swift.common.storage_policy import (StoragePolicy, ECStoragePolicy,
                                         POLICIES, EC_POLICY)
from swift.obj.reconstructor import REVERT

from test.unit import (patch_policies, debug_logger, mocked_http_conn,
                       FabricatedRing, make_timestamp_iter,
                       DEFAULT_TEST_EC_TYPE, encode_frag_archive_bodies,
                       quiet_eventlet_exceptions, skip_if_no_xattrs)
from test.unit.obj.common import write_diskfile


@contextmanager
def mock_ssync_sender(ssync_calls=None, response_callback=None, **kwargs):
    def fake_ssync(daemon, node, job, suffixes):
        if ssync_calls is not None:
            ssync_calls.append(
                {'node': node, 'job': job, 'suffixes': suffixes})

        def fake_call():
            if response_callback:
                response = response_callback(node, job, suffixes)
            else:
                response = True, {}
            return response
        return fake_call

    with mock.patch('swift.obj.reconstructor.ssync_sender', fake_ssync):
        yield fake_ssync


def make_ec_archive_bodies(policy, test_body):
    segment_size = policy.ec_segment_size
    # split up the body into buffers
    chunks = [test_body[x:x + segment_size]
              for x in range(0, len(test_body), segment_size)]
    # encode the buffers into fragment payloads
    fragment_payloads = []
    for chunk in chunks:
        fragments = \
            policy.pyeclib_driver.encode(chunk) * policy.ec_duplication_factor
        if not fragments:
            break
        fragment_payloads.append(fragments)

    # join up the fragment payloads per node
    ec_archive_bodies = [''.join(frags) for frags in zip(*fragment_payloads)]
    return ec_archive_bodies


def _create_test_rings(path, next_part_power=None):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2],
        [1, 2, 3],
        [2, 3, 0]
    ]

    intended_devs = [
        {'id': 0, 'device': 'sda1', 'zone': 0, 'ip': '127.0.0.0',
         'port': 6200},
        {'id': 1, 'device': 'sda1', 'zone': 1, 'ip': '127.0.0.1',
         'port': 6200},
        {'id': 2, 'device': 'sda1', 'zone': 2, 'ip': '127.0.0.2',
         'port': 6200},
        {'id': 3, 'device': 'sda1', 'zone': 4, 'ip': '127.0.0.3',
         'port': 6200}
    ]
    intended_part_shift = 30
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift,
                          next_part_power),
            f)

    testgz = os.path.join(path, 'object-1.ring.gz')
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift,
                          next_part_power),
            f)


def count_stats(logger, key, metric):
    count = 0
    for record in logger.log_dict[key]:
        log_args, log_kwargs = record
        m = log_args[0]
        if re.match(metric, m):
            count += 1
    return count


def get_header_frag_index(self, body):
    metadata = self.policy.pyeclib_driver.get_metadata(body)
    frag_index = struct.unpack('h', metadata[:2])[0]
    return {
        'X-Object-Sysmeta-Ec-Frag-Index': frag_index,
    }


@patch_policies([StoragePolicy(0, name='zero', is_default=True),
                 ECStoragePolicy(1, name='one',
                                 ec_type=DEFAULT_TEST_EC_TYPE,
                                 ec_ndata=2, ec_nparity=1)])
class TestGlobalSetupObjectReconstructor(unittest.TestCase):
    # Tests for reconstructor using real objects in test partition directories.
    legacy_durable = False

    def setUp(self):
        skip_if_no_xattrs()
        self.testdir = tempfile.mkdtemp()
        _create_test_rings(self.testdir)
        POLICIES[0].object_ring = ring.Ring(self.testdir, ring_name='object')
        POLICIES[1].object_ring = ring.Ring(self.testdir, ring_name='object-1')
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        self.devices = os.path.join(self.testdir, 'node')
        os.makedirs(self.devices)
        os.mkdir(os.path.join(self.devices, 'sda1'))
        self.objects = os.path.join(self.devices, 'sda1',
                                    diskfile.get_data_dir(POLICIES[0]))
        self.objects_1 = os.path.join(self.devices, 'sda1',
                                      diskfile.get_data_dir(POLICIES[1]))
        os.mkdir(self.objects)
        os.mkdir(self.objects_1)
        self.parts = {}
        self.parts_1 = {}
        self.part_nums = ['0', '1', '2']
        for part in self.part_nums:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(self.parts[part])
            self.parts_1[part] = os.path.join(self.objects_1, part)
            os.mkdir(self.parts_1[part])

        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')
        self.logger = debug_logger('test-reconstructor')
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)

        self.policy = POLICIES[1]

        # most of the reconstructor test methods require that there be
        # real objects in place, not just part dirs, so we'll create them
        # all here....
        # part 0: 3C1/hash/xxx#1#d.data  <-- job: sync_only - partners (FI 1)
        #         061/hash/xxx#1#d.data  <-- included in earlier job (FI 1)
        #                 /xxx#2#d.data  <-- job: sync_revert to index 2

        # part 1: 3C1/hash/xxx#0#d.data  <-- job: sync_only - partners (FI 0)
        #                 /xxx#1#d.data  <-- job: sync_revert to index 1
        #         061/hash/xxx#1#d.data  <-- included in earlier job (FI 1)

        # part 2: 3C1/hash/xxx#2#d.data  <-- job: sync_revert to index 2
        #         061/hash/xxx#0#d.data  <-- job: sync_revert to index 0

        def _create_frag_archives(policy, obj_path, local_id, obj_set):
            # we'll create 2 sets of objects in different suffix dirs
            # so we cover all the scenarios we want (3 of them)
            # 1) part dir with all FI's matching the local node index
            # 2) part dir with one local and mix of others
            # 3) part dir with no local FI and one or more others
            def part_0(set):
                if set == 0:
                    # just the local
                    return local_id
                else:
                    # one local and all of another
                    if obj_num == 0:
                        return local_id
                    else:
                        return (local_id + 1) % 3

            def part_1(set):
                if set == 0:
                    # one local and all of another
                    if obj_num == 0:
                        return local_id
                    else:
                        return (local_id + 2) % 3
                else:
                    # just the local node
                    return local_id

            def part_2(set):
                # this part is a handoff in our config (always)
                # so lets do a set with indices from different nodes
                if set == 0:
                    return (local_id + 1) % 3
                else:
                    return (local_id + 2) % 3

            # function dictionary for defining test scenarios base on set #
            scenarios = {'0': part_0,
                         '1': part_1,
                         '2': part_2}

            def _create_df(obj_num, part_num):
                self._create_diskfile(
                    part=part_num, object_name='o' + str(obj_set),
                    policy=policy, frag_index=scenarios[part_num](obj_set),
                    timestamp=utils.Timestamp(t))

            for part_num in self.part_nums:
                # create 3 unique objects per part, each part
                # will then have a unique mix of FIs for the
                # possible scenarios
                for obj_num in range(0, 3):
                    _create_df(obj_num, part_num)

        ips = utils.whataremyips()
        for policy in [p for p in POLICIES if p.policy_type == EC_POLICY]:
            self.ec_policy = policy
            self.ec_obj_ring = self.reconstructor.load_object_ring(
                self.ec_policy)
            data_dir = diskfile.get_data_dir(self.ec_policy)
            for local_dev in [dev for dev in self.ec_obj_ring.devs
                              if dev and dev['replication_ip'] in ips and
                              dev['replication_port'] ==
                              self.reconstructor.port]:
                self.ec_local_dev = local_dev
                dev_path = os.path.join(self.reconstructor.devices_dir,
                                        self.ec_local_dev['device'])
                self.ec_obj_path = os.path.join(dev_path, data_dir)

                # create a bunch of FA's to test
                t = 1421181937.70054  # time.time()
                with mock.patch('swift.obj.diskfile.time') as mock_time:
                    # since (a) we are using a fixed time here to create
                    # frags which corresponds to all the hardcoded hashes and
                    # (b) the EC diskfile will delete its .data file right
                    # after creating if it has expired, use this horrible hack
                    # to prevent the reclaim happening
                    mock_time.time.return_value = 0.0
                    _create_frag_archives(self.ec_policy, self.ec_obj_path,
                                          self.ec_local_dev['id'], 0)
                    _create_frag_archives(self.ec_policy, self.ec_obj_path,
                                          self.ec_local_dev['id'], 1)
                break
            break

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def _create_diskfile(self, policy=None, part=0, object_name='o',
                         frag_index=0, timestamp=None, test_data=None):
        policy = policy or self.policy
        df_mgr = self.reconstructor._df_router[policy]
        df = df_mgr.get_diskfile('sda1', part, 'a', 'c', object_name,
                                 policy=policy)
        timestamp = timestamp or utils.Timestamp.now()
        test_data = test_data or 'test data'
        write_diskfile(df, timestamp, data=test_data, frag_index=frag_index,
                       legacy_durable=self.legacy_durable)
        return df

    def assert_expected_jobs(self, part_num, jobs):
        for job in jobs:
            del job['path']
            del job['policy']
            if 'local_index' in job:
                del job['local_index']
            job['suffixes'].sort()

        expected = []
        # part num 0
        expected.append(
            [{
                'sync_to': [{
                    'index': 2,
                    'replication_port': 6200,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.2',
                    'device': 'sda1',
                    'id': 2,
                }],
                'job_type': object_reconstructor.REVERT,
                'suffixes': ['061'],
                'partition': 0,
                'frag_index': 2,
                'device': 'sda1',
                'local_dev': {
                    'replication_port': 6200,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1', 'port': 6200,
                },
                'hashes': {
                    '061': {
                        None: '85b02a5283704292a511078a5c483da5',
                        2: '0e6e8d48d801dc89fd31904ae3b31229',
                        1: '0e6e8d48d801dc89fd31904ae3b31229',
                    },
                    '3c1': {
                        None: '85b02a5283704292a511078a5c483da5',
                        1: '0e6e8d48d801dc89fd31904ae3b31229',
                    },
                },
            }, {
                'sync_to': [{
                    'index': 0,
                    'replication_port': 6200,
                    'zone': 0,
                    'ip': '127.0.0.0',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.0',
                    'device': 'sda1', 'id': 0,
                }, {
                    'index': 2,
                    'replication_port': 6200,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.2',
                    'device': 'sda1',
                    'id': 2,
                }],
                'job_type': object_reconstructor.SYNC,
                'sync_diskfile_builder': self.reconstructor.reconstruct_fa,
                'suffixes': ['061', '3c1'],
                'partition': 0,
                'frag_index': 1,
                'device': 'sda1',
                'local_dev': {
                    'replication_port': 6200,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6200,
                },
                'hashes':
                {
                    '061': {
                        None: '85b02a5283704292a511078a5c483da5',
                        2: '0e6e8d48d801dc89fd31904ae3b31229',
                        1: '0e6e8d48d801dc89fd31904ae3b31229'
                    },
                    '3c1': {
                        None: '85b02a5283704292a511078a5c483da5',
                        1: '0e6e8d48d801dc89fd31904ae3b31229',
                    },
                },
            }]
        )
        # part num 1
        expected.append(
            [{
                'sync_to': [{
                    'index': 1,
                    'replication_port': 6200,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.2',
                    'device': 'sda1',
                    'id': 2,
                }],
                'job_type': object_reconstructor.REVERT,
                'suffixes': ['061', '3c1'],
                'partition': 1,
                'frag_index': 1,
                'device': 'sda1',
                'local_dev': {
                    'replication_port': 6200,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6200,
                },
                'hashes':
                {
                    '061': {
                        None: '85b02a5283704292a511078a5c483da5',
                        1: '0e6e8d48d801dc89fd31904ae3b31229',
                    },
                    '3c1': {
                        0: '0e6e8d48d801dc89fd31904ae3b31229',
                        None: '85b02a5283704292a511078a5c483da5',
                        1: '0e6e8d48d801dc89fd31904ae3b31229',
                    },
                },
            }, {
                'sync_to': [{
                    'index': 2,
                    'replication_port': 6200,
                    'zone': 4,
                    'ip': '127.0.0.3',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.3',
                    'device': 'sda1', 'id': 3,
                }, {
                    'index': 1,
                    'replication_port': 6200,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.2',
                    'device': 'sda1',
                    'id': 2,
                }],
                'job_type': object_reconstructor.SYNC,
                'sync_diskfile_builder': self.reconstructor.reconstruct_fa,
                'suffixes': ['3c1'],
                'partition': 1,
                'frag_index': 0,
                'device': 'sda1',
                'local_dev': {
                    'replication_port': 6200,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6200,
                },
                'hashes': {
                    '061': {
                        None: '85b02a5283704292a511078a5c483da5',
                        1: '0e6e8d48d801dc89fd31904ae3b31229',
                    },
                    '3c1': {
                        0: '0e6e8d48d801dc89fd31904ae3b31229',
                        None: '85b02a5283704292a511078a5c483da5',
                        1: '0e6e8d48d801dc89fd31904ae3b31229',
                    },
                },
            }]
        )
        # part num 2
        expected.append(
            [{
                'sync_to': [{
                    'index': 0,
                    'replication_port': 6200,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.2',
                    'device': 'sda1', 'id': 2,
                }],
                'job_type': object_reconstructor.REVERT,
                'suffixes': ['061'],
                'partition': 2,
                'frag_index': 0,
                'device': 'sda1',
                'local_dev': {
                    'replication_port': 6200,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6200,
                },
                'hashes': {
                    '061': {
                        0: '0e6e8d48d801dc89fd31904ae3b31229',
                        None: '85b02a5283704292a511078a5c483da5'
                    },
                    '3c1': {
                        None: '85b02a5283704292a511078a5c483da5',
                        2: '0e6e8d48d801dc89fd31904ae3b31229'
                    },
                },
            }, {
                'sync_to': [{
                    'index': 2,
                    'replication_port': 6200,
                    'zone': 0,
                    'ip': '127.0.0.0',
                    'region': 1,
                    'port': 6200,
                    'replication_ip': '127.0.0.0',
                    'device': 'sda1',
                    'id': 0,
                }],
                'job_type': object_reconstructor.REVERT,
                'suffixes': ['3c1'],
                'partition': 2,
                'frag_index': 2,
                'device': 'sda1',
                'local_dev': {
                    'replication_port': 6200,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6200
                },
                'hashes': {
                    '061': {
                        0: '0e6e8d48d801dc89fd31904ae3b31229',
                        None: '85b02a5283704292a511078a5c483da5'
                    },
                    '3c1': {
                        None: '85b02a5283704292a511078a5c483da5',
                        2: '0e6e8d48d801dc89fd31904ae3b31229'
                    },
                },
            }]
        )

        def check_jobs(part_num):
            try:
                expected_jobs = expected[int(part_num)]
            except (IndexError, ValueError):
                self.fail('Unknown part number %r' % part_num)
            expected_by_part_frag_index = dict(
                ((j['partition'], j['frag_index']), j) for j in expected_jobs)
            for job in jobs:
                job_key = (job['partition'], job['frag_index'])
                if job_key in expected_by_part_frag_index:
                    for k, value in job.items():
                        expected_value = \
                            expected_by_part_frag_index[job_key][k]
                        try:
                            if isinstance(value, list):
                                value.sort()
                                expected_value.sort()
                            self.assertEqual(value, expected_value)
                        except AssertionError as e:
                            extra_info = \
                                '\n\n... for %r in part num %s job %r' % (
                                    k, part_num, job_key)
                            raise AssertionError(str(e) + extra_info)
                else:
                    self.fail(
                        'Unexpected job %r for part num %s - '
                        'expected jobs where %r' % (
                            job_key, part_num,
                            expected_by_part_frag_index.keys()))
            for expected_job in expected_jobs:
                if expected_job in jobs:
                    jobs.remove(expected_job)
            self.assertFalse(jobs)  # that should be all of them
        check_jobs(part_num)

    def _run_once(self, http_count, extra_devices, override_devices=None):
        ring_devs = list(self.policy.object_ring.devs)
        for device, parts in extra_devices.items():
            device_path = os.path.join(self.devices, device)
            os.mkdir(device_path)
            for part in range(parts):
                os.makedirs(os.path.join(device_path, 'objects-1', str(part)))
            # we update the ring to make is_local happy
            devs = [dict(d) for d in ring_devs]
            for d in devs:
                d['device'] = device
            self.policy.object_ring.devs.extend(devs)
        self.reconstructor.stats_interval = 0
        self.process_job = lambda j: sleep(0)
        with mocked_http_conn(*[200] * http_count, body=pickle.dumps({})):
            with mock_ssync_sender():
                self.reconstructor.run_once(devices=override_devices)

    def test_run_once(self):
        # sda1: 3 is done in setup
        extra_devices = {
            'sdb1': 4,
            'sdc1': 1,
            'sdd1': 0,
        }
        self._run_once(18, extra_devices)
        stats_lines = set()
        for line in self.logger.get_lines_for_level('info'):
            if 'reconstructed in' not in line:
                continue
            stat_line = line.split('reconstructed', 1)[0].strip()
            stats_lines.add(stat_line)
        acceptable = set([
            '3/8 (37.50%) partitions',
            '5/8 (62.50%) partitions',
            '8/8 (100.00%) partitions',
        ])
        matched = stats_lines & acceptable
        self.assertEqual(matched, acceptable,
                         'missing some expected acceptable:\n%s' % (
                             '\n'.join(sorted(acceptable - matched))))
        self.assertEqual(self.reconstructor.reconstruction_part_count, 8)
        self.assertEqual(self.reconstructor.part_count, 8)

    def test_run_once_override_devices(self):
        # sda1: 3 is done in setup
        extra_devices = {
            'sdb1': 4,
            'sdc1': 1,
            'sdd1': 0,
        }
        self._run_once(2, extra_devices, 'sdc1')
        stats_lines = set()
        for line in self.logger.get_lines_for_level('info'):
            if 'reconstructed in' not in line:
                continue
            stat_line = line.split('reconstructed', 1)[0].strip()
            stats_lines.add(stat_line)
        acceptable = set([
            '1/1 (100.00%) partitions',
        ])
        matched = stats_lines & acceptable
        self.assertEqual(matched, acceptable,
                         'missing some expected acceptable:\n%s' % (
                             '\n'.join(sorted(acceptable - matched))))
        self.assertEqual(self.reconstructor.reconstruction_part_count, 1)
        self.assertEqual(self.reconstructor.part_count, 1)

    def test_get_response(self):
        part = self.part_nums[0]
        node = self.policy.object_ring.get_part_nodes(int(part))[0]

        def do_test(stat_code):
            with mocked_http_conn(stat_code):
                resp = self.reconstructor._get_response(node, part,
                                                        path='nada',
                                                        headers={},
                                                        full_path='nada/nada')
            return resp

        resp = do_test(200)
        self.assertEqual(resp.status, 200)

        resp = do_test(400)
        # on the error case return value will be None instead of response
        self.assertIsNone(resp)
        # ... and log warnings for 400
        for line in self.logger.get_lines_for_level('warning'):
            self.assertIn('Invalid response 400', line)
        self.logger._clear()

        resp = do_test(Exception())
        self.assertIsNone(resp)
        # exception should result in error logs
        for line in self.logger.get_lines_for_level('error'):
            self.assertIn('Trying to GET', line)
        self.logger._clear()

        # Timeout also should result in error logs
        resp = do_test(Timeout())
        self.assertIsNone(resp)
        for line in self.logger.get_lines_for_level('error'):
            self.assertIn('Trying to GET', line)
            # sanity Timeout has extra message in the error log
            self.assertIn('Timeout', line)
        self.logger.clear()

        # we should get a warning on 503 (sanity)
        resp = do_test(503)
        self.assertIsNone(resp)
        warnings = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warnings))
        self.assertIn('Invalid response 503', warnings[0])
        self.logger.clear()

        # ... but no messages should be emitted for 404
        resp = do_test(404)
        self.assertIsNone(resp)
        for level, msgs in self.logger.lines_dict.items():
            self.assertFalse(msgs)

    def test_reconstructor_skips_bogus_partition_dirs(self):
        # A directory in the wrong place shouldn't crash the reconstructor
        self.reconstructor._reset_stats()
        rmtree(self.objects_1)
        os.mkdir(self.objects_1)

        os.mkdir(os.path.join(self.objects_1, "burrito"))
        jobs = []
        for part_info in self.reconstructor.collect_parts():
            jobs += self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertFalse(jobs)

    def test_check_ring(self):
        testring = tempfile.mkdtemp()
        _create_test_rings(testring)
        obj_ring = ring.Ring(testring, ring_name='object')  # noqa
        self.assertTrue(self.reconstructor.check_ring(obj_ring))
        orig_check = self.reconstructor.next_check
        self.reconstructor.next_check = orig_check - 30
        self.assertTrue(self.reconstructor.check_ring(obj_ring))
        self.reconstructor.next_check = orig_check
        orig_ring_time = obj_ring._mtime
        obj_ring._mtime = orig_ring_time - 30
        self.assertTrue(self.reconstructor.check_ring(obj_ring))
        self.reconstructor.next_check = orig_check - 30
        self.assertFalse(self.reconstructor.check_ring(obj_ring))
        rmtree(testring, ignore_errors=1)

    def test_reconstruct_check_ring(self):
        # test reconstruct logs info when check_ring is false and that
        # there are no jobs built
        with mock.patch('swift.obj.reconstructor.ObjectReconstructor.'
                        'check_ring', return_value=False):
            self.reconstructor.reconstruct()
        msgs = self.reconstructor.logger.get_lines_for_level('info')
        self.assertIn('Ring change detected. Aborting'
                      ' current reconstruction pass.', msgs[0])
        self.assertEqual(self.reconstructor.reconstruction_count, 0)

    def test_build_reconstruction_jobs(self):
        self.reconstructor._reset_stats()
        for part_info in self.reconstructor.collect_parts():
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
            self.assertTrue(jobs[0]['job_type'] in
                            (object_reconstructor.SYNC,
                             object_reconstructor.REVERT))
            self.assert_expected_jobs(part_info['partition'], jobs)

    def test_handoffs_only(self):
        self.reconstructor.handoffs_only = True

        found_job_types = set()

        def fake_process_job(job):
            # increment failure counter
            self.reconstructor.handoffs_remaining += 1
            found_job_types.add(job['job_type'])

        self.reconstructor.process_job = fake_process_job

        _orig_build_jobs = self.reconstructor.build_reconstruction_jobs
        built_jobs = []

        def capture_jobs(part_info):
            jobs = _orig_build_jobs(part_info)
            built_jobs.append((part_info, jobs))
            return jobs

        with mock.patch.object(self.reconstructor, 'build_reconstruction_jobs',
                               capture_jobs):
            self.reconstructor.reconstruct()
        # only revert jobs
        found = [(part_info['partition'], set(
                 j['job_type'] for j in jobs))
                 for part_info, jobs in built_jobs]
        self.assertEqual([
            # partition, job_types
            (2, {'sync_revert'}),
        ], found)
        self.assertEqual(found_job_types, {object_reconstructor.REVERT})
        # but failures keep handoffs remaining
        msgs = self.reconstructor.logger.get_lines_for_level('info')
        self.assertIn('Next pass will continue to revert handoffs', msgs[-1])
        self.logger._clear()

        found_job_types = set()

        def fake_process_job(job):
            # success does not increment failure counter
            found_job_types.add(job['job_type'])

        self.reconstructor.process_job = fake_process_job

        # only revert jobs ... but all handoffs cleared out successfully
        self.reconstructor.reconstruct()
        self.assertEqual(found_job_types, {object_reconstructor.REVERT})
        # it's time to turn off handoffs_only
        msgs = self.reconstructor.logger.get_lines_for_level('warning')
        self.assertIn('You should disable handoffs_only', msgs[-1])

    def test_get_partners(self):
        # we're going to perform an exhaustive test of every possible
        # combination of partitions and nodes in our custom test ring

        # format: [dev_id in question, 'part_num',
        #          [part_nodes for the given part], left id, right id...]
        expected_partners = sorted([
            (0, '0', [0, 1, 2], 2, 1), (0, '2', [2, 3, 0], 3, 2),
            (1, '0', [0, 1, 2], 0, 2), (1, '1', [1, 2, 3], 3, 2),
            (2, '0', [0, 1, 2], 1, 0), (2, '1', [1, 2, 3], 1, 3),
            (2, '2', [2, 3, 0], 0, 3), (3, '1', [1, 2, 3], 2, 1),
            (3, '2', [2, 3, 0], 2, 0), (0, '0', [0, 1, 2], 2, 1),
            (0, '2', [2, 3, 0], 3, 2), (1, '0', [0, 1, 2], 0, 2),
            (1, '1', [1, 2, 3], 3, 2), (2, '0', [0, 1, 2], 1, 0),
            (2, '1', [1, 2, 3], 1, 3), (2, '2', [2, 3, 0], 0, 3),
            (3, '1', [1, 2, 3], 2, 1), (3, '2', [2, 3, 0], 2, 0),
        ])

        got_partners = []
        for pol in POLICIES:
            obj_ring = pol.object_ring
            for part_num in self.part_nums:
                part_nodes = obj_ring.get_part_nodes(int(part_num))
                primary_ids = [n['id'] for n in part_nodes]
                for node in part_nodes:
                    partners = object_reconstructor._get_partners(
                        node['index'], part_nodes)
                    left = partners[0]['id']
                    right = partners[1]['id']
                    got_partners.append((
                        node['id'], part_num, primary_ids, left, right))

        self.assertEqual(expected_partners, sorted(got_partners))

    def test_collect_parts(self):
        self.reconstructor._reset_stats()
        parts = []
        for part_info in self.reconstructor.collect_parts():
            parts.append(part_info['partition'])
        self.assertEqual(sorted(parts), [0, 1, 2])

    def test_collect_parts_mkdirs_error(self):

        def blowup_mkdirs(path):
            raise OSError('Ow!')

        self.reconstructor._reset_stats()
        with mock.patch.object(object_reconstructor, 'mkdirs', blowup_mkdirs):
            rmtree(self.objects_1, ignore_errors=1)
            parts = []
            for part_info in self.reconstructor.collect_parts():
                parts.append(part_info['partition'])
            error_lines = self.logger.get_lines_for_level('error')
            self.assertEqual(len(error_lines), 1,
                             'Expected only one error, got %r' % error_lines)
            log_args, log_kwargs = self.logger.log_dict['error'][0]
            self.assertEqual(str(log_kwargs['exc_info'][1]), 'Ow!')

    def test_removes_zbf(self):
        # After running xfs_repair, a partition directory could become a
        # zero-byte file. If this happens, the reconstructor should clean it
        # up, log something, and move on to the next partition.

        # Surprise! Partition dir 1 is actually a zero-byte file.
        pol_1_part_1_path = os.path.join(self.objects_1, '1')
        rmtree(pol_1_part_1_path)
        with open(pol_1_part_1_path, 'w'):
            pass
        self.assertTrue(os.path.isfile(pol_1_part_1_path))  # sanity check

        self.reconstructor.process_job = lambda j: None
        self.reconstructor.reconstruct()

        self.assertFalse(os.path.exists(pol_1_part_1_path))
        warnings = self.reconstructor.logger.get_lines_for_level('warning')
        self.assertEqual(2, len(warnings))
        # first warning is due to get_hashes failing to take lock on non-dir
        self.assertIn(pol_1_part_1_path + '/hashes.pkl', warnings[0])
        self.assertIn('unable to read', warnings[0].lower())
        self.assertIn(pol_1_part_1_path, warnings[1])
        self.assertIn('not a directory', warnings[1].lower())

    def test_ignores_status_file(self):
        # Following fd86d5a, the auditor will leave status files on each device
        # until an audit can complete. The reconstructor should ignore these

        @contextmanager
        def status_files(*auditor_types):
            status_paths = [os.path.join(self.objects_1,
                                         'auditor_status_%s.json' % typ)
                            for typ in auditor_types]
            for status_path in status_paths:
                self.assertFalse(os.path.exists(status_path))  # sanity check
                with open(status_path, 'w'):
                    pass
                self.assertTrue(os.path.isfile(status_path))  # sanity check
            try:
                yield status_paths
            finally:
                for status_path in status_paths:
                    try:
                        os.unlink(status_path)
                    except OSError as e:
                        if e.errno != 2:
                            raise

        # since our collect_parts job is a generator, that yields directly
        # into build_jobs and then spawns it's safe to do the remove_files
        # without making reconstructor startup slow
        with status_files('ALL', 'ZBF') as status_paths:
            self.reconstructor._reset_stats()
            for part_info in self.reconstructor.collect_parts():
                self.assertNotIn(part_info['part_path'], status_paths)
            warnings = self.reconstructor.logger.get_lines_for_level('warning')
            self.assertEqual(0, len(warnings))
            for status_path in status_paths:
                self.assertTrue(os.path.exists(status_path))

    def _make_fake_ssync(self, ssync_calls, fail_jobs=None):
        """
        Replace SsyncSender with a thin Fake.

        :param ssync_calls: an empty list, a non_local, all calls to ssync will
                            be captured for assertion in the caller.
        :param fail_jobs: optional iter of dicts, any job passed into Fake that
                          matches a failure dict will return success == False.
        """
        class _fake_ssync(object):
            def __init__(self, daemon, node, job, suffixes, **kwargs):
                # capture context and generate an available_map of objs
                context = {}
                context['node'] = node
                context['job'] = job
                context['suffixes'] = suffixes
                self.suffixes = suffixes
                self.daemon = daemon
                self.job = job
                hash_gen = self.daemon._df_router[job['policy']].yield_hashes(
                    self.job['device'], self.job['partition'],
                    self.job['policy'], self.suffixes,
                    frag_index=self.job.get('frag_index'))
                self.available_map = {}
                for hash_, timestamps in hash_gen:
                    self.available_map[hash_] = timestamps
                context['available_map'] = self.available_map
                ssync_calls.append(context)
                self.success = True
                for failure in (fail_jobs or []):
                    if all(job.get(k) == v for (k, v) in failure.items()):
                        self.success = False
                        break
                context['success'] = self.success

            def __call__(self, *args, **kwargs):
                return self.success, self.available_map if self.success else {}

        return _fake_ssync

    def test_delete_reverted(self):
        # verify reconstructor deletes reverted frag indexes after ssync'ing

        def visit_obj_dirs(context):
            for suff in context['suffixes']:
                suff_dir = os.path.join(
                    context['job']['path'], suff)
                for root, dirs, files in os.walk(suff_dir):
                    for d in dirs:
                        dirpath = os.path.join(root, d)
                        files = os.listdir(dirpath)
                        yield dirpath, files

        n_files = n_files_after = 0

        # run reconstructor with delete function mocked out to check calls
        ssync_calls = []
        delete_func =\
            'swift.obj.reconstructor.ObjectReconstructor.delete_reverted_objs'
        with mock.patch('swift.obj.reconstructor.ssync_sender',
                        self._make_fake_ssync(ssync_calls)):
            with mocked_http_conn(*[200] * 12, body=pickle.dumps({})):
                with mock.patch(delete_func) as mock_delete:
                    self.reconstructor.reconstruct()
                expected_calls = []
                for context in ssync_calls:
                    if context['job']['job_type'] == REVERT:
                        for dirpath, files in visit_obj_dirs(context):
                            # sanity check - expect some files to be in dir,
                            # may not be for the reverted frag index
                            self.assertTrue(files)
                            n_files += len(files)
                        expected_calls.append(mock.call(context['job'],
                                              context['available_map'],
                                              context['node']['index']))
                mock_delete.assert_has_calls(expected_calls, any_order=True)

        ssync_calls = []
        with mock.patch('swift.obj.reconstructor.ssync_sender',
                        self._make_fake_ssync(ssync_calls)):
            with mocked_http_conn(*[200] * 12, body=pickle.dumps({})):
                self.reconstructor.reconstruct()
                for context in ssync_calls:
                    if context['job']['job_type'] == REVERT:
                        data_file_tail = ('#%s.data'
                                          % context['node']['index'])
                        for dirpath, files in visit_obj_dirs(context):
                            n_files_after += len(files)
                            for filename in files:
                                self.assertFalse(
                                    filename.endswith(data_file_tail))

        # sanity check that some files should were deleted
        self.assertGreater(n_files, n_files_after)

    def test_no_delete_failed_revert(self):
        # test will only process revert jobs
        self.reconstructor.handoffs_only = True

        captured_ssync = []
        # fail all jobs on part 2 on sda1
        fail_jobs = [
            {'device': 'sda1', 'partition': 2},
        ]
        with mock.patch('swift.obj.reconstructor.ssync_sender',
                        self._make_fake_ssync(
                            captured_ssync, fail_jobs=fail_jobs)), \
                mocked_http_conn() as request_log:
            self.reconstructor.reconstruct()
        self.assertFalse(request_log.unexpected_requests)

        # global setup has four revert jobs
        self.assertEqual(len(captured_ssync), 2)
        expected_ssync_calls = {
            # device, part, frag_index: expected_occurrences
            ('sda1', 2, 2): 1,
            ('sda1', 2, 0): 1,
        }
        self.assertEqual(expected_ssync_calls, dict(collections.Counter(
            (context['job']['device'],
             context['job']['partition'],
             context['job']['frag_index'])
            for context in captured_ssync
        )))

        # failed jobs don't sync suffixes
        self.assertFalse(
            self.reconstructor.logger.get_lines_for_level('warning'))
        self.assertFalse(
            self.reconstructor.logger.get_lines_for_level('error'))
        # handoffs remaining and part exists
        self.assertEqual(2, self.reconstructor.handoffs_remaining)
        self.assertTrue(os.path.exists(self.parts_1['2']))

        # again with no failures
        captured_ssync = []
        with mock.patch('swift.obj.reconstructor.ssync_sender',
                        self._make_fake_ssync(captured_ssync)), \
                mocked_http_conn(
                    200, 200, body=pickle.dumps({})) as request_log:
            self.reconstructor.reconstruct()
        self.assertFalse(request_log.unexpected_requests)
        # same jobs
        self.assertEqual(len(captured_ssync), 2)
        # but this time we rehash at the end
        expected_suffix_calls = []
        for context in captured_ssync:
            if not context['success']:
                # only successful jobs generate suffix rehash calls
                continue
            job = context['job']
            expected_suffix_calls.append(
                (job['sync_to'][0]['replication_ip'], '/%s/%s/%s' % (
                    job['device'], job['partition'],
                    '-'.join(sorted(job['suffixes']))))
            )
        self.assertEqual(set(expected_suffix_calls),
                         set((r['ip'], r['path'])
                             for r in request_log.requests))
        self.assertFalse(
            self.reconstructor.logger.get_lines_for_level('error'))
        # handoffs are cleaned up
        self.assertEqual(0, self.reconstructor.handoffs_remaining)
        warning_msgs = self.reconstructor.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warning_msgs))
        self.assertIn('no handoffs remaining', warning_msgs[0])

        # need one more pass to cleanup the part dir
        self.assertTrue(os.path.exists(self.parts_1['2']))
        with mock.patch('swift.obj.reconstructor.ssync_sender',
                        self._make_fake_ssync([])), \
                mocked_http_conn() as request_log:
            self.reconstructor.reconstruct()
        self.assertFalse(os.path.exists(self.parts_1['2']))

    def test_get_part_jobs(self):
        # yeah, this test code expects a specific setup
        self.assertEqual(len(self.part_nums), 3)

        # OK, at this point we should have 4 loaded parts with one
        jobs = []
        for partition in os.listdir(self.ec_obj_path):
            part_path = os.path.join(self.ec_obj_path, partition)
            jobs = self.reconstructor._get_part_jobs(
                self.ec_local_dev, part_path, int(partition), self.ec_policy)
            self.assert_expected_jobs(partition, jobs)

    def assertStatCount(self, stat_method, stat_prefix, expected_count):
        count = count_stats(self.logger, stat_method, stat_prefix)
        msg = 'expected %s != %s for %s %s' % (
            expected_count, count, stat_method, stat_prefix)
        self.assertEqual(expected_count, count, msg)

    def test_delete_partition(self):
        # part 2 is predefined to have all revert jobs
        part_path = os.path.join(self.objects_1, '2')
        self.assertTrue(os.access(part_path, os.F_OK))

        ssync_calls = []
        status = [200] * 2
        body = pickle.dumps({})
        with mocked_http_conn(*status, body=body) as request_log:
            with mock.patch('swift.obj.reconstructor.ssync_sender',
                            self._make_fake_ssync(ssync_calls)):
                self.reconstructor.reconstruct(override_partitions=[2])
        expected_repliate_calls = set([
            ('127.0.0.0', '/sda1/2/3c1'),
            ('127.0.0.2', '/sda1/2/061'),
        ])
        found_calls = set((r['ip'], r['path'])
                          for r in request_log.requests)
        self.assertEqual(expected_repliate_calls, found_calls)

        expected_ssync_calls = sorted([
            ('127.0.0.0', REVERT, 2, ['3c1']),
            ('127.0.0.2', REVERT, 2, ['061']),
        ])
        self.assertEqual(expected_ssync_calls, sorted((
            c['node']['ip'],
            c['job']['job_type'],
            c['job']['partition'],
            c['suffixes'],
        ) for c in ssync_calls))

        expected_stats = {
            ('increment', 'partition.delete.count.'): 2,
            ('timing_since', 'partition.delete.timing'): 2,
        }
        for stat_key, expected in expected_stats.items():
            stat_method, stat_prefix = stat_key
            self.assertStatCount(stat_method, stat_prefix, expected)
        # part 2 should be totally empty
        hash_gen = self.reconstructor._df_router[self.policy].yield_hashes(
            'sda1', '2', self.policy)
        for path, hash_, ts in hash_gen:
            self.fail('found %s with %s in %s' % (hash_, ts, path))
        # but the partition directory and hashes pkl still exist
        self.assertTrue(os.access(part_path, os.F_OK))
        hashes_path = os.path.join(self.objects_1, '2', diskfile.HASH_FILE)
        self.assertTrue(os.access(hashes_path, os.F_OK))

        # ... but on next pass
        ssync_calls = []
        with mocked_http_conn() as request_log:
            with mock.patch('swift.obj.reconstructor.ssync_sender',
                            self._make_fake_ssync(ssync_calls)):
                self.reconstructor.reconstruct(override_partitions=[2])
        # reconstruct won't generate any replicate or ssync_calls
        self.assertFalse(request_log.requests)
        self.assertFalse(ssync_calls)
        # and the partition will get removed!
        self.assertFalse(os.access(part_path, os.F_OK))

    def test_process_job_all_success(self):
        self.reconstructor._reset_stats()
        with mock_ssync_sender():
            with mocked_http_conn(*[200] * 12, body=pickle.dumps({})):
                found_jobs = []
                for part_info in self.reconstructor.collect_parts():
                    jobs = self.reconstructor.build_reconstruction_jobs(
                        part_info)
                    found_jobs.extend(jobs)
                    for job in jobs:
                        self.logger._clear()
                        node_count = len(job['sync_to'])
                        self.reconstructor.process_job(job)
                        if job['job_type'] == object_reconstructor.REVERT:
                            self.assertEqual(0, count_stats(
                                self.logger, 'update_stats', 'suffix.hashes'))
                        else:
                            self.assertStatCount('update_stats',
                                                 'suffix.hashes',
                                                 node_count)
                            self.assertEqual(node_count, count_stats(
                                self.logger, 'update_stats', 'suffix.hashes'))
                            self.assertEqual(node_count, count_stats(
                                self.logger, 'update_stats', 'suffix.syncs'))
                        self.assertNotIn('error', self.logger.all_log_lines())
        self.assertEqual(
            dict(collections.Counter(
                (job['device'], job['partition'], job['frag_index'])
                for job in found_jobs)),
            {('sda1', 0, 1): 1,
             ('sda1', 0, 2): 1,
             ('sda1', 1, 0): 1,
             ('sda1', 1, 1): 1,
             ('sda1', 2, 0): 1,
             ('sda1', 2, 2): 1})
        self.assertEqual(self.reconstructor.suffix_sync, 8)
        self.assertEqual(self.reconstructor.suffix_count, 8)
        self.assertEqual(self.reconstructor.reconstruction_count, 6)

    def test_process_job_all_insufficient_storage(self):
        self.reconstructor._reset_stats()
        with mock_ssync_sender():
            with mocked_http_conn(*[507] * 8):
                found_jobs = []
                for part_info in self.reconstructor.collect_parts():
                    jobs = self.reconstructor.build_reconstruction_jobs(
                        part_info)
                    found_jobs.extend(jobs)
                    for job in jobs:
                        self.logger._clear()
                        self.reconstructor.process_job(job)
                        for line in self.logger.get_lines_for_level('error'):
                            self.assertIn('responded as unmounted', line)
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.hashes'))
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.syncs'))
        self.assertEqual(
            dict(collections.Counter(
                (job['device'], job['partition'], job['frag_index'])
                for job in found_jobs)),
            {('sda1', 0, 1): 1,
             ('sda1', 0, 2): 1,
             ('sda1', 1, 0): 1,
             ('sda1', 1, 1): 1,
             ('sda1', 2, 0): 1,
             ('sda1', 2, 2): 1})
        self.assertEqual(self.reconstructor.suffix_sync, 0)
        self.assertEqual(self.reconstructor.suffix_count, 0)
        self.assertEqual(self.reconstructor.reconstruction_count, 6)

    def test_process_job_all_client_error(self):
        self.reconstructor._reset_stats()
        with mock_ssync_sender():
            with mocked_http_conn(*[400] * 8):
                found_jobs = []
                for part_info in self.reconstructor.collect_parts():
                    jobs = self.reconstructor.build_reconstruction_jobs(
                        part_info)
                    found_jobs.extend(jobs)
                    for job in jobs:
                        self.logger._clear()
                        self.reconstructor.process_job(job)
                        for line in self.logger.get_lines_for_level('error'):
                            self.assertIn('Invalid response 400', line)
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.hashes'))
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.syncs'))
        self.assertEqual(
            dict(collections.Counter(
                (job['device'], job['partition'], job['frag_index'])
                for job in found_jobs)),
            {('sda1', 0, 1): 1,
             ('sda1', 0, 2): 1,
             ('sda1', 1, 0): 1,
             ('sda1', 1, 1): 1,
             ('sda1', 2, 0): 1,
             ('sda1', 2, 2): 1})
        self.assertEqual(self.reconstructor.suffix_sync, 0)
        self.assertEqual(self.reconstructor.suffix_count, 0)
        self.assertEqual(self.reconstructor.reconstruction_count, 6)

    def test_process_job_all_timeout(self):
        self.reconstructor._reset_stats()
        with mock_ssync_sender(), mocked_http_conn(*[Timeout()] * 8):
            found_jobs = []
            for part_info in self.reconstructor.collect_parts():
                jobs = self.reconstructor.build_reconstruction_jobs(
                    part_info)
                found_jobs.extend(jobs)
                for job in jobs:
                    self.logger._clear()
                    self.reconstructor.process_job(job)
                    for line in self.logger.get_lines_for_level('error'):
                        self.assertIn('Timeout (Nones)', line)
                    self.assertStatCount(
                        'update_stats', 'suffix.hashes', 0)
                    self.assertStatCount(
                        'update_stats', 'suffix.syncs', 0)
        self.assertEqual(
            dict(collections.Counter(
                (job['device'], job['partition'], job['frag_index'])
                for job in found_jobs)),
            {('sda1', 0, 1): 1,
             ('sda1', 0, 2): 1,
             ('sda1', 1, 0): 1,
             ('sda1', 1, 1): 1,
             ('sda1', 2, 0): 1,
             ('sda1', 2, 2): 1})
        self.assertEqual(self.reconstructor.suffix_sync, 0)
        self.assertEqual(self.reconstructor.suffix_count, 0)
        self.assertEqual(self.reconstructor.reconstruction_count, 6)

    def test_reconstructor_skipped_partpower_increase(self):
        self.reconstructor._reset_stats()
        _create_test_rings(self.testdir, 10)
        # Enforce re-reading the EC ring
        POLICIES[1].object_ring = ring.Ring(self.testdir, ring_name='object-1')

        self.reconstructor.reconstruct()

        self.assertEqual(0, self.reconstructor.reconstruction_count)
        warnings = self.reconstructor.logger.get_lines_for_level('warning')
        self.assertIn(
            "next_part_power set in policy 'one'. Skipping", warnings)


class TestGlobalSetupObjectReconstructorLegacyDurable(
        TestGlobalSetupObjectReconstructor):
    # Tests for reconstructor using real objects in test partition directories.
    legacy_durable = True


@patch_policies(with_ec_default=True)
class TestWorkerReconstructor(unittest.TestCase):

    maxDiff = None

    def setUp(self):
        super(TestWorkerReconstructor, self).setUp()
        self.logger = debug_logger()
        self.testdir = tempfile.mkdtemp()
        self.recon_cache_path = os.path.join(self.testdir, 'recon')
        self.rcache = os.path.join(self.recon_cache_path, 'object.recon')
        # dump_recon_cache expects recon_cache_path to exist
        os.mkdir(self.recon_cache_path)

    def tearDown(self):
        super(TestWorkerReconstructor, self).tearDown()
        shutil.rmtree(self.testdir)

    def test_no_workers_by_default(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {}, logger=self.logger)
        self.assertEqual(0, reconstructor.reconstructor_workers)
        self.assertEqual(0, len(list(reconstructor.get_worker_args())))

    def test_bad_value_workers(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '-1'}, logger=self.logger)
        self.assertEqual(-1, reconstructor.reconstructor_workers)
        self.assertEqual(0, len(list(reconstructor.get_worker_args())))

    def test_workers_with_no_devices(self):
        def do_test(num_workers):
            reconstructor = object_reconstructor.ObjectReconstructor(
                {'reconstructor_workers': num_workers}, logger=self.logger)
            self.assertEqual(num_workers, reconstructor.reconstructor_workers)
            self.assertEqual(1, len(list(reconstructor.get_worker_args())))
            self.assertEqual([
                {'override_partitions': [], 'override_devices': []},
            ], list(reconstructor.get_worker_args()))
        do_test(1)
        do_test(10)

    def test_workers_with_devices_and_no_valid_overrides(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '2'}, logger=self.logger)
        reconstructor.get_local_devices = lambda: ['sdb', 'sdc']
        self.assertEqual(2, reconstructor.reconstructor_workers)
        # N.B. sdz is not in local_devices so there are no devices to process
        # but still expect a single worker process
        worker_args = list(reconstructor.get_worker_args(
            once=True, devices='sdz'))
        self.assertEqual(1, len(worker_args))
        self.assertEqual([{'override_partitions': [],
                           'override_devices': ['sdz']}],
                         worker_args)
        # overrides are ignored in forever mode
        worker_args = list(reconstructor.get_worker_args(
            once=False, devices='sdz'))
        self.assertEqual(2, len(worker_args))
        self.assertEqual([
            {'override_partitions': [], 'override_devices': ['sdb']},
            {'override_partitions': [], 'override_devices': ['sdc']}
        ], worker_args)

    def test_workers_with_devices(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '2'}, logger=self.logger)
        reconstructor.get_local_devices = lambda: ['sdb', 'sdc']
        self.assertEqual(2, reconstructor.reconstructor_workers)
        self.assertEqual(2, len(list(reconstructor.get_worker_args())))
        expected = [
            {'override_partitions': [], 'override_devices': ['sdb']},
            {'override_partitions': [], 'override_devices': ['sdc']},
        ]
        worker_args = list(reconstructor.get_worker_args(once=False))
        self.assertEqual(2, len(worker_args))
        self.assertEqual(expected, worker_args)
        worker_args = list(reconstructor.get_worker_args(once=True))
        self.assertEqual(2, len(worker_args))
        self.assertEqual(expected, worker_args)

    def test_workers_with_devices_and_overrides(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '2'}, logger=self.logger)
        reconstructor.get_local_devices = lambda: ['sdb', 'sdc']
        self.assertEqual(2, reconstructor.reconstructor_workers)
        # check we don't get more workers than override devices...
        # N.B. sdz is not in local_devices so should be ignored for the
        # purposes of generating workers
        worker_args = list(reconstructor.get_worker_args(
            once=True, devices='sdb,sdz', partitions='99,333'))
        self.assertEqual(1, len(worker_args))
        self.assertEqual(
            [{'override_partitions': [99, 333], 'override_devices': ['sdb']}],
            worker_args)
        # overrides are ignored in forever mode
        worker_args = list(reconstructor.get_worker_args(
            once=False, devices='sdb,sdz', partitions='99,333'))
        self.assertEqual(2, len(worker_args))
        self.assertEqual([
            {'override_partitions': [], 'override_devices': ['sdb']},
            {'override_partitions': [], 'override_devices': ['sdc']}
        ], worker_args)

    def test_workers_with_lots_of_devices(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '2'}, logger=self.logger)
        reconstructor.get_local_devices = lambda: [
            'sdb', 'sdc', 'sdd', 'sde', 'sdf']
        self.assertEqual(2, reconstructor.reconstructor_workers)
        self.assertEqual(2, len(list(reconstructor.get_worker_args())))
        self.assertEqual([
            {'override_partitions': [], 'override_devices': [
                'sdb', 'sdd', 'sdf']},
            {'override_partitions': [], 'override_devices': [
                'sdc', 'sde']},
        ], list(reconstructor.get_worker_args()))

    def test_workers_with_lots_of_devices_and_overrides(self):
        # check that override devices get distributed across workers
        # in similar fashion to all devices
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '2'}, logger=self.logger)
        reconstructor.get_local_devices = lambda: [
            'sdb', 'sdc', 'sdd', 'sde', 'sdf']
        self.assertEqual(2, reconstructor.reconstructor_workers)
        worker_args = list(reconstructor.get_worker_args(
            once=True, devices='sdb,sdd,sdf', partitions='99,333'))
        # 3 devices to operate on, 2 workers -> one worker gets two devices
        # and the other worker just gets one
        self.assertEqual([{
            'override_partitions': [99, 333],
            'override_devices': ['sdb', 'sdf'],
        }, {
            'override_partitions': [99, 333],
            'override_devices': ['sdd'],
        }], worker_args)

        # with 4 override devices, expect 2 per worker
        worker_args = list(reconstructor.get_worker_args(
            once=True, devices='sdb,sdc,sdd,sdf', partitions='99,333'))
        self.assertEqual(2, len(worker_args))
        self.assertEqual([
            {'override_partitions': [99, 333], 'override_devices': [
                'sdb', 'sdd']},
            {'override_partitions': [99, 333], 'override_devices': [
                'sdc', 'sdf']},
        ], worker_args)

    def test_workers_with_lots_of_workers(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '10'}, logger=self.logger)
        reconstructor.get_local_devices = lambda: ['sdb', 'sdc']
        self.assertEqual(10, reconstructor.reconstructor_workers)
        self.assertEqual(2, len(list(reconstructor.get_worker_args())))
        self.assertEqual([
            {'override_partitions': [], 'override_devices': ['sdb']},
            {'override_partitions': [], 'override_devices': ['sdc']},
        ], list(reconstructor.get_worker_args()))

    def test_workers_with_lots_of_workers_and_devices(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'reconstructor_workers': '10'}, logger=self.logger)
        reconstructor.get_local_devices = lambda: [
            'sdb', 'sdc', 'sdd', 'sde', 'sdf']
        self.assertEqual(10, reconstructor.reconstructor_workers)
        self.assertEqual(5, len(list(reconstructor.get_worker_args())))
        self.assertEqual([
            {'override_partitions': [], 'override_devices': ['sdb']},
            {'override_partitions': [], 'override_devices': ['sdc']},
            {'override_partitions': [], 'override_devices': ['sdd']},
            {'override_partitions': [], 'override_devices': ['sde']},
            {'override_partitions': [], 'override_devices': ['sdf']},
        ], list(reconstructor.get_worker_args()))

    def test_workers_with_some_workers_and_devices(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {}, logger=self.logger)
        reconstructor.get_local_devices = lambda: [
            'd%s' % (i + 1) for i in range(21)]

        # With more devices than workers, the work is spread out as evenly
        # as we can manage. When number-of-devices is a multiple of
        # number-of-workers, every worker has the same number of devices to
        # operate on.
        reconstructor.reconstructor_workers = 7
        worker_args = list(reconstructor.get_worker_args())
        self.assertEqual([len(a['override_devices']) for a in worker_args],
                         [3] * 7)

        # When number-of-devices is not a multiple of number-of-workers,
        # device counts differ by at most 1.
        reconstructor.reconstructor_workers = 5
        worker_args = list(reconstructor.get_worker_args())
        self.assertEqual(
            sorted([len(a['override_devices']) for a in worker_args]),
            [4, 4, 4, 4, 5])

        # With more workers than devices, we don't create useless workers.
        # We'll only make one per device.
        reconstructor.reconstructor_workers = 22
        worker_args = list(reconstructor.get_worker_args())
        self.assertEqual(
            [len(a['override_devices']) for a in worker_args],
            [1] * 21)

        # This is true even if we have far more workers than devices.
        reconstructor.reconstructor_workers = 2 ** 16
        worker_args = list(reconstructor.get_worker_args())
        self.assertEqual(
            [len(a['override_devices']) for a in worker_args],
            [1] * 21)

        # Spot check one full result for sanity's sake
        reconstructor.reconstructor_workers = 11
        self.assertEqual([
            {'override_partitions': [], 'override_devices': ['d1', 'd12']},
            {'override_partitions': [], 'override_devices': ['d2', 'd13']},
            {'override_partitions': [], 'override_devices': ['d3', 'd14']},
            {'override_partitions': [], 'override_devices': ['d4', 'd15']},
            {'override_partitions': [], 'override_devices': ['d5', 'd16']},
            {'override_partitions': [], 'override_devices': ['d6', 'd17']},
            {'override_partitions': [], 'override_devices': ['d7', 'd18']},
            {'override_partitions': [], 'override_devices': ['d8', 'd19']},
            {'override_partitions': [], 'override_devices': ['d9', 'd20']},
            {'override_partitions': [], 'override_devices': ['d10', 'd21']},
            {'override_partitions': [], 'override_devices': ['d11']},
        ], list(reconstructor.get_worker_args()))

    def test_next_rcache_update_configured_with_stats_interval(self):
        now = time.time()
        with mock.patch('swift.obj.reconstructor.time.time', return_value=now):
            reconstructor = object_reconstructor.ObjectReconstructor(
                {}, logger=self.logger)
            self.assertEqual(now + 300, reconstructor._next_rcache_update)
            reconstructor = object_reconstructor.ObjectReconstructor(
                {'stats_interval': '30'}, logger=self.logger)
            self.assertEqual(now + 30, reconstructor._next_rcache_update)

    def test_is_healthy_rcache_update_waits_for_next_update(self):
        now = time.time()
        with mock.patch('swift.obj.reconstructor.time.time', return_value=now):
            reconstructor = object_reconstructor.ObjectReconstructor(
                {'recon_cache_path': self.recon_cache_path},
                logger=self.logger)
        # file does not exist to start
        self.assertFalse(os.path.exists(self.rcache))
        self.assertTrue(reconstructor.is_healthy())
        # ... and isn't created until _next_rcache_update
        self.assertFalse(os.path.exists(self.rcache))
        # ... but if we wait 5 mins (by default)
        orig_next_update = reconstructor._next_rcache_update
        with mock.patch('swift.obj.reconstructor.time.time',
                        return_value=now + 301):
            self.assertTrue(reconstructor.is_healthy())
        self.assertGreater(reconstructor._next_rcache_update, orig_next_update)
        # ... it will be created
        self.assertTrue(os.path.exists(self.rcache))
        with open(self.rcache) as f:
            data = json.load(f)
        # and empty
        self.assertEqual({}, data)

    def test_is_healthy(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'recon_cache_path': self.recon_cache_path},
            logger=self.logger)
        self.assertTrue(reconstructor.is_healthy())
        reconstructor.get_local_devices = lambda: {
            'sdb%d' % p for p in reconstructor.policies}
        self.assertFalse(reconstructor.is_healthy())
        reconstructor.all_local_devices = {
            'sdb%d' % p for p in reconstructor.policies}
        self.assertTrue(reconstructor.is_healthy())

    def test_is_healthy_detects_ring_change(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'recon_cache_path': self.recon_cache_path,
             'reconstructor_workers': 1,
             # bind ip and port will not match any dev in first version of ring
             'bind_ip': '10.0.0.20', 'bind_port': '1020'},
            logger=self.logger)
        p = random.choice(reconstructor.policies)
        self.assertEqual(14, len(p.object_ring.devs))  # sanity check
        worker_args = list(reconstructor.get_worker_args())
        self.assertFalse(worker_args[0]['override_devices'])  # no local devs
        self.assertTrue(reconstructor.is_healthy())
        # expand ring - now there are local devices
        p.object_ring.set_replicas(28)
        self.assertEqual(28, len(p.object_ring.devs))  # sanity check
        self.assertFalse(reconstructor.is_healthy())
        self.assertNotEqual(worker_args, list(reconstructor.get_worker_args()))
        self.assertTrue(reconstructor.is_healthy())

    def test_final_recon_dump(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'recon_cache_path': self.recon_cache_path},
            logger=self.logger)
        reconstructor.all_local_devices = ['sda', 'sdc']
        total = 12.0
        now = time.time()
        with mock.patch('swift.obj.reconstructor.time.time', return_value=now):
            reconstructor.final_recon_dump(total)
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': now,
            'object_reconstruction_time': total,
        }, data)
        total = 14.0
        now += total * 60
        with mock.patch('swift.obj.reconstructor.time.time', return_value=now):
            reconstructor.final_recon_dump(total)
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': now,
            'object_reconstruction_time': total,
        }, data)

        def check_per_disk_stats(before, now, old_total, total,
                                 override_devices):
            with mock.patch('swift.obj.reconstructor.time.time',
                            return_value=now), \
                    mock.patch('swift.obj.reconstructor.os.getpid',
                               return_value='pid-1'):
                    reconstructor.final_recon_dump(
                        total, override_devices=override_devices)
            with open(self.rcache) as f:
                data = json.load(f)
            self.assertEqual({
                'object_reconstruction_last': before,
                'object_reconstruction_time': old_total,
                'object_reconstruction_per_disk': {
                    'sda': {
                        'object_reconstruction_last': now,
                        'object_reconstruction_time': total,
                        'pid': 'pid-1',
                    },
                    'sdc': {
                        'object_reconstruction_last': now,
                        'object_reconstruction_time': total,
                        'pid': 'pid-1',
                    },

                },
            }, data)

        # per_disk_stats with workers and local_devices
        reconstructor.reconstructor_workers = 1
        old_total = total
        total = 16.0
        before = now
        now += total * 60
        check_per_disk_stats(before, now, old_total, total, ['sda', 'sdc'])

        # per_disk_stats with workers and local_devices but no overrides
        reconstructor.reconstructor_workers = 1
        total = 17.0
        now += total * 60
        check_per_disk_stats(before, now, old_total, total, [])

        # and without workers we clear it out
        reconstructor.reconstructor_workers = 0
        total = 18.0
        now += total * 60
        with mock.patch('swift.obj.reconstructor.time.time', return_value=now):
            reconstructor.final_recon_dump(total)
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': now,
            'object_reconstruction_time': total,
        }, data)

        # set per disk stats again...
        reconstructor.reconstructor_workers = 1
        old_total = total
        total = 18.0
        before = now
        now += total * 60
        check_per_disk_stats(before, now, old_total, total, ['sda', 'sdc'])

        # ...then remove all devices and check we clear out per-disk stats
        reconstructor.all_local_devices = []
        total = 20.0
        now += total * 60
        with mock.patch('swift.obj.reconstructor.time.time', return_value=now):
            reconstructor.final_recon_dump(total)
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': now,
            'object_reconstruction_time': total,
        }, data)

    def test_dump_recon_run_once_inline(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'recon_cache_path': self.recon_cache_path},
            logger=self.logger)
        reconstructor.reconstruct = mock.MagicMock()
        now = time.time()
        later = now + 300  # 5 mins
        with mock.patch('swift.obj.reconstructor.time.time', side_effect=[
                now, later, later]):
            reconstructor.run_once()
        # no override args passed to reconstruct
        self.assertEqual([mock.call(
            override_devices=[],
            override_partitions=[]
        )], reconstructor.reconstruct.call_args_list)
        # script mode with no override args, we expect recon dumps
        self.assertTrue(os.path.exists(self.rcache))
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': later,
            'object_reconstruction_time': 5.0,
        }, data)
        total = 10.0
        later += total * 60
        with mock.patch('swift.obj.reconstructor.time.time',
                        return_value=later):
            reconstructor.final_recon_dump(total)
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': later,
            'object_reconstruction_time': 10.0,
        }, data)

    def test_dump_recon_run_once_in_worker(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'recon_cache_path': self.recon_cache_path,
             'reconstructor_workers': 1},
            logger=self.logger)
        reconstructor.get_local_devices = lambda: {'sda'}
        now = time.time()
        later = now + 300  # 5 mins

        def do_test(run_kwargs, expected_device):
            # get the actual kwargs that would be passed to run_once in a
            # worker
            run_once_kwargs = list(
                reconstructor.get_worker_args(once=True, **run_kwargs))[0]
            reconstructor.reconstruct = mock.MagicMock()
            with mock.patch('swift.obj.reconstructor.time.time',
                            side_effect=[now, later, later]):
                reconstructor.run_once(**run_once_kwargs)
            self.assertEqual([mock.call(
                override_devices=[expected_device],
                override_partitions=[]
            )], reconstructor.reconstruct.call_args_list)
            self.assertTrue(os.path.exists(self.rcache))
            with open(self.rcache) as f:
                data = json.load(f)
            self.assertEqual({
                # no aggregate is written but perhaps it should be, in which
                # case this assertion will need to change
                'object_reconstruction_per_disk': {
                    expected_device: {
                        'object_reconstruction_last': later,
                        'object_reconstruction_time': 5.0,
                        'pid': mock.ANY
                    }
                }
            }, data)

        # script mode with no CLI override args, we expect recon dumps
        do_test({}, 'sda')
        # script mode *with* CLI override devices, we expect recon dumps
        os.unlink(self.rcache)
        do_test(dict(devices='sda'), 'sda')
        # if the override device is not in local devices we still get
        # a recon dump, but it'll get cleaned up in the next aggregation
        os.unlink(self.rcache)
        do_test(dict(devices='sdz'), 'sdz')
        # repeat with no local devices
        reconstructor.get_local_devices = lambda: set()
        os.unlink(self.rcache)
        do_test(dict(devices='sdz'), 'sdz')

        # now disable workers and check that inline run_once updates rcache
        # and clears out per disk stats
        reconstructor.get_local_devices = lambda: {'sda'}
        now = time.time()
        later = now + 600  # 10 mins
        reconstructor.reconstructor_workers = 0
        with mock.patch('swift.obj.reconstructor.time.time',
                        side_effect=[now, later, later]):
            reconstructor.run_once()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': later,
            'object_reconstruction_time': 10.0,
        }, data)

    def test_no_dump_recon_run_once(self):
        reconstructor = object_reconstructor.ObjectReconstructor(
            {'recon_cache_path': self.recon_cache_path},
            logger=self.logger)
        reconstructor.get_local_devices = lambda: {'sda', 'sdb', 'sdc'}

        def do_test(run_once_kwargs, expected_devices, expected_partitions):
            reconstructor.reconstruct = mock.MagicMock()
            now = time.time()
            later = now + 300  # 5 mins
            with mock.patch('swift.obj.reconstructor.time.time', side_effect=[
                    now, later, later]):
                reconstructor.run_once(**run_once_kwargs)
            # override args passed to reconstruct
            actual_calls = reconstructor.reconstruct.call_args_list
            self.assertEqual({'override_devices', 'override_partitions'},
                             set(actual_calls[0][1]))
            self.assertEqual(sorted(expected_devices),
                             sorted(actual_calls[0][1]['override_devices']))
            self.assertEqual(sorted(expected_partitions),
                             sorted(actual_calls[0][1]['override_partitions']))
            self.assertFalse(actual_calls[1:])
            self.assertEqual(False, os.path.exists(self.rcache))

        # inline mode with overrides never does recon dump
        reconstructor.reconstructor_workers = 0
        kwargs = {'devices': 'sda,sdb'}
        do_test(kwargs, ['sda', 'sdb'], [])

        # Have partition override, so no recon dump
        kwargs = {'partitions': '1,2,3'}
        do_test(kwargs, [], [1, 2, 3])
        reconstructor.reconstructor_workers = 1
        worker_kwargs = list(
            reconstructor.get_worker_args(once=True, **kwargs))[0]
        do_test(worker_kwargs, ['sda', 'sdb', 'sdc'], [1, 2, 3])

        reconstructor.reconstructor_workers = 0
        kwargs = {'devices': 'sda,sdb', 'partitions': '1,2,3'}
        do_test(kwargs, ['sda', 'sdb'], [1, 2, 3])
        reconstructor.reconstructor_workers = 1
        worker_kwargs = list(
            reconstructor.get_worker_args(once=True, **kwargs))[0]
        do_test(worker_kwargs, ['sda', 'sdb'], [1, 2, 3])

        # 'sdz' is not in local devices
        reconstructor.reconstructor_workers = 0
        kwargs = {'devices': 'sdz'}
        do_test(kwargs, ['sdz'], [])

    def test_run_forever_recon_aggregation(self):

        class StopForever(Exception):
            pass

        reconstructor = object_reconstructor.ObjectReconstructor({
            'reconstructor_workers': 2,
            'recon_cache_path': self.recon_cache_path
        }, logger=self.logger)
        reconstructor.get_local_devices = lambda: ['sda', 'sdb', 'sdc', 'sdd']
        reconstructor.reconstruct = mock.MagicMock()
        now = time.time()
        later = now + 300  # 5 mins
        worker_args = list(
            # include 'devices' kwarg as a sanity check - it should be ignored
            # in run_forever mode
            reconstructor.get_worker_args(once=False, devices='sda'))
        with mock.patch('swift.obj.reconstructor.time.time',
                        side_effect=[now, later, later]), \
                mock.patch('swift.obj.reconstructor.os.getpid',
                           return_value='pid-1'), \
                mock.patch('swift.obj.reconstructor.sleep',
                           side_effect=[StopForever]), \
                Timeout(.3), quiet_eventlet_exceptions(), \
                self.assertRaises(StopForever):
            gt = spawn(reconstructor.run_forever, **worker_args[0])
            gt.wait()
        # override args are passed to reconstruct
        self.assertEqual([mock.call(
            override_devices=['sda', 'sdc'],
            override_partitions=[]
        )], reconstructor.reconstruct.call_args_list)
        # forever mode with override args, we expect per-disk recon dumps
        self.assertTrue(os.path.exists(self.rcache))
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_per_disk': {
                'sda': {
                    'object_reconstruction_last': later,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-1',
                },
                'sdc': {
                    'object_reconstruction_last': later,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-1',
                },
            }
        }, data)
        reconstructor.reconstruct.reset_mock()
        # another worker would get *different* disks
        before = now = later
        later = now + 300  # 5 more minutes
        with mock.patch('swift.obj.reconstructor.time.time',
                        side_effect=[now, later, later]), \
                mock.patch('swift.obj.reconstructor.os.getpid',
                           return_value='pid-2'), \
                mock.patch('swift.obj.reconstructor.sleep',
                           side_effect=[StopForever]), \
                Timeout(.3), quiet_eventlet_exceptions(), \
                self.assertRaises(StopForever):
            gt = spawn(reconstructor.run_forever, **worker_args[1])
            gt.wait()
        # override args are parsed
        self.assertEqual([mock.call(
            override_devices=['sdb', 'sdd'],
            override_partitions=[]
        )], reconstructor.reconstruct.call_args_list)
        # forever mode with override args, we expect per-disk recon dumps
        self.assertTrue(os.path.exists(self.rcache))
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_per_disk': {
                'sda': {
                    'object_reconstruction_last': before,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-1',
                },
                'sdb': {
                    'object_reconstruction_last': later,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-2',
                },
                'sdc': {
                    'object_reconstruction_last': before,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-1',
                },
                'sdd': {
                    'object_reconstruction_last': later,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-2',
                },
            }
        }, data)

        # aggregation is done in the parent thread even later
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': later,
            'object_reconstruction_time': 10.0,
            'object_reconstruction_per_disk': {
                'sda': {
                    'object_reconstruction_last': before,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-1',
                },
                'sdb': {
                    'object_reconstruction_last': later,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-2',
                },
                'sdc': {
                    'object_reconstruction_last': before,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-1',
                },
                'sdd': {
                    'object_reconstruction_last': later,
                    'object_reconstruction_time': 5.0,
                    'pid': 'pid-2',
                },
            }
        }, data)

    def test_run_forever_recon_no_devices(self):

        class StopForever(Exception):
            pass

        reconstructor = object_reconstructor.ObjectReconstructor({
            'reconstructor_workers': 2,
            'recon_cache_path': self.recon_cache_path
        }, logger=self.logger)

        def run_forever_but_stop(pid, mock_times, worker_kwargs):
            with mock.patch('swift.obj.reconstructor.time.time',
                            side_effect=mock_times), \
                    mock.patch('swift.obj.reconstructor.os.getpid',
                               return_value=pid), \
                    mock.patch('swift.obj.reconstructor.sleep',
                               side_effect=[StopForever]), \
                    Timeout(.3), quiet_eventlet_exceptions(), \
                    self.assertRaises(StopForever):
                gt = spawn(reconstructor.run_forever, **worker_kwargs)
                gt.wait()

        reconstructor.reconstruct = mock.MagicMock()
        now = time.time()
        # first run_forever with no devices
        reconstructor.get_local_devices = lambda: []
        later = now + 6  # 6 sec
        worker_args = list(
            # include 'devices' kwarg as a sanity check - it should be ignored
            # in run_forever mode
            reconstructor.get_worker_args(once=False, devices='sda'))
        run_forever_but_stop('pid-1', [now, later, later], worker_args[0])
        # override args are passed to reconstruct
        self.assertEqual([mock.call(
            override_devices=[],
            override_partitions=[]
        )], reconstructor.reconstruct.call_args_list)
        # forever mode with no args, we expect total recon dumps
        self.assertTrue(os.path.exists(self.rcache))
        with open(self.rcache) as f:
            data = json.load(f)
        expected = {
            'object_reconstruction_last': later,
            'object_reconstruction_time': 0.1,
        }
        self.assertEqual(expected, data)
        reconstructor.reconstruct.reset_mock()

        # aggregation is done in the parent thread even later
        now = later + 300
        with mock.patch('swift.obj.reconstructor.time.time',
                        side_effect=[now]):
            reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual(expected, data)

    def test_recon_aggregation_waits_for_all_devices(self):
        reconstructor = object_reconstructor.ObjectReconstructor({
            'reconstructor_workers': 2,
            'recon_cache_path': self.recon_cache_path
        }, logger=self.logger)
        reconstructor.all_local_devices = set([
            'd0', 'd1', 'd2', 'd3',
            # unreported device definitely matters
            'd4'])
        start = time.time() - 1000
        for i in range(4):
            with mock.patch('swift.obj.reconstructor.time.time',
                            return_value=start + (300 * i)), \
                    mock.patch('swift.obj.reconstructor.os.getpid',
                               return_value='pid-%s' % i):
                reconstructor.final_recon_dump(
                    i, override_devices=['d%s' % i])
        # sanity
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_per_disk': {
                'd0': {
                    'object_reconstruction_last': start,
                    'object_reconstruction_time': 0.0,
                    'pid': 'pid-0',
                },
                'd1': {
                    'object_reconstruction_last': start + 300,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-1',
                },
                'd2': {
                    'object_reconstruction_last': start + 600,
                    'object_reconstruction_time': 2,
                    'pid': 'pid-2',
                },
                'd3': {
                    'object_reconstruction_last': start + 900,
                    'object_reconstruction_time': 3,
                    'pid': 'pid-3',
                },
            }
        }, data)

        # unreported device d4 prevents aggregation
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertNotIn('object_reconstruction_last', data)
        self.assertNotIn('object_reconstruction_time', data)
        self.assertEqual(set(['d0', 'd1', 'd2', 'd3']),
                         set(data['object_reconstruction_per_disk'].keys()))

        # it's idempotent
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertNotIn('object_reconstruction_last', data)
        self.assertNotIn('object_reconstruction_time', data)
        self.assertEqual(set(['d0', 'd1', 'd2', 'd3']),
                         set(data['object_reconstruction_per_disk'].keys()))

        # remove d4, we no longer wait on it for aggregation
        reconstructor.all_local_devices = set(['d0', 'd1', 'd2', 'd3'])
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual(start + 900, data['object_reconstruction_last'])
        self.assertEqual(15, data['object_reconstruction_time'])
        self.assertEqual(set(['d0', 'd1', 'd2', 'd3']),
                         set(data['object_reconstruction_per_disk'].keys()))

    def test_recon_aggregation_removes_devices(self):
        reconstructor = object_reconstructor.ObjectReconstructor({
            'reconstructor_workers': 2,
            'recon_cache_path': self.recon_cache_path
        }, logger=self.logger)
        reconstructor.all_local_devices = set(['d0', 'd1', 'd2', 'd3'])
        start = time.time() - 1000
        for i in range(4):
            with mock.patch('swift.obj.reconstructor.time.time',
                            return_value=start + (300 * i)), \
                    mock.patch('swift.obj.reconstructor.os.getpid',
                               return_value='pid-%s' % i):
                reconstructor.final_recon_dump(
                    i, override_devices=['d%s' % i])
        # sanity
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_per_disk': {
                'd0': {
                    'object_reconstruction_last': start,
                    'object_reconstruction_time': 0.0,
                    'pid': 'pid-0',
                },
                'd1': {
                    'object_reconstruction_last': start + 300,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-1',
                },
                'd2': {
                    'object_reconstruction_last': start + 600,
                    'object_reconstruction_time': 2,
                    'pid': 'pid-2',
                },
                'd3': {
                    'object_reconstruction_last': start + 900,
                    'object_reconstruction_time': 3,
                    'pid': 'pid-3',
                },
            }
        }, data)

        reconstructor.all_local_devices = set(['d0', 'd1', 'd2', 'd3'])
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual(start + 900, data['object_reconstruction_last'])
        self.assertEqual(15, data['object_reconstruction_time'])
        self.assertEqual(set(['d0', 'd1', 'd2', 'd3']),
                         set(data['object_reconstruction_per_disk'].keys()))

        # it's idempotent
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': start + 900,
            'object_reconstruction_time': 15,
            'object_reconstruction_per_disk': {
                'd0': {
                    'object_reconstruction_last': start,
                    'object_reconstruction_time': 0.0,
                    'pid': 'pid-0',
                },
                'd1': {
                    'object_reconstruction_last': start + 300,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-1',
                },
                'd2': {
                    'object_reconstruction_last': start + 600,
                    'object_reconstruction_time': 2,
                    'pid': 'pid-2',
                },
                'd3': {
                    'object_reconstruction_last': start + 900,
                    'object_reconstruction_time': 3,
                    'pid': 'pid-3',
                },
            }
        }, data)

        # if a device is removed from the ring
        reconstructor.all_local_devices = set(['d1', 'd2', 'd3'])
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        # ... it's per-disk stats are removed (d0)
        self.assertEqual({
            'object_reconstruction_last': start + 900,
            'object_reconstruction_time': 11,
            'object_reconstruction_per_disk': {
                'd1': {
                    'object_reconstruction_last': start + 300,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-1',
                },
                'd2': {
                    'object_reconstruction_last': start + 600,
                    'object_reconstruction_time': 2,
                    'pid': 'pid-2',
                },
                'd3': {
                    'object_reconstruction_last': start + 900,
                    'object_reconstruction_time': 3,
                    'pid': 'pid-3',
                },
            }
        }, data)

        # which can affect the aggregates!
        reconstructor.all_local_devices = set(['d1', 'd2'])
        reconstructor.aggregate_recon_update()
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': start + 600,
            'object_reconstruction_time': 6,
            'object_reconstruction_per_disk': {
                'd1': {
                    'object_reconstruction_last': start + 300,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-1',
                },
                'd2': {
                    'object_reconstruction_last': start + 600,
                    'object_reconstruction_time': 2,
                    'pid': 'pid-2',
                },
            }
        }, data)

    def test_recon_aggregation_races_with_final_recon_dump(self):
        reconstructor = object_reconstructor.ObjectReconstructor({
            'reconstructor_workers': 2,
            'recon_cache_path': self.recon_cache_path
        }, logger=self.logger)
        reconstructor.all_local_devices = set(['d0', 'd1'])
        start = time.time() - 1000
        # first worker dumps to recon cache
        with mock.patch('swift.obj.reconstructor.time.time',
                        return_value=start), \
                mock.patch('swift.obj.reconstructor.os.getpid',
                           return_value='pid-0'):
            reconstructor.final_recon_dump(
                1, override_devices=['d0'])
        # sanity
        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_per_disk': {
                'd0': {
                    'object_reconstruction_last': start,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-0',
                },
            }
        }, data)

        # simulate a second worker concurrently dumping to recon cache while
        # parent is aggregatng existing results; mock dump_recon_cache as a
        # convenient way to interrupt parent aggregate_recon_update and 'pass
        # control' to second worker
        updated_data = []  # state of recon cache just after second worker dump

        def simulate_other_process_final_recon_dump():
            with mock.patch('swift.obj.reconstructor.time.time',
                            return_value=start + 999), \
                    mock.patch('swift.obj.reconstructor.os.getpid',
                               return_value='pid-1'):
                reconstructor.final_recon_dump(
                    1000, override_devices=['d1'])
                with open(self.rcache) as f:
                    updated_data.append(json.load(f))

        def fake_dump_recon_cache(*args, **kwargs):
            # temporarily put back real dump_recon_cache
            with mock.patch('swift.obj.reconstructor.dump_recon_cache',
                            dump_recon_cache):
                simulate_other_process_final_recon_dump()
            # and now proceed with parent dump_recon_cache
            dump_recon_cache(*args, **kwargs)

        reconstructor.dump_recon_cache = fake_dump_recon_cache
        with mock.patch('swift.obj.reconstructor.dump_recon_cache',
                        fake_dump_recon_cache):
            reconstructor.aggregate_recon_update()

        self.assertEqual([{  # sanity check - second process did dump its data
            'object_reconstruction_per_disk': {
                'd0': {
                    'object_reconstruction_last': start,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-0',
                },
                'd1': {
                    'object_reconstruction_last': start + 999,
                    'object_reconstruction_time': 1000,
                    'pid': 'pid-1',
                },
            }
        }], updated_data)

        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_per_disk': {
                'd0': {
                    'object_reconstruction_last': start,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-0',
                },
                'd1': {
                    'object_reconstruction_last': start + 999,
                    'object_reconstruction_time': 1000,
                    'pid': 'pid-1',
                },
            }
        }, data)

        # next aggregation will find d1 stats
        reconstructor.aggregate_recon_update()

        with open(self.rcache) as f:
            data = json.load(f)
        self.assertEqual({
            'object_reconstruction_last': start + 999,
            'object_reconstruction_time': 1000,
            'object_reconstruction_per_disk': {
                'd0': {
                    'object_reconstruction_last': start,
                    'object_reconstruction_time': 1,
                    'pid': 'pid-0',
                },
                'd1': {
                    'object_reconstruction_last': start + 999,
                    'object_reconstruction_time': 1000,
                    'pid': 'pid-1',
                },
            }
        }, data)


@patch_policies(with_ec_default=True)
class BaseTestObjectReconstructor(unittest.TestCase):
    def setUp(self):
        skip_if_no_xattrs()
        self.policy = POLICIES.default
        self.policy.object_ring._rtime = time.time() + 3600
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'devices')
        self.local_dev = self.policy.object_ring.devs[0]
        self.ip = self.local_dev['replication_ip']
        self.port = self.local_dev['replication_port']
        self.conf = {
            'devices': self.devices,
            'mount_check': False,
            'bind_ip': self.ip,
            'bind_port': self.port,
        }
        self.logger = debug_logger('object-reconstructor')
        self._configure_reconstructor()
        self.policy.object_ring.max_more_nodes = \
            self.policy.object_ring.replicas
        self.ts_iter = make_timestamp_iter()
        self.fabricated_ring = FabricatedRing(replicas=14, devices=28)

    def _configure_reconstructor(self, **kwargs):
        self.conf.update(kwargs)
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.reconstructor._reset_stats()
        # some tests bypass build_reconstruction_jobs and go to process_job
        # directly, so you end up with a /0 when you try to show the
        # percentage of complete jobs as ratio of the total job count
        self.reconstructor.job_count = 1
        # if we ever let a test through without properly patching the
        # REPLICATE and SSYNC calls - let's fail sort fast-ish
        self.reconstructor.lockup_timeout = 3

    def tearDown(self):
        self.reconstructor._reset_stats()
        self.reconstructor.stats_line()
        shutil.rmtree(self.testdir)

    def ts(self):
        return next(self.ts_iter)


class TestObjectReconstructor(BaseTestObjectReconstructor):
    def test_handoffs_only_default(self):
        # sanity neither option added to default conf
        self.conf.pop('handoffs_first', None)
        self.conf.pop('handoffs_only', None)
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertFalse(self.reconstructor.handoffs_only)

    def test_handoffs_first_enables_handoffs_only(self):
        self.conf['handoffs_first'] = "True"
        self.conf.pop('handoffs_only', None)  # sanity
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertTrue(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        expected = [
            'The handoffs_first option is deprecated in favor '
            'of handoffs_only. This option may be ignored in a '
            'future release.',
            'Handoff only mode is not intended for normal operation, '
            'use handoffs_only with care.',
        ]
        self.assertEqual(expected, warnings)

    def test_handoffs_only_ignores_handoffs_first(self):
        self.conf['handoffs_first'] = "True"
        self.conf['handoffs_only'] = "False"
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertFalse(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        expected = [
            'The handoffs_first option is deprecated in favor of '
            'handoffs_only. This option may be ignored in a future release.',
            'Ignored handoffs_first option in favor of handoffs_only.',
        ]
        self.assertEqual(expected, warnings)

    def test_handoffs_only_enabled(self):
        self.conf.pop('handoffs_first', None)  # sanity
        self.conf['handoffs_only'] = "True"
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertTrue(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        expected = [
            'Handoff only mode is not intended for normal operation, '
            'use handoffs_only with care.',
        ]
        self.assertEqual(expected, warnings)

    def test_handoffs_only_true_and_first_true(self):
        self.conf['handoffs_first'] = "True"
        self.conf['handoffs_only'] = "True"
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertTrue(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        expected = [
            'The handoffs_first option is deprecated in favor of '
            'handoffs_only. This option may be ignored in a future release.',
            'Handoff only mode is not intended for normal operation, '
            'use handoffs_only with care.',
        ]
        self.assertEqual(expected, warnings)

    def test_handoffs_only_false_and_first_false(self):
        self.conf['handoffs_only'] = "False"
        self.conf['handoffs_first'] = "False"
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertFalse(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        expected = [
            'The handoffs_first option is deprecated in favor of '
            'handoffs_only. This option may be ignored in a future release.',
        ]
        self.assertEqual(expected, warnings)

    def test_handoffs_only_none_and_first_false(self):
        self.conf['handoffs_first'] = "False"
        self.conf.pop('handoffs_only', None)  # sanity
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertFalse(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        expected = [
            'The handoffs_first option is deprecated in favor of '
            'handoffs_only. This option may be ignored in a future release.',
        ]
        self.assertEqual(expected, warnings)

    def test_handoffs_only_false_and_first_none(self):
        self.conf.pop('handoffs_first', None)  # sanity
        self.conf['handoffs_only'] = "False"
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertFalse(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        self.assertFalse(warnings)

    def test_handoffs_only_true_and_first_false(self):
        self.conf['handoffs_first'] = "False"
        self.conf['handoffs_only'] = "True"
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.assertTrue(self.reconstructor.handoffs_only)
        warnings = self.logger.get_lines_for_level('warning')
        expected = [
            'The handoffs_first option is deprecated in favor of '
            'handoffs_only. This option may be ignored in a future release.',
            'Handoff only mode is not intended for normal operation, '
            'use handoffs_only with care.',
        ]
        self.assertEqual(expected, warnings)

    def test_two_ec_policies(self):
        with patch_policies([
                StoragePolicy(0, name='zero', is_deprecated=True),
                ECStoragePolicy(1, name='one', is_default=True,
                                ec_type=DEFAULT_TEST_EC_TYPE,
                                ec_ndata=4, ec_nparity=3),
                ECStoragePolicy(2, name='two',
                                ec_type=DEFAULT_TEST_EC_TYPE,
                                ec_ndata=8, ec_nparity=2)],
                            fake_ring_args=[
                                {}, {'replicas': 7}, {'replicas': 10}]):
            self._configure_reconstructor()
            jobs = []

            def process_job(job):
                jobs.append(job)

            self.reconstructor.process_job = process_job

            os.makedirs(os.path.join(self.devices, 'sda', 'objects-1', '0'))
            self.reconstructor.run_once()
            self.assertEqual(1, len(jobs))

    def test_collect_parts_skips_non_ec_policy_and_device(self):
        stub_parts = (371, 78, 419, 834)
        for policy in POLICIES:
            datadir = diskfile.get_data_dir(policy)
            for part in stub_parts:
                utils.mkdirs(os.path.join(
                    self.devices, self.local_dev['device'],
                    datadir, str(part)))
        part_infos = list(self.reconstructor.collect_parts())
        found_parts = sorted(int(p['partition']) for p in part_infos)
        self.assertEqual(found_parts, sorted(stub_parts))
        for part_info in part_infos:
            self.assertEqual(part_info['local_dev'], self.local_dev)
            self.assertEqual(part_info['policy'], self.policy)
            self.assertEqual(part_info['part_path'],
                             os.path.join(self.devices,
                                          self.local_dev['device'],
                                          diskfile.get_data_dir(self.policy),
                                          str(part_info['partition'])))

    def test_collect_parts_skips_non_local_devs_servers_per_port(self):
        self._configure_reconstructor(devices=self.devices, mount_check=False,
                                      bind_ip=self.ip, bind_port=self.port,
                                      servers_per_port=2)

        device_parts = {
            'sda': (374,),
            'sdb': (179, 807),  # w/one-serv-per-port, same IP alone is local
            'sdc': (363, 468, 843),
            'sdd': (912,),  # "not local" via different IP
        }
        for policy in POLICIES:
            datadir = diskfile.get_data_dir(policy)
            for dev, parts in device_parts.items():
                for part in parts:
                    utils.mkdirs(os.path.join(
                        self.devices, dev,
                        datadir, str(part)))

        # we're only going to add sda and sdc into the ring
        local_devs = ('sda', 'sdb', 'sdc')
        stub_ring_devs = [{
            'id': i,
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port + 1 if dev == 'sdb' else self.port,
        } for i, dev in enumerate(local_devs)]
        stub_ring_devs.append({
            'id': i + 1,
            'device': 'sdd',
            'replication_ip': '127.0.0.88',  # not local via IP
            'replication_port': self.port,
        })
        self.reconstructor.bind_ip = '0.0.0.0'  # use whataremyips
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs):
            part_infos = list(self.reconstructor.collect_parts())
        found_parts = sorted(int(p['partition']) for p in part_infos)
        expected_parts = sorted(itertools.chain(
            *(device_parts[d] for d in local_devs)))
        self.assertEqual(found_parts, expected_parts)
        for part_info in part_infos:
            self.assertEqual(part_info['policy'], self.policy)
            self.assertIn(part_info['local_dev'], stub_ring_devs)
            dev = part_info['local_dev']
            self.assertEqual(part_info['part_path'],
                             os.path.join(self.devices,
                                          dev['device'],
                                          diskfile.get_data_dir(self.policy),
                                          str(part_info['partition'])))

    def test_collect_parts_multi_device_skips_non_non_local_devs(self):
        device_parts = {
            'sda': (374,),
            'sdb': (179, 807),  # "not local" via different port
            'sdc': (363, 468, 843),
            'sdd': (912,),  # "not local" via different IP
        }
        for policy in POLICIES:
            datadir = diskfile.get_data_dir(policy)
            for dev, parts in device_parts.items():
                for part in parts:
                    utils.mkdirs(os.path.join(
                        self.devices, dev,
                        datadir, str(part)))

        # we're only going to add sda and sdc into the ring
        local_devs = ('sda', 'sdc')
        stub_ring_devs = [{
            'id': i,
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port,
        } for i, dev in enumerate(local_devs)]
        stub_ring_devs.append({
            'id': i + 1,
            'device': 'sdb',
            'replication_ip': self.ip,
            'replication_port': self.port + 1,  # not local via port
        })
        stub_ring_devs.append({
            'id': i + 2,
            'device': 'sdd',
            'replication_ip': '127.0.0.88',  # not local via IP
            'replication_port': self.port,
        })
        self.reconstructor.bind_ip = '0.0.0.0'  # use whataremyips
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs):
            part_infos = list(self.reconstructor.collect_parts())
        found_parts = sorted(int(p['partition']) for p in part_infos)
        expected_parts = sorted(itertools.chain(
            *(device_parts[d] for d in local_devs)))
        self.assertEqual(found_parts, expected_parts)
        for part_info in part_infos:
            self.assertEqual(part_info['policy'], self.policy)
            self.assertIn(part_info['local_dev'], stub_ring_devs)
            dev = part_info['local_dev']
            self.assertEqual(part_info['part_path'],
                             os.path.join(self.devices,
                                          dev['device'],
                                          diskfile.get_data_dir(self.policy),
                                          str(part_info['partition'])))

    def test_collect_parts_multi_device_skips_non_ring_devices(self):
        device_parts = {
            'sda': (374,),
            'sdc': (363, 468, 843),
        }
        for policy in POLICIES:
            datadir = diskfile.get_data_dir(policy)
            for dev, parts in device_parts.items():
                for part in parts:
                    utils.mkdirs(os.path.join(
                        self.devices, dev,
                        datadir, str(part)))

        # we're only going to add sda and sdc into the ring
        local_devs = ('sda', 'sdc')
        stub_ring_devs = [{
            'id': i,
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port,
        } for i, dev in enumerate(local_devs)]
        self.reconstructor.bind_ip = '0.0.0.0'  # use whataremyips
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs):
            part_infos = list(self.reconstructor.collect_parts())
        found_parts = sorted(int(p['partition']) for p in part_infos)
        expected_parts = sorted(itertools.chain(
            *(device_parts[d] for d in local_devs)))
        self.assertEqual(found_parts, expected_parts)
        for part_info in part_infos:
            self.assertEqual(part_info['policy'], self.policy)
            self.assertIn(part_info['local_dev'], stub_ring_devs)
            dev = part_info['local_dev']
            self.assertEqual(part_info['part_path'],
                             os.path.join(self.devices,
                                          dev['device'],
                                          diskfile.get_data_dir(self.policy),
                                          str(part_info['partition'])))

    def test_collect_parts_mount_check(self):
        # each device has one part in it
        local_devs = ('sda', 'sdb')
        for i, dev in enumerate(local_devs):
            datadir = diskfile.get_data_dir(self.policy)
            utils.mkdirs(os.path.join(
                self.devices, dev, datadir, str(i)))
        stub_ring_devs = [{
            'id': i,
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port
        } for i, dev in enumerate(local_devs)]
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual(2, len(part_infos))  # sanity
        self.assertEqual(set(int(p['partition']) for p in part_infos),
                         set([0, 1]))

        paths = []

        def fake_check_drive(devices, device, mount_check):
            path = os.path.join(devices, device)
            if (not mount_check) and os.path.isdir(path):
                # while mount_check is false, the test still creates the dirs
                paths.append(path)
                return path
            return None

        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs), \
                mock.patch('swift.obj.diskfile.check_drive',
                           fake_check_drive):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual(2, len(part_infos))  # sanity, same jobs
        self.assertEqual(set(int(p['partition']) for p in part_infos),
                         set([0, 1]))

        # ... because fake_check_drive returned paths for both dirs
        self.assertEqual(set(paths), set([
            os.path.join(self.devices, dev) for dev in local_devs]))

        # ... now with mount check
        self._configure_reconstructor(mount_check=True)
        self.assertTrue(self.reconstructor.mount_check)
        paths = []
        for policy in POLICIES:
            self.assertTrue(self.reconstructor._df_router[policy].mount_check)
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs), \
                mock.patch('swift.obj.diskfile.check_drive',
                           fake_check_drive):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual([], part_infos)  # sanity, no jobs

        # ... because fake_check_drive returned False for both paths
        self.assertFalse(paths)

        def fake_check_drive(devices, device, mount_check):
            self.assertTrue(mount_check)
            if device == 'sda':
                return os.path.join(devices, device)
            else:
                return False

        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs), \
                mock.patch('swift.obj.diskfile.check_drive',
                           fake_check_drive):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual(1, len(part_infos))  # only sda picked up (part 0)
        self.assertEqual(part_infos[0]['partition'], 0)

    def test_collect_parts_cleans_tmp(self):
        local_devs = ('sda', 'sdc')
        stub_ring_devs = [{
            'id': i,
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port
        } for i, dev in enumerate(local_devs)]
        for device in local_devs:
            utils.mkdirs(os.path.join(self.devices, device))
        fake_unlink = mock.MagicMock()
        self._configure_reconstructor(reclaim_age=1000)
        now = time.time()
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch('swift.obj.reconstructor.time.time',
                           return_value=now), \
                mock.patch.object(self.policy.object_ring, '_devs',
                                  new=stub_ring_devs), \
                mock.patch('swift.obj.reconstructor.unlink_older_than',
                           fake_unlink):
            self.assertEqual([], list(self.reconstructor.collect_parts()))
        # each local device hash unlink_older_than called on it,
        # with now - self.reclaim_age
        tmpdir = diskfile.get_tmp_dir(self.policy)
        expected = now - 1000
        self.assertEqual(fake_unlink.mock_calls, [
            mock.call(os.path.join(self.devices, dev, tmpdir), expected)
            for dev in local_devs])

    def test_collect_parts_creates_datadir(self):
        # create just the device path
        dev_path = os.path.join(self.devices, self.local_dev['device'])
        utils.mkdirs(dev_path)
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]):
            self.assertEqual([], list(self.reconstructor.collect_parts()))
        datadir_path = os.path.join(dev_path,
                                    diskfile.get_data_dir(self.policy))
        self.assertTrue(os.path.exists(datadir_path))

    def test_collect_parts_creates_datadir_error(self):
        # create just the device path
        datadir_path = os.path.join(self.devices, self.local_dev['device'],
                                    diskfile.get_data_dir(self.policy))
        utils.mkdirs(os.path.dirname(datadir_path))
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch('swift.obj.reconstructor.mkdirs',
                           side_effect=OSError('kaboom!')):
            self.assertEqual([], list(self.reconstructor.collect_parts()))
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1,
                         'Expected only one error, got %r' % error_lines)
        line = error_lines[0]
        self.assertIn('Unable to create', line)
        self.assertIn(datadir_path, line)

    def test_collect_parts_skips_invalid_paths(self):
        datadir_path = os.path.join(self.devices, self.local_dev['device'],
                                    diskfile.get_data_dir(self.policy))
        utils.mkdirs(os.path.dirname(datadir_path))
        with open(datadir_path, 'w') as f:
            f.write('junk')
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]):
            self.assertEqual([], list(self.reconstructor.collect_parts()))
        self.assertTrue(os.path.exists(datadir_path))
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1,
                         'Expected only one error, got %r' % error_lines)
        line = error_lines[0]
        self.assertIn('Unable to list partitions', line)
        self.assertIn(datadir_path, line)

    def test_reconstruct_removes_non_partition_files(self):
        # create some junk next to partitions
        datadir_path = os.path.join(self.devices, self.local_dev['device'],
                                    diskfile.get_data_dir(self.policy))
        num_parts = 3
        for part in range(num_parts):
            utils.mkdirs(os.path.join(datadir_path, str(part)))

        # Add some clearly non-partition dentries
        utils.mkdirs(os.path.join(datadir_path, 'not/a/partition'))
        for junk_name in ('junk', '1234'):
            junk_file = os.path.join(datadir_path, junk_name)
            with open(junk_file, 'w') as f:
                f.write('junk')

        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
                mock.patch('swift.obj.reconstructor.'
                           'ObjectReconstructor.process_job'):
            self.reconstructor.reconstruct()

        # all the bad gets cleaned up
        errors = []
        for junk_name in ('junk', '1234', 'not'):
            junk_file = os.path.join(datadir_path, junk_name)
            if os.path.exists(junk_file):
                errors.append('%s still exists!' % junk_file)

        self.assertFalse(errors)

        error_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('Unexpected entity in data dir: %r'
                      % os.path.join(datadir_path, 'not'), error_lines)
        self.assertIn('Unexpected entity in data dir: %r'
                      % os.path.join(datadir_path, 'junk'), error_lines)
        self.assertIn('Unexpected entity %r is not a directory'
                      % os.path.join(datadir_path, '1234'), error_lines)
        self.assertEqual(self.reconstructor.reconstruction_part_count, 6)

    def test_collect_parts_overrides(self):
        # setup multiple devices, with multiple parts
        device_parts = {
            'sda': (374, 843),
            'sdb': (179, 807),
            'sdc': (363, 468, 843),
        }
        datadir = diskfile.get_data_dir(self.policy)
        for dev, parts in device_parts.items():
            for part in parts:
                utils.mkdirs(os.path.join(
                    self.devices, dev,
                    datadir, str(part)))

        # we're only going to add sda and sdc into the ring
        local_devs = ('sda', 'sdc')
        stub_ring_devs = [{
            'id': i,
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port
        } for i, dev in enumerate(local_devs)]

        expected = (
            ({}, [
                ('sda', 374),
                ('sda', 843),
                ('sdc', 363),
                ('sdc', 468),
                ('sdc', 843),
            ]),
            ({'override_devices': ['sda', 'sdc']}, [
                ('sda', 374),
                ('sda', 843),
                ('sdc', 363),
                ('sdc', 468),
                ('sdc', 843),
            ]),
            ({'override_devices': ['sdc']}, [
                ('sdc', 363),
                ('sdc', 468),
                ('sdc', 843),
            ]),
            ({'override_devices': ['sda']}, [
                ('sda', 374),
                ('sda', 843),
            ]),
            ({'override_devices': ['sdx']}, []),
            ({'override_partitions': [374]}, [
                ('sda', 374),
            ]),
            ({'override_partitions': [843]}, [
                ('sda', 843),
                ('sdc', 843),
            ]),
            ({'override_partitions': [843], 'override_devices': ['sda']}, [
                ('sda', 843),
            ]),
        )
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]), \
            mock.patch.object(self.policy.object_ring, '_devs',
                              new=stub_ring_devs):
            for kwargs, expected_parts in expected:
                part_infos = list(self.reconstructor.collect_parts(**kwargs))
                expected_paths = set(
                    os.path.join(self.devices, dev, datadir, str(part))
                    for dev, part in expected_parts)
                found_paths = set(p['part_path'] for p in part_infos)
                msg = 'expected %r != %r for %r' % (
                    expected_paths, found_paths, kwargs)
                self.assertEqual(expected_paths, found_paths, msg)

    def test_build_jobs_creates_empty_hashes(self):
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy), '0')
        utils.mkdirs(part_path)
        part_info = {
            'local_dev': self.local_dev,
            'policy': self.policy,
            'partition': 0,
            'part_path': part_path,
        }
        jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(1, len(jobs))
        job = jobs[0]
        self.assertEqual(job['job_type'], object_reconstructor.SYNC)
        self.assertEqual(job['frag_index'], 0)
        self.assertEqual(job['suffixes'], [])
        self.assertEqual(len(job['sync_to']), 2)
        self.assertEqual(job['partition'], 0)
        self.assertEqual(job['path'], part_path)
        self.assertEqual(job['hashes'], {})
        self.assertEqual(job['policy'], self.policy)
        self.assertEqual(job['local_dev'], self.local_dev)
        self.assertEqual(job['device'], self.local_dev['device'])
        hashes_file = os.path.join(part_path,
                                   diskfile.HASH_FILE)
        self.assertTrue(os.path.exists(hashes_file))
        suffixes = self.reconstructor._get_hashes(
            self.local_dev['device'], 0, self.policy, do_listdir=True)
        self.assertEqual(suffixes, {})

    def test_build_jobs_no_hashes(self):
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy), '0')
        part_info = {
            'local_dev': self.local_dev,
            'policy': self.policy,
            'partition': 0,
            'part_path': part_path,
        }
        stub_hashes = {}
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(1, len(jobs))
        job = jobs[0]
        self.assertEqual(job['job_type'], object_reconstructor.SYNC)
        self.assertEqual(job['frag_index'], 0)
        self.assertEqual(job['suffixes'], [])
        self.assertEqual(len(job['sync_to']), 2)
        self.assertEqual(job['partition'], 0)
        self.assertEqual(job['path'], part_path)
        self.assertEqual(job['hashes'], {})
        self.assertEqual(job['policy'], self.policy)
        self.assertEqual(job['local_dev'], self.local_dev)
        self.assertEqual(job['device'], self.local_dev['device'])

    def test_build_jobs_primary(self):
        ring = self.policy.object_ring = self.fabricated_ring
        # find a partition for which we're a primary
        for partition in range(2 ** ring.part_power):
            part_nodes = ring.get_part_nodes(partition)
            try:
                frag_index = [n['id'] for n in part_nodes].index(
                    self.local_dev['id'])
            except ValueError:
                pass
            else:
                break
        else:
            self.fail("the ring doesn't work: %r" % ring._replica2part2dev_id)
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        part_info = {
            'local_dev': self.local_dev,
            'policy': self.policy,
            'partition': partition,
            'part_path': part_path,
        }
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(1, len(jobs))
        job = jobs[0]
        self.assertEqual(job['job_type'], object_reconstructor.SYNC)
        self.assertEqual(job['frag_index'], frag_index)
        self.assertEqual(job['suffixes'], stub_hashes.keys())
        self.assertEqual(set([n['index'] for n in job['sync_to']]),
                         set([(frag_index + 1) % ring.replicas,
                              (frag_index - 1) % ring.replicas]))
        self.assertEqual(job['partition'], partition)
        self.assertEqual(job['path'], part_path)
        self.assertEqual(job['hashes'], stub_hashes)
        self.assertEqual(job['policy'], self.policy)
        self.assertEqual(job['local_dev'], self.local_dev)
        self.assertEqual(job['device'], self.local_dev['device'])

    def test_build_jobs_handoff(self):
        ring = self.policy.object_ring = self.fabricated_ring
        # find a partition for which we're a handoff
        for partition in range(2 ** ring.part_power):
            part_nodes = ring.get_part_nodes(partition)
            if self.local_dev['id'] not in [n['id'] for n in part_nodes]:
                break
        else:
            self.fail("the ring doesn't work: %r" % ring._replica2part2dev_id)
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        part_info = {
            'local_dev': self.local_dev,
            'policy': self.policy,
            'partition': partition,
            'part_path': part_path,
        }
        # since this part doesn't belong on us it doesn't matter what
        # frag_index we have
        frag_index = random.randint(0, self.policy.ec_n_unique_fragments - 1)
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {None: 'hash'},
        }
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(1, len(jobs), 'Expected only one job, got %r' % jobs)
        job = jobs[0]
        self.assertEqual(job['job_type'], object_reconstructor.REVERT)
        self.assertEqual(job['frag_index'], frag_index)
        self.assertEqual(sorted(job['suffixes']), sorted(stub_hashes.keys()))
        self.assertEqual(
            self.policy.ec_duplication_factor, len(job['sync_to']))
        # the sync_to node should be different each other
        node_ids = set([node['id'] for node in job['sync_to']])
        self.assertEqual(len(node_ids),
                         self.policy.ec_duplication_factor)
        # but all the nodes have same backend index to sync
        node_indexes = set(
            self.policy.get_backend_index(node['index'])
            for node in job['sync_to'])
        self.assertEqual(1, len(node_indexes))
        self.assertEqual(job['sync_to'][0]['index'], frag_index)
        self.assertEqual(job['path'], part_path)
        self.assertEqual(job['partition'], partition)
        self.assertEqual(sorted(job['hashes']), sorted(stub_hashes))
        self.assertEqual(job['local_dev'], self.local_dev)

    def test_build_jobs_mixed(self):
        ring = self.policy.object_ring = self.fabricated_ring
        # find a partition for which we're a primary
        for partition in range(2 ** ring.part_power):
            part_nodes = ring.get_part_nodes(partition)
            try:
                node_index = [n['id'] for n in part_nodes].index(
                    self.local_dev['id'])
            except ValueError:
                pass
            else:
                break
        else:
            self.fail("the ring doesn't work: %r" % ring._replica2part2dev_id)
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        part_info = {
            'local_dev': self.local_dev,
            'policy': self.policy,
            'partition': partition,
            'part_path': part_path,
        }
        frag_index = self.policy.get_backend_index(node_index)
        other_frag_index = random.choice(
            [f for f in range(self.policy.ec_n_unique_fragments)
             if f != node_index])
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            '456': {other_frag_index: 'hash', None: 'hash'},
            'abc': {None: 'hash'},
        }
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(2, len(jobs))
        sync_jobs, revert_jobs = [], []
        for job in jobs:
            self.assertEqual(job['partition'], partition)
            self.assertEqual(job['path'], part_path)
            self.assertEqual(sorted(job['hashes']), sorted(stub_hashes))
            self.assertEqual(job['policy'], self.policy)
            self.assertEqual(job['local_dev'], self.local_dev)
            self.assertEqual(job['device'], self.local_dev['device'])
            {
                object_reconstructor.SYNC: sync_jobs,
                object_reconstructor.REVERT: revert_jobs,
            }[job['job_type']].append(job)
        self.assertEqual(1, len(sync_jobs))
        job = sync_jobs[0]
        self.assertEqual(job['frag_index'], frag_index)
        self.assertEqual(sorted(job['suffixes']), sorted(['123', 'abc']))
        self.assertEqual(len(job['sync_to']), 2)
        self.assertEqual(set([n['index'] for n in job['sync_to']]),
                         set([(frag_index + 1) % ring.replicas,
                              (frag_index - 1) % ring.replicas]))
        self.assertEqual(1, len(revert_jobs))
        job = revert_jobs[0]
        self.assertEqual(job['frag_index'], other_frag_index)
        self.assertEqual(job['suffixes'], ['456'])
        self.assertEqual(len(job['sync_to']),
                         self.policy.ec_duplication_factor)
        self.assertEqual(job['sync_to'][0]['index'], other_frag_index)

    def test_build_jobs_revert_only_tombstones(self):
        ring = self.policy.object_ring = self.fabricated_ring
        # find a partition for which we're a handoff
        for partition in range(2 ** ring.part_power):
            part_nodes = ring.get_part_nodes(partition)
            if self.local_dev['id'] not in [n['id'] for n in part_nodes]:
                break
        else:
            self.fail("the ring doesn't work: %r" % ring._replica2part2dev_id)
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        part_info = {
            'local_dev': self.local_dev,
            'policy': self.policy,
            'partition': partition,
            'part_path': part_path,
        }
        # we have no fragment index to hint the jobs where they belong
        stub_hashes = {
            '123': {None: 'hash'},
            'abc': {None: 'hash'},
        }
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(len(jobs), 1, 'Expected only one job, got %r' % jobs)
        job = jobs[0]
        expected = {
            'job_type': object_reconstructor.REVERT,
            'frag_index': None,
            'suffixes': stub_hashes.keys(),
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': self.local_dev,
            'device': self.local_dev['device'],
        }
        self.assertEqual(ring.replica_count, len(part_nodes))
        expected_samples = (
            (self.policy.ec_n_unique_fragments *
             self.policy.ec_duplication_factor) -
            self.policy.ec_ndata + 1)
        self.assertEqual(len(job['sync_to']), expected_samples)
        for k, v in expected.items():
            msg = 'expected %s != %s for %s' % (
                v, job[k], k)
            self.assertEqual(v, job[k], msg)

    def test_get_suffix_delta(self):
        # different
        local_suff = {'123': {None: 'abc', 0: 'def'}}
        remote_suff = {'456': {None: 'ghi', 0: 'jkl'}}
        local_index = 0
        remote_index = 0
        suffs = self.reconstructor.get_suffix_delta(local_suff,
                                                    local_index,
                                                    remote_suff,
                                                    remote_index)
        self.assertEqual(suffs, ['123'])

        # now the same
        remote_suff = {'123': {None: 'abc', 0: 'def'}}
        suffs = self.reconstructor.get_suffix_delta(local_suff,
                                                    local_index,
                                                    remote_suff,
                                                    remote_index)
        self.assertEqual(suffs, [])

        # now with a mis-matched None key (missing durable)
        remote_suff = {'123': {None: 'ghi', 0: 'def'}}
        suffs = self.reconstructor.get_suffix_delta(local_suff,
                                                    local_index,
                                                    remote_suff,
                                                    remote_index)
        self.assertEqual(suffs, ['123'])

        # now with bogus local index
        local_suff = {'123': {None: 'abc', 99: 'def'}}
        remote_suff = {'456': {None: 'ghi', 0: 'jkl'}}
        suffs = self.reconstructor.get_suffix_delta(local_suff,
                                                    local_index,
                                                    remote_suff,
                                                    remote_index)
        self.assertEqual(suffs, ['123'])

    def test_process_job_primary_in_sync(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [n for n in self.policy.object_ring.devs
                   if n != self.local_dev][:2]
        # setup left and right hashes
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        left_index = sync_to[0]['index'] = (frag_index - 1) % replicas
        left_hashes = {
            '123': {left_index: 'hash', None: 'hash'},
            'abc': {left_index: 'hash', None: 'hash'},
        }
        right_index = sync_to[1]['index'] = (frag_index + 1) % replicas
        right_hashes = {
            '123': {right_index: 'hash', None: 'hash'},
            'abc': {right_index: 'hash', None: 'hash'},
        }
        partition = 0
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.SYNC,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        responses = [(200, pickle.dumps(hashes)) for hashes in (
            left_hashes, right_hashes)]
        codes, body_iter = zip(*responses)

        ssync_calls = []

        with mock_ssync_sender(ssync_calls), \
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)), \
                mocked_http_conn(*codes, body_iter=body_iter) as request_log:
            self.reconstructor.process_job(job)

        expected_suffix_calls = set([
            ('10.0.0.1', '/sdb/0'),
            ('10.0.0.2', '/sdc/0'),
        ])
        self.assertEqual(expected_suffix_calls,
                         set((r['ip'], r['path'])
                             for r in request_log.requests))

        self.assertFalse(ssync_calls)

    def test_process_job_primary_not_in_sync(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [n for n in self.policy.object_ring.devs
                   if n != self.local_dev][:2]
        # setup left and right hashes
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        sync_to[0]['index'] = (frag_index - 1) % replicas
        left_hashes = {}
        sync_to[1]['index'] = (frag_index + 1) % replicas
        right_hashes = {}

        partition = 0
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.SYNC,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        responses = [(200, pickle.dumps(hashes)) for hashes in (
            left_hashes, left_hashes, right_hashes, right_hashes)]
        codes, body_iter = zip(*responses)

        ssync_calls = []
        with mock_ssync_sender(ssync_calls), \
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)), \
                mocked_http_conn(*codes, body_iter=body_iter) as request_log:
            self.reconstructor.process_job(job)

        expected_suffix_calls = set([
            ('10.0.0.1', '/sdb/0'),
            ('10.0.0.1', '/sdb/0/123-abc'),
            ('10.0.0.2', '/sdc/0'),
            ('10.0.0.2', '/sdc/0/123-abc'),
        ])
        self.assertEqual(expected_suffix_calls,
                         set((r['ip'], r['path'])
                             for r in request_log.requests))

        expected_ssync_calls = sorted([
            ('10.0.0.1', 0, set(['123', 'abc'])),
            ('10.0.0.2', 0, set(['123', 'abc'])),
        ])
        self.assertEqual(expected_ssync_calls, sorted((
            c['node']['ip'],
            c['job']['partition'],
            set(c['suffixes']),
        ) for c in ssync_calls))

    def test_process_job_sync_missing_durable(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [n for n in self.policy.object_ring.devs
                   if n != self.local_dev][:2]
        # setup left and right hashes
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        # left hand side is in sync
        left_index = sync_to[0]['index'] = (frag_index - 1) % replicas
        left_hashes = {
            '123': {left_index: 'hash', None: 'hash'},
            'abc': {left_index: 'hash', None: 'hash'},
        }
        # right hand side has fragment, but no durable (None key is whack)
        right_index = sync_to[1]['index'] = (frag_index + 1) % replicas
        right_hashes = {
            '123': {right_index: 'hash', None: 'hash'},
            'abc': {right_index: 'hash', None: 'different-because-durable'},
        }

        partition = 0
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.SYNC,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        responses = [(200, pickle.dumps(hashes)) for hashes in (
            left_hashes, right_hashes, right_hashes)]
        codes, body_iter = zip(*responses)

        ssync_calls = []
        with mock_ssync_sender(ssync_calls), \
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)), \
                mocked_http_conn(*codes, body_iter=body_iter) as request_log:
            self.reconstructor.process_job(job)

        expected_suffix_calls = set([
            ('10.0.0.1', '/sdb/0'),
            ('10.0.0.2', '/sdc/0'),
            ('10.0.0.2', '/sdc/0/abc'),
        ])
        self.assertEqual(expected_suffix_calls,
                         set((r['ip'], r['path'])
                             for r in request_log.requests))

        expected_ssync_calls = sorted([
            ('10.0.0.2', 0, ['abc']),
        ])
        self.assertEqual(expected_ssync_calls, sorted((
            c['node']['ip'],
            c['job']['partition'],
            c['suffixes'],
        ) for c in ssync_calls))

    def test_process_job_primary_some_in_sync(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [n for n in self.policy.object_ring.devs
                   if n != self.local_dev][:2]
        # setup left and right hashes
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        left_index = sync_to[0]['index'] = (frag_index - 1) % replicas
        left_hashes = {
            '123': {left_index: 'hashX', None: 'hash'},
            'abc': {left_index: 'hash', None: 'hash'},
        }
        right_index = sync_to[1]['index'] = (frag_index + 1) % replicas
        right_hashes = {
            '123': {right_index: 'hash', None: 'hash'},
        }
        partition = 0
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.SYNC,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        responses = [(200, pickle.dumps(hashes)) for hashes in (
            left_hashes, left_hashes, right_hashes, right_hashes)]
        codes, body_iter = zip(*responses)

        ssync_calls = []

        with mock_ssync_sender(ssync_calls), \
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)), \
                mocked_http_conn(*codes, body_iter=body_iter) as request_log:
            self.reconstructor.process_job(job)

        expected_suffix_calls = set([
            ('10.0.0.1', '/sdb/0'),
            ('10.0.0.1', '/sdb/0/123'),
            ('10.0.0.2', '/sdc/0'),
            ('10.0.0.2', '/sdc/0/abc'),
        ])
        self.assertEqual(expected_suffix_calls,
                         set((r['ip'], r['path'])
                             for r in request_log.requests))

        self.assertEqual(
            dict(collections.Counter(
                (c['node']['index'], tuple(c['suffixes']))
                for c in ssync_calls)),
            {(left_index, ('123', )): 1,
             (right_index, ('abc', )): 1})

    def test_process_job_primary_down(self):
        partition = 0
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }

        part_nodes = self.policy.object_ring.get_part_nodes(partition)
        sync_to = part_nodes[:2]

        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.SYNC,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'device': self.local_dev['device'],
            'local_dev': self.local_dev,
        }

        non_local = {'called': 0}

        def ssync_response_callback(*args):
            # in this test, ssync fails on the first (primary sync_to) node
            if non_local['called'] >= 1:
                return True, {}
            non_local['called'] += 1
            return False, {}

        expected_suffix_calls = set()
        for node in part_nodes[:3]:
            expected_suffix_calls.update([
                (node['replication_ip'], '/%s/0' % node['device']),
                (node['replication_ip'], '/%s/0/123-abc' % node['device']),
            ])

        ssync_calls = []
        with mock_ssync_sender(ssync_calls,
                               response_callback=ssync_response_callback), \
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)), \
                mocked_http_conn(*[200] * len(expected_suffix_calls),
                                 body=pickle.dumps({})) as request_log:
            self.reconstructor.process_job(job)

        found_suffix_calls = set((r['ip'], r['path'])
                                 for r in request_log.requests)
        self.assertEqual(expected_suffix_calls, found_suffix_calls)

        expected_ssync_calls = sorted([
            ('10.0.0.0', 0, set(['123', 'abc'])),
            ('10.0.0.1', 0, set(['123', 'abc'])),
            ('10.0.0.2', 0, set(['123', 'abc'])),
        ])
        found_ssync_calls = sorted((
            c['node']['ip'],
            c['job']['partition'],
            set(c['suffixes']),
        ) for c in ssync_calls)
        self.assertEqual(expected_ssync_calls, found_ssync_calls)

    def test_process_job_suffix_call_errors(self):
        partition = 0
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }

        part_nodes = self.policy.object_ring.get_part_nodes(partition)
        sync_to = part_nodes[:2]

        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.SYNC,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'device': self.local_dev['device'],
            'local_dev': self.local_dev,
        }

        expected_suffix_calls = set((
            node['replication_ip'], '/%s/0' % node['device']
        ) for node in part_nodes)

        possible_errors = [404, 507, Timeout(), Exception('kaboom!')]
        codes = [random.choice(possible_errors)
                 for r in expected_suffix_calls]

        ssync_calls = []
        with mock_ssync_sender(ssync_calls), \
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)), \
                mocked_http_conn(*codes) as request_log:
            self.reconstructor.process_job(job)

        found_suffix_calls = set((r['ip'], r['path'])
                                 for r in request_log.requests)
        self.assertEqual(expected_suffix_calls, found_suffix_calls)

        self.assertFalse(ssync_calls)

    def test_process_job_handoff(self):
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [random.choice([n for n in self.policy.object_ring.devs
                                  if n != self.local_dev])]
        sync_to[0]['index'] = frag_index

        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        partition = 0
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.REVERT,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        ssync_calls = []
        with mock_ssync_sender(ssync_calls), \
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)), \
                mocked_http_conn(200, body=pickle.dumps({})) as request_log:
            self.reconstructor.process_job(job)

        expected_suffix_calls = set([
            (sync_to[0]['ip'], '/%s/0/123-abc' % sync_to[0]['device']),
        ])
        found_suffix_calls = set((r['ip'], r['path'])
                                 for r in request_log.requests)
        self.assertEqual(expected_suffix_calls, found_suffix_calls)

        self.assertEqual(
            sorted(collections.Counter(
                (c['node']['ip'], c['node']['port'], c['node']['device'],
                 tuple(sorted(c['suffixes'])))
                for c in ssync_calls).items()),
            [((sync_to[0]['ip'], sync_to[0]['port'], sync_to[0]['device'],
               ('123', 'abc')), 1)])

    def test_process_job_will_not_revert_to_handoff(self):
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [random.choice([n for n in self.policy.object_ring.devs
                                  if n != self.local_dev])]
        sync_to[0]['index'] = frag_index
        partition = 0

        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.REVERT,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        non_local = {'called': 0}

        def ssync_response_callback(*args):
            # in this test, ssync fails on the first (primary sync_to) node
            if non_local['called'] >= 1:
                return True, {}
            non_local['called'] += 1
            return False, {}

        ssync_calls = []
        with mock_ssync_sender(ssync_calls,
                               response_callback=ssync_response_callback), \
                mocked_http_conn() as request_log:
            self.reconstructor.process_job(job)

        # failed ssync job should not generate a suffix rehash
        self.assertEqual([], request_log.requests)

        self.assertEqual(
            sorted(collections.Counter(
                (c['node']['ip'], c['node']['port'], c['node']['device'],
                 tuple(sorted(c['suffixes'])))
                for c in ssync_calls).items()),
            [((sync_to[0]['ip'], sync_to[0]['port'], sync_to[0]['device'],
               ('123', 'abc')), 1)])

    def test_process_job_revert_is_handoff_fails(self):
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [random.choice([n for n in self.policy.object_ring.devs
                                  if n != self.local_dev])]
        sync_to[0]['index'] = frag_index
        partition = 0
        handoff_nodes = list(self.policy.object_ring.get_more_nodes(partition))

        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        job = {
            'job_type': object_reconstructor.REVERT,
            'frag_index': frag_index,
            'suffixes': stub_hashes.keys(),
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': stub_hashes,
            'policy': self.policy,
            'local_dev': handoff_nodes[-1],
        }

        def ssync_response_callback(*args):
            # in this test ssync always fails, until we encounter ourselves in
            # the list of possible handoff's to sync to, so handoffs_remaining
            # should increment
            return False, {}

        ssync_calls = []
        with mock_ssync_sender(ssync_calls,
                               response_callback=ssync_response_callback), \
                mocked_http_conn() as request_log:
            self.reconstructor.process_job(job)

        # failed ssync job should not generate a suffix rehash
        self.assertEqual([], request_log.requests)

        # this is ssync call to primary (which fails) and nothing else!
        self.assertEqual(
            sorted(collections.Counter(
                (c['node']['ip'], c['node']['port'], c['node']['device'],
                 tuple(sorted(c['suffixes'])))
                for c in ssync_calls).items()),
            [((sync_to[0]['ip'], sync_to[0]['port'], sync_to[0]['device'],
               ('123', 'abc')), 1)])
        self.assertEqual(self.reconstructor.handoffs_remaining, 1)

    def test_process_job_revert_cleanup(self):
        frag_index = random.randint(
            0, self.policy.ec_n_unique_fragments - 1)
        sync_to = [random.choice([n for n in self.policy.object_ring.devs
                                  if n != self.local_dev])]
        sync_to[0]['index'] = frag_index
        partition = 0

        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        os.makedirs(part_path)
        df_mgr = self.reconstructor._df_router[self.policy]
        df = df_mgr.get_diskfile(self.local_dev['device'], partition, 'a',
                                 'c', 'data-obj', policy=self.policy)
        ts = self.ts()
        with df.create() as writer:
            test_data = 'test data'
            writer.write(test_data)
            metadata = {
                'X-Timestamp': ts.internal,
                'Content-Length': len(test_data),
                'Etag': md5(test_data).hexdigest(),
                'X-Object-Sysmeta-Ec-Frag-Index': frag_index,
            }
            writer.put(metadata)
            writer.commit(ts)

        ohash = os.path.basename(df._datadir)
        suffix = os.path.basename(os.path.dirname(df._datadir))

        job = {
            'job_type': object_reconstructor.REVERT,
            'frag_index': frag_index,
            'suffixes': [suffix],
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': {},
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        def ssync_response_callback(*args):
            # success should not increment handoffs_remaining
            return True, {ohash: {'ts_data': ts}}

        ssync_calls = []
        with mock_ssync_sender(ssync_calls,
                               response_callback=ssync_response_callback):
            with mocked_http_conn(200, body=pickle.dumps({})) as request_log:
                self.reconstructor.process_job(job)

        self.assertEqual([
            (sync_to[0]['replication_ip'], '/%s/0/%s' % (
                sync_to[0]['device'], suffix)),
        ], [
            (r['ip'], r['path']) for r in request_log.requests
        ])
        # hashpath is still there, but all files have been purged
        files = os.listdir(df._datadir)
        self.assertFalse(files)

        # and more to the point, the next suffix recalc will clean it up
        df_mgr = self.reconstructor._df_router[self.policy]
        df_mgr.get_hashes(self.local_dev['device'], str(partition), [],
                          self.policy)
        self.assertFalse(os.access(df._datadir, os.F_OK))
        self.assertEqual(self.reconstructor.handoffs_remaining, 0)

    def test_process_job_revert_cleanup_tombstone(self):
        sync_to = [random.choice([n for n in self.policy.object_ring.devs
                                  if n != self.local_dev])]
        partition = 0

        part_path = os.path.join(self.devices, self.local_dev['device'],
                                 diskfile.get_data_dir(self.policy),
                                 str(partition))
        os.makedirs(part_path)
        df_mgr = self.reconstructor._df_router[self.policy]
        df = df_mgr.get_diskfile(self.local_dev['device'], partition, 'a',
                                 'c', 'data-obj', policy=self.policy)
        ts = self.ts()
        df.delete(ts)

        ohash = os.path.basename(df._datadir)
        suffix = os.path.basename(os.path.dirname(df._datadir))

        job = {
            'job_type': object_reconstructor.REVERT,
            'frag_index': None,
            'suffixes': [suffix],
            'sync_to': sync_to,
            'partition': partition,
            'path': part_path,
            'hashes': {},
            'policy': self.policy,
            'local_dev': self.local_dev,
        }

        def ssync_response_callback(*args):
            return True, {ohash: {'ts_data': ts}}

        ssync_calls = []
        with mock_ssync_sender(ssync_calls,
                               response_callback=ssync_response_callback):
            with mocked_http_conn(200, body=pickle.dumps({})) as request_log:
                self.reconstructor.process_job(job)

        self.assertEqual([
            (sync_to[0]['replication_ip'], '/%s/0/%s' % (
                sync_to[0]['device'], suffix)),
        ], [
            (r['ip'], r['path']) for r in request_log.requests
        ])
        # hashpath is still there, but it's empty
        self.assertEqual([], os.listdir(df._datadir))

    def test_get_local_devices(self):
        local_devs = self.reconstructor.get_local_devices()
        self.assertEqual({'sda'}, local_devs)

    @patch_policies(legacy_only=True)
    def test_get_local_devices_with_no_ec_policy_env(self):
        # even no ec_policy found on the server, it runs just like as
        # no ec device found
        self._configure_reconstructor()
        self.assertEqual([], self.reconstructor.policies)
        local_devs = self.reconstructor.get_local_devices()
        self.assertEqual(set(), local_devs)

    @patch_policies(legacy_only=True)
    def test_reconstruct_with_no_ec_policy_env(self):
        self._configure_reconstructor()
        self.assertEqual([], self.reconstructor.policies)
        collect_parts_results = []
        _orig_collect_parts = self.reconstructor.collect_parts

        def capture_collect_parts(**kwargs):
            part_infos = _orig_collect_parts(**kwargs)
            collect_parts_results.append(part_infos)
            return part_infos

        with mock.patch.object(self.reconstructor, 'collect_parts',
                               capture_collect_parts):
            self.reconstructor.reconstruct()

        # There is one call, and it returns an empty list
        self.assertEqual([[]], collect_parts_results)
        log_lines = self.logger.all_log_lines()
        self.assertEqual(log_lines, {'info': [mock.ANY]})
        line = log_lines['info'][0]
        self.assertTrue(line.startswith('Nothing reconstructed '), line)


class TestReconstructFragmentArchive(BaseTestObjectReconstructor):
    obj_path = '/a/c/o'  # subclass overrides this

    def setUp(self):
        super(TestReconstructFragmentArchive, self).setUp()
        self.obj_timestamp = self.ts()
        self.obj_metadata = {
            'name': self.obj_path,
            'Content-Length': '0',
            'ETag': 'etag',
            'X-Timestamp': self.obj_timestamp.normal
        }

    def test_reconstruct_fa_no_errors(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)
        broken_body = ec_archive_bodies.pop(1)

        responses = list()
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            responses.append((200, body, headers))

        # make a hook point at
        # swift.obj.reconstructor.ObjectReconstructor._get_response
        called_headers = []
        orig_func = object_reconstructor.ObjectReconstructor._get_response

        def _get_response_hook(self, node, part, path, headers, policy):
            called_headers.append(headers)
            return orig_func(self, node, part, path, headers, policy)

        codes, body_iter, headers = zip(*responses)
        get_response_path = \
            'swift.obj.reconstructor.ObjectReconstructor._get_response'
        with mock.patch(get_response_path, _get_response_hook):
            with mocked_http_conn(
                    *codes, body_iter=body_iter, headers=headers):
                df = self.reconstructor.reconstruct_fa(
                    job, node, self.obj_metadata)
                self.assertEqual(0, df.content_length)
                fixed_body = ''.join(df.reader())
        self.assertEqual(len(fixed_body), len(broken_body))
        self.assertEqual(md5(fixed_body).hexdigest(),
                         md5(broken_body).hexdigest())
        self.assertEqual(len(part_nodes) - 1, len(called_headers),
                         'Expected %d calls, got %r' % (len(part_nodes) - 1,
                                                        called_headers))
        for called_header in called_headers:
            called_header = HeaderKeyDict(called_header)
            self.assertIn('Content-Length', called_header)
            self.assertEqual(called_header['Content-Length'], '0')
            self.assertIn('User-Agent', called_header)
            user_agent = called_header['User-Agent']
            self.assertTrue(user_agent.startswith('obj-reconstructor'))
            self.assertIn('X-Backend-Storage-Policy-Index', called_header)
            self.assertEqual(called_header['X-Backend-Storage-Policy-Index'],
                             self.policy)
            self.assertIn('X-Backend-Fragment-Preferences', called_header)
            self.assertEqual(
                [{'timestamp': self.obj_timestamp.normal, 'exclude': []}],
                json.loads(called_header['X-Backend-Fragment-Preferences']))
            self.assertIn('X-Backend-Replication', called_header)
        # no error and warning
        self.assertFalse(self.logger.get_lines_for_level('error'))
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_reconstruct_fa_errors_works(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[4]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(4)

        base_responses = list()
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            base_responses.append((200, body, headers))

        # since we're already missing a fragment a +2 scheme can only support
        # one additional failure at a time
        for error in (Timeout(), 404, Exception('kaboom!')):
            responses = base_responses
            error_index = random.randint(0, len(responses) - 1)
            responses[error_index] = (error, '', '')
            codes, body_iter, headers_iter = zip(*responses)
            with mocked_http_conn(*codes, body_iter=body_iter,
                                  headers=headers_iter):
                df = self.reconstructor.reconstruct_fa(
                    job, node, dict(self.obj_metadata))
                fixed_body = ''.join(df.reader())
                self.assertEqual(len(fixed_body), len(broken_body))
                self.assertEqual(md5(fixed_body).hexdigest(),
                                 md5(broken_body).hexdigest())

    def test_reconstruct_fa_error_with_invalid_header(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[4]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(4)

        base_responses = list()
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            base_responses.append((200, body, headers))

        responses = base_responses
        # force the test to exercise the handling of this bad response by
        # sticking it in near the front
        error_index = random.randint(0, self.policy.ec_ndata - 1)
        status, body, headers = responses[error_index]
        # one esoteric failure is a literal string 'None' in place of the
        # X-Object-Sysmeta-EC-Frag-Index
        stub_node_job = {'some_keys': 'foo', 'but_not': 'frag_index'}
        headers['X-Object-Sysmeta-Ec-Frag-Index'] = str(
            stub_node_job.get('frag_index'))
        # oops!
        self.assertEqual('None',
                         headers.get('X-Object-Sysmeta-Ec-Frag-Index'))
        responses[error_index] = status, body, headers
        codes, body_iter, headers_iter = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter,
                              headers=headers_iter):
            df = self.reconstructor.reconstruct_fa(
                job, node, dict(self.obj_metadata))
            fixed_body = ''.join(df.reader())
            # ... this bad response should be ignored like any other failure
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

    def test_reconstruct_parity_fa_with_data_node_failure(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[-4]

        # make up some data (trim some amount to make it unaligned with
        # segment size)
        test_data = ('rebuild' * self.policy.ec_segment_size)[:-454]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)
        # the scheme is 10+4, so this gets a parity node
        broken_body = ec_archive_bodies.pop(-4)

        responses = list()
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            responses.append((200, body, headers))

        for error in (Timeout(), 404, Exception('kaboom!')):
            # grab a data node index
            error_index = random.randint(0, self.policy.ec_ndata - 1)
            responses[error_index] = (error, '', '')
            codes, body_iter, headers_iter = zip(*responses)
            with mocked_http_conn(*codes, body_iter=body_iter,
                                  headers=headers_iter):
                df = self.reconstructor.reconstruct_fa(
                    job, node, dict(self.obj_metadata))
                fixed_body = ''.join(df.reader())
                self.assertEqual(len(fixed_body), len(broken_body))
                self.assertEqual(md5(fixed_body).hexdigest(),
                                 md5(broken_body).hexdigest())

    def test_reconstruct_fa_exceptions_fails(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        policy = self.policy

        possible_errors = [Timeout(), Exception('kaboom!')]
        codes = [random.choice(possible_errors) for i in
                 range(policy.object_ring.replicas - 1)]
        with mocked_http_conn(*codes):
            self.assertRaises(DiskFileError, self.reconstructor.reconstruct_fa,
                              job, node, self.obj_metadata)
        error_lines = self.logger.get_lines_for_level('error')
        # # of replicas failed and one more error log to report not enough
        # responses to reconstruct.
        self.assertEqual(policy.object_ring.replicas, len(error_lines))
        for line in error_lines[:-1]:
            self.assertIn("Trying to GET", line)
        self.assertIn(
            'Unable to get enough responses (%s error responses)'
            % (policy.object_ring.replicas - 1),
            error_lines[-1],
            "Unexpected error line found: %s" % error_lines[-1])
        # no warning
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_reconstruct_fa_all_404s_fails(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        policy = self.policy

        codes = [404 for i in range(policy.object_ring.replicas - 1)]
        with mocked_http_conn(*codes):
            self.assertRaises(DiskFileError, self.reconstructor.reconstruct_fa,
                              job, node, self.obj_metadata)
        error_lines = self.logger.get_lines_for_level('error')
        # only 1 log to report not enough responses
        self.assertEqual(1, len(error_lines))
        self.assertIn(
            'Unable to get enough responses (%s error responses)'
            % (policy.object_ring.replicas - 1),
            error_lines[0],
            "Unexpected error line found: %s" % error_lines[0])
        # no warning
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_reconstruct_fa_with_mixed_old_etag(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        # bad response
        broken_body = ec_archive_bodies.pop(1)
        ts = make_timestamp_iter()
        bad_headers = get_header_frag_index(self, broken_body)
        bad_headers.update({
            'X-Object-Sysmeta-Ec-Etag': 'some garbage',
            'X-Backend-Timestamp': next(ts).internal,
        })

        # good responses
        responses = list()
        t1 = next(ts).internal
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag,
                            'X-Backend-Timestamp': t1})
            responses.append((200, body, headers))

        # include the one older frag with different etag in first responses
        error_index = random.randint(0, self.policy.ec_ndata - 1)
        error_headers = get_header_frag_index(self,
                                              (responses[error_index])[1])
        error_headers.update(bad_headers)
        bad_response = (200, '', bad_headers)
        responses[error_index] = bad_response
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, self.obj_metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # no error and warning
        self.assertFalse(self.logger.get_lines_for_level('error'))
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_reconstruct_fa_with_mixed_new_etag(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)
        ts = make_timestamp_iter()

        # good responses
        responses = list()
        t0 = next(ts).internal
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag,
                            'X-Backend-Timestamp': t0})
            responses.append((200, body, headers))

        # sanity check before negative test
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, dict(self.obj_metadata))
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # one newer etag won't spoil the bunch
        new_index = random.randint(0, self.policy.ec_ndata - 1)
        new_headers = get_header_frag_index(self, (responses[new_index])[1])
        new_headers.update({'X-Object-Sysmeta-Ec-Etag': 'some garbage',
                            'X-Backend-Timestamp': next(ts).internal})
        new_response = (200, '', new_headers)
        responses[new_index] = new_response
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, dict(self.obj_metadata))
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # no error and warning
        self.assertFalse(self.logger.get_lines_for_level('error'))
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_reconstruct_fa_with_mixed_etag_with_same_timestamp(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)

        # good responses
        responses = list()
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            responses.append((200, body, headers))

        # sanity check before negative test
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, dict(self.obj_metadata))
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # a response at same timestamp but different etag won't spoil the bunch
        # N.B. (FIXME). if we choose the first response as garbage, the
        # reconstruction fails because all other *correct* frags will be
        # assumed as garbage. To avoid the freaky failing set randint
        # as [1, self.policy.ec_ndata - 1] to make the first response
        # always have the correct etag to reconstruct
        new_index = random.randint(1, self.policy.ec_ndata - 1)
        new_headers = get_header_frag_index(self, (responses[new_index])[1])
        new_headers.update({'X-Object-Sysmeta-Ec-Etag': 'some garbage'})
        new_response = (200, '', new_headers)
        responses[new_index] = new_response
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, dict(self.obj_metadata))
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # expect an error log but no warnings
        error_log_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_log_lines))
        self.assertIn(
            'Mixed Etag (some garbage, %s) for 10.0.0.1:1001/sdb/0%s '
            'policy#%s frag#1' %
            (etag, self.obj_path.decode('utf8'), int(self.policy)),
            error_log_lines[0])
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_reconstruct_fa_with_mixed_not_enough_etags_fail(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        ec_archive_dict = dict()
        ts = make_timestamp_iter()
        # create 3 different ec bodies
        for i in range(3):
            body = test_data[i:]
            archive_bodies = encode_frag_archive_bodies(self.policy, body)
            # pop the index to the destination node
            archive_bodies.pop(1)
            ec_archive_dict[
                (md5(body).hexdigest(), next(ts).internal)] = archive_bodies

        responses = list()
        # fill out response list by 3 different etag bodies
        for etag, ts in itertools.cycle(ec_archive_dict):
            body = ec_archive_dict[(etag, ts)].pop(0)
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag,
                            'X-Backend-Timestamp': ts})
            responses.append((200, body, headers))
            if len(responses) >= (self.policy.object_ring.replicas - 1):
                break

        # sanity, there is 3 different etag and each etag
        # doesn't have > ec_k bodies
        etag_count = collections.Counter(
            [in_resp_headers['X-Object-Sysmeta-Ec-Etag']
             for _, _, in_resp_headers in responses])
        self.assertEqual(3, len(etag_count))
        for etag, count in etag_count.items():
            self.assertLess(count, self.policy.ec_ndata)

        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            self.assertRaises(DiskFileError, self.reconstructor.reconstruct_fa,
                              job, node, self.obj_metadata)

        error_lines = self.logger.get_lines_for_level('error')
        # 1 error log per etag to report not enough responses
        self.assertEqual(3, len(error_lines))
        for error_line in error_lines:
            for expected_etag, ts in ec_archive_dict:
                if expected_etag in error_line:
                    break
            else:
                self.fail(
                    "no expected etag %s found: %s" %
                    (list(ec_archive_dict), error_line))
            # remove the found etag which should not be found in the
            # following error lines
            del ec_archive_dict[(expected_etag, ts)]

            expected = 'Unable to get enough responses (%s/10) to ' \
                       'reconstruct 10.0.0.1:1001/sdb/0%s policy#0 ' \
                       'frag#1 with ETag' % \
                       (etag_count[expected_etag],
                        self.obj_path.decode('utf8'))
            self.assertIn(
                expected, error_line,
                "Unexpected error line found: Expected: %s Got: %s"
                % (expected, error_line))
        # no warning
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_reconstruct_fa_finds_itself_does_not_fail(self):
        # verify that reconstruction of a missing frag can cope with finding
        # that missing frag in the responses it gets from other nodes while
        # attempting to rebuild the missing frag
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        broken_node = random.randint(0, self.policy.ec_ndata - 1)

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        # instead of popping the broken body, we'll just leave it in the list
        # of responses and take away something else.
        broken_body = ec_archive_bodies[broken_node]
        ec_archive_bodies = ec_archive_bodies[:-1]

        def make_header(body):
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            return headers

        responses = [(200, body, make_header(body))
                     for body in ec_archive_bodies]
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, part_nodes[broken_node], self.obj_metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # no error, no warning
        self.assertFalse(self.logger.get_lines_for_level('error'))
        self.assertFalse(self.logger.get_lines_for_level('warning'))
        # the found own frag will be reported in the debug message
        debug_log_lines = self.logger.get_lines_for_level('debug')
        # redundant frag found once in first ec_ndata responses
        self.assertIn(
            'Found existing frag #%s at' % broken_node,
            debug_log_lines[0])

        # N.B. in the future, we could avoid those check because
        # definitely sending the copy rather than reconstruct will
        # save resources. But one more reason, we're avoiding to
        # use the dest index fragment even if it goes to reconstruct
        # function is that it will cause a bunch of warning log from
        # liberasurecode[1].
        # 1: https://github.com/openstack/liberasurecode/blob/
        #    master/src/erasurecode.c#L870
        log_prefix = 'Reconstruct frag #%s with frag indexes' % broken_node
        self.assertIn(log_prefix, debug_log_lines[1])
        self.assertFalse(debug_log_lines[2:])
        got_frag_index_list = json.loads(
            debug_log_lines[1][len(log_prefix):])
        self.assertNotIn(broken_node, got_frag_index_list)

    def test_reconstruct_fa_finds_duplicate_does_not_fail(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)
        # add some duplicates
        num_duplicates = self.policy.ec_nparity - 1
        ec_archive_bodies = (ec_archive_bodies[:num_duplicates] +
                             ec_archive_bodies)[:-num_duplicates]

        def make_header(body):
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            return headers

        responses = [(200, body, make_header(body))
                     for body in ec_archive_bodies]
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, self.obj_metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # no error and warning
        self.assertFalse(self.logger.get_lines_for_level('error'))
        self.assertFalse(self.logger.get_lines_for_level('warning'))
        debug_log_lines = self.logger.get_lines_for_level('debug')
        self.assertEqual(1, len(debug_log_lines))
        expected_prefix = 'Reconstruct frag #1 with frag indexes'
        self.assertIn(expected_prefix, debug_log_lines[0])
        got_frag_index_list = json.loads(
            debug_log_lines[0][len(expected_prefix):])
        self.assertNotIn(1, got_frag_index_list)

    def test_reconstruct_fa_missing_headers(self):
        # This is much negative tests asserting when the expected
        # headers are missing in the responses to gather fragments
        # to reconstruct

        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)

        def make_header(body):
            headers = get_header_frag_index(self, body)
            headers.update(
                {'X-Object-Sysmeta-Ec-Etag': etag,
                 'X-Backend-Timestamp': self.obj_timestamp.internal})
            return headers

        def test_missing_header(missing_header, warning_extra):
            self.logger._clear()
            responses = [(200, body, make_header(body))
                         for body in ec_archive_bodies]

            # To drop the header from the response[0], set None as the value
            # explicitly instead of deleting the key because if no key exists
            # in the dict, fake_http_connect will insert some key/value pairs
            # automatically (e.g. X-Backend-Timestamp)
            responses[0][2].update({missing_header: None})

            codes, body_iter, headers = zip(*responses)
            with mocked_http_conn(
                    *codes, body_iter=body_iter, headers=headers) as mock_conn:
                df = self.reconstructor.reconstruct_fa(
                    job, node, self.obj_metadata)
                fixed_body = ''.join(df.reader())
                self.assertEqual(len(fixed_body), len(broken_body))
                self.assertEqual(md5(fixed_body).hexdigest(),
                                 md5(broken_body).hexdigest())

            # no errors
            self.assertFalse(self.logger.get_lines_for_level('error'))
            # ...but warning for the missing header
            warning_log_lines = self.logger.get_lines_for_level('warning')
            self.assertEqual(1, len(warning_log_lines))

            path = unquote(
                '%(ip)s:%(port)d%(path)s' % mock_conn.requests[0]
            ).encode('latin1').decode('utf8')
            expected_warning = 'Invalid resp from %s policy#0%s' % (
                path, warning_extra)
            self.assertIn(expected_warning, warning_log_lines)

        test_missing_header(
            'X-Object-Sysmeta-Ec-Frag-Index',
            ' (invalid X-Object-Sysmeta-Ec-Frag-Index: None)')
        test_missing_header(
            'X-Object-Sysmeta-Ec-Etag',
            ', frag index 0 (missing Etag)')
        test_missing_header(
            'X-Backend-Timestamp',
            ', frag index 0 (missing X-Backend-Timestamp)')

    def test_reconstruct_fa_invalid_frag_index_headers(self):
        # This is much negative tests asserting when the expected
        # ec frag index header has invalid value in the responses
        # to gather fragments to reconstruct

        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)

        def make_header(body):
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            return headers

        def test_invalid_ec_frag_index_header(invalid_frag_index):
            self.logger._clear()
            responses = [(200, body, make_header(body))
                         for body in ec_archive_bodies]

            responses[0][2].update({
                'X-Object-Sysmeta-Ec-Frag-Index': invalid_frag_index})

            codes, body_iter, headers = zip(*responses)
            with mocked_http_conn(
                    *codes, body_iter=body_iter, headers=headers) as mock_conn:
                df = self.reconstructor.reconstruct_fa(
                    job, node, self.obj_metadata)
                fixed_body = ''.join(df.reader())
                self.assertEqual(len(fixed_body), len(broken_body))
                self.assertEqual(md5(fixed_body).hexdigest(),
                                 md5(broken_body).hexdigest())

            # no errors
            self.assertFalse(self.logger.get_lines_for_level('error'))
            # ...but warning for the invalid header
            warning_log_lines = self.logger.get_lines_for_level('warning')
            self.assertEqual(1, len(warning_log_lines))

            path = unquote(
                '%(ip)s:%(port)d%(path)s' % mock_conn.requests[0]
            ).encode('latin1').decode('utf8')
            expected_warning = (
                'Invalid resp from %s policy#0 '
                '(invalid X-Object-Sysmeta-Ec-Frag-Index: %r)'
                % (path, invalid_frag_index))
            self.assertIn(expected_warning, warning_log_lines)

        for value in ('None', 'invalid'):
            test_invalid_ec_frag_index_header(value)


@patch_policies(with_ec_default=True)
class TestReconstructFragmentArchiveUTF8(TestReconstructFragmentArchive):
    # repeat superclass tests with an object path that contains non-ascii chars
    obj_path = '/a/c/o\xc3\xa8'


@patch_policies([ECStoragePolicy(0, name='ec', is_default=True,
                                 ec_type=DEFAULT_TEST_EC_TYPE,
                                 ec_ndata=10, ec_nparity=4,
                                 ec_segment_size=4096,
                                 ec_duplication_factor=2)],
                fake_ring_args=[{'replicas': 28}])
class TestObjectReconstructorECDuplicationFactor(TestObjectReconstructor):
    def setUp(self):
        super(TestObjectReconstructorECDuplicationFactor, self).setUp()
        self.fabricated_ring = FabricatedRing(replicas=28, devices=56)

    def _test_reconstruct_with_duplicate_frags_no_errors(self, index):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[index]
        metadata = {
            'name': '/a/c/o',
            'Content-Length': 0,
            'ETag': 'etag',
            'X-Timestamp': '1234567890.12345',
        }

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = encode_frag_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(index)

        responses = list()
        for body in ec_archive_bodies:
            headers = get_header_frag_index(self, body)
            headers.update({'X-Object-Sysmeta-Ec-Etag': etag})
            responses.append((200, body, headers))

        # make a hook point at
        # swift.obj.reconstructor.ObjectReconstructor._get_response
        called_headers = []
        orig_func = object_reconstructor.ObjectReconstructor._get_response

        def _get_response_hook(self, node, part, path, headers, policy):
            called_headers.append(headers)
            return orig_func(self, node, part, path, headers, policy)

        # need parity + 1 node failures to reach duplicated fragments
        failed_start_at = (
            self.policy.ec_n_unique_fragments - self.policy.ec_nparity - 1)

        # set Timeout for node #9, #10, #11, #12, #13
        for i in range(self.policy.ec_nparity + 1):
            responses[failed_start_at + i] = (Timeout(), '', '')

        codes, body_iter, headers = zip(*responses)
        get_response_path = \
            'swift.obj.reconstructor.ObjectReconstructor._get_response'
        with mock.patch(get_response_path, _get_response_hook):
            with mocked_http_conn(
                    *codes, body_iter=body_iter, headers=headers):
                df = self.reconstructor.reconstruct_fa(
                    job, node, metadata)
                fixed_body = ''.join(df.reader())
                self.assertEqual(len(fixed_body), len(broken_body))
                self.assertEqual(md5(fixed_body).hexdigest(),
                                 md5(broken_body).hexdigest())
                for called_header in called_headers:
                    called_header = HeaderKeyDict(called_header)
                    self.assertIn('Content-Length', called_header)
                    self.assertEqual(called_header['Content-Length'], '0')
                    self.assertIn('User-Agent', called_header)
                    user_agent = called_header['User-Agent']
                    self.assertTrue(user_agent.startswith('obj-reconstructor'))

    def test_reconstruct_with_duplicate_frags_no_errors(self):
        # any fragments can be broken
        for index in range(28):
            self._test_reconstruct_with_duplicate_frags_no_errors(index)


if __name__ == '__main__':
    unittest.main()
