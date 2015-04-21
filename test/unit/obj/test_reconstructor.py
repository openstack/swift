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
import unittest
import os
from hashlib import md5
import mock
import cPickle as pickle
import tempfile
import time
import shutil
import re
import random
from eventlet import Timeout

from contextlib import closing, nested, contextmanager
from gzip import GzipFile
from shutil import rmtree
from swift.common import utils
from swift.common.exceptions import DiskFileError
from swift.obj import diskfile, reconstructor as object_reconstructor
from swift.common import ring
from swift.common.storage_policy import (StoragePolicy, ECStoragePolicy,
                                         POLICIES, EC_POLICY)
from swift.obj.reconstructor import REVERT

from test.unit import (patch_policies, debug_logger, mocked_http_conn,
                       FabricatedRing, make_timestamp_iter)


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
        fragments = policy.pyeclib_driver.encode(chunk)
        if not fragments:
            break
        fragment_payloads.append(fragments)

    # join up the fragment payloads per node
    ec_archive_bodies = [''.join(fragments)
                         for fragments in zip(*fragment_payloads)]
    return ec_archive_bodies


def _ips():
    return ['127.0.0.1']
object_reconstructor.whataremyips = _ips


def _create_test_rings(path):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2],
        [1, 2, 3],
        [2, 3, 0]
    ]

    intended_devs = [
        {'id': 0, 'device': 'sda1', 'zone': 0, 'ip': '127.0.0.0',
         'port': 6000},
        {'id': 1, 'device': 'sda1', 'zone': 1, 'ip': '127.0.0.1',
         'port': 6000},
        {'id': 2, 'device': 'sda1', 'zone': 2, 'ip': '127.0.0.2',
         'port': 6000},
        {'id': 3, 'device': 'sda1', 'zone': 4, 'ip': '127.0.0.3',
         'port': 6000}
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


def count_stats(logger, key, metric):
    count = 0
    for record in logger.log_dict[key]:
        log_args, log_kwargs = record
        m = log_args[0]
        if re.match(metric, m):
            count += 1
    return count


@patch_policies([StoragePolicy(0, name='zero', is_default=True),
                 ECStoragePolicy(1, name='one', ec_type='jerasure_rs_vand',
                                 ec_ndata=2, ec_nparity=1)])
class TestGlobalSetupObjectReconstructor(unittest.TestCase):

    def setUp(self):
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
        # part 0: 3C1/hash/xxx-1.data  <-- job: sync_only - parnters (FI 1)
        #                 /xxx.durable <-- included in earlier job (FI 1)
        #         061/hash/xxx-1.data  <-- included in earlier job (FI 1)
        #                 /xxx.durable <-- included in earlier job (FI 1)
        #                 /xxx-2.data  <-- job: sync_revert to index 2

        # part 1: 3C1/hash/xxx-0.data  <-- job: sync_only - parnters (FI 0)
        #                 /xxx-1.data  <-- job: sync_revert to index 1
        #                 /xxx.durable <-- included in earlier jobs (FI 0, 1)
        #         061/hash/xxx-1.data  <-- included in earlier job (FI 1)
        #                 /xxx.durable <-- included in earlier job (FI 1)

        # part 2: 3C1/hash/xxx-2.data  <-- job: sync_revert to index 2
        #                 /xxx.durable <-- included in earlier job (FI 2)
        #         061/hash/xxx-0.data  <-- job: sync_revert to index 0
        #                 /xxx.durable <-- included in earlier job (FI 0)

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
                    # onde local and all of another
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
                # so lets do a set with indicies from different nodes
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
                # create 3 unique objcets per part, each part
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
        with df.create() as writer:
            timestamp = timestamp or utils.Timestamp(time.time())
            test_data = test_data or 'test data'
            writer.write(test_data)
            metadata = {
                'X-Timestamp': timestamp.internal,
                'Content-Length': len(test_data),
                'Etag': md5(test_data).hexdigest(),
                'X-Object-Sysmeta-Ec-Frag-Index': frag_index,
            }
            writer.put(metadata)
            writer.commit(timestamp)
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
                    'replication_port': 6000,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1', 'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 0,
                    'ip': '127.0.0.0',
                    'region': 1,
                    'port': 6000,
                    'replication_ip': '127.0.0.0',
                    'device': 'sda1', 'id': 0,
                }, {
                    'index': 2,
                    'replication_port': 6000,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 4,
                    'ip': '127.0.0.3',
                    'region': 1,
                    'port': 6000,
                    'replication_ip': '127.0.0.3',
                    'device': 'sda1', 'id': 3,
                }, {
                    'index': 1,
                    'replication_port': 6000,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 2,
                    'ip': '127.0.0.2',
                    'region': 1,
                    'port': 6000,
                    'replication_ip': '127.0.0.2',
                    'device': 'sda1', 'id': 2,
                }],
                'job_type': object_reconstructor.REVERT,
                'suffixes': ['061'],
                'partition': 2,
                'frag_index': 0,
                'device': 'sda1',
                'local_dev': {
                    'replication_port': 6000,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 0,
                    'ip': '127.0.0.0',
                    'region': 1,
                    'port': 6000,
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
                    'replication_port': 6000,
                    'zone': 1,
                    'ip': '127.0.0.1',
                    'region': 1,
                    'id': 1,
                    'replication_ip': '127.0.0.1',
                    'device': 'sda1',
                    'port': 6000
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

    def test_run_once(self):
        with mocked_http_conn(*[200] * 12, body=pickle.dumps({})):
            with mock_ssync_sender():
                self.reconstructor.run_once()

    def test_get_response(self):
        part = self.part_nums[0]
        node = POLICIES[0].object_ring.get_part_nodes(int(part))[0]
        for stat_code in (200, 400):
            with mocked_http_conn(stat_code):
                resp = self.reconstructor._get_response(node, part,
                                                        path='nada',
                                                        headers={},
                                                        policy=POLICIES[0])
                if resp:
                    self.assertEqual(resp.status, 200)
                else:
                    self.assertEqual(
                        len(self.reconstructor.logger.log_dict['warning']), 1)

    def test_reconstructor_skips_bogus_partition_dirs(self):
        # A directory in the wrong place shouldn't crash the reconstructor
        rmtree(self.objects_1)
        os.mkdir(self.objects_1)

        os.mkdir(os.path.join(self.objects_1, "burrito"))
        jobs = []
        for part_info in self.reconstructor.collect_parts():
            jobs += self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(len(jobs), 0)

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

    def test_build_reconstruction_jobs(self):
        self.reconstructor.handoffs_first = False
        self.reconstructor._reset_stats()
        for part_info in self.reconstructor.collect_parts():
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
            self.assertTrue(jobs[0]['job_type'] in
                            (object_reconstructor.SYNC,
                             object_reconstructor.REVERT))
            self.assert_expected_jobs(part_info['partition'], jobs)

        self.reconstructor.handoffs_first = True
        self.reconstructor._reset_stats()
        for part_info in self.reconstructor.collect_parts():
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
            self.assertTrue(jobs[0]['job_type'] ==
                            object_reconstructor.REVERT)
            self.assert_expected_jobs(part_info['partition'], jobs)

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
        parts = []
        for part_info in self.reconstructor.collect_parts():
            parts.append(part_info['partition'])
        self.assertEqual(sorted(parts), [0, 1, 2])

    def test_collect_parts_mkdirs_error(self):

        def blowup_mkdirs(path):
            raise OSError('Ow!')

        with mock.patch.object(object_reconstructor, 'mkdirs', blowup_mkdirs):
            rmtree(self.objects_1, ignore_errors=1)
            parts = []
            for part_info in self.reconstructor.collect_parts():
                parts.append(part_info['partition'])
            error_lines = self.logger.get_lines_for_level('error')
            self.assertEqual(len(error_lines), 1)
            log_args, log_kwargs = self.logger.log_dict['error'][0]
            self.assertEquals(str(log_kwargs['exc_info'][1]), 'Ow!')

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

        # since our collect_parts job is a generator, that yields directly
        # into build_jobs and then spawns it's safe to do the remove_files
        # without making reconstructor startup slow
        for part_info in self.reconstructor.collect_parts():
            self.assertNotEqual(pol_1_part_1_path, part_info['part_path'])
        self.assertFalse(os.path.exists(pol_1_part_1_path))
        warnings = self.reconstructor.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warnings))
        self.assertTrue('Unexpected entity in data dir:' in warnings[0],
                        'Warning not found in %s' % warnings)

    def _make_fake_ssync(self, ssync_calls):
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
                hash_gen = self.daemon._diskfile_mgr.yield_hashes(
                    self.job['device'], self.job['partition'],
                    self.job['policy'], self.suffixes,
                    frag_index=self.job.get('frag_index'))
                self.available_map = {}
                for path, hash_, ts in hash_gen:
                    self.available_map[hash_] = ts
                context['available_map'] = self.available_map
                ssync_calls.append(context)

            def __call__(self, *args, **kwargs):
                return True, self.available_map

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
        self.assertTrue(n_files > n_files_after)

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
        policy = POLICIES[1]
        hash_gen = self.reconstructor._df_router[policy].yield_hashes(
            'sda1', '2', policy)
        for path, hash_, ts in hash_gen:
            self.fail('found %s with %s in %s', (hash_, ts, path))
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
                        self.assertFalse('error' in
                                         self.logger.all_log_lines())
        self.assertEqual(self.reconstructor.suffix_sync, 8)
        self.assertEqual(self.reconstructor.suffix_count, 8)
        self.assertEqual(len(found_jobs), 6)

    def test_process_job_all_insufficient_storage(self):
        self.reconstructor._reset_stats()
        with mock_ssync_sender():
            with mocked_http_conn(*[507] * 10):
                found_jobs = []
                for part_info in self.reconstructor.collect_parts():
                    jobs = self.reconstructor.build_reconstruction_jobs(
                        part_info)
                    found_jobs.extend(jobs)
                    for job in jobs:
                        self.logger._clear()
                        self.reconstructor.process_job(job)
                        for line in self.logger.get_lines_for_level('error'):
                            self.assertTrue('responded as unmounted' in line)
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.hashes'))
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.syncs'))
        self.assertEqual(self.reconstructor.suffix_sync, 0)
        self.assertEqual(self.reconstructor.suffix_count, 0)
        self.assertEqual(len(found_jobs), 6)

    def test_process_job_all_client_error(self):
        self.reconstructor._reset_stats()
        with mock_ssync_sender():
            with mocked_http_conn(*[400] * 10):
                found_jobs = []
                for part_info in self.reconstructor.collect_parts():
                    jobs = self.reconstructor.build_reconstruction_jobs(
                        part_info)
                    found_jobs.extend(jobs)
                    for job in jobs:
                        self.logger._clear()
                        self.reconstructor.process_job(job)
                        for line in self.logger.get_lines_for_level('error'):
                            self.assertTrue('Invalid response 400' in line)
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.hashes'))
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.syncs'))
        self.assertEqual(self.reconstructor.suffix_sync, 0)
        self.assertEqual(self.reconstructor.suffix_count, 0)
        self.assertEqual(len(found_jobs), 6)

    def test_process_job_all_timeout(self):
        self.reconstructor._reset_stats()
        with mock_ssync_sender():
            with nested(mocked_http_conn(*[Timeout()] * 10)):
                found_jobs = []
                for part_info in self.reconstructor.collect_parts():
                    jobs = self.reconstructor.build_reconstruction_jobs(
                        part_info)
                    found_jobs.extend(jobs)
                    for job in jobs:
                        self.logger._clear()
                        self.reconstructor.process_job(job)
                        for line in self.logger.get_lines_for_level('error'):
                            self.assertTrue('Timeout (Nones)' in line)
                        self.assertStatCount(
                            'update_stats', 'suffix.hashes', 0)
                        self.assertStatCount(
                            'update_stats', 'suffix.syncs', 0)
            self.assertEqual(self.reconstructor.suffix_sync, 0)
            self.assertEqual(self.reconstructor.suffix_count, 0)
            self.assertEqual(len(found_jobs), 6)


@patch_policies(with_ec_default=True)
class TestObjectReconstructor(unittest.TestCase):

    def setUp(self):
        self.policy = POLICIES.default
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'devices')
        self.local_dev = self.policy.object_ring.devs[0]
        self.ip = self.local_dev['replication_ip']
        self.port = self.local_dev['replication_port']
        self.conf = {
            'devices': self.devices,
            'mount_check': False,
            'bind_port': self.port,
        }
        self.logger = debug_logger('object-reconstructor')
        self.reconstructor = object_reconstructor.ObjectReconstructor(
            self.conf, logger=self.logger)
        self.reconstructor._reset_stats()
        # some tests bypass build_reconstruction_jobs and go to process_job
        # directly, so you end up with a /0 when you try to show the
        # percentage of complete jobs as ratio of the total job count
        self.reconstructor.job_count = 1
        self.policy.object_ring.max_more_nodes = \
            self.policy.object_ring.replicas
        self.ts_iter = make_timestamp_iter()

    def tearDown(self):
        self.reconstructor.stats_line()
        shutil.rmtree(self.testdir)

    def ts(self):
        return next(self.ts_iter)

    def test_collect_parts_skips_non_ec_policy_and_device(self):
        stub_parts = (371, 78, 419, 834)
        for policy in POLICIES:
            datadir = diskfile.get_data_dir(policy)
            for part in stub_parts:
                utils.mkdirs(os.path.join(
                    self.devices, self.local_dev['device'],
                    datadir, str(part)))
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]):
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

    def test_collect_parts_multi_device_skips_non_ring_devices(self):
        device_parts = {
            'sda': (374,),
            'sdb': (179, 807),
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
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port
        } for dev in local_devs]
        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch.object(self.policy.object_ring, '_devs',
                                      new=stub_ring_devs)):
            part_infos = list(self.reconstructor.collect_parts())
        found_parts = sorted(int(p['partition']) for p in part_infos)
        expected_parts = sorted(itertools.chain(
            *(device_parts[d] for d in local_devs)))
        self.assertEqual(found_parts, expected_parts)
        for part_info in part_infos:
            self.assertEqual(part_info['policy'], self.policy)
            self.assertTrue(part_info['local_dev'] in stub_ring_devs)
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
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port
        } for dev in local_devs]
        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch.object(self.policy.object_ring, '_devs',
                                      new=stub_ring_devs)):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual(2, len(part_infos))  # sanity
        self.assertEqual(set(int(p['partition']) for p in part_infos),
                         set([0, 1]))

        paths = []

        def fake_ismount(path):
            paths.append(path)
            return False

        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch.object(self.policy.object_ring, '_devs',
                                      new=stub_ring_devs),
                    mock.patch('swift.obj.reconstructor.ismount',
                               fake_ismount)):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual(2, len(part_infos))  # sanity, same jobs
        self.assertEqual(set(int(p['partition']) for p in part_infos),
                         set([0, 1]))

        # ... because ismount was not called
        self.assertEqual(paths, [])

        # ... now with mount check
        self.reconstructor.mount_check = True
        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch.object(self.policy.object_ring, '_devs',
                                      new=stub_ring_devs),
                    mock.patch('swift.obj.reconstructor.ismount',
                               fake_ismount)):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual([], part_infos)  # sanity, no jobs

        # ... because fake_ismount returned False for both paths
        self.assertEqual(set(paths), set([
            os.path.join(self.devices, dev) for dev in local_devs]))

        def fake_ismount(path):
            if path.endswith('sda'):
                return True
            else:
                return False

        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch.object(self.policy.object_ring, '_devs',
                                      new=stub_ring_devs),
                    mock.patch('swift.obj.reconstructor.ismount',
                               fake_ismount)):
            part_infos = list(self.reconstructor.collect_parts())
        self.assertEqual(1, len(part_infos))  # only sda picked up (part 0)
        self.assertEqual(part_infos[0]['partition'], 0)

    def test_collect_parts_cleans_tmp(self):
        local_devs = ('sda', 'sdc')
        stub_ring_devs = [{
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port
        } for dev in local_devs]
        fake_unlink = mock.MagicMock()
        self.reconstructor.reclaim_age = 1000
        now = time.time()
        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch('swift.obj.reconstructor.time.time',
                               return_value=now),
                    mock.patch.object(self.policy.object_ring, '_devs',
                                      new=stub_ring_devs),
                    mock.patch('swift.obj.reconstructor.unlink_older_than',
                               fake_unlink)):
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
        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch('swift.obj.reconstructor.mkdirs',
                               side_effect=OSError('kaboom!'))):
            self.assertEqual([], list(self.reconstructor.collect_parts()))
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        line = error_lines[0]
        self.assertTrue('Unable to create' in line)
        self.assertTrue(datadir_path in line)

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
        self.assertEqual(len(error_lines), 1)
        line = error_lines[0]
        self.assertTrue('Unable to list partitions' in line)
        self.assertTrue(datadir_path in line)

    def test_collect_parts_removes_non_partition_files(self):
        # create some junk next to partitions
        datadir_path = os.path.join(self.devices, self.local_dev['device'],
                                    diskfile.get_data_dir(self.policy))
        num_parts = 3
        for part in range(num_parts):
            utils.mkdirs(os.path.join(datadir_path, str(part)))
        junk_file = os.path.join(datadir_path, 'junk')
        with open(junk_file, 'w') as f:
            f.write('junk')
        with mock.patch('swift.obj.reconstructor.whataremyips',
                        return_value=[self.ip]):
            part_infos = list(self.reconstructor.collect_parts())
        # the file is not included in the part_infos map
        self.assertEqual(sorted(p['part_path'] for p in part_infos),
                         sorted([os.path.join(datadir_path, str(i))
                                 for i in range(num_parts)]))
        # and gets cleaned up
        self.assertFalse(os.path.exists(junk_file))

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
            'device': dev,
            'replication_ip': self.ip,
            'replication_port': self.port
        } for dev in local_devs]

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
        with nested(mock.patch('swift.obj.reconstructor.whataremyips',
                               return_value=[self.ip]),
                    mock.patch.object(self.policy.object_ring, '_devs',
                                      new=stub_ring_devs)):
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
            self.policy, part_path, do_listdir=True)
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
        ring = self.policy.object_ring = FabricatedRing()
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
        ring = self.policy.object_ring = FabricatedRing()
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
        frag_index = random.randint(0, ring.replicas - 1)
        stub_hashes = {
            '123': {frag_index: 'hash', None: 'hash'},
            'abc': {None: 'hash'},
        }
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertEqual(1, len(jobs))
        job = jobs[0]
        self.assertEqual(job['job_type'], object_reconstructor.REVERT)
        self.assertEqual(job['frag_index'], frag_index)
        self.assertEqual(sorted(job['suffixes']), sorted(stub_hashes.keys()))
        self.assertEqual(len(job['sync_to']), 1)
        self.assertEqual(job['sync_to'][0]['index'], frag_index)
        self.assertEqual(job['path'], part_path)
        self.assertEqual(job['partition'], partition)
        self.assertEqual(sorted(job['hashes']), sorted(stub_hashes))
        self.assertEqual(job['local_dev'], self.local_dev)

    def test_build_jobs_mixed(self):
        ring = self.policy.object_ring = FabricatedRing()
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
        other_frag_index = random.choice([f for f in range(ring.replicas)
                                          if f != frag_index])
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
        self.assertEqual(len(job['sync_to']), 1)
        self.assertEqual(job['sync_to'][0]['index'], other_frag_index)

    def test_build_jobs_revert_only_tombstones(self):
        ring = self.policy.object_ring = FabricatedRing()
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
        self.assertEqual(len(jobs), 1)
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
        self.assertEqual(ring.replica_count, len(job['sync_to']))
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
        frag_index = random.randint(0, replicas - 1)
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

        with nested(
                mock_ssync_sender(ssync_calls),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*codes, body_iter=body_iter) as request_log:
                self.reconstructor.process_job(job)

        expected_suffix_calls = set([
            ('10.0.0.1', '/sdb/0'),
            ('10.0.0.2', '/sdc/0'),
        ])
        self.assertEqual(expected_suffix_calls,
                         set((r['ip'], r['path'])
                             for r in request_log.requests))

        self.assertEqual(len(ssync_calls), 0)

    def test_process_job_primary_not_in_sync(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(0, replicas - 1)
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
        with nested(
                mock_ssync_sender(ssync_calls),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*codes, body_iter=body_iter) as request_log:
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
        frag_index = random.randint(0, replicas - 1)
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
        with nested(
                mock_ssync_sender(ssync_calls),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*codes, body_iter=body_iter) as request_log:
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
        frag_index = random.randint(0, replicas - 1)
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

        with nested(
                mock_ssync_sender(ssync_calls),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*codes, body_iter=body_iter) as request_log:
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

        self.assertEqual(len(ssync_calls), 2)
        self.assertEqual(set(c['node']['index'] for c in ssync_calls),
                         set([left_index, right_index]))
        for call in ssync_calls:
            if call['node']['index'] == left_index:
                self.assertEqual(call['suffixes'], ['123'])
            elif call['node']['index'] == right_index:
                self.assertEqual(call['suffixes'], ['abc'])
            else:
                self.fail('unexpected call %r' % call)

    def test_process_job_primary_down(self):
        replicas = self.policy.object_ring.replicas
        partition = 0
        frag_index = random.randint(0, replicas - 1)
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
        with nested(
                mock_ssync_sender(ssync_calls,
                                  response_callback=ssync_response_callback),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*[200] * len(expected_suffix_calls),
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
        replicas = self.policy.object_ring.replicas
        partition = 0
        frag_index = random.randint(0, replicas - 1)
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
        with nested(
                mock_ssync_sender(ssync_calls),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*codes) as request_log:
                self.reconstructor.process_job(job)

        found_suffix_calls = set((r['ip'], r['path'])
                                 for r in request_log.requests)
        self.assertEqual(expected_suffix_calls, found_suffix_calls)

        self.assertFalse(ssync_calls)

    def test_process_job_handoff(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(0, replicas - 1)
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
        with nested(
                mock_ssync_sender(ssync_calls),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(200, body=pickle.dumps({})) as request_log:
                self.reconstructor.process_job(job)

        expected_suffix_calls = set([
            (sync_to[0]['ip'], '/%s/0/123-abc' % sync_to[0]['device']),
        ])
        found_suffix_calls = set((r['ip'], r['path'])
                                 for r in request_log.requests)
        self.assertEqual(expected_suffix_calls, found_suffix_calls)

        self.assertEqual(len(ssync_calls), 1)
        call = ssync_calls[0]
        self.assertEqual(call['node'], sync_to[0])
        self.assertEqual(set(call['suffixes']), set(['123', 'abc']))

    def test_process_job_revert_to_handoff(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(0, replicas - 1)
        sync_to = [random.choice([n for n in self.policy.object_ring.devs
                                  if n != self.local_dev])]
        sync_to[0]['index'] = frag_index
        partition = 0
        handoff = next(self.policy.object_ring.get_more_nodes(partition))

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

        expected_suffix_calls = set([
            (node['replication_ip'], '/%s/0/123-abc' % node['device'])
            for node in (sync_to[0], handoff)
        ])

        ssync_calls = []
        with nested(
                mock_ssync_sender(ssync_calls,
                                  response_callback=ssync_response_callback),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*[200] * len(expected_suffix_calls),
                                  body=pickle.dumps({})) as request_log:
                self.reconstructor.process_job(job)

        found_suffix_calls = set((r['ip'], r['path'])
                                 for r in request_log.requests)
        self.assertEqual(expected_suffix_calls, found_suffix_calls)

        self.assertEqual(len(ssync_calls), len(expected_suffix_calls))
        call = ssync_calls[0]
        self.assertEqual(call['node'], sync_to[0])
        self.assertEqual(set(call['suffixes']), set(['123', 'abc']))

    def test_process_job_revert_is_handoff(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(0, replicas - 1)
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
            # the list of possible handoff's to sync to
            return False, {}

        expected_suffix_calls = set([
            (sync_to[0]['replication_ip'],
             '/%s/0/123-abc' % sync_to[0]['device'])
        ] + [
            (node['replication_ip'], '/%s/0/123-abc' % node['device'])
            for node in handoff_nodes[:-1]
        ])

        ssync_calls = []
        with nested(
                mock_ssync_sender(ssync_calls,
                                  response_callback=ssync_response_callback),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes))):
            with mocked_http_conn(*[200] * len(expected_suffix_calls),
                                  body=pickle.dumps({})) as request_log:
                self.reconstructor.process_job(job)

        found_suffix_calls = set((r['ip'], r['path'])
                                 for r in request_log.requests)
        self.assertEqual(expected_suffix_calls, found_suffix_calls)

        # this is ssync call to primary (which fails) plus the ssync call to
        # all of the handoffs (except the last one - which is the local_dev)
        self.assertEqual(len(ssync_calls), len(handoff_nodes))
        call = ssync_calls[0]
        self.assertEqual(call['node'], sync_to[0])
        self.assertEqual(set(call['suffixes']), set(['123', 'abc']))

    def test_process_job_revert_cleanup(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(0, replicas - 1)
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
            return True, {ohash: ts}

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
        # hashpath is still there, but only the durable remains
        files = os.listdir(df._datadir)
        self.assertEqual(1, len(files))
        self.assertTrue(files[0].endswith('.durable'))

        # and more to the point, the next suffix recalc will clean it up
        df_mgr = self.reconstructor._df_router[self.policy]
        df_mgr.get_hashes(self.local_dev['device'], str(partition), [],
                          self.policy)
        self.assertFalse(os.access(df._datadir, os.F_OK))

    def test_process_job_revert_cleanup_tombstone(self):
        replicas = self.policy.object_ring.replicas
        frag_index = random.randint(0, replicas - 1)
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
        df.delete(ts)

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
            return True, {ohash: ts}

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

    def test_reconstruct_fa_no_errors(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        metadata = {
            'name': '/a/c/o',
            'Content-Length': 0,
            'ETag': 'etag',
        }

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = make_ec_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)

        responses = list((200, body) for body in ec_archive_bodies)
        headers = {'X-Object-Sysmeta-Ec-Etag': etag}
        codes, body_iter = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

    def test_reconstruct_fa_errors_works(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[4]
        metadata = {
            'name': '/a/c/o',
            'Content-Length': 0,
            'ETag': 'etag',
        }

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = make_ec_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(4)

        base_responses = list((200, body) for body in ec_archive_bodies)
        # since we're already missing a fragment a +2 scheme can only support
        # one additional failure at a time
        for error in (Timeout(), 404, Exception('kaboom!')):
            responses = list(base_responses)
            error_index = random.randint(0, len(responses) - 1)
            responses[error_index] = (error, '')
            headers = {'X-Object-Sysmeta-Ec-Etag': etag}
            codes, body_iter = zip(*responses)
            with mocked_http_conn(*codes, body_iter=body_iter,
                                  headers=headers):
                df = self.reconstructor.reconstruct_fa(
                    job, node, dict(metadata))
                fixed_body = ''.join(df.reader())
                self.assertEqual(len(fixed_body), len(broken_body))
                self.assertEqual(md5(fixed_body).hexdigest(),
                                 md5(broken_body).hexdigest())

    def test_reconstruct_fa_errors_fails(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        policy = self.policy
        metadata = {
            'name': '/a/c/o',
            'Content-Length': 0,
            'ETag': 'etag',
        }

        possible_errors = [404, Timeout(), Exception('kaboom!')]
        codes = [random.choice(possible_errors) for i in
                 range(policy.object_ring.replicas - 1)]
        with mocked_http_conn(*codes):
            self.assertRaises(DiskFileError, self.reconstructor.reconstruct_fa,
                              job, node, metadata)

    def test_reconstruct_fa_with_mixed_old_etag(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        metadata = {
            'name': '/a/c/o',
            'Content-Length': 0,
            'ETag': 'etag',
        }

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = make_ec_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)

        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        # bad response
        bad_response = (200, '', {
            'X-Object-Sysmeta-Ec-Etag': 'some garbage',
            'X-Backend-Timestamp': next(ts).internal,
        })

        # good responses
        headers = {
            'X-Object-Sysmeta-Ec-Etag': etag,
            'X-Backend-Timestamp': next(ts).internal
        }
        responses = [(200, body, headers)
                     for body in ec_archive_bodies]
        # mixed together
        error_index = random.randint(0, len(responses) - 2)
        responses[error_index] = bad_response
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

    def test_reconstruct_fa_with_mixed_new_etag(self):
        job = {
            'partition': 0,
            'policy': self.policy,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        metadata = {
            'name': '/a/c/o',
            'Content-Length': 0,
            'ETag': 'etag',
        }

        test_data = ('rebuild' * self.policy.ec_segment_size)[:-777]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = make_ec_archive_bodies(self.policy, test_data)

        broken_body = ec_archive_bodies.pop(1)

        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        # good responses
        headers = {
            'X-Object-Sysmeta-Ec-Etag': etag,
            'X-Backend-Timestamp': next(ts).internal
        }
        responses = [(200, body, headers)
                     for body in ec_archive_bodies]
        codes, body_iter, headers = zip(*responses)

        # sanity check before negative test
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, dict(metadata))
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # one newer etag can spoil the bunch
        new_response = (200, '', {
            'X-Object-Sysmeta-Ec-Etag': 'some garbage',
            'X-Backend-Timestamp': next(ts).internal,
        })
        new_index = random.randint(0, len(responses) - self.policy.ec_nparity)
        responses[new_index] = new_response
        codes, body_iter, headers = zip(*responses)
        with mocked_http_conn(*codes, body_iter=body_iter, headers=headers):
            self.assertRaises(DiskFileError, self.reconstructor.reconstruct_fa,
                              job, node, dict(metadata))


if __name__ == '__main__':
    unittest.main()
