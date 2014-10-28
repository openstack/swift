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
from swift.obj import diskfile, reconstructor as object_reconstructor
from swift.common import ring
from swift.common.storage_policy import StoragePolicy, POLICIES, \
    REPL_POLICY, EC_POLICY

from test.unit import (patch_policies, debug_logger, fake_http_connect,
                       FabricatedRing)


@contextmanager
def mock_http_connect(*args, **kwargs):
    fake_conn = fake_http_connect(*args, **kwargs)
    with mock.patch('swift.obj.reconstructor.http_connect', fake_conn):
        yield fake_conn
        left_over_status = list(fake_conn.code_iter)
        if left_over_status:
            raise AssertionError('left over status %r' % left_over_status)


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


# these tests require these policies just as they are, do not mess with
# them or things will likely explode
@patch_policies([
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 0, 'name': 'zero', 'is_default': True}),
    StoragePolicy.from_conf(
        EC_POLICY, {'idx': 1, 'name': 'one', 'is_default': False,
                    'ec_type': 'jerasure_rs_vand', 'ec_ndata': 2,
                    'ec_nparity': 1})
])
class TestGlobalSetupObjectReconstructor(unittest.TestCase):

    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        _create_test_rings(self.testdir)
        object_ring_0 = ring.Ring(self.testdir, ring_name='object')  # noqa
        object_ring_1 = ring.Ring(self.testdir, ring_name='object-1')  # noqa
        POLICIES[0].object_ring = object_ring_0
        POLICIES[1].object_ring = object_ring_1
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
            self.ec_obj_ring = \
                self.reconstructor.get_object_ring(self.ec_policy.idx)
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
                'X-Object-Sysmeta-Ec-Archive-Index': frag_index,
            }
            writer.put(metadata)
            writer.commit(timestamp)
        return df

    def debug_wtf(self):
        # won't include this in the final, just handy reminder of where
        # things are...
        for pol in [p for p in POLICIES if p.policy_type == EC_POLICY]:
            obj_ring = self.reconstructor.get_object_ring(pol.idx)
            for part_num in self.part_nums:
                print "\n part_num %s " % part_num
                part_nodes = obj_ring.get_part_nodes(int(part_num))
                print "\n part_nodes %s " % part_nodes
                for local_dev in obj_ring.devs:
                    partners = self.reconstructor._get_partners(
                        local_dev['id'], obj_ring, part_num)
                    if partners:
                        print "\n local_dev %s \n partners %s " % (local_dev,
                                                                   partners)

    def assert_exepcted_jobs(self, part_num, jobs):
        for job in jobs:
            del job['path']
            del job['policy']
            if 'local_index' in job:
                del job['local_index']
            job['suffix_list'].sort()

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
                'sync_type': object_reconstructor.REVERT,
                'suffix_list': ['061'],
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
                'sync_type': object_reconstructor.SYNC,
                'suffix_list': ['061', '3c1'],
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
                'sync_type': object_reconstructor.REVERT,
                'suffix_list': ['061', '3c1'],
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
                'sync_type': object_reconstructor.SYNC,
                'suffix_list': ['3c1'],
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
                'sync_type': object_reconstructor.REVERT,
                'suffix_list': ['061'],
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
                'sync_type': object_reconstructor.REVERT,
                'suffix_list': ['3c1'],
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
        with nested(mock_http_connect(*[200] * 16, body=pickle.dumps({})),
                    mock.patch('swift.obj.reconstructor.ssync_sender',
                               mock.MagicMock())):
            self.reconstructor.run_once()

    def test_get_response(self):
        part = self.part_nums[0]
        node = POLICIES[0].object_ring.get_part_nodes(int(part))[0]
        for stat_code in (200, 400):
            with mock_http_connect(stat_code):
                resp = self.reconstructor._get_response(node, part,
                                                        path='nada',
                                                        headers={},
                                                        policy=POLICIES[0])
                if resp:
                    self.assertEqual(resp.status, 200)
                else:
                    self.assertEqual(
                        len(self.reconstructor.logger.log_dict['error']), 1)

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
        for part_info in self.reconstructor.collect_parts():
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
            self.assertTrue(jobs[0]['sync_type'] in
                            (object_reconstructor.SYNC,
                             object_reconstructor.REVERT))
            self.assert_exepcted_jobs(part_info['partition'], jobs)

        self.reconstructor.handoffs_first = True
        for part_info in self.reconstructor.collect_parts():
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
            self.assertTrue(jobs[0]['sync_type'] ==
                            object_reconstructor.REVERT)
            self.assert_exepcted_jobs(part_info['partition'], jobs)

    def test_get_partners(self):
        # both of the expected values below are exhaustive possible results
        # from get_part nodes given the polices and dev lists defined in
        # our custom test ring.  We confirm every possible call returns
        # the expected values
        # format: [(dev_id in question, 'part_num',
        #          [part_nodes for the given part)], ...]
        expected_handoffs = \
            [(0, '1', [1, 2, 3]), (1, '2', [2, 3, 0]), (3, '0', [0, 1, 2]),
             (0, '1', [1, 2, 3]), (1, '2', [2, 3, 0]), (3, '0', [0, 1, 2])]

        # format: [dev_id in question, 'part_num',
        #          [part_nodes for the given part], left id, right id...]
        expected_partners = \
            [(0, '0', [0, 1, 2], 2, 1), (0, '2', [2, 3, 0], 3, 2),
             (1, '0', [0, 1, 2], 0, 2), (1, '1', [1, 2, 3], 3, 2),
             (2, '0', [0, 1, 2], 1, 0), (2, '1', [1, 2, 3], 1, 3),
             (2, '2', [2, 3, 0], 0, 3), (3, '1', [1, 2, 3], 2, 1),
             (3, '2', [2, 3, 0], 2, 0), (0, '0', [0, 1, 2], 2, 1),
             (0, '2', [2, 3, 0], 3, 2), (1, '0', [0, 1, 2], 0, 2),
             (1, '1', [1, 2, 3], 3, 2), (2, '0', [0, 1, 2], 1, 0),
             (2, '1', [1, 2, 3], 1, 3), (2, '2', [2, 3, 0], 0, 3),
             (3, '1', [1, 2, 3], 2, 1), (3, '2', [2, 3, 0], 2, 0)]

        got_handoffs = []
        got_partners = []
        for pol in POLICIES:
            obj_ring = self.reconstructor.get_object_ring(pol.idx)
            for local_dev in obj_ring.devs:
                for part_num in self.part_nums:
                    part_nodes = obj_ring.get_part_nodes(int(part_num))
                    ids = []
                    partners = []
                    for node in part_nodes:
                        ids.append(node['id'])
                    partners = self.reconstructor._get_partners(
                        local_dev['id'], obj_ring, part_num)
                    if partners:
                        left = partners[0]['id']
                        right = partners[1]['id']
                        got_partners.append((local_dev['id'], part_num, ids,
                                             left, right))
                    else:
                        got_handoffs.append((local_dev['id'], part_num, ids))

        self.assertEqual(expected_handoffs, got_handoffs)
        self.assertEqual(expected_partners, got_partners)

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

        class _fake_ssync(object):
            def __init__(self, *args, **kwargs):
                pass

            def __call__(self):
                pass

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

    def test_delete_partition(self):
        # part 2 is predefined to have all revert jobs
        part_path = os.path.join(self.objects_1, '2')
        self.assertTrue(os.access(part_path, os.F_OK))
        # the idea here is to test the delete logic in reconstruct()
        # by faking what ssync_sender would do

        class _fake_ssync(object):
            def __init__(self, *args, **kwargs):
                if args[2]['sync_type'] == object_reconstructor.REVERT:
                    for suff in args[3]:
                        suff_dir = os.path.join(args[2]['path'], suff)
                        for root, dirs, files in os.walk(suff_dir):
                            for d in dirs:
                                shutil.rmtree(os.path.join(root, d))

            def __call__(self, *args, **kwargs):
                pass

        with nested(mock_http_connect(*[200] * 16, body=pickle.dumps({})),
                    mock.patch('swift.obj.reconstructor.ssync_sender',
                               _fake_ssync)):
                self.reconstructor.reconstruct()

        #part 2 should be totally gone
        self.assertFalse(os.access(part_path, os.F_OK))

    def test_get_job_info(self):
        # yeah, this test code expects a specific setup
        self.assertEqual(len(self.part_nums), 3)

        # OK, at this point we should have 4 loaded parts with one
        jobs = []
        for partition in os.listdir(self.ec_obj_path):
            part_path = os.path.join(self.ec_obj_path, partition)
            jobs = self.reconstructor._get_job_info(
                self.ec_local_dev, part_path, int(partition), self.ec_policy)
            self.assert_exepcted_jobs(partition, jobs)

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

        # now with bogus local index
        local_suff = {'123': {None: 'abc', 99: 'def'}}
        remote_suff = {'456': {None: 'ghi', 0: 'jkl'}}
        suffs = self.reconstructor.get_suffix_delta(local_suff,
                                                    local_index,
                                                    remote_suff,
                                                    remote_index)
        self.assertEqual(suffs, ['123'])

    def test_process_job_all_success(self):
        self.reconstructor._reset_stats()
        with nested(mock_http_connect(*[200] * 16, body=pickle.dumps({})),
                    mock.patch('swift.obj.reconstructor.ssync_sender')):
            found_jobs = []
            for part_info in self.reconstructor.collect_parts():
                jobs = self.reconstructor.build_reconstruction_jobs(part_info)
                found_jobs.extend(jobs)
                for job in jobs:
                    self.logger._clear()
                    node_count = len(job['sync_to'])
                    self.reconstructor.process_job(job)
                    self.assertEqual(1, count_stats(
                        self.logger, 'increment',
                        'partition.delete.count.'))
                    self.assertEqual(1, count_stats(
                        self.logger, 'timing_since',
                        'partition.delete.timing'))
                    if job['sync_type'] == object_reconstructor.REVERT:
                        self.assertEqual(0, count_stats(
                            self.logger, 'update_stats', 'suffix.hashes'))
                    else:
                        self.assertEqual(node_count, count_stats(
                            self.logger, 'update_stats', 'suffix.hashes'))
                    self.assertEqual(node_count, count_stats(
                        self.logger, 'update_stats', 'suffix.syncs'))
                    self.assertFalse('error' in
                                     self.logger.all_log_lines())
        self.assertEqual(self.reconstructor.suffix_sync, 16)
        self.assertEqual(self.reconstructor.suffix_count, 8)
        self.assertEqual(len(found_jobs), 6)

    def test_process_job_all_insufficient_storage(self):
        self.reconstructor._reset_stats()
        with nested(mock_http_connect(*[507] * 14),
                    mock.patch('swift.obj.reconstructor.ssync_sender')):
            found_jobs = []
            for part_info in self.reconstructor.collect_parts():
                jobs = self.reconstructor.build_reconstruction_jobs(part_info)
                found_jobs.extend(jobs)
                for job in jobs:
                    self.logger._clear()
                    self.reconstructor.process_job(job)
                    for line in self.logger.get_lines_for_level('error'):
                        self.assertTrue('responded as unmounted' in line)
                    self.assertEqual(1, count_stats(
                        self.logger, 'increment',
                        'partition.delete.count.'))
                    self.assertEqual(1, count_stats(
                        self.logger, 'timing_since',
                        'partition.delete.timing'))
                    self.assertEqual(0, count_stats(
                        self.logger, 'update_stats', 'suffix.hashes'))
                    self.assertEqual(0, count_stats(
                        self.logger, 'update_stats', 'suffix.syncs'))
        self.assertEqual(self.reconstructor.suffix_sync, 0)
        self.assertEqual(self.reconstructor.suffix_count, 0)
        self.assertEqual(len(found_jobs), 6)

    def test_process_job_all_client_error(self):
        self.reconstructor._reset_stats()
        with nested(mock_http_connect(*[400] * 8),
                    mock.patch('swift.obj.reconstructor.ssync_sender')):
            found_jobs = []
            for part_info in self.reconstructor.collect_parts():
                jobs = self.reconstructor.build_reconstruction_jobs(part_info)
                found_jobs.extend(jobs)
                for job in jobs:
                    self.logger._clear()
                    self.reconstructor.process_job(job)
                    self.assertEqual(1, count_stats(
                        self.logger, 'increment',
                        'partition.delete.count.'))
                    self.assertEqual(1, count_stats(
                        self.logger, 'timing_since',
                        'partition.delete.timing'))
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
        with nested(mock_http_connect(*[Timeout()] * 14),
                    mock.patch('swift.obj.reconstructor.ssync_sender')):
            found_jobs = []
            for part_info in self.reconstructor.collect_parts():
                jobs = self.reconstructor.build_reconstruction_jobs(part_info)
                found_jobs.extend(jobs)
                for job in jobs:
                    self.logger._clear()
                    self.reconstructor.process_job(job)
                    self.assertEqual(1, count_stats(
                        self.logger, 'increment',
                        'partition.delete.count.'))
                    self.assertEqual(1, count_stats(
                        self.logger, 'timing_since',
                        'partition.delete.timing'))
                    for line in self.logger.get_lines_for_level('error'):
                        self.assertTrue('Timeout (Nones)' in line)
                    self.assertEqual(0, count_stats(
                        self.logger, 'update_stats', 'suffix.hashes'))
                    self.assertEqual(0, count_stats(
                        self.logger, 'update_stats', 'suffix.syncs'))
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

    def tearDown(self):
        shutil.rmtree(self.testdir)

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
        self.assertEqual(jobs, [])
        hashes_file = os.path.join(part_path,
                                   diskfile.HASH_FILE)
        self.assertTrue(os.path.exists(hashes_file))
        suffixes = self.reconstructor.tpool_get_info(
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
        self.assertEqual(jobs, [])

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
        self.assertTrue(jobs)
        for job in jobs:
            self.assertEqual(job['partition'], partition)
            self.assertEqual(job['frag_index'], frag_index)
            self.assertEqual(set([n['index'] for n in job['sync_to']]),
                             set([(frag_index + 1) % ring.replicas,
                                  (frag_index - 1) % ring.replicas]))
            self.assertEqual(job['hashes'], stub_hashes)

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
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertTrue(jobs)
        for job in jobs:
            self.assertEqual(job['partition'], partition)
            self.assertEqual(job['frag_index'], frag_index)
            self.assertEqual(len(job['sync_to']), 1)
            self.assertEqual(job['sync_to'][0]['index'], frag_index)
            self.assertEqual(job['hashes'], stub_hashes)

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
            'abc': {frag_index: 'hash', None: 'hash'},
        }
        with mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                        return_value=(None, stub_hashes)):
            jobs = self.reconstructor.build_reconstruction_jobs(part_info)
        self.assertTrue(jobs)
        for job in jobs:
            self.assertEqual(job['partition'], partition)
            if len(job['sync_to']) > 1:
                # primary job
                self.assertEqual(job['frag_index'], frag_index)
                self.assertEqual(len(job['sync_to']), 2)
                self.assertEqual(set([n['index'] for n in job['sync_to']]),
                                 set([(frag_index + 1) % ring.replicas,
                                      (frag_index - 1) % ring.replicas]))
            else:
                # handoff job
                self.assertEqual(job['frag_index'], other_frag_index)
                self.assertEqual(len(job['sync_to']), 1)
                self.assertEqual(job['sync_to'][0]['index'], other_frag_index)
            self.assertEqual(job['hashes'], stub_hashes)

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
            'policy': self.policy,
            'device': self.local_dev,
            'frag_index': frag_index,
            'sync_type': object_reconstructor.SYNC,
            'sync_to': sync_to,
            'hashes': stub_hashes,
            'partition': partition,
            'path': part_path,
        }

        responses = [(200, pickle.dumps(hashes)) for hashes in (
            left_hashes, right_hashes)]
        codes, body_iter = zip(*responses)

        ssync_calls = []

        def fake_ssync(daemon, node, job, suffixes):
            ssync_calls.append(
                {'node': node, 'job': job, 'hashes': suffixes})
            return lambda: None

        with nested(
                mock.patch('swift.obj.reconstructor.ssync_sender',
                           fake_ssync),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)),
                mock_http_connect(*codes, body_iter=body_iter)):
            self.reconstructor.process_job(job)

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
            'policy': self.policy,
            'device': self.local_dev,
            'frag_index': frag_index,
            'sync_type': object_reconstructor.SYNC,
            'sync_to': sync_to,
            'hashes': stub_hashes,
            'partition': partition,
            'path': part_path,
        }

        responses = [(200, pickle.dumps(hashes)) for hashes in (
            left_hashes, left_hashes, right_hashes, right_hashes)]
        codes, body_iter = zip(*responses)

        ssync_calls = []

        def fake_ssync(daemon, node, job, suffixes):
            ssync_calls.append(
                {'node': node, 'job': job, 'hashes': suffixes})
            return lambda: None

        with nested(
                mock.patch('swift.obj.reconstructor.ssync_sender',
                           fake_ssync),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)),
                mock_http_connect(*codes, body_iter=body_iter)):
            self.reconstructor.process_job(job)

        self.assertEqual(len(ssync_calls), 2)
        for call in ssync_calls:
            self.assertEqual(set(call['hashes']), set(['123', 'abc']))

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
            'policy': self.policy,
            'device': self.local_dev,
            'frag_index': frag_index,
            'sync_type': object_reconstructor.SYNC,
            'sync_to': sync_to,
            'hashes': stub_hashes,
            'partition': partition,
            'path': part_path,
        }

        responses = [(200, pickle.dumps(hashes)) for hashes in (
            left_hashes, left_hashes, right_hashes, right_hashes)]
        codes, body_iter = zip(*responses)

        ssync_calls = []

        def fake_ssync(daemon, node, job, suffixes):
            ssync_calls.append(
                {'node': node, 'job': job, 'hashes': suffixes})
            return lambda: None

        with nested(
                mock.patch('swift.obj.reconstructor.ssync_sender',
                           fake_ssync),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)),
                mock_http_connect(*codes, body_iter=body_iter)):
            self.reconstructor.process_job(job)

        self.assertEqual(len(ssync_calls), 2)
        self.assertEqual(set(c['node']['index'] for c in ssync_calls),
                         set([left_index, right_index]))
        for call in ssync_calls:
            if call['node']['index'] == left_index:
                self.assertEqual(call['hashes'], ['123'])
            elif call['node']['index'] == right_index:
                self.assertEqual(call['hashes'], ['abc'])
            else:
                self.fail('unexpected call %r' % call)

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
            'policy': self.policy,
            'device': self.local_dev,
            'frag_index': frag_index,
            'sync_type': object_reconstructor.SYNC,
            'sync_to': sync_to,
            'hashes': stub_hashes,
            'partition': partition,
            'path': part_path,
        }

        # I think at least on of these requests is not needed
        responses = [(200, pickle.dumps({}))] * 2
        codes, body_iter = zip(*responses)

        ssync_calls = []

        def fake_ssync(daemon, node, job, suffixes):
            ssync_calls.append(
                {'node': node, 'job': job, 'hashes': suffixes})
            return lambda: None

        with nested(
                mock.patch('swift.obj.reconstructor.ssync_sender',
                           fake_ssync),
                mock.patch('swift.obj.diskfile.ECDiskFileManager._get_hashes',
                           return_value=(None, stub_hashes)),
                mock_http_connect(*codes, body_iter=body_iter)):
            self.reconstructor.process_job(job)
        self.assertEqual(len(ssync_calls), 1)
        call = ssync_calls[0]
        self.assertEqual(call['node'], sync_to[0])
        self.assertEqual(set(call['hashes']), set(['123', 'abc']))

    def test_reconstruct_fa_no_errors(self):
        job = {
            'partition': 0,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        policy = self.policy
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
        with mock_http_connect(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, policy, metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

    def test_reconstruct_fa_errors_works(self):
        job = {
            'partition': 0,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[4]
        policy = self.policy
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
            with mock_http_connect(*codes, body_iter=body_iter,
                                   headers=headers):
                df = self.reconstructor.reconstruct_fa(
                    job, node, policy, dict(metadata))
                fixed_body = ''.join(df.reader())
                self.assertEqual(len(fixed_body), len(broken_body))
                self.assertEqual(md5(fixed_body).hexdigest(),
                                 md5(broken_body).hexdigest())

    def test_reconstruct_fa_errors_fails(self):
        job = {
            'partition': 0,
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
                 range(policy.ec_ndata - 1)]
        with mock_http_connect(*codes):
            self.assertEqual(self.reconstructor.reconstruct_fa(
                job, node, policy, metadata), None)

    def test_reconstruct_fa_with_mixed_old_etag(self):
        job = {
            'partition': 0,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        policy = self.policy
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
        with mock_http_connect(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, policy, metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

    def test_reconstruct_fa_with_mixed_new_etag(self):
        job = {
            'partition': 0,
        }
        part_nodes = self.policy.object_ring.get_part_nodes(0)
        node = part_nodes[1]
        policy = self.policy
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
        with mock_http_connect(*codes, body_iter=body_iter, headers=headers):
            df = self.reconstructor.reconstruct_fa(
                job, node, policy, metadata)
            fixed_body = ''.join(df.reader())
            self.assertEqual(len(fixed_body), len(broken_body))
            self.assertEqual(md5(fixed_body).hexdigest(),
                             md5(broken_body).hexdigest())

        # one newer etag can spoil the bunch
        new_response = (200, '', {
            'X-Object-Sysmeta-Ec-Etag': 'some garbage',
            'X-Backend-Timestamp': next(ts).internal,
        })
        new_index = random.randint(0, len(responses) - 2)
        responses[new_index] = new_response
        codes, body_iter, headers = zip(*responses)
        with mock_http_connect(*codes, body_iter=body_iter, headers=headers):
            self.assertEqual(self.reconstructor.reconstruct_fa(
                job, node, policy, metadata), None)

    def test_reconstruct_overrides(self):
        pass

    def test_reconstruct_mount_check(self):
        pass

    def test_reconstruct_ring_check(self):
        pass

    def test_reconstruct_revert_paths(self):
        pass


if __name__ == '__main__':
    unittest.main()
