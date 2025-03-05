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

import os
import time
import random
import shutil
import tempfile
import unittest

from unittest.mock import patch, call, DEFAULT
import eventlet

from swift.account import reaper
from swift.account.backend import DATADIR
from swift.common.exceptions import ClientException
from swift.common.utils import normalize_timestamp, Timestamp

from test import unit
from test.debug_logger import debug_logger
from swift.common.storage_policy import StoragePolicy, POLICIES


class FakeBroker(object):
    def __init__(self):
        self.info = {}

    def get_info(self):
        return self.info


class FakeAccountBroker(object):
    def __init__(self, containers, logger):
        self.containers = containers
        self.containers_yielded = []

    def get_info(self):
        info = {'account': 'a',
                'delete_timestamp': time.time() - 10}
        return info

    def list_containers_iter(self, limit, marker, *args, **kwargs):
        if not kwargs.pop('allow_reserved'):
            raise RuntimeError('Expected allow_reserved to be True!')
        if kwargs:
            raise RuntimeError('Got unexpected keyword arguments: %r' % (
                kwargs, ))
        for cont in self.containers:
            if cont > marker:
                yield cont, None, None, None, None, None
            limit -= 1
            if limit <= 0:
                break

    def is_status_deleted(self):
        return True

    def empty(self):
        return False


class FakeRing(object):
    def __init__(self):
        self.nodes = [{'id': '1',
                       'ip': '10.10.10.1',
                       'port': 6202,
                       'device': 'sda1'},
                      {'id': '2',
                       'ip': '10.10.10.2',
                       'port': 6202,
                       'device': 'sda1'},
                      {'id': '3',
                       'ip': '10.10.10.3',
                       'port': 6202,
                       'device': None},
                      {'id': '4',
                       'ip': '10.10.10.1',
                       'port': 6202,
                       'device': 'sda2'},
                      {'id': '5',
                       'ip': '10.10.10.1',
                       'port': 6202,
                       'device': 'sda3'},
                      ]

    def get_nodes(self, *args, **kwargs):
        return ('partition', self.nodes)

    def get_part_nodes(self, *args, **kwargs):
        return self.nodes


acc_nodes = [{'device': 'sda1',
              'ip': '',
              'port': ''},
             {'device': 'sda1',
              'ip': '',
              'port': ''},
             {'device': 'sda1',
              'ip': '',
              'port': ''},
             {'device': 'sda1',
              'ip': '',
              'port': ''},
             {'device': 'sda1',
              'ip': '',
              'port': ''}]

cont_nodes = [{'device': 'sda1',
               'ip': '',
               'port': ''},
              {'device': 'sda1',
               'ip': '',
               'port': ''},
              {'device': 'sda1',
               'ip': '',
               'port': ''},
              {'device': 'sda1',
               'ip': '',
               'port': ''},
              {'device': 'sda1',
               'ip': '',
               'port': ''}]


@unit.patch_policies([StoragePolicy(0, 'zero', False,
                                    object_ring=unit.FakeRing()),
                      StoragePolicy(1, 'one', True,
                                    object_ring=unit.FakeRing(replicas=4))])
class TestReaper(unittest.TestCase):

    def setUp(self):
        self.to_delete = []
        self.myexp = ClientException("", http_host=None,
                                     http_port=None,
                                     http_device=None,
                                     http_status=404,
                                     http_reason=None
                                     )

    def tearDown(self):
        for todel in self.to_delete:
            shutil.rmtree(todel)

    def fake_direct_delete_object(self, *args, **kwargs):
        if self.amount_fail < self.max_fail:
            self.amount_fail += 1
            raise self.myexp
        if self.reap_obj_timeout:
            raise eventlet.Timeout()

    def fake_direct_delete_container(self, *args, **kwargs):
        if self.amount_delete_fail < self.max_delete_fail:
            self.amount_delete_fail += 1
            raise self.myexp

    def fake_direct_get_container(self, *args, **kwargs):
        if self.get_fail:
            raise self.myexp
        if self.timeout:
            raise eventlet.Timeout()
        objects = [{'name': u'o1'},
                   {'name': u'o2'},
                   {'name': u'o3'},
                   {'name': u'o4'}]
        return None, [o for o in objects if o['name'] > kwargs['marker']]

    def fake_container_ring(self):
        return FakeRing()

    def fake_reap_object(self, *args, **kwargs):
        if self.reap_obj_fail:
            raise Exception

    def prepare_data_dir(self, ts=False, device='sda1'):
        devices_path = tempfile.mkdtemp()
        # will be deleted by teardown
        self.to_delete.append(devices_path)
        path = os.path.join(devices_path, device, DATADIR)
        os.makedirs(path)
        path = os.path.join(path, '100',
                            'a86', 'a8c682d2472e1720f2d81ff8993aba6')
        os.makedirs(path)
        suffix = 'db'
        if ts:
            suffix = 'ts'
        with open(os.path.join(path, 'a8c682203aba6.%s' % suffix), 'w') as fd:
            fd.write('')
        return devices_path

    def init_reaper(self, conf=None, myips=None, fakelogger=False):
        if conf is None:
            conf = {}
        if myips is None:
            myips = ['10.10.10.1']

        r = reaper.AccountReaper(conf)
        r.myips = myips
        if fakelogger:
            r.logger = debug_logger('test-reaper')
        return r

    def fake_reap_account(self, *args, **kwargs):
        self.called_amount += 1

    def fake_account_ring(self):
        return FakeRing()

    def test_creation(self):
        # later config should be extended to assert more config options
        r = reaper.AccountReaper({'node_timeout': '3.5'})
        self.assertEqual(r.node_timeout, 3.5)

    def test_delay_reaping_conf_default(self):
        r = reaper.AccountReaper({})
        self.assertEqual(r.delay_reaping, 0)
        r = reaper.AccountReaper({'delay_reaping': ''})
        self.assertEqual(r.delay_reaping, 0)

    def test_delay_reaping_conf_set(self):
        r = reaper.AccountReaper({'delay_reaping': '123'})
        self.assertEqual(r.delay_reaping, 123)

    def test_delay_reaping_conf_bad_value(self):
        self.assertRaises(ValueError, reaper.AccountReaper,
                          {'delay_reaping': 'abc'})

    def test_reap_warn_after_conf_set(self):
        conf = {'delay_reaping': '2', 'reap_warn_after': '3'}
        r = reaper.AccountReaper(conf)
        self.assertEqual(r.reap_not_done_after, 5)

    def test_reap_warn_after_conf_bad_value(self):
        self.assertRaises(ValueError, reaper.AccountReaper,
                          {'reap_warn_after': 'abc'})

    def test_reap_delay(self):
        time_value = [100]

        def _time():
            return time_value[0]

        time_orig = reaper.time
        try:
            reaper.time = _time
            r = reaper.AccountReaper({'delay_reaping': '10'})
            b = FakeBroker()
            b.info['delete_timestamp'] = normalize_timestamp(110)
            self.assertFalse(r.reap_account(b, 0, None))
            b.info['delete_timestamp'] = normalize_timestamp(100)
            self.assertFalse(r.reap_account(b, 0, None))
            b.info['delete_timestamp'] = normalize_timestamp(90)
            self.assertFalse(r.reap_account(b, 0, None))
            # KeyError raised immediately as reap_account tries to get the
            # account's name to do the reaping.
            b.info['delete_timestamp'] = normalize_timestamp(89)
            self.assertRaises(KeyError, r.reap_account, b, 0, None)
            b.info['delete_timestamp'] = normalize_timestamp(1)
            self.assertRaises(KeyError, r.reap_account, b, 0, None)
        finally:
            reaper.time = time_orig

    def test_reset_stats(self):
        conf = {}
        r = reaper.AccountReaper(conf)
        self.assertDictEqual(r.stats_return_codes, {})
        self.assertEqual(r.stats_containers_deleted, 0)
        self.assertEqual(r.stats_containers_remaining, 0)
        self.assertEqual(r.stats_containers_possibly_remaining, 0)
        self.assertEqual(r.stats_objects_deleted, 0)
        self.assertEqual(r.stats_objects_remaining, 0)
        self.assertEqual(r.stats_objects_possibly_remaining, 0)
        # also make sure reset actually resets values
        r.stats_return_codes = {"hello": "swift"}
        r.stats_containers_deleted = random.randint(1, 100)
        r.stats_containers_remaining = random.randint(1, 100)
        r.stats_containers_possibly_remaining = random.randint(1, 100)
        r.stats_objects_deleted = random.randint(1, 100)
        r.stats_objects_remaining = random.randint(1, 100)
        r.stats_objects_possibly_remaining = random.randint(1, 100)
        r.reset_stats()
        self.assertDictEqual(r.stats_return_codes, {})
        self.assertEqual(r.stats_containers_deleted, 0)
        self.assertEqual(r.stats_containers_remaining, 0)
        self.assertEqual(r.stats_containers_possibly_remaining, 0)
        self.assertEqual(r.stats_objects_deleted, 0)
        self.assertEqual(r.stats_objects_remaining, 0)
        self.assertEqual(r.stats_objects_possibly_remaining, 0)

    def test_reap_object(self):
        conf = {
            'mount_check': 'false',
        }
        r = reaper.AccountReaper(conf, logger=debug_logger())
        mock_path = 'swift.account.reaper.direct_delete_object'
        for policy in POLICIES:
            r.reset_stats()
            with patch(mock_path) as fake_direct_delete:
                with patch('swift.common.utils.Timestamp.now') as mock_now:
                    mock_now.return_value = Timestamp(1429117638.86767)
                    r.reap_object('a', 'c', 'partition', cont_nodes, 'o',
                                  policy.idx)
                    mock_now.assert_called_once_with()
                    for i, call_args in enumerate(
                            fake_direct_delete.call_args_list):
                        cnode = cont_nodes[i % len(cont_nodes)]
                        host = '%(ip)s:%(port)s' % cnode
                        device = cnode['device']
                        headers = {
                            'X-Container-Host': host,
                            'X-Container-Partition': 'partition',
                            'X-Container-Device': device,
                            'X-Backend-Storage-Policy-Index': policy.idx,
                            'X-Timestamp': '1429117638.86767',
                            'x-backend-use-replication-network': 'true',
                        }
                        ring = r.get_object_ring(policy.idx)
                        expected = call(dict(ring.devs[i], index=i), 0,
                                        'a', 'c', 'o',
                                        headers=headers, conn_timeout=0.5,
                                        response_timeout=10)
                        self.assertEqual(call_args, expected)
                    self.assertEqual(policy.object_ring.replicas - 1, i)
            self.assertEqual(r.stats_objects_deleted,
                             policy.object_ring.replicas)

    def test_reap_object_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.amount_fail = 0
        self.max_fail = 1
        self.reap_obj_timeout = False
        policy = random.choice(list(POLICIES))
        with patch('swift.account.reaper.direct_delete_object',
                   self.fake_direct_delete_object):
            r.reap_object('a', 'c', 'partition', cont_nodes, 'o',
                          policy.idx)
        # IMHO, the stat handling in the node loop of reap object is
        # over indented, but no one has complained, so I'm not inclined
        # to move it.  However it's worth noting we're currently keeping
        # stats on deletes per *replica* - which is rather obvious from
        # these tests, but this results is surprising because of some
        # funny logic to *skip* increments on successful deletes of
        # replicas until we have more successful responses than
        # failures.  This means that while the first replica doesn't
        # increment deleted because of the failure, the second one
        # *does* get successfully deleted, but *also does not* increment
        # the counter (!?).
        #
        # In the three replica case this leaves only the last deleted
        # object incrementing the counter - in the four replica case
        # this leaves the last two.
        #
        # Basically this test will always result in:
        #   deleted == num_replicas - 2
        self.assertEqual(r.stats_objects_deleted,
                         policy.object_ring.replicas - 2)
        self.assertEqual(r.stats_objects_remaining, 1)
        self.assertEqual(r.stats_objects_possibly_remaining, 1)
        self.assertEqual(r.stats_return_codes[2],
                         policy.object_ring.replicas - 1)
        self.assertEqual(r.stats_return_codes[4], 1)

    def test_reap_object_timeout(self):
        r = self.init_reaper({}, fakelogger=True)
        self.amount_fail = 1
        self.max_fail = 0
        self.reap_obj_timeout = True
        with patch('swift.account.reaper.direct_delete_object',
                   self.fake_direct_delete_object):
            r.reap_object('a', 'c', 'partition', cont_nodes, 'o', 1)
        self.assertEqual(r.stats_objects_deleted, 0)
        self.assertEqual(r.stats_objects_remaining, 4)
        self.assertEqual(r.stats_objects_possibly_remaining, 0)
        self.assertTrue(r.logger.get_lines_for_level(
            'error')[-1].startswith('Timeout Exception'))

    def test_reap_object_non_exist_policy_index(self):
        r = self.init_reaper({}, fakelogger=True)
        r.reap_object('a', 'c', 'partition', cont_nodes, 'o', 2)
        self.assertEqual(r.stats_objects_deleted, 0)
        self.assertEqual(r.stats_objects_remaining, 1)
        self.assertEqual(r.stats_objects_possibly_remaining, 0)

    @patch('swift.account.reaper.Ring',
           lambda *args, **kwargs: unit.FakeRing())
    def test_reap_container(self):
        policy = random.choice(list(POLICIES))
        r = self.init_reaper({}, fakelogger=True)
        with patch.multiple('swift.account.reaper',
                            direct_get_container=DEFAULT,
                            direct_delete_object=DEFAULT,
                            direct_delete_container=DEFAULT) as mocks:
            headers = {'X-Backend-Storage-Policy-Index': policy.idx}
            obj_listing = [{'name': 'o'}]

            def fake_get_container(*args, **kwargs):
                try:
                    obj = obj_listing.pop(0)
                except IndexError:
                    obj_list = []
                else:
                    obj_list = [obj]
                return headers, obj_list

            mocks['direct_get_container'].side_effect = fake_get_container
            with patch('swift.common.utils.Timestamp.now') as mock_now:
                mock_now.side_effect = [Timestamp(1429117638.86767),
                                        Timestamp(1429117639.67676)]
                r.reap_container('a', 'partition', acc_nodes, 'c')

            # verify calls to direct_delete_object
            mock_calls = mocks['direct_delete_object'].call_args_list
            self.assertEqual(policy.object_ring.replicas, len(mock_calls))
            for call_args in mock_calls:
                _args, kwargs = call_args
                self.assertEqual(kwargs['headers']
                                 ['X-Backend-Storage-Policy-Index'],
                                 policy.idx)
                self.assertEqual(kwargs['headers']
                                 ['X-Timestamp'],
                                 '1429117638.86767')

            # verify calls to direct_delete_container
            self.assertEqual(mocks['direct_delete_container'].call_count, 3)
            for i, call_args in enumerate(
                    mocks['direct_delete_container'].call_args_list):
                anode = acc_nodes[i % len(acc_nodes)]
                host = '%(ip)s:%(port)s' % anode
                device = anode['device']
                headers = {
                    'X-Account-Host': host,
                    'X-Account-Partition': 'partition',
                    'X-Account-Device': device,
                    'X-Account-Override-Deleted': 'yes',
                    'X-Timestamp': '1429117639.67676',
                    'x-backend-use-replication-network': 'true',
                }
                ring = r.get_object_ring(policy.idx)
                expected = call(dict(ring.devs[i], index=i), 0, 'a', 'c',
                                headers=headers, conn_timeout=0.5,
                                response_timeout=10)
                self.assertEqual(call_args, expected)
        self.assertEqual(r.stats_objects_deleted, policy.object_ring.replicas)

    def test_reap_container_get_object_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.get_fail = True
        self.reap_obj_fail = False
        self.amount_delete_fail = 0
        self.max_delete_fail = 0
        with patch('swift.account.reaper.direct_get_container',
                   self.fake_direct_get_container), \
                patch('swift.account.reaper.direct_delete_container',
                      self.fake_direct_delete_container), \
                patch('swift.account.reaper.AccountReaper.get_container_ring',
                      self.fake_container_ring), \
                patch('swift.account.reaper.AccountReaper.reap_object',
                      self.fake_reap_object):
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertEqual(
            r.logger.statsd_client.get_increment_counts()['return_codes.4'], 1)
        self.assertEqual(r.stats_containers_deleted, 1)

    def test_reap_container_partial_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.get_fail = False
        self.timeout = False
        self.reap_obj_fail = False
        self.amount_delete_fail = 0
        self.max_delete_fail = 4
        with patch('swift.account.reaper.direct_get_container',
                   self.fake_direct_get_container), \
                patch('swift.account.reaper.direct_delete_container',
                      self.fake_direct_delete_container), \
                patch('swift.account.reaper.AccountReaper.get_container_ring',
                      self.fake_container_ring), \
                patch('swift.account.reaper.AccountReaper.reap_object',
                      self.fake_reap_object):
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertEqual(
            r.logger.statsd_client.get_increment_counts()['return_codes.4'], 4)
        self.assertEqual(r.stats_containers_possibly_remaining, 1)

    def test_reap_container_full_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.get_fail = False
        self.timeout = False
        self.reap_obj_fail = False
        self.amount_delete_fail = 0
        self.max_delete_fail = 5
        with patch('swift.account.reaper.direct_get_container',
                   self.fake_direct_get_container), \
                patch('swift.account.reaper.direct_delete_container',
                      self.fake_direct_delete_container), \
                patch('swift.account.reaper.AccountReaper.get_container_ring',
                      self.fake_container_ring), \
                patch('swift.account.reaper.AccountReaper.reap_object',
                      self.fake_reap_object):
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertEqual(
            r.logger.statsd_client.get_increment_counts()['return_codes.4'], 5)
        self.assertEqual(r.stats_containers_remaining, 1)

    def test_reap_container_get_object_timeout(self):
        r = self.init_reaper({}, fakelogger=True)
        self.get_fail = False
        self.timeout = True
        self.reap_obj_fail = False
        self.amount_delete_fail = 0
        self.max_delete_fail = 0
        with patch('swift.account.reaper.direct_get_container',
                   self.fake_direct_get_container), \
                patch('swift.account.reaper.direct_delete_container',
                      self.fake_direct_delete_container), \
                patch('swift.account.reaper.AccountReaper.get_container_ring',
                      self.fake_container_ring), \
                patch('swift.account.reaper.AccountReaper.reap_object',
                      self.fake_reap_object):
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertTrue(r.logger.get_lines_for_level(
            'error')[-1].startswith('Timeout Exception'))

    @patch('swift.account.reaper.Ring',
           lambda *args, **kwargs: unit.FakeRing())
    def test_reap_container_non_exist_policy_index(self):
        r = self.init_reaper({}, fakelogger=True)
        with patch.multiple('swift.account.reaper',
                            direct_get_container=DEFAULT,
                            direct_delete_object=DEFAULT,
                            direct_delete_container=DEFAULT) as mocks:
            headers = {'X-Backend-Storage-Policy-Index': 2}
            obj_listing = [{'name': 'o'}]

            def fake_get_container(*args, **kwargs):
                try:
                    obj = obj_listing.pop(0)
                except IndexError:
                    obj_list = []
                else:
                    obj_list = [obj]
                return headers, obj_list

            mocks['direct_get_container'].side_effect = fake_get_container
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertEqual(r.logger.get_lines_for_level('error'), [
            'ERROR: invalid storage policy index: 2'])

    def fake_reap_container(self, *args, **kwargs):
        self.called_amount += 1
        self.r.stats_containers_deleted = 1
        self.r.stats_objects_deleted = 1
        self.r.stats_containers_remaining = 1
        self.r.stats_objects_remaining = 1
        self.r.stats_containers_possibly_remaining = 1
        self.r.stats_objects_possibly_remaining = 1
        self.r.stats_return_codes[2] = \
            self.r.stats_return_codes.get(2, 0) + 1

    def test_reap_account(self):
        containers = ('c1', 'c2', 'c3', 'c4')
        broker = FakeAccountBroker(containers, debug_logger())
        self.called_amount = 0
        self.r = r = self.init_reaper({}, fakelogger=True)
        r.start_time = time.time()
        with patch('swift.account.reaper.AccountReaper.reap_container',
                   self.fake_reap_container), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring):
            nodes = r.get_account_ring().get_part_nodes()
            for container_shard, node in enumerate(nodes):
                self.assertTrue(
                    r.reap_account(broker, 'partition', nodes,
                                   container_shard=container_shard))
        self.assertEqual(self.called_amount, 4)
        info_lines = r.logger.get_lines_for_level('info')
        self.assertEqual(len(info_lines), 10)
        for start_line, stat_line in zip(*[iter(info_lines)] * 2):
            self.assertEqual(start_line, 'Beginning pass on account a')
            self.assertTrue(stat_line.find('1 containers deleted'))
            self.assertTrue(stat_line.find('1 objects deleted'))
            self.assertTrue(stat_line.find('1 containers remaining'))
            self.assertTrue(stat_line.find('1 objects remaining'))
            self.assertTrue(stat_line.find('1 containers possibly remaining'))
            self.assertTrue(stat_line.find('1 objects possibly remaining'))
            self.assertTrue(stat_line.find('return codes: 2 2xxs'))

    @patch('swift.account.reaper.Ring',
           lambda *args, **kwargs: unit.FakeRing())
    def test_basic_reap_account(self):
        self.r = reaper.AccountReaper({})
        self.r.account_ring = None
        self.r.get_account_ring()
        self.assertEqual(self.r.account_ring.replica_count, 3)
        self.assertEqual(len(self.r.account_ring.devs), 3)

    def test_reap_account_no_container(self):
        broker = FakeAccountBroker(tuple(), debug_logger())
        self.r = r = self.init_reaper({}, fakelogger=True)
        self.called_amount = 0
        r.start_time = time.time()
        with patch('swift.account.reaper.AccountReaper.reap_container',
                   self.fake_reap_container), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring):
            nodes = r.get_account_ring().get_part_nodes()
            self.assertTrue(r.reap_account(broker, 'partition', nodes))
        self.assertTrue(r.logger.get_lines_for_level(
            'info')[-1].startswith('Completed pass'))
        self.assertEqual(self.called_amount, 0)

    def test_reap_device(self):
        devices = self.prepare_data_dir()
        self.called_amount = 0
        conf = {'devices': devices}
        r = self.init_reaper(conf)
        with patch('swift.account.reaper.AccountBroker',
                   FakeAccountBroker), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring), \
                patch('swift.account.reaper.AccountReaper.reap_account',
                      self.fake_reap_account):
            r.reap_device('sda1')
        self.assertEqual(self.called_amount, 1)

    def test_reap_device_with_ts(self):
        devices = self.prepare_data_dir(ts=True)
        self.called_amount = 0
        conf = {'devices': devices}
        r = self.init_reaper(conf=conf)
        with patch('swift.account.reaper.AccountBroker',
                   FakeAccountBroker), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring), \
                patch('swift.account.reaper.AccountReaper.reap_account',
                      self.fake_reap_account):
            r.reap_device('sda1')
        self.assertEqual(self.called_amount, 0)

    def test_reap_device_with_not_my_ip(self):
        devices = self.prepare_data_dir()
        self.called_amount = 0
        conf = {'devices': devices}
        r = self.init_reaper(conf, myips=['10.10.1.2'])
        with patch('swift.account.reaper.AccountBroker',
                   FakeAccountBroker), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring), \
                patch('swift.account.reaper.AccountReaper.reap_account',
                      self.fake_reap_account):
            r.reap_device('sda1')
        self.assertEqual(self.called_amount, 0)

    def test_reap_device_with_sharding(self):
        devices = self.prepare_data_dir()
        conf = {'devices': devices}
        r = self.init_reaper(conf, myips=['10.10.10.2'])
        container_shard_used = [-1]

        def fake_reap_account(*args, **kwargs):
            container_shard_used[0] = kwargs.get('container_shard')

        with patch('swift.account.reaper.AccountBroker',
                   FakeAccountBroker), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring), \
                patch('swift.account.reaper.AccountReaper.reap_account',
                      fake_reap_account):
            r.reap_device('sda1')
        # 10.10.10.2 is second node from ring
        self.assertEqual(container_shard_used[0], 1)

    def test_reap_device_with_sharding_and_various_devices(self):
        devices = self.prepare_data_dir(device='sda2')
        conf = {'devices': devices}
        r = self.init_reaper(conf)
        container_shard_used = [-1]

        def fake_reap_account(*args, **kwargs):
            container_shard_used[0] = kwargs.get('container_shard')

        with patch('swift.account.reaper.AccountBroker',
                   FakeAccountBroker), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring), \
                patch('swift.account.reaper.AccountReaper.reap_account',
                      fake_reap_account):
            r.reap_device('sda2')

        # 10.10.10.2 is second node from ring
        self.assertEqual(container_shard_used[0], 3)

        devices = self.prepare_data_dir(device='sda3')
        conf = {'devices': devices}
        r = self.init_reaper(conf)
        container_shard_used = [-1]

        with patch('swift.account.reaper.AccountBroker',
                   FakeAccountBroker), \
                patch('swift.account.reaper.AccountReaper.get_account_ring',
                      self.fake_account_ring), \
                patch('swift.account.reaper.AccountReaper.reap_account',
                      fake_reap_account):
            r.reap_device('sda3')

        # 10.10.10.2 is second node from ring
        self.assertEqual(container_shard_used[0], 4)

    def test_reap_account_with_sharding(self):
        devices = self.prepare_data_dir()
        self.called_amount = 0
        conf = {'devices': devices}
        r = self.init_reaper(conf, myips=['10.10.10.2'], fakelogger=True)

        container_reaped = [0]

        def fake_list_containers_iter(self, *args, **kwargs):
            if not kwargs.pop('allow_reserved'):
                raise RuntimeError('Expected allow_reserved to be True!')
            if kwargs:
                raise RuntimeError('Got unexpected keyword arguments: %r' % (
                    kwargs, ))
            for container in self.containers:
                if container in self.containers_yielded:
                    continue

                yield container, None, None, None, None, None
                self.containers_yielded.append(container)

        def fake_reap_container(self, account, account_partition,
                                account_nodes, container):
            container_reaped[0] += 1

        fake_ring = FakeRing()
        fake_logger = debug_logger()
        with patch('swift.account.reaper.AccountBroker',
                   FakeAccountBroker), \
                patch(
                    'swift.account.reaper.AccountBroker.list_containers_iter',
                    fake_list_containers_iter), \
                patch('swift.account.reaper.AccountReaper.reap_container',
                      fake_reap_container):

            fake_broker = FakeAccountBroker(['c', 'd', 'e', 'f', 'g'],
                                            fake_logger)
            r.reap_account(fake_broker, 10, fake_ring.nodes, 0)
            self.assertEqual(container_reaped[0], 0)

            fake_broker = FakeAccountBroker(['c', 'd', 'e', 'f', 'g'],
                                            fake_logger)
            container_reaped[0] = 0
            r.reap_account(fake_broker, 10, fake_ring.nodes, 1)
            self.assertEqual(container_reaped[0], 1)

            container_reaped[0] = 0
            fake_broker = FakeAccountBroker(['c', 'd', 'e', 'f', 'g'],
                                            fake_logger)
            r.reap_account(fake_broker, 10, fake_ring.nodes, 2)
            self.assertEqual(container_reaped[0], 0)

            container_reaped[0] = 0
            fake_broker = FakeAccountBroker(['c', 'd', 'e', 'f', 'g'],
                                            fake_logger)
            r.reap_account(fake_broker, 10, fake_ring.nodes, 3)
            self.assertEqual(container_reaped[0], 3)

            container_reaped[0] = 0
            fake_broker = FakeAccountBroker(['c', 'd', 'e', 'f', 'g'],
                                            fake_logger)
            r.reap_account(fake_broker, 10, fake_ring.nodes, 4)
            self.assertEqual(container_reaped[0], 1)

    def test_run_once(self):
        def prepare_data_dir():
            devices_path = tempfile.mkdtemp()
            # will be deleted by teardown
            self.to_delete.append(devices_path)
            path = os.path.join(devices_path, 'sda1', DATADIR)
            os.makedirs(path)
            return devices_path

        def init_reaper(devices):
            r = reaper.AccountReaper({'devices': devices})
            return r

        devices = prepare_data_dir()
        r = init_reaper(devices)

        with patch('swift.account.reaper.AccountReaper.reap_device') as foo, \
                unit.mock_check_drive(ismount=True):
            r.run_once()
        self.assertEqual(foo.called, 1)

        with patch('swift.account.reaper.AccountReaper.reap_device') as foo, \
                unit.mock_check_drive(ismount=False):
            r.run_once()
        self.assertFalse(foo.called)

        with patch('swift.account.reaper.AccountReaper.reap_device') as foo:
            r.logger = debug_logger('test-reaper')
            r.devices = 'thisdeviceisbad'
            r.run_once()
        self.assertTrue(r.logger.get_lines_for_level(
            'error')[-1].startswith('Exception in top-level account reaper'))

    def test_run_forever(self):
        def fake_sleep(val):
            self.val = val

        def fake_random():
            return 1

        def fake_run_once():
            raise Exception('exit')

        def init_reaper():
            r = reaper.AccountReaper({'interval': 1})
            r.run_once = fake_run_once
            return r

        r = init_reaper()
        with patch('swift.account.reaper.sleep', fake_sleep):
            with patch('swift.account.reaper.random.random', fake_random):
                with self.assertRaises(Exception) as raised:
                    r.run_forever()
        self.assertEqual(self.val, 1)
        self.assertEqual(str(raised.exception), 'exit')


if __name__ == '__main__':
    unittest.main()
