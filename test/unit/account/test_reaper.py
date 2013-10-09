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
import shutil
import tempfile
import unittest

from logging import DEBUG
from mock import patch
from contextlib import nested

from swift.account import reaper
from swift.account.server import DATADIR
from swift.common.utils import normalize_timestamp
from swift.common.direct_client import ClientException


class FakeLogger(object):
    def __init__(self, *args, **kwargs):
        self.inc = {'return_codes.4': 0,
                    'return_codes.2': 0,
                    'objects_failures': 0,
                    'objects_deleted': 0,
                    'objects_remaining': 0,
                    'objects_possibly_remaining': 0,
                    'containers_failures': 0,
                    'containers_deleted': 0,
                    'containers_remaining': 0,
                    'containers_possibly_remaining': 0}
        self.exp = []

    def info(self, msg, *args):
        self.msg = msg

    def timing_since(*args, **kwargs):
        pass

    def getEffectiveLevel(self):
        return DEBUG

    def exception(self, *args):
        self.exp.append(args)

    def increment(self, key):
        self.inc[key] += 1


class FakeBroker(object):
    def __init__(self):
        self.info = {}

    def get_info(self):
        return self.info


class FakeAccountBroker():
    def __init__(self, containers):
        self.containers = containers

    def get_info(self):
        info = {'account': 'a',
                'delete_timestamp': time.time() - 10}
        return info

    def list_containers_iter(self, *args):
        for cont in self.containers:
            yield cont, None, None, None

    def is_status_deleted(self):
        return True

    def empty(self):
        return False


class FakeRing():
    def __init__(self):
        self.nodes = [{'id': '1',
                       'ip': '10.10.10.1',
                       'port': None,
                       'device': None},
                      {'id': '2',
                       'ip': '10.10.10.1',
                       'port': None,
                       'device': None},
                      {'id': '3',
                       'ip': '10.10.10.1',
                       'port': None,
                       'device': None},
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
              'port': ''}]

cont_nodes = [{'device': 'sda1',
               'ip': '',
               'port': ''},
              {'device': 'sda1',
               'ip': '',
               'port': ''},
              {'device': 'sda1',
               'ip': '',
               'port': ''}]


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

    def fake_object_ring(self):
        return FakeRing()

    def fake_direct_delete_container(self, *args, **kwargs):
        if self.amount_delete_fail < self.max_delete_fail:
            self.amount_delete_fail += 1
            raise self.myexp

    def fake_direct_get_container(self, *args, **kwargs):
        if self.get_fail:
            raise self.myexp
        objects = [{'name': 'o1'},
                   {'name': 'o2'},
                   {'name': unicode('o3')},
                   {'name': ''}]
        return None, objects

    def fake_container_ring(self):
        return FakeRing()

    def fake_reap_object(self, *args, **kwargs):
        if self.reap_obj_fail:
            raise Exception

    def prepare_data_dir(self, ts=False):
        devices_path = tempfile.mkdtemp()
        # will be deleted by teardown
        self.to_delete.append(devices_path)
        path = os.path.join(devices_path, 'sda1', DATADIR)
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

    def init_reaper(self, conf={}, myips=['10.10.10.1'], fakelogger=False):
        r = reaper.AccountReaper(conf)
        r.stats_return_codes = {}
        r.stats_containers_deleted = 0
        r.stats_containers_remaining = 0
        r.stats_containers_possibly_remaining = 0
        r.stats_objects_deleted = 0
        r.stats_objects_remaining = 0
        r.stats_objects_possibly_remaining = 0
        r.myips = myips
        if fakelogger:
            r.logger = FakeLogger()
        return r

    def fake_reap_account(self, *args, **kwargs):
        self.called_amount += 1

    def fake_account_ring(self):
        return FakeRing()

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

    def test_reap_object(self):
        r = self.init_reaper({}, fakelogger=True)
        self.amount_fail = 0
        self.max_fail = 0
        with patch('swift.account.reaper.AccountReaper.get_object_ring',
                   self.fake_object_ring):
            with patch('swift.account.reaper.direct_delete_object',
                       self.fake_direct_delete_object):
                r.reap_object('a', 'c', 'partition', cont_nodes, 'o')
        self.assertEqual(r.stats_objects_deleted, 3)

    def test_reap_object_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.amount_fail = 0
        self.max_fail = 1
        ctx = [patch('swift.account.reaper.AccountReaper.get_object_ring',
                     self.fake_object_ring),
               patch('swift.account.reaper.direct_delete_object',
                     self.fake_direct_delete_object)]
        with nested(*ctx):
            r.reap_object('a', 'c', 'partition', cont_nodes, 'o')
        self.assertEqual(r.stats_objects_deleted, 1)
        self.assertEqual(r.stats_objects_remaining, 1)
        self.assertEqual(r.stats_objects_possibly_remaining, 1)

    def test_reap_container_get_object_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.get_fail = True
        self.reap_obj_fail = False
        self.amount_delete_fail = 0
        self.max_delete_fail = 0
        ctx = [patch('swift.account.reaper.direct_get_container',
                     self.fake_direct_get_container),
               patch('swift.account.reaper.direct_delete_container',
                     self.fake_direct_delete_container),
               patch('swift.account.reaper.AccountReaper.get_container_ring',
                     self.fake_container_ring),
               patch('swift.account.reaper.AccountReaper.reap_object',
                     self.fake_reap_object)]
        with nested(*ctx):
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertEqual(r.logger.inc['return_codes.4'], 1)
        self.assertEqual(r.stats_containers_deleted, 1)

    def test_reap_container_partial_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.get_fail = False
        self.reap_obj_fail = False
        self.amount_delete_fail = 0
        self.max_delete_fail = 2
        ctx = [patch('swift.account.reaper.direct_get_container',
                     self.fake_direct_get_container),
               patch('swift.account.reaper.direct_delete_container',
                     self.fake_direct_delete_container),
               patch('swift.account.reaper.AccountReaper.get_container_ring',
                     self.fake_container_ring),
               patch('swift.account.reaper.AccountReaper.reap_object',
                     self.fake_reap_object)]
        with nested(*ctx):
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertEqual(r.logger.inc['return_codes.4'], 2)
        self.assertEqual(r.stats_containers_possibly_remaining, 1)

    def test_reap_container_full_fail(self):
        r = self.init_reaper({}, fakelogger=True)
        self.get_fail = False
        self.reap_obj_fail = False
        self.amount_delete_fail = 0
        self.max_delete_fail = 3
        ctx = [patch('swift.account.reaper.direct_get_container',
                     self.fake_direct_get_container),
               patch('swift.account.reaper.direct_delete_container',
                     self.fake_direct_delete_container),
               patch('swift.account.reaper.AccountReaper.get_container_ring',
                     self.fake_container_ring),
               patch('swift.account.reaper.AccountReaper.reap_object',
                     self.fake_reap_object)]
        with nested(*ctx):
            r.reap_container('a', 'partition', acc_nodes, 'c')
        self.assertEqual(r.logger.inc['return_codes.4'], 3)
        self.assertEqual(r.stats_containers_remaining, 1)

    def fake_reap_container(self, *args, **kwargs):
        self.called_amount += 1
        self.r.stats_containers_deleted = 1
        self.r.stats_objects_deleted = 1
        self.r.stats_containers_remaining = 1
        self.r.stats_objects_remaining = 1
        self.r.stats_containers_possibly_remaining = 1
        self.r.stats_objects_possibly_remaining = 1

    def test_reap_account(self):
        containers = ('c1', 'c2', 'c3', '')
        broker = FakeAccountBroker(containers)
        self.called_amount = 0
        self.r = r = self.init_reaper({}, fakelogger=True)
        r.start_time = time.time()
        ctx = [patch('swift.account.reaper.AccountReaper.reap_container',
                     self.fake_reap_container),
               patch('swift.account.reaper.AccountReaper.get_account_ring',
                     self.fake_account_ring)]
        with nested(*ctx):
            nodes = r.get_account_ring().get_part_nodes()
            self.assertTrue(r.reap_account(broker, 'partition', nodes))
        self.assertEqual(self.called_amount, 4)
        self.assertEqual(r.logger.msg.find('Completed pass'), 0)
        self.assertTrue(r.logger.msg.find('1 containers deleted'))
        self.assertTrue(r.logger.msg.find('1 objects deleted'))
        self.assertTrue(r.logger.msg.find('1 containers remaining'))
        self.assertTrue(r.logger.msg.find('1 objects remaining'))
        self.assertTrue(r.logger.msg.find('1 containers possibly remaining'))
        self.assertTrue(r.logger.msg.find('1 objects possibly remaining'))

    def test_reap_account_no_container(self):
        broker = FakeAccountBroker(tuple())
        self.r = r = self.init_reaper({}, fakelogger=True)
        self.called_amount = 0
        r.start_time = time.time()
        ctx = [patch('swift.account.reaper.AccountReaper.reap_container',
                     self.fake_reap_container),
               patch('swift.account.reaper.AccountReaper.get_account_ring',
                     self.fake_account_ring)]
        with nested(*ctx):
            nodes = r.get_account_ring().get_part_nodes()
            self.assertTrue(r.reap_account(broker, 'partition', nodes))
        self.assertEqual(r.logger.msg.find('Completed pass'), 0)
        self.assertEqual(self.called_amount, 0)

    def test_reap_device(self):
        devices = self.prepare_data_dir()
        self.called_amount = 0
        conf = {'devices': devices}
        r = self.init_reaper(conf)
        ctx = [patch('swift.account.reaper.AccountBroker',
                     FakeAccountBroker),
               patch('swift.account.reaper.AccountReaper.get_account_ring',
                     self.fake_account_ring),
               patch('swift.account.reaper.AccountReaper.reap_account',
                     self.fake_reap_account)]
        with nested(*ctx):
            r.reap_device('sda1')
        self.assertEqual(self.called_amount, 1)

    def test_reap_device_with_ts(self):
        devices = self.prepare_data_dir(ts=True)
        self.called_amount = 0
        conf = {'devices': devices}
        r = self.init_reaper(conf=conf)
        ctx = [patch('swift.account.reaper.AccountBroker',
                     FakeAccountBroker),
               patch('swift.account.reaper.AccountReaper.get_account_ring',
                     self.fake_account_ring),
               patch('swift.account.reaper.AccountReaper.reap_account',
                     self.fake_reap_account)]
        with nested(*ctx):
            r.reap_device('sda1')
        self.assertEqual(self.called_amount, 0)

    def test_reap_device_with_not_my_ip(self):
        devices = self.prepare_data_dir()
        self.called_amount = 0
        conf = {'devices': devices}
        r = self.init_reaper(conf, myips=['10.10.1.2'])
        ctx = [patch('swift.account.reaper.AccountBroker',
                     FakeAccountBroker),
               patch('swift.account.reaper.AccountReaper.get_account_ring',
                     self.fake_account_ring),
               patch('swift.account.reaper.AccountReaper.reap_account',
                     self.fake_reap_account)]
        with nested(*ctx):
            r.reap_device('sda1')
        self.assertEqual(self.called_amount, 0)

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

        with patch('swift.account.reaper.os.path.ismount', lambda x: True):
            with patch(
                    'swift.account.reaper.AccountReaper.reap_device') as foo:
                r.run_once()
        self.assertEqual(foo.called, 1)

        with patch('swift.account.reaper.os.path.ismount', lambda x: False):
            with patch(
                    'swift.account.reaper.AccountReaper.reap_device') as foo:
                r.run_once()
        self.assertFalse(foo.called)

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
                try:
                    r.run_forever()
                except Exception, err:
                    pass
        self.assertEqual(self.val, 1)
        self.assertEqual(str(err), 'exit')


if __name__ == '__main__':
    unittest.main()
