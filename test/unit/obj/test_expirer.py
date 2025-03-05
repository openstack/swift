# -*- coding: utf-8 -*-
# Copyright (c) 2011 OpenStack Foundation
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
import contextlib
import os
import itertools
from time import time
from unittest import main, TestCase
from test.debug_logger import debug_logger
from test.unit import FakeRing, mocked_http_conn, make_timestamp_iter
from tempfile import mkdtemp
from shutil import rmtree
from collections import defaultdict
from copy import deepcopy

from unittest import mock
import urllib.parse

from swift.common import internal_client, utils, swob
from swift.common.utils import Timestamp
from swift.common.swob import Response
from swift.obj import expirer, diskfile
from swift.obj.expirer import ExpirerConfig


def not_random():
    return 0.5


last_not_sleep = 0


def not_sleep(seconds):
    global last_not_sleep
    last_not_sleep = seconds


class FakeInternalClient(object):
    container_ring = FakeRing()

    def __init__(self, aco_dict):
        """
        :param aco_dict: A dict of account ,container, object that
            FakeInternalClient can return when each method called. Each account
            has container name dict, and each container dict has a list of
            objects in the container.
            e.g. {'account1': {
                      'container1: ['obj1', 'obj2', {'name': 'obj3'}],
                      'container2: [],
                      },
                  'account2': {},
                  'account3': {
                      'some_bad_container': UnexpectedResponse(),
                  },
                 }
            N.B. the objects entries should be the container-server JSON style
            db rows, but this fake will dynamically detect when names are given
            and wrap them for convenience.
        """
        self.aco_dict = defaultdict(dict)
        self.aco_dict.update(aco_dict)
        self._calls = []

    def get_account_info(self, account):
        acc_dict = self.aco_dict[account]
        container_count = len(acc_dict)
        obj_count = 0
        for obj_list_or_err in acc_dict.values():
            if isinstance(obj_list_or_err, Exception):
                continue
            obj_count += len(obj_list_or_err)
        return container_count, obj_count

    def iter_containers(self, account, prefix=''):
        acc_dict = self.aco_dict[account]
        return [{'name': str(container)}
                for container in sorted(acc_dict)
                if container.startswith(prefix)]

    def delete_container(self, account, container, **kwargs):
        self._calls.append(
            ('delete_container', '/'.join((account, container)), kwargs)
        )

    def iter_objects(self, account, container, **kwargs):
        self._calls.append(
            ('iter_objects', '/'.join((account, container)), kwargs)
        )
        acc_dict = self.aco_dict[account]
        obj_iter = acc_dict.get(container, [])
        if isinstance(obj_iter, Exception):
            raise obj_iter
        resp = []
        for obj in obj_iter:
            if not isinstance(obj, dict):
                obj = {'name': str(obj)}
            resp.append(obj)
        return resp

    def delete_object(self, account, container, obj, **kwargs):
        self._calls.append(
            ('delete_object', '/'.join((account, container, obj)), kwargs)
        )


class TestExpirerConfig(TestCase):
    def setUp(self):
        self.logger = debug_logger()

    @mock.patch('swift.obj.expirer.utils.hash_path', return_value=hex(101)[2:])
    def test_get_expirer_container(self, mock_hash_path):
        expirer_config = ExpirerConfig(
            {'expiring_objects_container_divisor': 200}, logger=self.logger)
        container = expirer_config.get_expirer_container(
            12340, 'a', 'c', 'o')
        self.assertEqual(container, '0000012199')
        expirer_config = ExpirerConfig(
            {'expiring_objects_container_divisor': 2000}, logger=self.logger)
        container = expirer_config.get_expirer_container(
            12340, 'a', 'c', 'o')
        self.assertEqual(container, '0000011999')

    def test_is_expected_task_container(self):
        expirer_config = ExpirerConfig({}, logger=self.logger)
        self.assertEqual('.expiring_objects', expirer_config.account_name)
        self.assertEqual(86400, expirer_config.expirer_divisor)
        self.assertEqual(100, expirer_config.task_container_per_day)
        self.assertFalse(expirer_config.is_expected_task_container(172801))
        self.assertTrue(expirer_config.is_expected_task_container(172800))
        self.assertTrue(expirer_config.is_expected_task_container(172799))
        self.assertTrue(expirer_config.is_expected_task_container(172701))
        self.assertFalse(expirer_config.is_expected_task_container(172700))
        self.assertFalse(expirer_config.is_expected_task_container(86401))
        self.assertTrue(expirer_config.is_expected_task_container(86400))
        self.assertTrue(expirer_config.is_expected_task_container(86399))
        self.assertTrue(expirer_config.is_expected_task_container(86301))
        self.assertFalse(expirer_config.is_expected_task_container(86300))

        expirer_config = ExpirerConfig({
            'expiring_objects_container_divisor': 1000,
        }, logger=self.logger)
        self.assertEqual('.expiring_objects', expirer_config.account_name)
        self.assertEqual(1000, expirer_config.expirer_divisor)
        self.assertEqual(100, expirer_config.task_container_per_day)
        self.assertFalse(expirer_config.is_expected_task_container(2001))
        self.assertTrue(expirer_config.is_expected_task_container(2000))
        self.assertTrue(expirer_config.is_expected_task_container(1999))
        self.assertTrue(expirer_config.is_expected_task_container(1901))
        self.assertFalse(expirer_config.is_expected_task_container(1900))
        self.assertFalse(expirer_config.is_expected_task_container(1001))
        self.assertTrue(expirer_config.is_expected_task_container(1000))
        self.assertTrue(expirer_config.is_expected_task_container(999))
        self.assertTrue(expirer_config.is_expected_task_container(901))
        self.assertFalse(expirer_config.is_expected_task_container(900))

    def test_get_expirer_container_legacy_config(self):
        per_divisor = 100
        expirer_config = ExpirerConfig({
            'expiring_objects_container_divisor': 86400 * 2,
        }, logger=self.logger)
        delete_at = time()
        found = set()
        for i in range(per_divisor * 10):
            c = expirer_config.get_expirer_container(
                delete_at, 'a', 'c', 'obj%s' % i)
            found.add(c)
        self.assertEqual(per_divisor, len(found))

    def test_get_expirer_config_default(self):
        conf = {}
        config = ExpirerConfig(conf, logger=self.logger)
        self.assertEqual('.expiring_objects', config.account_name)
        self.assertEqual(86400, config.expirer_divisor)
        self.assertEqual(100, config.task_container_per_day)
        self.assertFalse(self.logger.all_log_lines())

    def test_get_expirer_config_legacy(self):
        conf = {
            'expiring_objects_account_name': 'exp',
            'expiring_objects_container_divisor': '1000',
        }
        config = ExpirerConfig(conf, logger=self.logger)
        self.assertEqual('.exp', config.account_name)
        self.assertEqual(1000, config.expirer_divisor)
        self.assertEqual(100, config.task_container_per_day)
        self.assertEqual([
            'expiring_objects_container_divisor is deprecated',
            'expiring_objects_account_name is deprecated; you need to '
            'migrate to the standard .expiring_objects account',
        ], self.logger.get_lines_for_level('warning'))

    def test_get_expirer_config_legacy_no_logger_given(self):
        # verify that a logger is constructed from conf if not given
        conf = {
            'expiring_objects_account_name': 'exp',
            'expiring_objects_container_divisor': '1000',
            'log_route': 'test',
        }

        with mock.patch(
                'swift.obj.expirer.get_logger', return_value=self.logger
        ) as mock_get_logger:
            config = ExpirerConfig(conf, logger=None)
        self.assertEqual('.exp', config.account_name)
        self.assertEqual(1000, config.expirer_divisor)
        self.assertEqual(100, config.task_container_per_day)
        self.assertEqual([
            'expiring_objects_container_divisor is deprecated',
            'expiring_objects_account_name is deprecated; you need to '
            'migrate to the standard .expiring_objects account',
        ], self.logger.get_lines_for_level('warning'))
        self.assertEqual([mock.call(conf)], mock_get_logger.call_args_list)

    def test_get_expirer_account_and_container_default(self):
        expirer_config = ExpirerConfig({}, logger=self.logger)
        delete_at = time()
        account, container = \
            expirer_config.get_expirer_account_and_container(
                delete_at, 'a', 'c', 'o')
        self.assertEqual('.expiring_objects', account)
        self.assertTrue(expirer_config.is_expected_task_container(
            int(container)))

    def test_get_expirer_account_and_container_legacy(self):
        expirer_config = ExpirerConfig({
            'expiring_objects_account_name': 'exp',
            'expiring_objects_container_divisor': 1000,
        }, logger=self.logger)
        delete_at = time()
        account, container = expirer_config.get_expirer_account_and_container(
            delete_at, 'a', 'c', 'o')
        self.assertEqual('.exp', account)
        self.assertEqual(1000, expirer_config.expirer_divisor)
        self.assertEqual(100, expirer_config.task_container_per_day)
        self.assertTrue(expirer_config.is_expected_task_container(
            int(container)))

    def test_get_delete_at_nodes(self):
        container_ring = FakeRing()
        # it seems default FakeRing is very predictable
        self.assertEqual(32, container_ring._part_shift)
        self.assertEqual(3, container_ring.replicas)
        self.assertEqual(3, len(container_ring.devs))
        expirer_config = ExpirerConfig(
            {}, logger=self.logger, container_ring=container_ring)
        delete_at = time()
        part, nodes, task_container = expirer_config.get_delete_at_nodes(
            delete_at, 'a', 'c', 'o2')
        self.assertEqual(0, part)  # only one part
        self.assertEqual([
            dict(n, index=i) for i, n in enumerate(container_ring.devs)
        ], nodes)  # assigned to all ring devices
        self.assertTrue(expirer_config.is_expected_task_container(
            int(task_container)))

    def test_get_delete_at_nodes_no_ring(self):
        expirer_config = ExpirerConfig({}, logger=self.logger)
        delete_at = time()
        with self.assertRaises(RuntimeError) as ctx:
            expirer_config.get_delete_at_nodes(
                delete_at, 'a', 'c', 'o2')
        self.assertIn('ExpirerConfig', str(ctx.exception))
        self.assertIn('container_ring', str(ctx.exception))


class TestExpirerHelpers(TestCase):

    def test_add_expirer_bytes_to_ctype(self):
        self.assertEqual(
            'text/plain;swift_expirer_bytes=10',
            expirer.embed_expirer_bytes_in_ctype(
                'text/plain', {'Content-Length': 10}))
        self.assertEqual(
            'text/plain;some_foo=bar;swift_expirer_bytes=10',
            expirer.embed_expirer_bytes_in_ctype(
                'text/plain;some_foo=bar', {'Content-Length': '10'}))
        # you could probably make a case it'd be better to replace an existing
        # value if the swift_expirer_bytes key already exists in the content
        # type; but in the only case we use this function currently the content
        # type is hard coded to text/plain
        self.assertEqual(
            'text/plain;some_foo=bar;swift_expirer_bytes=10;'
            'swift_expirer_bytes=11',
            expirer.embed_expirer_bytes_in_ctype(
                'text/plain;some_foo=bar;swift_expirer_bytes=10',
                {'Content-Length': '11'}))

    def test_extract_expirer_bytes_from_ctype(self):
        self.assertEqual(10, expirer.extract_expirer_bytes_from_ctype(
            'text/plain;swift_expirer_bytes=10'))
        self.assertEqual(10, expirer.extract_expirer_bytes_from_ctype(
            'text/plain;swift_expirer_bytes=10;some_foo=bar'))

    def test_inverse_add_extract_bytes_from_ctype(self):
        ctype_bytes = [
            ('null', 0),
            ('text/plain', 10),
            ('application/octet-stream', 42),
            ('application/json', 512),
            ('gzip', 1000044),
        ]
        for ctype, expirer_bytes in ctype_bytes:
            embedded_ctype = expirer.embed_expirer_bytes_in_ctype(
                ctype, {'Content-Length': expirer_bytes})
            found_bytes = expirer.extract_expirer_bytes_from_ctype(
                embedded_ctype)
            self.assertEqual(expirer_bytes, found_bytes)

    def test_add_invalid_expirer_bytes_to_ctype(self):
        self.assertRaises(TypeError,
                          expirer.embed_expirer_bytes_in_ctype, 'nill', None)
        self.assertRaises(TypeError,
                          expirer.embed_expirer_bytes_in_ctype, 'bar', 'foo')
        self.assertRaises(KeyError,
                          expirer.embed_expirer_bytes_in_ctype, 'nill', {})
        self.assertRaises(TypeError,
                          expirer.embed_expirer_bytes_in_ctype, 'nill',
                          {'Content-Length': None})
        self.assertRaises(ValueError,
                          expirer.embed_expirer_bytes_in_ctype, 'nill',
                          {'Content-Length': 'foo'})
        # perhaps could be an error
        self.assertEqual(
            'weird/float;swift_expirer_bytes=15',
            expirer.embed_expirer_bytes_in_ctype('weird/float',
                                                 {'Content-Length': 15.9}))

    def test_embed_expirer_bytes_from_diskfile_metadata(self):
        self.logger = debug_logger('test-expirer')
        self.ts = make_timestamp_iter()
        self.devices = mkdtemp()
        self.conf = {
            'mount_check': 'false',
            'devices': self.devices,
        }
        self.df_mgr = diskfile.DiskFileManager(self.conf, logger=self.logger)
        utils.mkdirs(os.path.join(self.devices, 'sda1'))
        df = self.df_mgr.get_diskfile('sda1', '0', 'a', 'c', 'o', policy=0)

        ts = next(self.ts)
        with df.create() as writer:
            writer.write(b'test')
            writer.put({
                # wrong key/case here would KeyError
                'X-Timestamp': ts.internal,
                # wrong key/case here would cause quarantine on read
                'Content-Length': '4',
            })

        metadata = df.read_metadata()
        # the Content-Type in the metadata is irrelevant; this method is used
        # to create the content_type of an expirer queue task object
        embeded_ctype_entry = expirer.embed_expirer_bytes_in_ctype(
            'text/plain', metadata)
        self.assertEqual('text/plain;swift_expirer_bytes=4',
                         embeded_ctype_entry)

    def test_extract_missing_bytes_from_ctype(self):
        self.assertEqual(
            None, expirer.extract_expirer_bytes_from_ctype('text/plain'))
        self.assertEqual(
            None, expirer.extract_expirer_bytes_from_ctype(
                'text/plain;swift_bytes=10'))
        self.assertEqual(
            None, expirer.extract_expirer_bytes_from_ctype(
                'text/plain;bytes=21'))
        self.assertEqual(
            None, expirer.extract_expirer_bytes_from_ctype(
                'text/plain;some_foo=bar;other-baz=buz'))


class TestObjectExpirer(TestCase):
    maxDiff = None
    internal_client = None

    def get_expirer_container(self, delete_at, target_account='a',
                              target_container='c', target_object='o'):
        # the actual target a/c/o used only matters for consistent
        # distribution, tests typically only create one task container per-day,
        # but we want the task container names to be realistic
        expirer = getattr(self, 'expirer', None)
        expirer_config = expirer.expirer_config if expirer else \
            ExpirerConfig(self.conf, self.logger)
        return expirer_config.get_expirer_container(
            delete_at, target_account, target_container, target_object)

    def setUp(self):
        global not_sleep

        self.old_sleep = internal_client.sleep

        internal_client.sleep = not_sleep

        self.rcache = mkdtemp()
        self.conf = {'recon_cache_path': self.rcache}
        self.logger = debug_logger('test-expirer')

        self.ts = make_timestamp_iter()

        self.now = now = int(time())

        self.empty_time = str(now - 864000)
        self.empty_time_container = self.get_expirer_container(
            self.empty_time)
        self.past_time = str(now - 86400)
        self.past_time_container = self.get_expirer_container(
            self.past_time)
        self.just_past_time = str(now - 1)
        self.just_past_time_container = self.get_expirer_container(
            self.just_past_time)
        self.future_time = str(now + 86400)
        self.future_time_container = self.get_expirer_container(
            self.future_time)
        # Dummy task queue for test
        self._setup_fake_swift({
            '.expiring_objects': {
                # this task container will be checked
                self.empty_time_container: [],
                self.past_time_container: [
                    # tasks ready for execution
                    self.past_time + '-a0/c0/o0',
                    self.past_time + '-a1/c1/o1',
                    self.past_time + '-a2/c2/o2',
                    self.past_time + '-a3/c3/o3',
                    self.past_time + '-a4/c4/o4'],
                self.just_past_time_container: [
                    self.just_past_time + '-a5/c5/o5',
                    self.just_past_time + '-a6/c6/o6',
                    self.just_past_time + '-a7/c7/o7',
                    # task objects for unicode test
                    self.just_past_time + u'-a8/c8/o8\u2661',
                    self.just_past_time + u'-a9/c9/o9\xf8',
                    # this task will be skipped and prevent us from even
                    # *trying* to delete the container
                    self.future_time + '-a10/c10/o10'],
                # this task container will be skipped
                self.future_time_container: [
                    self.future_time + '-a11/c11/o11']}
        })

        # map of times to target object paths which should be expirerd now
        self.expired_target_paths = {
            self.past_time: [
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0', 'a1/c1/o1', 'a2/c2/o2', 'a3/c3/o3', 'a4/c4/o4',
                )
            ],
            self.just_past_time: [
                swob.wsgi_to_str(tgt) for tgt in (
                    'a5/c5/o5', 'a6/c6/o6', 'a7/c7/o7',
                    'a8/c8/o8\xe2\x99\xa1', 'a9/c9/o9\xc3\xb8',
                )
            ],
        }

    def _setup_fake_swift(self, aco_dict):
        self.fake_swift = FakeInternalClient(aco_dict)
        self.expirer = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                             swift=self.fake_swift)
        self.expirer_config = self.expirer.expirer_config

    def make_fake_ic(self, app):
        app._pipeline_final_app = mock.MagicMock()
        return internal_client.InternalClient(None, 'fake-ic', 1, app=app)

    def tearDown(self):
        rmtree(self.rcache)
        internal_client.sleep = self.old_sleep

    def test_init(self):
        with mock.patch.object(expirer, 'InternalClient',
                               return_value=self.fake_swift) as mock_ic:
            x = expirer.ObjectExpirer({}, logger=self.logger)
        self.assertEqual(mock_ic.mock_calls, [mock.call(
            '/etc/swift/object-expirer.conf', 'Swift Object Expirer', 3,
            use_replication_network=True,
            global_conf={'log_name': 'object-expirer-ic'})])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        self.assertEqual(x.expirer_config.account_name, '.expiring_objects')
        self.assertIs(x.swift, self.fake_swift)

    def test_init_default_round_robin_cache_default(self):
        conf = {}
        x = expirer.ObjectExpirer(conf, logger=self.logger,
                                  swift=self.fake_swift)
        self.assertEqual(x.round_robin_task_cache_size,
                         expirer.MAX_OBJECTS_TO_CACHE)

    def test_init_large_round_robin_cache(self):
        conf = {
            'round_robin_task_cache_size': '1000000',
        }
        x = expirer.ObjectExpirer(conf, logger=self.logger,
                                  swift=self.fake_swift)
        self.assertEqual(x.round_robin_task_cache_size, 1000000)

    def test_init_internal_client_path_from_expirer_conf(self):
        # conf read from object-expirer.conf, no internal_client_conf_path
        conf = {'__file__': '/etc/swift/object-expirer.conf'}
        with mock.patch.object(expirer, 'InternalClient',
                               return_value=self.fake_swift) as mock_ic:
            x = expirer.ObjectExpirer(conf, logger=self.logger)
        self.assertEqual(mock_ic.mock_calls, [mock.call(
            '/etc/swift/object-expirer.conf', 'Swift Object Expirer', 3,
            use_replication_network=True,
            global_conf={'log_name': 'object-expirer-ic'})])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        self.assertIs(x.swift, self.fake_swift)

    def test_init_internal_client_path_from_internal_and_other_conf(self):
        # conf read from /etc/swift/object-expirer.conf
        # -> /etc/swift/object-expirer.conf
        conf = {'__file__': '/etc/swift/object-expirer.conf',
                'internal_client_conf_path':
                    '/etc/swift/other-internal-client.conf'}
        with mock.patch.object(expirer, 'InternalClient',
                               return_value=self.fake_swift) as mock_ic:
            x = expirer.ObjectExpirer(conf, logger=self.logger)
        self.assertEqual(mock_ic.mock_calls, [mock.call(
            '/etc/swift/other-internal-client.conf', 'Swift Object Expirer', 3,
            use_replication_network=True,
            global_conf={'log_name': 'object-expirer-ic'})])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        self.assertIs(x.swift, self.fake_swift)

    def test_init_internal_client_path_from_server_conf(self):
        # conf read from object-server.conf, no internal_client_conf_path
        # specified -> /etc/swift/internal-client.conf
        conf = {'__file__': '/etc/swift/object-server.conf'}
        with mock.patch.object(expirer, 'InternalClient',
                               return_value=self.fake_swift) as mock_ic:
            x = expirer.ObjectExpirer(conf, logger=self.logger)
        self.assertEqual(mock_ic.mock_calls, [mock.call(
            '/etc/swift/internal-client.conf', 'Swift Object Expirer', 3,
            use_replication_network=True,
            global_conf={'log_name': 'object-expirer-ic'})])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        self.assertIs(x.swift, self.fake_swift)

    def test_init_internal_client_path_from_server_and_other_conf(self):
        # conf read from object-server.conf, internal_client_conf_path is
        # specified -> internal_client_conf_path value
        conf = {'__file__': '/etc/swift/object-server.conf',
                'internal_client_conf_path':
                    '/etc/swift/other-internal-client.conf'}
        with mock.patch.object(expirer, 'InternalClient',
                               return_value=self.fake_swift) as mock_ic:
            x = expirer.ObjectExpirer(conf, logger=self.logger)
        self.assertEqual(mock_ic.mock_calls, [mock.call(
            '/etc/swift/other-internal-client.conf', 'Swift Object Expirer', 3,
            use_replication_network=True,
            global_conf={'log_name': 'object-expirer-ic'})])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        self.assertIs(x.swift, self.fake_swift)

    def test_init_internal_client_path_from_other_and_other_conf(self):
        # conf read from other file, internal_client_conf_path is
        # specified -> internal_client_conf_path value
        conf = {'__file__': '/etc/swift/other-object-server.conf',
                'internal_client_conf_path':
                    '/etc/swift/other-internal-client.conf'}
        with mock.patch.object(expirer, 'InternalClient',
                               return_value=self.fake_swift) as mock_ic:
            x = expirer.ObjectExpirer(conf, logger=self.logger)
        self.assertEqual(mock_ic.mock_calls, [mock.call(
            '/etc/swift/other-internal-client.conf', 'Swift Object Expirer', 3,
            use_replication_network=True,
            global_conf={'log_name': 'object-expirer-ic'})])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        self.assertIs(x.swift, self.fake_swift)

    def test_init_internal_client_path_from_empty_conf(self):
        conf = {}
        with mock.patch.object(expirer, 'InternalClient',
                               return_value=self.fake_swift) as mock_ic:
            x = expirer.ObjectExpirer(conf, logger=self.logger)
        self.assertEqual(mock_ic.mock_calls, [mock.call(
            '/etc/swift/object-expirer.conf', 'Swift Object Expirer', 3,
            use_replication_network=True,
            global_conf={'log_name': 'object-expirer-ic'})])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        self.assertIs(x.swift, self.fake_swift)

    def test_init_internal_client_log_name(self):
        def _do_test_init_ic_log_name(conf, exp_internal_client_log_name):
            with mock.patch(
                    'swift.obj.expirer.InternalClient') \
                    as mock_ic:
                expirer.ObjectExpirer(conf)
            mock_ic.assert_called_once_with(
                '/etc/swift/object-expirer.conf',
                'Swift Object Expirer', 3,
                global_conf={'log_name': exp_internal_client_log_name},
                use_replication_network=True)

        _do_test_init_ic_log_name({}, 'object-expirer-ic')
        _do_test_init_ic_log_name({'log_name': 'my-object-expirer'},
                                  'my-object-expirer-ic')

    def test_set_process_values_from_kwargs(self):
        x = expirer.ObjectExpirer({}, swift=self.fake_swift)
        vals = {
            'processes': 5,
            'process': 1,
        }
        x.override_proceses_config_from_command_line(**vals)
        self.assertEqual(x.processes, 5)
        self.assertEqual(x.process, 1)

    def test_set_process_values_from_config(self):
        conf = {
            'processes': 5,
            'process': 1,
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.processes, 5)
        self.assertEqual(x.process, 1)

    def test_set_process_values_negative_process(self):
        vals = {
            'processes': 5,
            'process': -1,
        }
        # from config
        expected_msg = 'must be a non-negative integer'
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(vals, swift=self.fake_swift)
        self.assertIn(expected_msg, str(ctx.exception))
        # from kwargs
        x = expirer.ObjectExpirer({}, swift=self.fake_swift)
        with self.assertRaises(ValueError) as ctx:
            x.override_proceses_config_from_command_line(**vals)
        self.assertIn(expected_msg, str(ctx.exception))

    def test_set_process_values_negative_processes(self):
        vals = {
            'processes': -5,
            'process': 1,
        }
        # from config
        expected_msg = 'must be a non-negative integer'
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(vals, swift=self.fake_swift)
        self.assertIn(expected_msg, str(ctx.exception))
        # from kwargs
        x = expirer.ObjectExpirer({}, swift=self.fake_swift)
        with self.assertRaises(ValueError) as ctx:
            x.override_proceses_config_from_command_line(**vals)
        self.assertIn(expected_msg, str(ctx.exception))

    def test_set_process_values_process_greater_than_processes(self):
        vals = {
            'processes': 5,
            'process': 7,
        }
        # from config
        expected_msg = 'process must be less than processes'
        with self.assertRaises(ValueError) as ctx:
            x = expirer.ObjectExpirer(vals, swift=self.fake_swift)
        self.assertEqual(str(ctx.exception), expected_msg)
        # from kwargs
        x = expirer.ObjectExpirer({}, swift=self.fake_swift)
        with self.assertRaises(ValueError) as ctx:
            x.override_proceses_config_from_command_line(**vals)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_set_process_values_process_equal_to_processes(self):
        vals = {
            'processes': 5,
            'process': 5,
        }
        # from config
        expected_msg = 'process must be less than processes'
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(vals, swift=self.fake_swift)
        self.assertEqual(str(ctx.exception), expected_msg)
        # from kwargs
        x = expirer.ObjectExpirer({}, swift=self.fake_swift)
        with self.assertRaises(ValueError) as ctx:
            x.override_proceses_config_from_command_line(**vals)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_valid_delay_reaping(self):
        conf = {}
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.delay_reaping_times, {})

        conf = {
            'delay_reaping_a': 1.0,
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.delay_reaping_times, {('a', None): 1.0})

        # allow delay_reaping to be 0
        conf = {
            'delay_reaping_a': 0.0,
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.delay_reaping_times, {('a', None): 0.0})

        conf = {
            'delay_reaping_a/b': 0.0,
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.delay_reaping_times, {('a', 'b'): 0.0})

        # test configure multi-account delay_reaping
        conf = {
            'delay_reaping_a': 1.0,
            'delay_reaping_b': '259200.0',
            'delay_reaping_AUTH_aBC': 999,
            u'delay_reaping_AUTH_aBáC': 555,
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.delay_reaping_times, {
            ('a', None): 1.0,
            ('b', None): 259200.0,
            ('AUTH_aBC', None): 999,
            (u'AUTH_aBáC', None): 555,
        })

        # test configure multi-account delay_reaping with containers
        conf = {
            'delay_reaping_a': 10.0,
            'delay_reaping_a/test': 1.0,
            'delay_reaping_b': '259200.0',
            'delay_reaping_AUTH_aBC/test2': 999,
            u'delay_reaping_AUTH_aBáC/tést': 555,
            'delay_reaping_AUTH_test/special%0Achars%3Dare%20quoted': 777,
            'delay_reaping_AUTH_test/plus+signs+are+preserved': 888,
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.delay_reaping_times, {
            ('a', None): 10.0,
            ('a', 'test'): 1.0,
            ('b', None): 259200.0,
            ('AUTH_aBC', 'test2'): 999,
            (u'AUTH_aBáC', u'tést'): 555,
            ('AUTH_test', 'special\nchars=are quoted'): 777,
            ('AUTH_test', 'plus+signs+are+preserved'): 888,
        })

    def test_invalid_delay_reaping_keys(self):
        # there is no global delay_reaping
        conf = {
            'delay_reaping': 0.0,
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(x.delay_reaping_times, {})

        # Multiple "/" or invalid parsing
        conf = {
            'delay_reaping_A_U_TH_foo_bar/my-container_name/with/slash': 60400,
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_A_U_TH_foo_bar/my-container_name/with/slash '
            'should be in the form delay_reaping_<account> '
            'or delay_reaping_<account>/<container> '
            '(at most one "/" is allowed)',
            str(ctx.exception))

        # Can't sneak around it by escaping
        conf = {
            'delay_reaping_AUTH_test/sneaky%2fsneaky': 60400,
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_AUTH_test/sneaky%2fsneaky '
            'should be in the form delay_reaping_<account> '
            'or delay_reaping_<account>/<container> '
            '(at most one "/" is allowed)',
            str(ctx.exception))

        conf = {
            'delay_reaping_': 60400
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_ '
            'should be in the form delay_reaping_<account> '
            'or delay_reaping_<account>/<container> '
            '(at most one "/" is allowed)',
            str(ctx.exception))

        # Leading and trailing "/"
        conf = {
            'delay_reaping_/a': 60400,
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_/a '
            'should be in the form delay_reaping_<account> '
            'or delay_reaping_<account>/<container> '
            '(leading or trailing "/" is not allowed)',
            str(ctx.exception))

        conf = {
            'delay_reaping_a/': 60400,
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_a/ '
            'should be in the form delay_reaping_<account> '
            'or delay_reaping_<account>/<container> '
            '(leading or trailing "/" is not allowed)',
            str(ctx.exception))

        conf = {
            'delay_reaping_/a/c/': 60400,
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_/a/c/ '
            'should be in the form delay_reaping_<account> '
            'or delay_reaping_<account>/<container> '
            '(leading or trailing "/" is not allowed)',
            str(ctx.exception))

    def test_invalid_delay_reaping_values(self):
        # negative tests
        conf = {
            'delay_reaping_a': -1.0,
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_a must be a float greater than or equal to 0',
            str(ctx.exception))
        conf = {
            'delay_reaping_a': '-259200.0'
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_a must be a float greater than or equal to 0',
            str(ctx.exception))
        conf = {
            'delay_reaping_a': 'foo'
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_a must be a float greater than or equal to 0',
            str(ctx.exception))

        # negative tests with containers
        conf = {
            'delay_reaping_a/b': -100.0
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_a/b must be a float greater than or equal to 0',
            str(ctx.exception))
        conf = {
            'delay_reaping_a/b': '-259200.0'
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_a/b must be a float greater than or equal to 0',
            str(ctx.exception))
        conf = {
            'delay_reaping_a/b': 'foo'
        }
        with self.assertRaises(ValueError) as ctx:
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(
            'delay_reaping_a/b must be a float greater than or equal to 0',
            str(ctx.exception))

    def test_get_delay_reaping(self):
        conf = {
            'delay_reaping_a': 1.0,
            'delay_reaping_a/test': 2.0,
            'delay_reaping_b': '259200.0',
            'delay_reaping_b/a': '0.0',
            'delay_reaping_c/test': '3.0'
        }
        x = expirer.ObjectExpirer(conf, swift=self.fake_swift)
        self.assertEqual(1.0, x.get_delay_reaping('a', None))
        self.assertEqual(1.0, x.get_delay_reaping('a', 'not-test'))
        self.assertEqual(2.0, x.get_delay_reaping('a', 'test'))
        self.assertEqual(259200.0, x.get_delay_reaping('b', None))
        self.assertEqual(0.0, x.get_delay_reaping('b', 'a'))
        self.assertEqual(259200.0, x.get_delay_reaping('b', 'test'))
        self.assertEqual(3.0, x.get_delay_reaping('c', 'test'))
        self.assertEqual(0.0, x.get_delay_reaping('c', 'not-test'))
        self.assertEqual(0.0, x.get_delay_reaping('no-conf', 'test'))

    def test_init_concurrency_too_small(self):
        conf = {
            'concurrency': 0,
        }
        with self.assertRaises(ValueError):
            expirer.ObjectExpirer(conf, swift=self.fake_swift)
        conf = {
            'concurrency': -1,
        }
        with self.assertRaises(ValueError):
            expirer.ObjectExpirer(conf, swift=self.fake_swift)

    def test_process_based_concurrency(self):

        @contextlib.contextmanager
        def capture_deleted_objects(exp):
            captured = defaultdict(set)

            def mock_delete_object(target_path, delete_timestamp,
                                   task_account, task_container, task_object,
                                   is_async_delete):
                captured[task_container].add(task_object)

            with mock.patch.object(exp, 'delete_object', mock_delete_object):
                yield captured

        conf = dict(self.conf, processes=3)
        x = expirer.ObjectExpirer(
            conf, swift=self.fake_swift, logger=self.logger)

        deleted_objects = defaultdict(set)
        for i in range(3):
            x.process = i
            # reset progress so we know we don't double-up work among processes
            with capture_deleted_objects(x) as captured_deleted_objects:
                x.run_once()
            for task_container, deleted in captured_deleted_objects.items():
                self.assertFalse(deleted_objects[task_container] & deleted)
                deleted_objects[task_container] |= deleted

        self.assertEqual({
            'tasks.assigned': 10,
            'tasks.skipped': 20,
        }, self.logger.statsd_client.get_increment_counts())

        # sort for comparison
        deleted_objects = {
            con: sorted(o_set) for con, o_set in deleted_objects.items()}
        expected = {
            self.past_time_container: [
                self.past_time + '-' + target_path
                for target_path in self.expired_target_paths[self.past_time]],
            self.just_past_time_container: [
                self.just_past_time + '-' + target_path
                for target_path
                in self.expired_target_paths[self.just_past_time]]}
        self.assertEqual(deleted_objects, expected)

    def test_delete_object(self):
        x = expirer.ObjectExpirer({}, logger=self.logger,
                                  swift=self.fake_swift)
        actual_obj = 'actual_obj'
        timestamp = int(time())
        reclaim_ts = timestamp - x.reclaim_age
        account = 'account'
        container = 'container'
        obj = 'obj'

        http_exc = {
            resp_code:
                internal_client.UnexpectedResponse(
                    str(resp_code), swob.HTTPException(status=resp_code))
            for resp_code in {404, 412, 500}
        }
        exc_other = Exception()

        def check_call_to_delete_object(exc, ts, should_pop):
            x.logger.clear()
            start_reports = x.report_objects
            with mock.patch.object(x, 'delete_actual_object',
                                   side_effect=exc) as delete_actual:
                with mock.patch.object(x, 'pop_queue') as pop_queue:
                    x.delete_object(actual_obj, ts, account, container, obj,
                                    False)

            delete_actual.assert_called_once_with(actual_obj, ts, False)
            log_lines = x.logger.get_lines_for_level('error')
            if should_pop:
                pop_queue.assert_called_once_with(account, container, obj)
                self.assertEqual(start_reports + 1, x.report_objects)
                self.assertFalse(log_lines)
            else:
                self.assertFalse(pop_queue.called)
                self.assertEqual(start_reports, x.report_objects)
                self.assertEqual(1, len(log_lines))
                if isinstance(exc, internal_client.UnexpectedResponse):
                    self.assertEqual(
                        log_lines[0],
                        'Unexpected response while deleting object '
                        'account container obj: %s' % exc.resp.status_int)
                else:
                    self.assertTrue(log_lines[0].startswith(
                        'Exception while deleting object '
                        'account container obj'))

        # verify pop_queue logic on exceptions
        for exc, ts, should_pop in [(None, timestamp, True),
                                    (http_exc[404], timestamp, False),
                                    (http_exc[412], timestamp, False),
                                    (http_exc[500], reclaim_ts, False),
                                    (exc_other, reclaim_ts, False),
                                    (http_exc[404], reclaim_ts, True),
                                    (http_exc[412], reclaim_ts, True)]:

            try:
                check_call_to_delete_object(exc, ts, should_pop)
            except AssertionError as err:
                self.fail("Failed on %r at %f: %s" % (exc, ts, err))

    def test_report(self):
        x = expirer.ObjectExpirer({}, logger=self.logger,
                                  swift=self.fake_swift)

        x.report()
        self.assertEqual(x.logger.get_lines_for_level('info'), [])

        x.logger._clear()
        x.report(final=True)
        self.assertTrue(
            'completed' in str(x.logger.get_lines_for_level('info')))
        self.assertTrue(
            'so far' not in str(x.logger.get_lines_for_level('info')))

        x.logger._clear()
        x.report_last_time = time() - x.report_interval
        x.report()
        self.assertTrue(
            'completed' not in str(x.logger.get_lines_for_level('info')))
        self.assertTrue(
            'so far' in str(x.logger.get_lines_for_level('info')))

    def test_parse_task_obj(self):
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=self.fake_swift)

        def assert_parse_task_obj(task_obj, expected_delete_at,
                                  expected_account, expected_container,
                                  expected_obj):
            delete_at, account, container, obj = x.parse_task_obj(task_obj)
            self.assertEqual(delete_at, expected_delete_at)
            self.assertEqual(account, expected_account)
            self.assertEqual(container, expected_container)
            self.assertEqual(obj, expected_obj)

        assert_parse_task_obj('0000-a/c/o', 0, 'a', 'c', 'o')
        assert_parse_task_obj('0001-a/c/o', 1, 'a', 'c', 'o')
        assert_parse_task_obj('1000-a/c/o', 1000, 'a', 'c', 'o')
        assert_parse_task_obj('0000-acc/con/obj', 0, 'acc', 'con', 'obj')

    def make_task(self, task_container, delete_at, target,
                  is_async_delete=False):
        return {
            'task_account': '.expiring_objects',
            'task_container': task_container,
            'task_object': delete_at + '-' + target,
            'delete_timestamp': Timestamp(delete_at),
            'target_path': target,
            'is_async_delete': is_async_delete,
        }

    def test_round_robin_order(self):
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=self.fake_swift)

        def make_task(delete_at, target_path, is_async_delete=False):
            a, c, o = utils.split_path('/' + target_path, 1, 3, True)
            task_container = self.get_expirer_container(
                delete_at, a, c or 'c', o or 'o')
            return self.make_task(task_container, delete_at, target_path,
                                  is_async_delete=is_async_delete)

        task_con_obj_list = [
            # objects in 0000 timestamp container
            make_task('0000', 'a/c0/o0'),
            make_task('0000', 'a/c0/o1'),
            # objects in 0001 timestamp container
            make_task('0001', 'a/c1/o0'),
            make_task('0001', 'a/c1/o1'),
            # objects in 0002 timestamp container
            make_task('0002', 'a/c2/o0'),
            make_task('0002', 'a/c2/o1'),
        ]
        result = list(x.round_robin_order(task_con_obj_list))

        # sorted by popping one object to delete for each target_container
        expected = [
            make_task('0000', 'a/c0/o0'),
            make_task('0001', 'a/c1/o0'),
            make_task('0002', 'a/c2/o0'),
            make_task('0000', 'a/c0/o1'),
            make_task('0001', 'a/c1/o1'),
            make_task('0002', 'a/c2/o1'),
        ]
        self.assertEqual(expected, result)

        # task containers have some task objects with invalid target paths
        task_con_obj_list = [
            # objects in 0000 timestamp container
            make_task('0000', 'invalid0'),
            make_task('0000', 'a/c0/o0'),
            make_task('0000', 'a/c0/o1'),
            # objects in 0001 timestamp container
            make_task('0001', 'a/c1/o0'),
            make_task('0001', 'invalid1'),
            make_task('0001', 'a/c1/o1'),
            # objects in 0002 timestamp container
            make_task('0002', 'a/c2/o0'),
            make_task('0002', 'a/c2/o1'),
            make_task('0002', 'invalid2'),
        ]
        result = list(x.round_robin_order(task_con_obj_list))

        # the invalid task objects are ignored
        expected = [
            make_task('0000', 'a/c0/o0'),
            make_task('0001', 'a/c1/o0'),
            make_task('0002', 'a/c2/o0'),
            make_task('0000', 'a/c0/o1'),
            make_task('0001', 'a/c1/o1'),
            make_task('0002', 'a/c2/o1'),
        ]
        self.assertEqual(expected, result)

        # for a given target container, tasks won't necessarily all go in
        # the same timestamp container
        task_con_obj_list = [
            # objects in 0000 timestamp container
            make_task('0000', 'a/c0/o0'),
            make_task('0000', 'a/c0/o1'),
            make_task('0000', 'a/c2/o2'),
            make_task('0000', 'a/c2/o3'),
            # objects in 0001 timestamp container
            make_task('0001', 'a/c0/o2'),
            make_task('0001', 'a/c0/o3'),
            make_task('0001', 'a/c1/o0'),
            make_task('0001', 'a/c1/o1'),
            # objects in 0002 timestamp container
            make_task('0002', 'a/c2/o0'),
            make_task('0002', 'a/c2/o1'),
        ]
        result = list(x.round_robin_order(task_con_obj_list))

        # so we go around popping by *target* container, not *task* container
        expected = [
            make_task('0000', 'a/c0/o0'),
            make_task('0001', 'a/c1/o0'),
            make_task('0000', 'a/c2/o2'),
            make_task('0000', 'a/c0/o1'),
            make_task('0001', 'a/c1/o1'),
            make_task('0000', 'a/c2/o3'),
            make_task('0001', 'a/c0/o2'),
            make_task('0002', 'a/c2/o0'),
            make_task('0001', 'a/c0/o3'),
            make_task('0002', 'a/c2/o1'),
        ]
        self.assertEqual(expected, result)

        # all of the work to be done could be for different target containers
        task_con_obj_list = [
            # objects in 0000 timestamp container
            make_task('0000', 'a/c0/o'),
            make_task('0000', 'a/c1/o'),
            make_task('0000', 'a/c2/o'),
            make_task('0000', 'a/c3/o'),
            # objects in 0001 timestamp container
            make_task('0001', 'a/c4/o'),
            make_task('0001', 'a/c5/o'),
            make_task('0001', 'a/c6/o'),
            make_task('0001', 'a/c7/o'),
            # objects in 0002 timestamp container
            make_task('0002', 'a/c8/o'),
            make_task('0002', 'a/c9/o'),
        ]
        result = list(x.round_robin_order(task_con_obj_list))

        # in which case, we kind of hammer the task containers
        self.assertEqual(task_con_obj_list, result)

    def test_hash_mod(self):
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=self.fake_swift)
        mod_count = [0, 0, 0]
        for i in range(1000):
            name = 'obj%d' % i
            mod = x.hash_mod(name, 3)
            mod_count[mod] += 1

        # 1000 names are well shuffled
        self.assertGreater(mod_count[0], 300)
        self.assertGreater(mod_count[1], 300)
        self.assertGreater(mod_count[2], 300)

    def test_iter_task_accounts_to_expire(self):
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=self.fake_swift)
        results = [_ for _ in x.iter_task_accounts_to_expire()]
        self.assertEqual(results, [('.expiring_objects', 0, 1)])

        self.conf['processes'] = '2'
        self.conf['process'] = '1'
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=self.fake_swift)
        results = [_ for _ in x.iter_task_accounts_to_expire()]
        self.assertEqual(results, [('.expiring_objects', 1, 2)])

    def test_run_once_nothing_to_do(self):
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=self.fake_swift)
        x.swift = 'throw error because a string does not have needed methods'
        x.run_once()
        self.assertEqual(x.logger.get_lines_for_level('error'),
                         ["Unhandled exception: "])
        log_args, log_kwargs = x.logger.log_dict['error'][0]
        self.assertEqual(str(log_kwargs['exc_info'][1]),
                         "'str' object has no attribute 'get_account_info'")

    def test_run_once_calls_report(self):
        with mock.patch.object(self.expirer, 'pop_queue',
                               lambda a, c, o: None):
            self.expirer.run_once()
        self.assertEqual(
            self.expirer.logger.get_lines_for_level('info'), [
                'Pass beginning for task account .expiring_objects; '
                '4 possible containers; 12 possible objects',
                'Pass completed in 0s; 10 objects expired',
            ])

    def test_run_once_rate_limited(self):
        x = expirer.ObjectExpirer(
            dict(self.conf, tasks_per_second=2),
            logger=self.logger,
            swift=self.fake_swift)
        x.pop_queue = lambda a, c, o: None

        calls = []

        def fake_ratelimiter(iterator, elements_per_second):
            captured_iter = list(iterator)
            calls.append((captured_iter, elements_per_second))
            return captured_iter

        with mock.patch('swift.obj.expirer.RateLimitedIterator',
                        side_effect=fake_ratelimiter):
            x.run_once()
        self.assertEqual(calls, [([
            self.make_task(self.past_time_container, self.past_time,
                           target_path)
            for target_path in self.expired_target_paths[self.past_time]
        ] + [
            self.make_task(self.just_past_time_container, self.just_past_time,
                           target_path)
            for target_path in self.expired_target_paths[self.just_past_time]
        ], 2)])

    def test_skip_task_account_without_task_container(self):
        fake_swift = FakeInternalClient({
            # task account has no containers
            '.expiring_objects': dict()
        })
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        x.run_once()
        self.assertEqual(
            x.logger.get_lines_for_level('info'), [
                'Pass completed in 0s; 0 objects expired',
            ])

    def test_get_task_containers_unexpected_container(self):
        expected = self.get_expirer_container(time())
        unexpected = str(int(expected) - 200)
        for name in (expected, unexpected):
            self.assertTrue(name.isdigit())  # sanity

        container_list = [{'name': unexpected}, {'name': expected}]
        with mock.patch.object(self.expirer.swift, 'iter_containers',
                               return_value=container_list):
            self.assertEqual(
                self.expirer.get_task_containers_to_expire('task_account'),
                [unexpected, expected])
        self.assertEqual(self.expirer.logger.all_log_lines(), {'info': [
            'processing 1 unexpected task containers (e.g. %s)' % unexpected,
        ]})

    def test_get_task_containers_invalid_container(self):
        ok_names = ['86301', '86400']
        bad_names = ['-1', 'rogue']
        unexpected = ['86300', '86401']

        container_list = [{'name': name} for name in bad_names] + \
                         [{'name': name} for name in ok_names] + \
                         [{'name': name} for name in unexpected]
        with mock.patch.object(self.expirer.swift, 'iter_containers',
                               return_value=container_list):
            self.assertEqual(
                self.expirer.get_task_containers_to_expire('task_account'),
                ok_names + unexpected)
        lines = self.expirer.logger.get_lines_for_level('error')
        self.assertEqual(lines, [
            'skipping invalid task container: task_account/-1',
            'skipping invalid task container: task_account/rogue',
        ])
        lines = self.expirer.logger.get_lines_for_level('info')
        self.assertEqual(lines, [
            'processing 2 unexpected task containers (e.g. 86300 86401)'
        ])

    def _expirer_run_once_with_mocks(self, now=None, stub_pop_queue=None):
        """
        call self.expirer.run_once() with some things (optionally) stubbed out
        """
        now = now or time()
        # IME abuse of MagicMock's call tracking will pop OOM
        memory_efficient_noop = lambda *args, **kwargs: None
        stub_pop_queue = stub_pop_queue or memory_efficient_noop
        memory_efficient_time = lambda: now
        with mock.patch.object(self.expirer, 'pop_queue', stub_pop_queue), \
                mock.patch('eventlet.sleep', memory_efficient_noop), \
                mock.patch('swift.common.utils.timestamp.time.time',
                           memory_efficient_time), \
                mock.patch('swift.obj.expirer.time', memory_efficient_time):
            self.expirer.run_once()

    def test_run_once_with_invalid_container(self):
        now = time()
        t0 = Timestamp(now - 100000)
        t1 = Timestamp(now - 10000)
        normal_task_container = self.get_expirer_container(t0)
        self.assertTrue(normal_task_container.isdigit())
        next_task_container = self.get_expirer_container(t1)
        for name in (normal_task_container, next_task_container):
            self.assertTrue(name.isdigit())  # sanity

        strange_task_container = normal_task_container + '-crazy'
        self.assertFalse(strange_task_container.isdigit())

        task_per_container = 3
        self._setup_fake_swift({
            '.expiring_objects': {
                normal_task_container: [
                    expirer.build_task_obj(t0, 'a', 'c1', 'o%s' % i)
                    for i in range(task_per_container)
                ],
                strange_task_container: [
                    expirer.build_task_obj(t0, 'a', 'c2', 'o%s' % i)
                    for i in range(task_per_container)
                ],
                next_task_container: [
                    expirer.build_task_obj(t1, 'a', 'c3', 'o%s' % i)
                    for i in range(task_per_container)
                ],
            }
        })
        # sanity
        self.assertEqual(
            sorted(self.expirer.swift.aco_dict['.expiring_objects'].keys()), [
                normal_task_container,
                strange_task_container,
                next_task_container,
            ])
        self._expirer_run_once_with_mocks(now=now)
        # we processed all tasks in all valid containers
        self.assertEqual(task_per_container * 2, self.expirer.report_objects)

    def test_iter_task_to_expire(self):
        # In this test, all tasks are assigned to the tested expirer
        my_index = 0
        divisor = 1

        # empty container gets deleted inline
        task_account_container_list = [
            ('.expiring_objects', self.empty_time_container)
        ]
        with mock.patch.object(self.expirer.swift, 'delete_container') \
                as mock_delete_container:
            self.assertEqual(
                list(self.expirer.iter_task_to_expire(
                    task_account_container_list, my_index, divisor)),
                [])
        self.assertEqual(mock_delete_container.mock_calls, [
            mock.call('.expiring_objects', self.empty_time_container,
                      acceptable_statuses=(2, 404, 409))])
        self.assertEqual(
            {}, self.expirer.logger.statsd_client.get_increment_counts())

        # 404 (account/container list race) gets deleted inline
        task_account_container_list = [
            ('.expiring_objects', 'does-not-matter')
        ]
        with mock.patch.object(self.expirer.swift, 'delete_container') \
                as mock_delete_container:
            self.assertEqual(
                list(self.expirer.iter_task_to_expire(
                    task_account_container_list, my_index, divisor)),
                [])
        self.assertEqual(mock_delete_container.mock_calls, [
            mock.call('.expiring_objects', 'does-not-matter',
                      acceptable_statuses=(2, 404, 409))])
        self.assertEqual(
            {}, self.expirer.logger.statsd_client.get_increment_counts())

        # ready containers are processed
        task_account_container_list = [
            ('.expiring_objects', self.past_time_container)]

        expected = [
            self.make_task(self.past_time_container, self.past_time,
                           target_path)
            for target_path in self.expired_target_paths[self.past_time]]

        with mock.patch.object(self.expirer.swift, 'delete_container') \
                as mock_delete_container:
            self.assertEqual(
                list(self.expirer.iter_task_to_expire(
                    task_account_container_list, my_index, divisor)),
                expected)
        # not empty; not deleted
        self.assertEqual(mock_delete_container.mock_calls, [])
        self.assertEqual(
            {'tasks.assigned': 5},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

        # the task queue has invalid task object
        self.expirer.logger.statsd_client.clear()
        invalid_aco_dict = deepcopy(self.fake_swift.aco_dict)
        invalid_aco_dict['.expiring_objects'][self.past_time_container].insert(
            0, self.past_time + '-invalid0')
        invalid_aco_dict['.expiring_objects'][self.past_time_container].insert(
            5, self.past_time + '-invalid1')
        invalid_fake_swift = FakeInternalClient(invalid_aco_dict)
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=invalid_fake_swift)

        # but the invalid tasks are skipped
        self.assertEqual(
            list(x.iter_task_to_expire(
                task_account_container_list, my_index, divisor)),
            expected)
        self.assertEqual(
            {'tasks.assigned': 5, 'tasks.parse_errors': 2},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

        # test some of that async delete
        self.expirer.logger.statsd_client.clear()
        async_delete_aco_dict = {
            '.expiring_objects': {
                # this task container will be checked
                self.past_time_container: [
                    # tasks ready for execution
                    {'name': self.past_time + '-a0/c0/o0',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a1/c1/o1',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a2/c2/o2',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a3/c3/o3',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a4/c4/o4',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a5/c5/o5',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a6/c6/o6',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a7/c7/o7',
                     'content_type': 'application/async-deleted'},
                    # task objects for unicode test
                    {'name': self.past_time + u'-a8/c8/o8\u2661',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + u'-a9/c9/o9\xf8',
                     'content_type': 'application/async-deleted'},
                ]
            }
        }
        async_delete_fake_swift = FakeInternalClient(async_delete_aco_dict)
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=async_delete_fake_swift)

        expected = [
            self.make_task(self.past_time_container, self.past_time,
                           target_path, is_async_delete=True)
            for target_path in (
                self.expired_target_paths[self.past_time] +
                self.expired_target_paths[self.just_past_time]
            )
        ]

        found = list(x.iter_task_to_expire(
            task_account_container_list, my_index, divisor))

        self.assertEqual(expected, found)
        self.assertEqual(
            {'tasks.assigned': 10},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

    def test_iter_task_to_expire_with_skipped_tasks_single_process(self):
        # Only one task is assigned to the tested expirer
        my_index = 0
        divisor = 10
        task_account_container_list = [
            (".expiring_objects", self.past_time_container)
        ]

        expected = [
            self.make_task(
                self.past_time_container,
                self.past_time,
                self.expired_target_paths[self.past_time][0],
            )
        ]
        # Use mock of hash_mod to output predictable result.
        with mock.patch.object(self.expirer, "hash_mod",
                               side_effect=itertools.cycle(range(10))):
            self.assertEqual(
                expected,
                list(
                    self.expirer.iter_task_to_expire(
                        task_account_container_list, my_index, divisor
                    )
                )
            )
        self.assertEqual(
            {"tasks.assigned": 1, "tasks.skipped": 4},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

    def test_iter_task_to_expire_with_skipped_tasks_multi_processes(self):
        processes = 10
        task_account_container_list = [
            (".expiring_objects", self.past_time_container),
            (".expiring_objects", self.just_past_time_container),
            (".expiring_objects", self.future_time_container),
        ]

        total_tasks = 0
        for i in range(processes):
            yielded_tasks = list(
                self.expirer.iter_task_to_expire(
                    task_account_container_list, i, processes
                ))
            total_tasks += len(yielded_tasks)
        # Ten tasks, each process gets 1 on average.
        # N.B. each process may get 0 or multiple tasks, since hash_mod is
        # based on names of current time.
        self.assertEqual(10, total_tasks)

        # On average, each process was assigned 1 task and skipped 9
        self.assertEqual({
            'tasks.assigned': 10,
            'tasks.skipped': 90,
        }, self.expirer.logger.statsd_client.get_increment_counts())

    def test_iter_task_to_expire_with_skipped_and_delayed_tasks(self):
        divisor = 3
        task_account_container_list = [
            (".expiring_objects", self.past_time_container),
            (".expiring_objects", self.just_past_time_container),
        ]
        expected_task_paths = [
            path
            for path in sorted(self.expired_target_paths[self.past_time] +
                               self.expired_target_paths[self.just_past_time])
            if not path.startswith('a1')  # delayed task
        ]
        self.assertEqual(9, len(expected_task_paths))  # sanity check

        actual_task_paths = []
        proc_stats = defaultdict(int)
        for process in range(divisor):
            self.conf['delay_reaping_a1'] = 2 * 86400
            self.conf['process'] = process
            self.conf['processes'] = 3
            x = expirer.ObjectExpirer(self.conf, logger=debug_logger(),
                                      swift=self.fake_swift)
            actual_task_paths.extend(
                sorted([task['target_path'] for task in
                       x.iter_task_to_expire(
                        task_account_container_list, process, divisor)]))
            for k, v in x.logger.statsd_client.get_increment_counts().items():
                proc_stats[k] += v

        self.assertEqual(
            {"tasks.skipped": 20,
             "tasks.delayed": 1,
             "tasks.assigned": 9},
            proc_stats,
        )
        self.assertEqual(expected_task_paths, sorted(actual_task_paths))

    def test_iter_task_to_expire_with_delay_reaping(self):
        aco_dict = {
            '.expiring_objects': {
                self.past_time_container: [
                    # tasks well past ready for execution
                    {'name': self.past_time + '-a0/c0/o0'},
                    {'name': self.past_time + '-a1/c1/o1'},
                    {'name': self.past_time + '-a1/c2/o2'},
                ],
                self.just_past_time_container: [
                    # tasks only just ready for execution
                    {'name': self.just_past_time + '-a0/c0/o0'},
                    {'name': self.just_past_time + '-a1/c1/o1'},
                    {'name': self.just_past_time + '-a1/c2/o2'},
                ],
                self.future_time_container: [
                    # tasks not yet ready for execution
                    {'name': self.future_time + '-a0/c0/o0'},
                    {'name': self.future_time + '-a1/c1/o1'},
                    {'name': self.future_time + '-a1/c2/o2'},
                ],
            }
        }
        fake_swift = FakeInternalClient(aco_dict)
        # sanity, no accounts configured with delay_reaping
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        # ... we expect tasks past time to yield
        expected = [
            self.make_task(self.past_time_container, self.past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                    'a1/c1/o1',
                    'a1/c2/o2',
                )
            )
        ] + [
            self.make_task(self.just_past_time_container, self.just_past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                    'a1/c1/o1',
                    'a1/c2/o2',
                )
            )
        ]
        task_account_container_list = [
            ('.expiring_objects', self.past_time_container),
            ('.expiring_objects', self.just_past_time_container),
        ]
        observed = list(x.iter_task_to_expire(
            task_account_container_list, 0, 1))
        self.assertEqual(expected, observed)
        self.assertEqual(
            {'tasks.assigned': 6},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

        # configure delay for account a1
        self.expirer.logger.statsd_client.clear()
        self.conf['delay_reaping_a1'] = 300.0
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        # ... and we don't expect *recent* a1 tasks or future tasks
        expected = [
            self.make_task(self.past_time_container, self.past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                    'a1/c1/o1',
                    'a1/c2/o2',
                )
            )
        ] + [
            self.make_task(self.just_past_time_container, self.just_past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                )
            )
        ]
        observed = list(x.iter_task_to_expire(
            task_account_container_list, 0, 1))
        self.assertEqual(expected, observed)
        self.assertEqual(
            {'tasks.assigned': 4, 'tasks.delayed': 2},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

        # configure delay for account a1 and for account a1 and container c2
        # container a1/c2 expires expires almost immediately
        # but other containers in account a1 remain (a1/c1 and a1/c3)
        self.expirer.logger.statsd_client.clear()
        self.conf['delay_reaping_a1'] = 300.0
        self.conf['delay_reaping_a1/c2'] = 0.1
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        # ... and we don't expect *recent* a1 tasks, excluding c2
        # or future tasks
        expected = [
            self.make_task(self.past_time_container, self.past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                    'a1/c1/o1',
                    'a1/c2/o2',
                )
            )
        ] + [
            self.make_task(self.just_past_time_container, self.just_past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                    'a1/c2/o2',
                )
            )
        ]
        observed = list(x.iter_task_to_expire(
            task_account_container_list, 0, 1))
        self.assertEqual(expected, observed)
        self.assertEqual(
            {'tasks.assigned': 5, 'tasks.delayed': 1},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

        # configure delay for account a1 and for account a1 and container c2
        # container a1/c2 does not expire but others in account a1 do
        self.expirer.logger.statsd_client.clear()
        self.conf['delay_reaping_a1'] = 0.1
        self.conf['delay_reaping_a1/c2'] = 300.0
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        # ... and we don't expect *recent* a1 tasks, excluding c2
        # or future tasks
        expected = [
            self.make_task(self.past_time_container, self.past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                    'a1/c1/o1',
                    'a1/c2/o2',
                )
            )
        ] + [
            self.make_task(self.just_past_time_container, self.just_past_time,
                           target_path)
            for target_path in (
                swob.wsgi_to_str(tgt) for tgt in (
                    'a0/c0/o0',
                    'a1/c1/o1',
                )
            )
        ]
        observed = list(x.iter_task_to_expire(
            task_account_container_list, 0, 1))
        self.assertEqual(expected, observed)
        self.assertEqual(
            {'tasks.assigned': 5, 'tasks.delayed': 1},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

    def test_iter_task_to_expire_with_delay_reaping_is_async(self):
        aco_dict = {
            '.expiring_objects': {
                self.past_time_container: [
                    # tasks 86400s past ready for execution
                    {'name': self.past_time + '-a0/c0/o00',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a0/c0/o01',
                     'content_type': 'text/plain'},
                    {'name': self.past_time + '-a1/c0/o02',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a1/c0/o03',
                     'content_type': 'text/plain'},
                    {'name': self.past_time + '-a1/c1/o04',
                     'content_type': 'application/async-deleted'},
                    {'name': self.past_time + '-a1/c1/o05',
                     'content_type': 'text/plain'},
                ],
                self.just_past_time_container: [
                    # tasks only just 1s ready for execution
                    {'name': self.just_past_time + '-a0/c0/o06',
                     'content_type': 'application/async-deleted'},
                    {'name': self.just_past_time + '-a0/c0/o07',
                     'content_type': 'text/plain'},
                    {'name': self.just_past_time + '-a1/c0/o08',
                     'content_type': 'application/async-deleted'},
                    {'name': self.just_past_time + '-a1/c0/o09',
                     'content_type': 'text/plain'},
                    {'name': self.just_past_time + '-a1/c1/o10',
                     'content_type': 'application/async-deleted'},
                    {'name': self.just_past_time + '-a1/c1/o11',
                     'content_type': 'text/plain'},
                ],
                self.future_time_container: [
                    # tasks not yet ready for execution
                    {'name': self.future_time + '-a0/c0/o12',
                     'content_type': 'application/async-deleted'},
                    {'name': self.future_time + '-a0/c0/o13',
                     'content_type': 'text/plain'},
                    {'name': self.future_time + '-a1/c0/o14',
                     'content_type': 'application/async-deleted'},
                    {'name': self.future_time + '-a1/c0/o15',
                     'content_type': 'text/plain'},
                    {'name': self.future_time + '-a1/c1/o16',
                     'content_type': 'application/async-deleted'},
                    {'name': self.future_time + '-a1/c1/o17',
                     'content_type': 'text/plain'},
                ],
            }
        }
        fake_swift = FakeInternalClient(aco_dict)
        # no accounts configured with delay_reaping
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        # ... we expect all past async tasks to yield
        expected = [
            self.make_task(self.past_time_container, self.past_time,
                           swob.wsgi_to_str(tgt), is_async_delete=is_async)
            for (tgt, is_async) in (
                ('a0/c0/o00', True),
                ('a0/c0/o01', False),  # a0 no delay
                ('a1/c0/o02', True),
                # a1/c0/o03 a1 long delay
                ('a1/c1/o04', True),
                ('a1/c1/o05', False),  # c1 short delay
            )
        ] + [
            self.make_task(self.just_past_time_container, self.just_past_time,
                           swob.wsgi_to_str(tgt), is_async_delete=is_async)
            for (tgt, is_async) in (
                ('a0/c0/o06', True),
                ('a0/c0/o07', False),  # a0 no delay
                ('a1/c0/o08', True),
                # a1/c0/o09 a1 delay
                ('a1/c1/o10', True),  # async
                # a1/c1/o11 c1 delay
            )
        ]
        # configure delays
        self.conf['delay_reaping_a1'] = 86500.0
        self.conf['delay_reaping_a1/c1'] = 300.0
        x = expirer.ObjectExpirer(self.conf, logger=self.logger,
                                  swift=fake_swift)
        # ... and we still expect all past async tasks to yield
        task_account_container_list = [
            ('.expiring_objects', self.past_time_container),
            ('.expiring_objects', self.just_past_time_container),
            ('.expiring_objects', self.future_time_container),
        ]
        observed = list(x.iter_task_to_expire(
            task_account_container_list, 0, 1))
        self.assertEqual(expected, observed)

    def test_iter_task_to_expire_unexpected_response(self):
        # Test that object listing on the first container returns 503 and raise
        # UnexpectedResponse, and expect the second task container will
        # continue to be processed.

        self.expirer.swift.aco_dict['.expiring_objects'][
            self.just_past_time_container] = \
            internal_client.UnexpectedResponse(
                'Mocked error', Response(status=503))

        with mock.patch.object(self.expirer, 'pop_queue'):
            self.expirer.run_once()
        # everything but the broken container
        expected = sorted(
            p
            for c, paths in self.expired_target_paths.items()
            for p in paths
            if c != self.just_past_time
        )
        self.assertEqual(
            expected, sorted(
                path
                for method, path, kwargs in self.expirer.swift._calls
                if method == 'delete_object'
            ))
        self.assertEqual(
            [('.expiring_objects/%s' % self.empty_time_container,
              {'acceptable_statuses': (2, 404, 409)})], [
                (path, kwargs)
                for method, path, kwargs in self.expirer.swift._calls
                if method == 'delete_container'
            ])

        log_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(
            log_lines,
            ['Unexpected response while listing objects in container '
             '.expiring_objects %s: Mocked error'
             % self.just_past_time_container]
        )
        self.assertEqual(
            {'tasks.assigned': 5, 'objects': 5},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

    def test_iter_task_to_expire_exception(self):
        # Test that object listing on the first container raise Exception, and
        # expect the second task container will continue to be processed.

        self.expirer.swift.aco_dict['.expiring_objects'][
            self.just_past_time_container] = Exception('failed to connect')

        with mock.patch.object(self.expirer, 'pop_queue'):
            self.expirer.run_once()

        # everything but the broken container
        expected = sorted(
            p
            for c, paths in self.expired_target_paths.items()
            for p in paths
            if c != self.just_past_time
        )
        self.assertEqual(expected, sorted(
            path
            for method, path, kwargs in self.expirer.swift._calls
            if method == 'delete_object'
        ))
        self.assertEqual(
            [('.expiring_objects/%s' % self.empty_time_container,
              {'acceptable_statuses': (2, 404, 409)})], [
                (path, kwargs)
                for method, path, kwargs in self.expirer.swift._calls
                if method == 'delete_container'
            ])

        log_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(
            log_lines,
            ['Exception while listing objects in container '
             '.expiring_objects %s: failed to connect'
             % self.just_past_time_container]
        )
        self.assertEqual(
            {'tasks.assigned': 5, 'objects': 5},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

    def test_iter_task_to_expire_404_response_on_missing_container(self):
        # Test that object listing on a missing container returns 404 and
        # raise UnexpectedResponse, and expect ``iter_task_to_expire`` won't
        # delete this task container.
        missing_time = str(self.now - 172800)
        missing_time_container = self.get_expirer_container(missing_time)
        self.expirer.swift.aco_dict[
            '.expiring_objects'][missing_time_container] = \
            internal_client.UnexpectedResponse(
                'Mocked error', Response(status=404))

        with mock.patch.object(self.expirer, 'pop_queue'):
            self.expirer.run_once()

        # all containers iter'd
        self.assertEqual([
            ('.expiring_objects/%s' % c, {'acceptable_statuses': [2]})
            for c in [
                self.empty_time_container,
                missing_time_container,
                self.past_time_container,
                self.just_past_time_container,
            ]
        ], [
            (path, kwargs) for method, path, kwargs in
            self.expirer.swift._calls
            if method == 'iter_objects'
        ])
        # everything is still expired
        expected = sorted(
            p
            for c, paths in self.expired_target_paths.items()
            for p in paths
        )
        self.assertEqual(expected, sorted(
            path
            for method, path, kwargs in self.expirer.swift._calls
            if method == 'delete_object'
        ))
        # Only the empty task container gets deleted.
        self.assertEqual(
            [('.expiring_objects/%s' % self.empty_time_container,
              {'acceptable_statuses': (2, 404, 409)})], [
                (path, kwargs)
                for method, path, kwargs in self.expirer.swift._calls
                if method == 'delete_container'
            ])

        log_lines = self.logger.get_lines_for_level('error')
        self.assertFalse(log_lines)

    def test_iter_task_to_expire_503_response_on_container(self):
        # Test that object listing on a container returns 503 and raise
        # UnexpectedResponse, and expect ``iter_task_to_expire`` won't delete
        # this task container.
        missing_time = str(self.now - 172800)
        missing_time_container = self.get_expirer_container(missing_time)
        self.expirer.swift.aco_dict[
            '.expiring_objects'][missing_time_container] = \
            internal_client.UnexpectedResponse(
                'Mocked error', Response(status=503))

        with mock.patch.object(self.expirer, 'pop_queue'):
            self.expirer.run_once()

        # all containers iter'd
        self.assertEqual([
            ('.expiring_objects/%s' % c, {'acceptable_statuses': [2]})
            for c in [
                self.empty_time_container,
                missing_time_container,
                self.past_time_container,
                self.just_past_time_container,
            ]
        ], [
            (path, kwargs) for method, path, kwargs in
            self.expirer.swift._calls
            if method == 'iter_objects'
        ])
        # everything is still expired
        expected = sorted(
            path
            for c, paths in self.expired_target_paths.items()
            for path in paths
        )
        self.assertEqual(expected, sorted(
            path
            for method, path, kwargs in self.expirer.swift._calls
            if method == 'delete_object'
        ))
        # Only the empty task container gets deleted.
        self.assertEqual(
            [('.expiring_objects/%s' % self.empty_time_container,
              {'acceptable_statuses': (2, 404, 409)})], [
                (path, kwargs)
                for method, path, kwargs in self.expirer.swift._calls
                if method == 'delete_container'
            ])

        log_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(
            log_lines[0],
            'Unexpected response while listing objects in container '
            '.expiring_objects %s: Mocked error'
            % missing_time_container,
        )

    def test_run_once_unicode_problem(self):
        requests = []

        def capture_requests(ipaddr, port, method, path, *args, **kwargs):
            requests.append((method, path))

        # 3 DELETE requests for each 10 executed task objects to pop_queue
        code_list = [200] * 3 * 10
        with mocked_http_conn(*code_list, give_connect=capture_requests):
            self.expirer.run_once()
        self.assertEqual(len(requests), 30)

    def test_container_timestamp_break(self):
        with mock.patch.object(self.fake_swift, 'iter_objects') as mock_method:
            self.expirer.run_once()

        # iter_objects is called only for past_time, not future_time
        self.assertEqual(mock_method.call_args_list, [
            mock.call('.expiring_objects',
                      self.empty_time_container,
                      acceptable_statuses=[2]),
            mock.call('.expiring_objects',
                      self.past_time_container,
                      acceptable_statuses=[2]),
            mock.call('.expiring_objects',
                      self.just_past_time_container,
                      acceptable_statuses=[2])])

    def test_object_timestamp_break(self):
        with mock.patch.object(self.expirer, 'delete_actual_object') \
                as mock_method, \
                mock.patch.object(self.expirer, 'pop_queue'):
            self.expirer.run_once()

        # executed tasks are with past time
        self.assertEqual(
            mock_method.call_args_list,
            [mock.call(target_path, self.past_time, False)
             for target_path in self.expired_target_paths[self.past_time]] +
            [mock.call(target_path, self.just_past_time, False)
             for target_path
             in self.expired_target_paths[self.just_past_time]])

    def test_failed_delete_keeps_entry(self):
        def deliberately_blow_up(actual_obj, timestamp):
            raise Exception('failed to delete actual object')

        # any tasks are not done
        with mock.patch.object(self.expirer, 'delete_actual_object',
                               deliberately_blow_up), \
                mock.patch.object(self.expirer, 'pop_queue') as mock_method:
            self.expirer.run_once()

        # no tasks are popped from the queue
        self.assertEqual(mock_method.call_args_list, [])
        self.assertEqual(
            {'errors': 10, 'tasks.assigned': 10},
            self.expirer.logger.statsd_client.get_increment_counts())

        # all tasks are done
        self.expirer.logger.clear()
        with mock.patch.object(self.expirer, 'delete_actual_object',
                               lambda o, t, b: None), \
                mock.patch.object(self.expirer, 'pop_queue') as mock_method:
            self.expirer.run_once()

        # all tasks are popped from the queue
        self.assertEqual(
            mock_method.call_args_list,
            [mock.call('.expiring_objects', self.past_time_container,
             self.past_time + '-' + target_path)
             for target_path in self.expired_target_paths[self.past_time]] +
            [mock.call('.expiring_objects', self.just_past_time_container,
             self.just_past_time + '-' + target_path)
             for target_path
             in self.expired_target_paths[self.just_past_time]])
        self.assertEqual(
            {'objects': 10, 'tasks.assigned': 10},
            self.expirer.logger.statsd_client.get_increment_counts())

    def test_success_gets_counted(self):
        self.assertEqual(self.expirer.report_objects, 0)
        with mock.patch.object(self.expirer,
                               'round_robin_task_cache_size', 0), \
                mock.patch.object(self.expirer, 'delete_actual_object',
                                  lambda o, t, b: None), \
                mock.patch.object(self.expirer, 'pop_queue',
                                  lambda a, c, o: None):
            self.expirer.run_once()
        self.assertEqual(self.expirer.report_objects, 10)
        self.assertEqual(
            {'tasks.assigned': 10, 'objects': 10},
            self.expirer.logger.statsd_client.get_increment_counts()
        )

    def test_delete_actual_object_gets_native_string(self):
        got_str = [False]

        def delete_actual_object_test_for_string(actual_obj, timestamp,
                                                 is_async_delete):
            if isinstance(actual_obj, str):
                got_str[0] = True

        self.assertEqual(self.expirer.report_objects, 0)

        with mock.patch.object(self.expirer, 'delete_actual_object',
                               delete_actual_object_test_for_string), \
                mock.patch.object(self.expirer, 'pop_queue',
                                  lambda a, c, o: None):
            self.expirer.run_once()

        self.assertEqual(self.expirer.report_objects, 10)
        self.assertTrue(got_str[0])

    def test_failed_delete_continues_on(self):
        def fail_delete_container(*a, **kw):
            raise Exception('failed to delete container')

        def fail_delete_actual_object(actual_obj, timestamp, is_async_delete):
            if timestamp == self.just_past_time:
                raise Exception('failed to delete actual object')

        with mock.patch.object(self.fake_swift, 'delete_container',
                               fail_delete_container), \
                mock.patch.object(self.expirer, 'delete_actual_object',
                                  fail_delete_actual_object), \
                mock.patch.object(self.expirer, 'pop_queue') as mock_pop:
            self.expirer.run_once()

        error_lines = self.expirer.logger.get_lines_for_level('error')

        self.assertEqual(error_lines, [
            'Exception while deleting container .expiring_objects %s failed '
            'to delete container: ' % self.empty_time_container
        ] + [
            'Exception while deleting object %s %s %s '
            'failed to delete actual object: ' % (
                '.expiring_objects', self.just_past_time_container,
                self.just_past_time + '-' + target_path)
            for target_path in self.expired_target_paths[self.just_past_time]
        ])
        self.assertEqual(self.expirer.logger.get_lines_for_level('info'), [
            'Pass beginning for task account .expiring_objects; '
            '4 possible containers; 12 possible objects',
            'Pass completed in 0s; 5 objects expired',
        ])
        self.assertEqual(mock_pop.mock_calls, [
            mock.call('.expiring_objects', self.past_time_container,
                      self.past_time + '-' + target_path)
            for target_path in self.expired_target_paths[self.past_time]
        ])
        self.assertEqual(
            {'errors': 5, 'objects': 5, 'tasks.assigned': 10},
            self.expirer.logger.statsd_client.get_increment_counts())

    def test_run_forever_initial_sleep_random(self):
        global last_not_sleep

        def raise_system_exit():
            raise SystemExit('test_run_forever')

        interval = 1234
        x = expirer.ObjectExpirer(
            {'__file__': 'unit_test', 'interval': interval},
            swift=self.fake_swift)
        with mock.patch.object(expirer, 'random', not_random), \
                mock.patch.object(expirer, 'sleep', not_sleep), \
                self.assertRaises(SystemExit) as caught:
            x.run_once = raise_system_exit
            x.run_forever()
        self.assertEqual(str(caught.exception), 'test_run_forever')
        self.assertEqual(last_not_sleep, 0.5 * interval)

    def test_run_forever_catches_usual_exceptions(self):
        raises = [0]

        def raise_exceptions():
            raises[0] += 1
            if raises[0] < 2:
                raise Exception('exception %d' % raises[0])
            raise SystemExit('exiting exception %d' % raises[0])

        x = expirer.ObjectExpirer({}, logger=self.logger,
                                  swift=self.fake_swift)
        orig_sleep = expirer.sleep
        try:
            expirer.sleep = not_sleep
            x.run_once = raise_exceptions
            x.run_forever()
        except SystemExit as err:
            self.assertEqual(str(err), 'exiting exception 2')
        finally:
            expirer.sleep = orig_sleep
        self.assertEqual(x.logger.get_lines_for_level('error'),
                         ['Unhandled exception: '])
        log_args, log_kwargs = x.logger.log_dict['error'][0]
        self.assertEqual(str(log_kwargs['exc_info'][1]),
                         'exception 1')

    def test_run_forever_bad_process_values_config(self):
        conf = {
            'processes': -1,
            'process': -2,
            'interval': 1,
        }
        iterations = [0]

        def wrap_with_exit(orig_f, exit_after_count=3):
            def wrapped_f(*args, **kwargs):
                iterations[0] += 1
                if iterations[0] > exit_after_count:
                    raise SystemExit('that is enough for now')
                return orig_f(*args, **kwargs)
            return wrapped_f

        with self.assertRaises(ValueError) as ctx:
            # we should blow up here
            x = expirer.ObjectExpirer(conf, logger=self.logger,
                                      swift=self.fake_swift)
            x.pop_queue = lambda a, c, o: None
            x.run_once = wrap_with_exit(x.run_once)
            # at least we should hopefully we blow up here?
            x.run_forever()

        # bad config should exit immediately with ValueError
        self.assertIn('must be a non-negative integer', str(ctx.exception))

    def test_run_forever_bad_process_values_command_line(self):
        conf = {
            'interval': 1,
        }
        bad_kwargs = {
            'processes': -1,
            'process': -2,
        }
        iterations = [0]

        def wrap_with_exit(orig_f, exit_after_count=3):
            def wrapped_f(*args, **kwargs):
                iterations[0] += 1
                if iterations[0] > exit_after_count:
                    raise SystemExit('that is enough for now')
                return orig_f(*args, **kwargs)
            return wrapped_f

        with self.assertRaises(ValueError) as ctx:
            x = expirer.ObjectExpirer(conf, logger=self.logger,
                                      swift=self.fake_swift)
            x.run_once = wrap_with_exit(x.run_once)
            x.run_forever(**bad_kwargs)

        # bad command args should exit immediately with ValueError
        self.assertIn('must be a non-negative integer', str(ctx.exception))

    def test_delete_actual_object(self):
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(fake_app))
        ts = Timestamp('1234')
        x.delete_actual_object('path/to/object', ts, False)
        self.assertEqual(got_env[0]['HTTP_X_IF_DELETE_AT'], ts)
        self.assertEqual(got_env[0]['HTTP_X_TIMESTAMP'],
                         got_env[0]['HTTP_X_IF_DELETE_AT'])
        self.assertEqual(
            got_env[0]['HTTP_X_BACKEND_CLEAN_EXPIRING_OBJECT_QUEUE'], 'no')

    def test_delete_actual_object_bulk(self):
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(fake_app))
        ts = Timestamp('1234')
        x.delete_actual_object('path/to/object', ts, True)
        self.assertNotIn('HTTP_X_IF_DELETE_AT', got_env[0])
        self.assertNotIn('HTTP_X_BACKEND_CLEAN_EXPIRING_OBJECT_QUEUE',
                         got_env[0])
        self.assertEqual(got_env[0]['HTTP_X_TIMESTAMP'], ts.internal)

    def test_delete_actual_object_nourlquoting(self):
        # delete_actual_object should not do its own url quoting because
        # internal client's make_request handles that.
        got_env = [None]

        def fake_app(env, start_response):
            got_env[0] = env
            start_response('204 No Content', [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(fake_app))
        ts = Timestamp('1234')
        x.delete_actual_object('path/to/object name', ts, False)
        self.assertEqual(got_env[0]['HTTP_X_IF_DELETE_AT'], ts)
        self.assertEqual(got_env[0]['HTTP_X_TIMESTAMP'],
                         got_env[0]['HTTP_X_IF_DELETE_AT'])
        self.assertEqual(got_env[0]['PATH_INFO'], '/v1/path/to/object name')

    def test_delete_actual_object_async_returns_expected_error(self):
        def do_test(test_status, should_raise):
            calls = [0]

            def fake_app(env, start_response):
                calls[0] += 1
                calls.append(env['PATH_INFO'])
                start_response(test_status, [('Content-Length', '0')])
                return []

            x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(fake_app))
            ts = Timestamp('1234')
            if should_raise:
                with self.assertRaises(internal_client.UnexpectedResponse):
                    x.delete_actual_object('path/to/object', ts, True)
            else:
                x.delete_actual_object('path/to/object', ts, True)
            self.assertEqual(calls[0], 1, calls)

        # object was deleted and tombstone reaped
        do_test('404 Not Found', False)
        # object was overwritten *after* the original delete, or
        # object was deleted but tombstone still exists, or ...
        do_test('409 Conflict', False)
        # Anything else, raise
        do_test('400 Bad Request', True)

    def test_delete_actual_object_returns_expected_error(self):
        def do_test(test_status, should_raise):
            calls = [0]

            def fake_app(env, start_response):
                calls[0] += 1
                start_response(test_status, [('Content-Length', '0')])
                return []

            x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(fake_app))
            ts = Timestamp('1234')
            if should_raise:
                with self.assertRaises(internal_client.UnexpectedResponse):
                    x.delete_actual_object('path/to/object', ts, False)
            else:
                x.delete_actual_object('path/to/object', ts, False)
            self.assertEqual(calls[0], 1)

        # object was deleted and tombstone reaped
        do_test('404 Not Found', True)
        # object was overwritten *after* the original expiration, or
        do_test('409 Conflict', False)
        # object was deleted but tombstone still exists, or
        # object was overwritten ahead of the original expiration, or
        # object was POSTed to with a new (or no) expiration, or ...
        do_test('412 Precondition Failed', True)

    def test_delete_actual_object_does_not_handle_odd_stuff(self):

        def fake_app(env, start_response):
            start_response(
                '503 Internal Server Error',
                [('Content-Length', '0')])
            return []

        x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(fake_app))
        exc = None
        try:
            x.delete_actual_object('path/to/object', Timestamp('1234'), False)
        except Exception as err:
            exc = err
        finally:
            pass
        self.assertEqual(503, exc.resp.status_int)

    def test_delete_actual_object_quotes(self):
        name = 'this name/should get/quoted'
        timestamp = Timestamp('1366063156.863045')
        x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(self.fake_swift))
        x.swift.make_request = mock.Mock()
        x.swift.make_request.return_value.status_int = 204
        x.swift.make_request.return_value.app_iter = []
        x.delete_actual_object(name, timestamp, False)
        self.assertEqual(x.swift.make_request.call_count, 1)
        self.assertEqual(x.swift.make_request.call_args[0][1],
                         '/v1/' + urllib.parse.quote(name))

    def test_delete_actual_object_queue_cleaning(self):
        name = 'acc/cont/something'
        timestamp = Timestamp('1515544858.80602')
        x = expirer.ObjectExpirer({}, swift=self.make_fake_ic(self.fake_swift))
        x.swift.make_request = mock.MagicMock(
            return_value=swob.HTTPNoContent())
        x.delete_actual_object(name, timestamp, False)
        self.assertEqual(x.swift.make_request.call_count, 1)
        header = 'X-Backend-Clean-Expiring-Object-Queue'
        self.assertEqual(
            x.swift.make_request.call_args[0][2].get(header),
            'no')

    def test_pop_queue(self):
        x = expirer.ObjectExpirer({}, logger=self.logger,
                                  swift=FakeInternalClient({}))
        requests = []

        def capture_requests(ipaddr, port, method, path, *args, **kwargs):
            requests.append((method, path))
        with mocked_http_conn(
                200, 200, 200, give_connect=capture_requests) as fake_conn:
            x.pop_queue('a', 'c', 'o')
            with self.assertRaises(StopIteration):
                next(fake_conn.code_iter)
        for method, path in requests:
            self.assertEqual(method, 'DELETE')
            device, part, account, container, obj = utils.split_path(
                path, 5, 5, True)
            self.assertEqual(account, 'a')
            self.assertEqual(container, 'c')
            self.assertEqual(obj, 'o')

    def test_build_task_obj_round_trip(self):
        ts = next(self.ts)
        a = 'a1'
        c = 'c2'
        o = 'obj1'
        args = (ts, a, c, o)
        self.assertEqual(args, expirer.parse_task_obj(
            expirer.build_task_obj(ts, a, c, o)))
        self.assertEqual(args, expirer.parse_task_obj(
            expirer.build_task_obj(ts, a, c, o, high_precision=True)))

        ts = Timestamp(next(self.ts), delta=1234)
        a = u'\N{SNOWMAN}'
        c = u'\N{SNOWFLAKE}'
        o = u'\U0001F334'
        args = (ts, a, c, o)
        self.assertNotEqual(args, expirer.parse_task_obj(
            expirer.build_task_obj(ts, a, c, o)))
        self.assertEqual(args, expirer.parse_task_obj(
            expirer.build_task_obj(ts, a, c, o, high_precision=True)))


if __name__ == '__main__':
    main()
