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

import errno
import fcntl
import json
from contextlib import contextmanager
import logging
from textwrap import dedent

from unittest import mock
import os
import pickle
import shutil
import tempfile
import time
import unittest
import uuid

from io import StringIO

from swift.cli import relinker
from swift.common import ring, utils
from swift.common import storage_policy
from swift.common.exceptions import PathNotDir
from swift.common.storage_policy import (
    StoragePolicy, StoragePolicyCollection, POLICIES, ECStoragePolicy,
    get_policy_string)

from swift.obj.diskfile import write_metadata, DiskFileRouter, \
    DiskFileManager, relink_paths, BaseDiskFileManager

from test.debug_logger import debug_logger
from test.unit import skip_if_no_xattrs, DEFAULT_TEST_EC_TYPE, \
    patch_policies


PART_POWER = 8


class TestRelinker(unittest.TestCase):

    maxDiff = None

    def setUp(self):
        skip_if_no_xattrs()
        self.logger = debug_logger()
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        self.recon_cache_path = os.path.join(self.testdir, 'cache')
        self.recon_cache = os.path.join(self.recon_cache_path,
                                        'relinker.recon')
        shutil.rmtree(self.testdir, ignore_errors=True)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        os.mkdir(self.recon_cache_path)

        self.rb = ring.RingBuilder(PART_POWER, 6.0, 1)

        for i in range(6):
            ip = "127.0.0.%s" % i
            self.rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 1,
                             'ip': ip, 'port': 10000, 'device': 'sda1'})
        self.rb.rebalance(seed=1)

        self.conf_file = os.path.join(self.testdir, 'relinker.conf')
        self._setup_config()

        self.existing_device = 'sda1'
        os.mkdir(os.path.join(self.devices, self.existing_device))
        self.objects = os.path.join(self.devices, self.existing_device,
                                    'objects')
        self.policy = StoragePolicy(0, 'platinum', True)
        storage_policy._POLICIES = StoragePolicyCollection([self.policy])
        self._setup_object(policy=self.policy)

        patcher = mock.patch('swift.cli.relinker.hubs')
        self.mock_hubs = patcher.start()
        self.addCleanup(patcher.stop)

    def _setup_config(self):
        config = """
        [DEFAULT]
        swift_dir = {swift_dir}
        devices = {devices}
        mount_check = {mount_check}

        [object-relinker]
        recon_cache_path = {recon_cache_path}
        # update every chance we get!
        stats_interval = 0
        """.format(
            swift_dir=self.testdir,
            devices=self.devices,
            mount_check=False,
            recon_cache_path=self.recon_cache_path,
        )
        with open(self.conf_file, 'w') as f:
            f.write(dedent(config))

    def _get_object_name(self, condition=None):
        attempts = []
        for _ in range(50):
            account = 'a'
            container = 'c'
            obj = 'o-' + str(uuid.uuid4())
            _hash = utils.hash_path(account, container, obj)
            part = utils.get_partition_for_hash(_hash, PART_POWER)
            next_part = utils.get_partition_for_hash(_hash, PART_POWER + 1)
            obj_path = os.path.join(os.path.sep, account, container, obj)
            # There's 1/512 chance that both old and new parts will be 0;
            # that's not a terribly interesting case, as there's nothing to do
            attempts.append((part, next_part, 2**PART_POWER))
            if (part != next_part and
                    (condition(part) if condition else True)):
                break
        else:
            self.fail('Failed to setup object satisfying test preconditions %s'
                      % attempts)
        return _hash, part, next_part, obj_path

    def _create_object(self, policy, part, _hash, ext='.data'):
        objects_dir = os.path.join(self.devices, self.existing_device,
                                   get_policy_string('objects', policy))
        shutil.rmtree(objects_dir, ignore_errors=True)
        os.mkdir(objects_dir)
        objdir = os.path.join(objects_dir, str(part), _hash[-3:], _hash)
        os.makedirs(objdir)
        timestamp = utils.Timestamp.now()
        filename = timestamp.internal + ext
        objname = os.path.join(objdir, filename)
        with open(objname, "wb") as dummy:
            dummy.write(b"Hello World!")
            write_metadata(dummy,
                           {'name': self.obj_path, 'Content-Length': '12'})
        return objdir, filename, timestamp

    def _setup_object(self, condition=None, policy=None, ext='.data'):
        policy = policy or self.policy
        _hash, part, next_part, obj_path = self._get_object_name(condition)
        self._hash = _hash
        self.part = part
        self.next_part = next_part
        self.obj_path = obj_path
        objects_dir = os.path.join(self.devices, self.existing_device,
                                   get_policy_string('objects', policy))

        self.objdir, self.object_fname, self.obj_ts = self._create_object(
            policy, part, _hash, ext)

        self.objname = os.path.join(self.objdir, self.object_fname)
        self.part_dir = os.path.join(objects_dir, str(self.part))
        self.suffix = self._hash[-3:]
        self.suffix_dir = os.path.join(self.part_dir, self.suffix)
        self.next_part_dir = os.path.join(objects_dir, str(self.next_part))
        self.next_suffix_dir = os.path.join(self.next_part_dir, self.suffix)
        self.expected_dir = os.path.join(self.next_suffix_dir, self._hash)
        self.expected_file = os.path.join(self.expected_dir, self.object_fname)

    def _make_link(self, filename, part_power):
        # make a file in the older part_power location and link it to a file in
        # the next part power location
        new_filepath = os.path.join(self.expected_dir, filename)
        older_filepath = utils.replace_partition_in_path(
            self.devices, new_filepath, part_power)
        os.makedirs(os.path.dirname(older_filepath))
        with open(older_filepath, 'w') as fd:
            fd.write(older_filepath)
        os.makedirs(self.expected_dir)
        os.link(older_filepath, new_filepath)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(older_filepath, fd.read())  # sanity check
        return older_filepath, new_filepath

    def _save_ring(self, policies=POLICIES):
        self.rb._ring = None
        rd = self.rb.get_ring()
        for policy in policies:
            rd.save(os.path.join(
                self.testdir, '%s.ring.gz' % policy.ring_name))
            # Enforce ring reloading in relinker
            policy.object_ring = None

    def tearDown(self):
        shutil.rmtree(self.testdir, ignore_errors=True)
        storage_policy.reload_storage_policies()

    @contextmanager
    def _mock_listdir(self):
        orig_listdir = utils.listdir

        def mocked(path):
            if path == self.objects:
                raise OSError
            return orig_listdir(path)

        with mock.patch('swift.common.utils.listdir', mocked):
            yield

    @contextmanager
    def _mock_relinker(self):
        with mock.patch.object(relinker.logging, 'getLogger',
                               return_value=self.logger), \
                mock.patch.object(relinker, 'get_logger',
                                  return_value=self.logger), \
                mock.patch('swift.cli.relinker.DEFAULT_RECON_CACHE_PATH',
                           self.recon_cache_path):
            yield

    def test_workers_parent(self):
        os.mkdir(os.path.join(self.devices, 'sda2'))
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()
        pids = {
            2: 0,
            3: 0,
        }

        def mock_wait():
            return pids.popitem()

        with mock.patch('os.fork', side_effect=list(pids.keys())), \
                mock.patch('os.wait', mock_wait):
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--workers', '2',
                '--skip-mount',
            ]))
        self.assertEqual(pids, {})

    def test_workers_parent_bubbles_up_errors(self):
        def do_test(wait_result, msg):
            pids = {
                2: 0,
                3: 0,
                4: 0,
                5: wait_result,
                6: 0,
            }

            with mock.patch('os.fork', side_effect=list(pids.keys())), \
                    mock.patch('os.wait', lambda: pids.popitem()), \
                    self._mock_relinker():
                self.assertEqual(1, relinker.main([
                    'cleanup',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                ]))
            self.assertEqual(pids, {})
            self.assertEqual([], self.logger.get_lines_for_level('error'))
            warning_lines = self.logger.get_lines_for_level('warning')
            self.assertTrue(
                warning_lines[0].startswith('Worker (pid=5, devs='))
            self.assertTrue(
                warning_lines[0].endswith(msg),
                'Expected log line to end with %r; got %r'
                % (msg, warning_lines[0]))
            self.assertFalse(warning_lines[1:])
            info_lines = self.logger.get_lines_for_level('info')
            self.assertEqual(2, len(info_lines))
            self.assertIn('Starting relinker (cleanup=True) using 5 workers:',
                          info_lines[0])
            self.assertIn('Finished relinker (cleanup=True):',
                          info_lines[1])
            print(info_lines)
            self.logger.clear()

        os.mkdir(os.path.join(self.devices, 'sda2'))
        os.mkdir(os.path.join(self.devices, 'sda3'))
        os.mkdir(os.path.join(self.devices, 'sda4'))
        os.mkdir(os.path.join(self.devices, 'sda5'))
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()
        # signals get the low bits
        do_test(9, 'exited in 0.0s after receiving signal: 9')
        # exit codes get the high
        do_test(1 << 8, 'completed in 0.0s with errors')
        do_test(42 << 8, 'exited in 0.0s with unexpected status 42')

    def test_workers_children(self):
        os.mkdir(os.path.join(self.devices, 'sda2'))
        os.mkdir(os.path.join(self.devices, 'sda3'))
        os.mkdir(os.path.join(self.devices, 'sda4'))
        os.mkdir(os.path.join(self.devices, 'sda5'))
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()

        calls = []

        def fake_fork():
            calls.append('fork')
            return 0

        def fake_run(self):
            calls.append(('run', self.device_list))
            return 0

        def fake_exit(status):
            calls.append(('exit', status))

        with mock.patch('os.fork', fake_fork), \
                mock.patch('os._exit', fake_exit), \
                mock.patch('swift.cli.relinker.Relinker.run', fake_run):
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--workers', '2',
                '--skip-mount',
            ]))
        self.assertEqual([
            'fork',
            ('run', ['sda1', 'sda3', 'sda5']),
            ('exit', 0),
            'fork',
            ('run', ['sda2', 'sda4']),
            ('exit', 0),
        ], calls)

        # test too many workers
        calls = []

        with mock.patch('os.fork', fake_fork), \
                mock.patch('os._exit', fake_exit), \
                mock.patch('swift.cli.relinker.Relinker.run', fake_run):
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--workers', '6',
                '--skip-mount',
            ]))
        self.assertEqual([
            'fork',
            ('run', ['sda1']),
            ('exit', 0),
            'fork',
            ('run', ['sda2']),
            ('exit', 0),
            'fork',
            ('run', ['sda3']),
            ('exit', 0),
            'fork',
            ('run', ['sda4']),
            ('exit', 0),
            'fork',
            ('run', ['sda5']),
            ('exit', 0),
        ], calls)

    def _do_test_relinker_drop_privileges(self, command):
        @contextmanager
        def do_mocks():
            # attach mocks to call_capture so that call order can be asserted
            call_capture = mock.Mock()
            mod = 'swift.cli.relinker.'
            with mock.patch(mod + 'Relinker') as mock_relinker, \
                    mock.patch(mod + 'drop_privileges') as mock_dp, \
                    mock.patch(mod + 'os.listdir',
                               return_value=['sda', 'sdb']):
                mock_relinker.return_value.run.return_value = 0
                call_capture.attach_mock(mock_dp, 'drop_privileges')
                call_capture.attach_mock(mock_relinker, 'run')
                yield call_capture

        # no user option
        with do_mocks() as capture:
            self.assertEqual(0, relinker.main([command, '--workers', '0']))
        self.assertEqual([mock.call.run(mock.ANY, mock.ANY, ['sda', 'sdb'],
                                        do_cleanup=(command == 'cleanup'))],
                         capture.method_calls)

        # cli option --user
        with do_mocks() as capture:
            self.assertEqual(0, relinker.main([command, '--user', 'cli_user',
                                               '--workers', '0']))
        self.assertEqual([('drop_privileges', ('cli_user',), {}),
                          mock.call.run(mock.ANY, mock.ANY, ['sda', 'sdb'],
                                        do_cleanup=(command == 'cleanup'))],
                         capture.method_calls)

        # cli option --user takes precedence over conf file user
        with do_mocks() as capture:
            with mock.patch('swift.cli.relinker.readconf',
                            return_value={'user': 'conf_user'}):
                self.assertEqual(0, relinker.main([command, 'conf_file',
                                                   '--user', 'cli_user',
                                                   '--workers', '0']))
        self.assertEqual([('drop_privileges', ('cli_user',), {}),
                          mock.call.run(mock.ANY, mock.ANY, ['sda', 'sdb'],
                                        do_cleanup=(command == 'cleanup'))],
                         capture.method_calls)

        # conf file user
        with do_mocks() as capture:
            with mock.patch('swift.cli.relinker.readconf',
                            return_value={'user': 'conf_user',
                                          'workers': '0'}):
                self.assertEqual(0, relinker.main([command, 'conf_file']))
        self.assertEqual([('drop_privileges', ('conf_user',), {}),
                          mock.call.run(mock.ANY, mock.ANY, ['sda', 'sdb'],
                                        do_cleanup=(command == 'cleanup'))],
                         capture.method_calls)

    def test_relinker_drop_privileges(self):
        self._do_test_relinker_drop_privileges('relink')
        self._do_test_relinker_drop_privileges('cleanup')

    def _do_test_relinker_files_per_second(self, command):
        # no files per second
        with mock.patch('swift.cli.relinker.RateLimitedIterator') as it:
            self.assertEqual(0, relinker.main([
                command,
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        it.assert_not_called()

        # zero files per second
        with mock.patch('swift.cli.relinker.RateLimitedIterator') as it:
            self.assertEqual(0, relinker.main([
                command,
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
                '--files-per-second', '0'
            ]))
        it.assert_not_called()

        # positive files per second
        locations = iter([])
        with mock.patch('swift.cli.relinker.audit_location_generator',
                        return_value=locations):
            with mock.patch('swift.cli.relinker.RateLimitedIterator') as it:
                self.assertEqual(0, relinker.main([
                    command,
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                    '--files-per-second', '1.23'
                ]))
        it.assert_called_once_with(locations, 1.23)

        # negative files per second
        err = StringIO()
        with mock.patch('sys.stderr', err):
            with self.assertRaises(SystemExit) as cm:
                relinker.main([
                    command,
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                    '--files-per-second', '-1'
                ])
        self.assertEqual(2, cm.exception.code)  # NB exit code 2 from argparse
        self.assertIn('--files-per-second: invalid non_negative_float value',
                      err.getvalue())

    def test_relink_files_per_second(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        self._do_test_relinker_files_per_second('relink')

    def test_cleanup_files_per_second(self):
        self._common_test_cleanup()
        self._do_test_relinker_files_per_second('cleanup')

    @patch_policies(
        [StoragePolicy(0, name='gold', is_default=True),
         ECStoragePolicy(1, name='platinum', ec_type=DEFAULT_TEST_EC_TYPE,
                         ec_ndata=4, ec_nparity=2)],
        fake_ring_args=[{}, {}])
    def test_conf_file(self):
        config = """
        [DEFAULT]
        swift_dir = %s
        devices = /test/node
        mount_check = false
        reclaim_age = 5184000

        [object-relinker]
        log_level = WARNING
        log_name = test-relinker
        """ % self.testdir
        conf_file = os.path.join(self.testdir, 'relinker.conf')
        with open(conf_file, 'w') as f:
            f.write(dedent(config))

        # cite conf file on command line
        with mock.patch('swift.cli.relinker.Relinker') as mock_relinker:
            relinker.main(['relink', conf_file, '--device', 'sdx', '--debug'])
        exp_conf = {
            '__file__': mock.ANY,
            'swift_dir': self.testdir,
            'devices': '/test/node',
            'mount_check': False,
            'reclaim_age': '5184000',
            'files_per_second': 0.0,
            'log_name': 'test-relinker',
            'log_level': 'DEBUG',
            'policies': POLICIES,
            'workers': 'auto',
            'partitions': set(),
            'recon_cache_path': '/var/cache/swift',
            'stats_interval': 300.0,
        }
        mock_relinker.assert_called_once_with(
            exp_conf, mock.ANY, ['sdx'], do_cleanup=False)
        logger = mock_relinker.call_args[0][1]
        # --debug overrides conf file
        self.assertEqual(logging.DEBUG, logger.getEffectiveLevel())
        self.assertEqual('test-relinker', logger.logger.name)

        # check the conf is passed to DiskFileRouter
        self._save_ring()
        with mock.patch('swift.cli.relinker.diskfile.DiskFileRouter',
                        side_effect=DiskFileRouter) as mock_dfr:
            relinker.main(['relink', conf_file, '--device', 'sdx', '--debug'])
        mock_dfr.assert_called_once_with(exp_conf, mock.ANY)

        # flip mount_check, no --debug...
        config = """
        [DEFAULT]
        swift_dir = test/swift/dir
        devices = /test/node
        mount_check = true

        [object-relinker]
        log_level = WARNING
        log_name = test-relinker
        files_per_second = 11.1
        recon_cache_path = /var/cache/swift-foo
        stats_interval = 111
        """
        with open(conf_file, 'w') as f:
            f.write(dedent(config))
        with mock.patch('swift.cli.relinker.Relinker') as mock_relinker:
            relinker.main(['relink', conf_file, '--device', 'sdx'])
        mock_relinker.assert_called_once_with({
            '__file__': mock.ANY,
            'swift_dir': 'test/swift/dir',
            'devices': '/test/node',
            'mount_check': True,
            'files_per_second': 11.1,
            'log_name': 'test-relinker',
            'log_level': 'WARNING',
            'policies': POLICIES,
            'partitions': set(),
            'workers': 'auto',
            'recon_cache_path': '/var/cache/swift-foo',
            'stats_interval': 111.0,
        }, mock.ANY, ['sdx'], do_cleanup=False)
        logger = mock_relinker.call_args[0][1]
        self.assertEqual(logging.WARNING, logger.getEffectiveLevel())
        self.assertEqual('test-relinker', logger.logger.name)

        # override with cli options...
        logger = debug_logger()
        with mock.patch('swift.cli.relinker.Relinker') as mock_relinker:
            with mock.patch('swift.cli.relinker.get_logger',
                            return_value=logger):
                relinker.main([
                    'relink', conf_file, '--device', 'sdx', '--debug',
                    '--swift-dir', 'cli-dir', '--devices', 'cli-devs',
                    '--skip-mount-check', '--files-per-second', '2.2',
                    '--policy', '1', '--partition', '123',
                    '--partition', '123', '--partition', '456',
                    '--workers', '2',
                    '--stats-interval', '222',
                ])
        mock_relinker.assert_called_once_with({
            '__file__': mock.ANY,
            'swift_dir': 'cli-dir',
            'devices': 'cli-devs',
            'mount_check': False,
            'files_per_second': 2.2,
            'log_level': 'DEBUG',
            'log_name': 'test-relinker',
            'policies': {POLICIES[1]},
            'partitions': {123, 456},
            'workers': 2,
            'recon_cache_path': '/var/cache/swift-foo',
            'stats_interval': 222.0,
        }, mock.ANY, ['sdx'], do_cleanup=False)

        with mock.patch('swift.cli.relinker.Relinker') as mock_relinker, \
                mock.patch('logging.basicConfig') as mock_logging_config:
            relinker.main(['relink', '--device', 'sdx',
                           '--swift-dir', 'cli-dir', '--devices', 'cli-devs',
                           '--skip-mount-check'])
        mock_relinker.assert_called_once_with({
            'swift_dir': 'cli-dir',
            'devices': 'cli-devs',
            'mount_check': False,
            'files_per_second': 0.0,
            'log_level': 'INFO',
            'policies': POLICIES,
            'partitions': set(),
            'workers': 'auto',
            'recon_cache_path': '/var/cache/swift',
            'stats_interval': 300.0,
        }, mock.ANY, ['sdx'], do_cleanup=False)
        mock_logging_config.assert_called_once_with(
            format='%(message)s', level=logging.INFO, filename=None)

        with mock.patch('swift.cli.relinker.Relinker') as mock_relinker, \
                mock.patch('logging.basicConfig') as mock_logging_config:
            relinker.main([
                'relink', '--debug',
                '--swift-dir', 'cli-dir',
                '--devices', 'cli-devs',
                '--device', 'sdx',
                '--skip-mount-check',
                '--policy', '0',
                '--policy', '1',
                '--policy', '0',
            ])
        mock_relinker.assert_called_once_with({
            'swift_dir': 'cli-dir',
            'devices': 'cli-devs',
            'mount_check': False,
            'files_per_second': 0.0,
            'log_level': 'DEBUG',
            'policies': set(POLICIES),
            'partitions': set(),
            'workers': 'auto',
            'recon_cache_path': '/var/cache/swift',
            'stats_interval': 300.0,
        }, mock.ANY, ['sdx'], do_cleanup=False)
        # --debug is now effective
        mock_logging_config.assert_called_once_with(
            format='%(message)s', level=logging.DEBUG, filename=None)

        # now test overriding workers back to auto
        config = """
                [DEFAULT]
                swift_dir = test/swift/dir
                devices = /test/node
                mount_check = true

                [object-relinker]
                log_level = WARNING
                log_name = test-relinker
                files_per_second = 11.1
                workers = 8
                """
        with open(conf_file, 'w') as f:
            f.write(dedent(config))
        devices = ['sdx%d' % i for i in range(8, 1)]
        cli_cmd = ['relink', conf_file, '--device', 'sdx', '--workers', 'auto']
        for device in devices:
            cli_cmd.extend(['--device', device])
        with mock.patch('swift.cli.relinker.Relinker') as mock_relinker:
            relinker.main(cli_cmd)
        mock_relinker.assert_called_once_with({
            '__file__': mock.ANY,
            'swift_dir': 'test/swift/dir',
            'devices': '/test/node',
            'mount_check': True,
            'files_per_second': 11.1,
            'log_name': 'test-relinker',
            'log_level': 'WARNING',
            'policies': POLICIES,
            'partitions': set(),
            'workers': 'auto',
            'recon_cache_path': '/var/cache/swift',
            'stats_interval': 300.0,
        }, mock.ANY, ['sdx'], do_cleanup=False)
        logger = mock_relinker.call_args[0][1]
        self.assertEqual(logging.WARNING, logger.getEffectiveLevel())
        self.assertEqual('test-relinker', logger.logger.name)

        # and now globally
        config = """
                        [DEFAULT]
                        swift_dir = test/swift/dir
                        devices = /test/node
                        mount_check = true
                        workers = 8

                        [object-relinker]
                        log_level = WARNING
                        log_name = test-relinker
                        files_per_second = 11.1
                        """
        with open(conf_file, 'w') as f:
            f.write(dedent(config))
        with mock.patch('swift.cli.relinker.Relinker') as mock_relinker:
            relinker.main(cli_cmd)
        mock_relinker.assert_called_once_with({
            '__file__': mock.ANY,
            'swift_dir': 'test/swift/dir',
            'devices': '/test/node',
            'mount_check': True,
            'files_per_second': 11.1,
            'log_name': 'test-relinker',
            'log_level': 'WARNING',
            'policies': POLICIES,
            'partitions': set(),
            'workers': 'auto',
            'recon_cache_path': '/var/cache/swift',
            'stats_interval': 300.0,
        }, mock.ANY, ['sdx'], do_cleanup=False)
        logger = mock_relinker.call_args[0][1]
        self.assertEqual(logging.WARNING, logger.getEffectiveLevel())
        self.assertEqual('test-relinker', logger.logger.name)

    def test_relinker_utils_get_hub(self):
        cli_cmd = ['relink', '--device', 'sdx', '--workers', 'auto',
                   '--device', '/some/device']
        with mock.patch('swift.cli.relinker.Relinker'):
            relinker.main(cli_cmd)

        self.mock_hubs.use_hub.assert_called_with(utils.get_hub())

    def test_relink_first_quartile_no_rehash(self):
        # we need object name in lower half of current part
        self._setup_object(lambda part: part < 2 ** (PART_POWER - 1))
        self.assertLess(self.next_part, 2 ** PART_POWER)
        self.rb.prepare_increase_partition_power()
        self._save_ring()

        with mock.patch('swift.obj.diskfile.DiskFileManager._hash_suffix',
                        return_value='foo') as mock_hash_suffix:
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        # ... and no rehash
        self.assertEqual([], mock_hash_suffix.call_args_list)

        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))

        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)
        # Invalidated now, rehashed during cleanup
        with open(os.path.join(self.next_part_dir, 'hashes.invalid')) as fp:
            self.assertEqual(fp.read(), self._hash[-3:] + '\n')
        self.assertFalse(os.path.exists(
            os.path.join(self.next_part_dir, 'hashes.pkl')))

    def test_relink_second_quartile_does_rehash(self):
        # we need a part in upper half of current part power
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.assertGreaterEqual(self.next_part, 2 ** PART_POWER)
        self.assertTrue(self.rb.prepare_increase_partition_power())
        self._save_ring()

        with mock.patch('swift.obj.diskfile.DiskFileManager._hash_suffix',
                        return_value='foo') as mock_hash_suffix:
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        # we rehash the new suffix dirs as we go
        self.assertEqual([mock.call(self.next_suffix_dir, policy=self.policy)],
                         mock_hash_suffix.call_args_list)

        # Invalidated and rehashed during relinking
        with open(os.path.join(self.next_part_dir, 'hashes.invalid')) as fp:
            self.assertEqual(fp.read(), '')
        with open(os.path.join(self.next_part_dir, 'hashes.pkl'), 'rb') as fp:
            hashes = pickle.load(fp)
        self.assertIn(self._hash[-3:], hashes)
        self.assertEqual('foo', hashes[self._hash[-3:]])
        self.assertFalse(os.path.exists(
            os.path.join(self.part_dir, 'hashes.invalid')))
        # Check that only the dirty partition in upper half of next part power
        # has been created and rehashed
        other_next_part = self.next_part ^ 1
        other_next_part_dir = os.path.join(self.objects, str(other_next_part))
        self.assertFalse(os.path.exists(other_next_part_dir))

    def _do_link_test(self, command, old_file_specs, new_file_specs,
                      conflict_file_specs, exp_old_specs, exp_new_specs,
                      exp_ret_code=0, relink_errors=None,
                      mock_relink_paths=None, extra_options=None):
        # Each 'spec' is a tuple (file extension, timestamp offset); files are
        # created for each old_file_specs and links are created for each in
        # new_file_specs, then cleanup is run and checks made that
        # exp_old_specs and exp_new_specs exist.
        # - conflict_file_specs are files in the new partition that are *not*
        #   linked to the same file in the old partition
        # - relink_errors is a dict ext->exception; the exception will be
        #   raised each time relink_paths is called with a target_path ending
        #   with 'ext'
        self.assertFalse(relink_errors and mock_relink_paths)  # sanity check
        new_file_specs = [] if new_file_specs is None else new_file_specs
        conflict_file_specs = ([] if conflict_file_specs is None
                               else conflict_file_specs)
        exp_old_specs = [] if exp_old_specs is None else exp_old_specs
        relink_errors = {} if relink_errors is None else relink_errors
        extra_options = extra_options if extra_options else []
        # remove the file created by setUp - we'll create it again if wanted
        os.unlink(self.objname)

        def make_filenames(specs):
            filenames = []
            for ext, ts_delta in specs:
                ts = utils.Timestamp(float(self.obj_ts) + ts_delta)
                filename = '.'.join([ts.internal, ext])
                filenames.append(filename)
            return filenames

        old_filenames = make_filenames(old_file_specs)
        new_filenames = make_filenames(new_file_specs)
        conflict_filenames = make_filenames(conflict_file_specs)
        if new_filenames or conflict_filenames:
            os.makedirs(self.expected_dir)
        for filename in old_filenames:
            filepath = os.path.join(self.objdir, filename)
            with open(filepath, 'w') as fd:
                fd.write(filepath)
        for filename in new_filenames:
            new_filepath = os.path.join(self.expected_dir, filename)
            if filename in old_filenames:
                filepath = os.path.join(self.objdir, filename)
                os.link(filepath, new_filepath)
            else:
                with open(new_filepath, 'w') as fd:
                    fd.write(new_filepath)
        for filename in conflict_filenames:
            new_filepath = os.path.join(self.expected_dir, filename)
            with open(new_filepath, 'w') as fd:
                fd.write(new_filepath)

        orig_relink_paths = relink_paths

        def default_mock_relink_paths(target_path, new_target_path, **kwargs):
            for ext, error in relink_errors.items():
                if target_path.endswith(ext):
                    raise error
            return orig_relink_paths(target_path, new_target_path,
                                     **kwargs)

        with mock.patch('swift.cli.relinker.diskfile.relink_paths',
                        mock_relink_paths if mock_relink_paths
                        else default_mock_relink_paths):
            with self._mock_relinker():
                self.assertEqual(exp_ret_code, relinker.main([
                    command,
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                ] + extra_options), [self.logger.all_log_lines()])

        if exp_new_specs:
            self.assertTrue(os.path.isdir(self.expected_dir))
            exp_filenames = make_filenames(exp_new_specs)
            actual_new = sorted(os.listdir(self.expected_dir))
            self.assertEqual(sorted(exp_filenames), sorted(actual_new))
        else:
            self.assertFalse(os.path.exists(self.expected_dir))
        if exp_old_specs:
            exp_filenames = make_filenames(exp_old_specs)
            actual_old = sorted(os.listdir(self.objdir))
            self.assertEqual(sorted(exp_filenames), sorted(actual_old))
        else:
            self.assertFalse(os.path.exists(self.objdir))
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def _relink_test(self, old_file_specs, new_file_specs,
                     exp_old_specs, exp_new_specs):
        # force the rehash to not happen during relink so that we can inspect
        # files in the new partition hash dir before they are cleaned up
        self._setup_object(lambda part: part < 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        self._do_link_test('relink', old_file_specs, new_file_specs, None,
                           exp_old_specs, exp_new_specs)

    def test_relink_data_file(self):
        self._relink_test((('data', 0),),
                          None,
                          (('data', 0),),
                          (('data', 0),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_meta_files(self):
        self._relink_test((('data', 0), ('meta', 1)),
                          None,
                          (('data', 0), ('meta', 1)),
                          (('data', 0), ('meta', 1)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(2 files, 2 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_meta_file(self):
        self._relink_test((('meta', 0),),
                          None,
                          (('meta', 0),),
                          (('meta', 0),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_ts_file(self):
        self._relink_test((('ts', 0),),
                          None,
                          (('ts', 0),),
                          (('ts', 0),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_meta_ts_files(self):
        self._relink_test((('data', 0), ('meta', 1), ('ts', 2)),
                          None,
                          (('ts', 2),),
                          (('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_ts_meta_files(self):
        self._relink_test((('data', 0), ('ts', 1), ('meta', 2)),
                          None,
                          (('ts', 1), ('meta', 2)),
                          (('ts', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(2 files, 2 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_ts_data_meta_files(self):
        self._relink_test((('ts', 0), ('data', 1), ('meta', 2)),
                          None,
                          (('data', 1), ('meta', 2)),
                          (('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(2 files, 2 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_data_meta_files(self):
        self._relink_test((('data', 0), ('data', 1), ('meta', 2)),
                          None,
                          (('data', 1), ('meta', 2)),
                          (('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(2 files, 2 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_existing_meta_files(self):
        self._relink_test((('data', 0), ('meta', 1)),
                          (('meta', 1),),
                          (('data', 0), ('meta', 1)),
                          (('data', 0), ('meta', 1)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(2 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_meta_existing_newer_data_files(self):
        self._relink_test((('data', 0), ('meta', 2)),
                          (('data', 1),),
                          (('data', 0), ('meta', 2)),
                          (('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_existing_older_data_files_no_cleanup(self):
        self._relink_test((('data', 1),),
                          (('data', 0),),
                          (('data', 1),),
                          (('data', 0), ('data', 1)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_existing_older_meta_files(self):
        self._relink_test((('data', 0), ('meta', 2)),
                          (('meta', 1),),
                          (('data', 0), ('meta', 2)),
                          (('data', 0), ('meta', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(2 files, 2 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_existing_data_meta_ts_files(self):
        self._relink_test((('data', 0), ('meta', 1), ('ts', 2)),
                          (('data', 0),),
                          (('ts', 2),),
                          (('data', 0), ('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 1 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_existing_data_meta_older_ts_files(self):
        self._relink_test((('data', 1), ('meta', 2)),
                          (('ts', 0),),
                          (('data', 1), ('meta', 2)),
                          (('ts', 0), ('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(2 files, 2 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_meta_existing_ts_files(self):
        self._relink_test((('data', 0), ('meta', 1), ('ts', 2)),
                          (('ts', 2),),
                          (('ts', 2),),
                          (('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 0 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_data_meta_existing_newer_ts_files(self):
        self._relink_test((('data', 0), ('meta', 1)),
                          (('ts', 2),),
                          (('data', 0), ('meta', 1)),
                          (('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(0 files, 0 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_ts_existing_newer_data_files(self):
        self._relink_test((('ts', 0),),
                          (('data', 2),),
                          (('ts', 0),),
                          (('data', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(0 files, 0 linked, 0 removed, 0 errors)', info_lines)

    def test_relink_conflicting_ts_file(self):
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        self._do_link_test('relink',
                           (('ts', 0),),
                           None,
                           (('ts', 0),),
                           (('ts', 0),),
                           (('ts', 0),),
                           exp_ret_code=0)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual([], warning_lines)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 0 linked, 0 removed, 0 errors)',
                      info_lines)

    def test_relink_link_already_exists_but_different_inode(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()

        # make a file where we'd expect the link to be created
        os.makedirs(self.expected_dir)
        with open(self.expected_file, 'w'):
            pass

        # expect an error
        with self._mock_relinker():
            self.assertEqual(1, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))

        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('Error relinking: failed to relink %s to %s: '
                      '[Errno 17] File exists'
                      % (self.objname, self.expected_file),
                      warning_lines[0])
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 0 linked, 0 removed, 1 errors)',
                      warning_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_relink_link_already_exists(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        orig_relink_paths = relink_paths

        def mock_relink_paths(target_path, new_target_path, **kwargs):
            # pretend another process has created the link before this one
            os.makedirs(self.expected_dir)
            os.link(target_path, new_target_path)
            return orig_relink_paths(target_path, new_target_path,
                                     **kwargs)

        with self._mock_relinker():
            with mock.patch('swift.cli.relinker.diskfile.relink_paths',
                            mock_relink_paths):
                self.assertEqual(0, relinker.main([
                    'relink',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                ]))

        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 0 linked, 0 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_relink_link_target_disappears(self):
        # we need object name in lower half of current part so that there is no
        # rehash of the new partition which wold erase the empty new partition
        # - we want to assert it was created
        self._setup_object(lambda part: part < 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        orig_relink_paths = relink_paths

        def mock_relink_paths(target_path, new_target_path, **kwargs):
            # pretend another process has cleaned up the target path
            os.unlink(target_path)
            return orig_relink_paths(target_path, new_target_path,
                                     **kwargs)

        with self._mock_relinker():
            with mock.patch('swift.cli.relinker.diskfile.relink_paths',
                            mock_relink_paths):
                self.assertEqual(0, relinker.main([
                    'relink',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                ]))

        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertFalse(os.path.isfile(self.expected_file))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=False) '
                      '(1 files, 0 linked, 0 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_relink_no_applicable_policy(self):
        # NB do not prepare part power increase
        self._save_ring()
        with self._mock_relinker():
            self.assertEqual(2, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
            ]))
        self.assertEqual(self.logger.get_lines_for_level('warning'),
                         ['No policy found to increase the partition power.'])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_relink_not_mounted(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        with self._mock_relinker():
            self.assertEqual(1, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
            ]))
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            'Skipping sda1 as it is not mounted',
            '1 disks were unmounted',
            '0 hash dirs processed (cleanup=False) '
            '(0 files, 0 linked, 0 removed, 0 errors)',
        ])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_relink_listdir_error(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        with self._mock_relinker():
            with self._mock_listdir():
                self.assertEqual(1, relinker.main([
                    'relink',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount-check'
                ]))
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            'Skipping %s because ' % self.objects,
            'There were 1 errors listing partition directories',
            '0 hash dirs processed (cleanup=False) '
            '(0 files, 0 linked, 0 removed, 1 errors)',
        ])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_relink_device_filter(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        self.assertEqual(0, relinker.main([
            'relink',
            '--swift-dir', self.testdir,
            '--devices', self.devices,
            '--skip-mount',
            '--device', self.existing_device,
        ]))

        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))

        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)

    def test_relink_device_filter_invalid(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        self.assertEqual(0, relinker.main([
            'relink',
            '--swift-dir', self.testdir,
            '--devices', self.devices,
            '--skip-mount',
            '--device', 'none',
        ]))

        self.assertFalse(os.path.isdir(self.expected_dir))
        self.assertFalse(os.path.isfile(self.expected_file))

    def test_relink_partition_filter(self):
        # ensure partitions are in second quartile so that new partitions are
        # not included in the relinked partitions when the relinker is re-run:
        # this makes the number of partitions visited predictable (i.e. 3)
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        # create some other test files in different partitions
        other_objs = []
        used_parts = [self.part, self.part + 1]
        for i in range(2):
            _hash, part, next_part, obj = self._get_object_name(
                lambda part:
                part >= 2 ** (PART_POWER - 1) and part not in used_parts)
            obj_dir = os.path.join(self.objects, str(part), _hash[-3:], _hash)
            os.makedirs(obj_dir)
            obj_file = os.path.join(obj_dir, self.object_fname)
            with open(obj_file, 'w'):
                pass
            other_objs.append((part, obj_file))
            used_parts.append(part)

        self.rb.prepare_increase_partition_power()
        self._save_ring()

        # invalid partition
        with mock.patch('sys.stdout'), mock.patch('sys.stderr'):
            with self.assertRaises(SystemExit) as cm:
                self.assertEqual(0, relinker.main([
                    'relink',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                    '--partition', '-1',
                ]))
        self.assertEqual(2, cm.exception.code)

        with mock.patch('sys.stdout'), mock.patch('sys.stderr'):
            with self.assertRaises(SystemExit) as cm:
                self.assertEqual(0, relinker.main([
                    'relink',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                    '--partition', 'abc',
                ]))
        self.assertEqual(2, cm.exception.code)

        # restrict to a partition with no test object
        self.logger.clear()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
                '--partition', str(self.part + 1),
            ]))
        self.assertFalse(os.path.isdir(self.expected_dir))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(4, len(info_lines))
        self.assertIn('Starting relinker (cleanup=False) using 1 workers:',
                      info_lines[0])
        self.assertEqual(
            ['Processing files for policy platinum under %s (cleanup=False)'
             % os.path.join(self.devices, 'sda1'),
             '0 hash dirs processed (cleanup=False) (0 files, 0 linked, '
             '0 removed, 0 errors)'], info_lines[1:3]
        )
        self.assertIn('Finished relinker (cleanup=False):',
                      info_lines[3])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

        # restrict to one partition with a test object
        self.logger.clear()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
                '--partition', str(self.part),
            ]))
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(5, len(info_lines))
        self.assertIn('Starting relinker (cleanup=False) using 1 workers:',
                      info_lines[0])
        self.assertEqual(
            ['Processing files for policy platinum under %s (cleanup=False)'
             % os.path.join(self.devices, 'sda1'),
             'Step: relink Device: sda1 Policy: platinum Partitions: 1/3',
             '1 hash dirs processed (cleanup=False) (1 files, 1 linked, '
             '0 removed, 0 errors)'], info_lines[1:4]
        )
        self.assertIn('Finished relinker (cleanup=False):',
                      info_lines[4])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

        # restrict to two partitions with test objects
        self.logger.clear()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
                '--partition', str(other_objs[0][0]),
                '-p', str(other_objs[0][0]),  # duplicates should be ignored
                '-p', str(other_objs[1][0]),
            ]))
        expected_file = utils.replace_partition_in_path(
            self.devices, other_objs[0][1], PART_POWER + 1)
        self.assertTrue(os.path.isfile(expected_file))
        stat_old = os.stat(other_objs[0][1])
        stat_new = os.stat(expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)
        expected_file = utils.replace_partition_in_path(
            self.devices, other_objs[1][1], PART_POWER + 1)
        self.assertTrue(os.path.isfile(expected_file))
        stat_old = os.stat(other_objs[1][1])
        stat_new = os.stat(expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(6, len(info_lines))
        self.assertIn('Starting relinker (cleanup=False) using 1 workers:',
                      info_lines[0])
        self.assertEqual(
            ['Processing files for policy platinum under %s (cleanup=False)'
             % os.path.join(self.devices, 'sda1'),
             'Step: relink Device: sda1 Policy: platinum Partitions: 2/3',
             'Step: relink Device: sda1 Policy: platinum Partitions: 3/3',
             '2 hash dirs processed (cleanup=False) (2 files, 2 linked, '
             '0 removed, 0 errors)'], info_lines[1:5]
        )
        self.assertIn('Finished relinker (cleanup=False):',
                      info_lines[5])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    @patch_policies(
        [StoragePolicy(0, name='gold', is_default=True),
         ECStoragePolicy(1, name='platinum', ec_type=DEFAULT_TEST_EC_TYPE,
                         ec_ndata=4, ec_nparity=2)])
    def test_relink_policy_option(self):
        self._setup_object()
        self.rb.prepare_increase_partition_power()
        self._save_ring()

        # invalid policy
        with mock.patch('sys.stdout'), mock.patch('sys.stderr'):
            with self.assertRaises(SystemExit) as cm:
                relinker.main([
                    'relink',
                    '--swift-dir', self.testdir,
                    '--policy', '9',
                    '--skip-mount',
                    '--devices', self.devices,
                    '--device', self.existing_device,
                ])
        self.assertEqual(2, cm.exception.code)

        with mock.patch('sys.stdout'), mock.patch('sys.stderr'):
            with self.assertRaises(SystemExit) as cm:
                relinker.main([
                    'relink',
                    '--swift-dir', self.testdir,
                    '--policy', 'pewter',
                    '--skip-mount',
                    '--devices', self.devices,
                    '--device', self.existing_device,
                ])
            self.assertEqual(2, cm.exception.code)

        # policy with no object
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--policy', '1',
                '--skip-mount',
                '--devices', self.devices,
                '--device', self.existing_device,
            ]))
        self.assertFalse(os.path.isdir(self.expected_dir))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(4, len(info_lines))
        self.assertIn('Starting relinker (cleanup=False) using 1 workers:',
                      info_lines[0])
        self.assertEqual(
            ['Processing files for policy platinum under %s/%s (cleanup=False)'
             % (self.devices, self.existing_device),
             '0 hash dirs processed (cleanup=False) (0 files, 0 linked, '
             '0 removed, 0 errors)'], info_lines[1:3]
        )
        self.assertIn('Finished relinker (cleanup=False):',
                      info_lines[3])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

        # policy with object
        self.logger.clear()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--policy', '0',
                '--skip-mount',
                '--devices', self.devices,
                '--device', self.existing_device,
            ]))
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(5, len(info_lines))
        self.assertIn('Starting relinker (cleanup=False) using 1 workers:',
                      info_lines[0])
        self.assertEqual(
            ['Processing files for policy gold under %s/%s (cleanup=False)'
             % (self.devices, self.existing_device),
             'Step: relink Device: sda1 Policy: gold Partitions: 1/1',
             '1 hash dirs processed (cleanup=False) (1 files, 1 linked, '
             '0 removed, 0 errors)'], info_lines[1:4]
        )
        self.assertIn('Finished relinker (cleanup=False):',
                      info_lines[4])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

        # policy name works, too
        self.logger.clear()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--policy', 'gold',
                '--skip-mount',
                '--devices', self.devices,
                '--device', self.existing_device,
            ]))
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(4, len(info_lines))
        self.assertIn('Starting relinker (cleanup=False) using 1 workers:',
                      info_lines[0])
        self.assertEqual(
            ['Processing files for policy gold under %s/%s (cleanup=False)'
             % (self.devices, self.existing_device),
             '0 hash dirs processed (cleanup=False) '
             '(0 files, 0 linked, 0 removed, 0 errors)'], info_lines[1:3]
        )
        self.assertIn('Finished relinker (cleanup=False):',
                      info_lines[3])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    @patch_policies(
        [StoragePolicy(0, name='gold', is_default=True),
         ECStoragePolicy(1, name='platinum', ec_type=DEFAULT_TEST_EC_TYPE,
                         ec_ndata=4, ec_nparity=2)])
    def test_relink_all_policies(self):
        # verify that only policies in appropriate state are processed
        def do_relink(options=None):
            options = [] if options is None else options
            with self._mock_relinker():
                with mock.patch(
                        'swift.cli.relinker.Relinker.process_policy') \
                        as mocked:
                    res = relinker.main([
                        'relink',
                        '--swift-dir', self.testdir,
                        '--skip-mount',
                        '--devices', self.devices,
                        '--device', self.existing_device,
                    ] + options)
                self.assertEqual([], self.logger.get_lines_for_level('error'))
                return res, mocked

        self._save_ring(POLICIES)  # no ring prepared for increase
        res, mocked = do_relink()
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        self._save_ring([POLICIES[0]])  # not prepared for increase
        self.rb.prepare_increase_partition_power()
        self._save_ring([POLICIES[1]])  # prepared for increase
        res, mocked = do_relink()
        self.assertEqual([mock.call(POLICIES[1])], mocked.call_args_list)
        self.assertEqual(0, res)

        res, mocked = do_relink(['--policy', '0'])
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        self._save_ring([POLICIES[0]])  # prepared for increase
        res, mocked = do_relink()
        self.assertEqual([mock.call(POLICIES[0]), mock.call(POLICIES[1])],
                         mocked.call_args_list)
        self.assertEqual(0, res)

        self.rb.increase_partition_power()
        self._save_ring([POLICIES[0]])  # increased
        res, mocked = do_relink()
        self.assertEqual([mock.call(POLICIES[1])], mocked.call_args_list)
        self.assertEqual(0, res)

        self._save_ring([POLICIES[1]])  # increased
        res, mocked = do_relink()
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        res, mocked = do_relink(['--policy', '0'])
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        self.rb.finish_increase_partition_power()
        self._save_ring(POLICIES)  # all rings finished
        res, mocked = do_relink()
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

    def test_relink_conflicting_ts_is_linked_to_part_power(self):
        # link from next partition to current partition;
        # different file in current-1 partition
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        filename = '.'.join([self.obj_ts.internal, 'ts'])
        new_filepath = os.path.join(self.expected_dir, filename)
        old_filepath = os.path.join(self.objdir, filename)
        # setup a file in the current-1 part power (PART_POWER - 1) location
        # that is *not* linked to the file in the next part power location
        older_filepath = utils.replace_partition_in_path(
            self.devices, new_filepath, PART_POWER - 1)
        os.makedirs(os.path.dirname(older_filepath))
        with open(older_filepath, 'w') as fd:
            fd.write(older_filepath)
        self._do_link_test('relink',
                           (('ts', 0),),
                           (('ts', 0),),
                           None,
                           (('ts', 0),),
                           (('ts', 0),),
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        # both the PART_POWER and PART_POWER - N partitions are visited, no new
        # links are created, and both the older files are retained
        self.assertIn('2 hash dirs processed (cleanup=False) '
                      '(2 files, 0 linked, 0 removed, 0 errors)',
                      info_lines)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(old_filepath, fd.read())
        self.assertTrue(os.path.exists(older_filepath))

    def test_relink_conflicting_ts_is_linked_to_part_power_minus_1(self):
        # link from next partition to current-1 partition;
        # different file in current partition
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        # setup a file in the next part power (PART_POWER + 1) location that is
        # linked to a file in an older (PART_POWER - 1) location
        filename = '.'.join([self.obj_ts.internal, 'ts'])
        older_filepath, new_filepath = self._make_link(filename,
                                                       PART_POWER - 1)
        self._do_link_test('relink',
                           (('ts', 0),),
                           None,
                           None,  # we already made file linked to older part
                           (('ts', 0),),  # retained
                           (('ts', 0),),
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        # both the PART_POWER and PART_POWER - N partitions are visited, no new
        # links are created, and both the older files are retained
        self.assertIn('2 hash dirs processed (cleanup=False) '
                      '(2 files, 0 linked, 0 removed, 0 errors)',
                      info_lines)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(older_filepath, fd.read())
        # prev part power file is retained because it is link target
        self.assertTrue(os.path.exists(older_filepath))

    def test_relink_conflicting_ts_is_linked_to_part_power_minus_2_err(self):
        # link from next partition to current-2 partition;
        # different file in current partition
        # by default the relinker will NOT validate the current-2 location
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        # setup a file in the next part power (PART_POWER + 1) location that is
        # linked to a file in an older (PART_POWER - 2) location
        filename = '.'.join([self.obj_ts.internal, 'ts'])
        older_filepath, new_filepath = self._make_link(filename,
                                                       PART_POWER - 2)

        self._do_link_test('relink',
                           (('ts', 0),),
                           None,
                           None,  # we already made file linked to older part
                           (('ts', 0),),  # retained
                           (('ts', 0),),
                           exp_ret_code=0)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual([], warning_lines)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('2 hash dirs processed (cleanup=False) '
                      '(2 files, 0 linked, 0 removed, 0 errors)',
                      info_lines)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(older_filepath, fd.read())
        # prev-1 part power file is always retained because it is link target
        self.assertTrue(os.path.exists(older_filepath))

    def test_relink_conflicting_ts_both_in_older_part_powers(self):
        # link from next partition to current-1 partition;
        # different file in current partition
        # different file in current-2 location
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 2))
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        # setup a file in the next part power (PART_POWER + 1) location that is
        # linked to a file in an older (PART_POWER - 1) location
        filename = '.'.join([self.obj_ts.internal, 'ts'])
        older_filepath, new_filepath = self._make_link(filename,
                                                       PART_POWER - 1)
        # setup a file in an even older part power (PART_POWER - 2) location
        # that is *not* linked to the file in the next part power location
        oldest_filepath = utils.replace_partition_in_path(
            self.devices, new_filepath, PART_POWER - 2)
        os.makedirs(os.path.dirname(oldest_filepath))
        with open(oldest_filepath, 'w') as fd:
            fd.write(oldest_filepath)

        self._do_link_test('relink',
                           (('ts', 0),),
                           None,
                           None,  # we already made file linked to older part
                           (('ts', 0),),  # retained
                           (('ts', 0),),
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        # both the PART_POWER and PART_POWER - N partitions are visited, no new
        # links are created, and both the older files are retained
        self.assertIn('3 hash dirs processed (cleanup=False) '
                      '(3 files, 0 linked, 0 removed, 0 errors)',
                      info_lines)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(older_filepath, fd.read())
        self.assertTrue(os.path.exists(older_filepath))  # linked so retained
        self.assertTrue(os.path.exists(oldest_filepath))  # retained anyway

    @patch_policies(
        [StoragePolicy(0, name='gold', is_default=True),
         ECStoragePolicy(1, name='platinum', ec_type=DEFAULT_TEST_EC_TYPE,
                         ec_ndata=4, ec_nparity=2)])
    def test_cleanup_all_policies(self):
        # verify that only policies in appropriate state are processed
        def do_cleanup(options=None):
            options = [] if options is None else options
            with mock.patch(
                    'swift.cli.relinker.Relinker.process_policy') as mocked:
                res = relinker.main([
                    'cleanup',
                    '--swift-dir', self.testdir,
                    '--skip-mount',
                    '--devices', self.devices,
                    '--device', self.existing_device,
                ] + options)
            return res, mocked

        self._save_ring(POLICIES)  # no ring prepared for increase
        res, mocked = do_cleanup()
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        self.rb.prepare_increase_partition_power()
        self._save_ring(POLICIES)  # all rings prepared for increase
        res, mocked = do_cleanup()
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        self.rb.increase_partition_power()
        self._save_ring([POLICIES[0]])  # increased
        res, mocked = do_cleanup()
        self.assertEqual([mock.call(POLICIES[0])], mocked.call_args_list)
        self.assertEqual(0, res)

        res, mocked = do_cleanup(['--policy', '1'])
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        self._save_ring([POLICIES[1]])  # increased
        res, mocked = do_cleanup()
        self.assertEqual([mock.call(POLICIES[0]), mock.call(POLICIES[1])],
                         mocked.call_args_list)
        self.assertEqual(0, res)

        self.rb.finish_increase_partition_power()
        self._save_ring([POLICIES[1]])  # finished
        res, mocked = do_cleanup()
        self.assertEqual([mock.call(POLICIES[0])], mocked.call_args_list)
        self.assertEqual(0, res)

        self._save_ring([POLICIES[0]])  # finished
        res, mocked = do_cleanup()
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

        res, mocked = do_cleanup(['--policy', '1'])
        self.assertEqual([], mocked.call_args_list)
        self.assertEqual(2, res)

    def _common_test_cleanup(self, relink=True):
        # Create a ring that has prev_part_power set
        self.rb.prepare_increase_partition_power()
        self._save_ring()

        if relink:
            conf = {'swift_dir': self.testdir,
                    'devices': self.devices,
                    'mount_check': False,
                    'files_per_second': 0,
                    'policies': POLICIES,
                    'recon_cache_path': self.recon_cache_path,
                    'workers': 0}
            self.assertEqual(0, relinker.Relinker(
                conf, logger=self.logger, device_list=[self.existing_device],
                do_cleanup=False).run())
        self.rb.increase_partition_power()
        self._save_ring()

    def _cleanup_test(self, old_file_specs, new_file_specs,
                      conflict_file_specs, exp_old_specs, exp_new_specs,
                      exp_ret_code=0, relink_errors=None):
        # force the new partitions to be greater than the median so that they
        # are not rehashed during cleanup, meaning we can inspect the outcome
        # of the cleanup relinks and removes
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()
        self._do_link_test('cleanup', old_file_specs, new_file_specs,
                           conflict_file_specs, exp_old_specs, exp_new_specs,
                           exp_ret_code, relink_errors)

    def test_cleanup_data_meta_files(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           (('data', 0), ('meta', 1)),
                           None,
                           None,
                           (('data', 0), ('meta', 1)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_file(self):
        self._cleanup_test((('data', 0),),
                           None,
                           None,
                           None,
                           (('data', 0),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_missing_meta_files(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           None,
                           None,
                           None,
                           (('data', 0), ('meta', 1)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 2 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_meta_file(self):
        self._cleanup_test((('meta', 0),),
                           None,
                           None,
                           None,
                           (('meta', 0),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_ts_file(self):
        self._cleanup_test((('ts', 0),),
                           None,
                           None,
                           None,
                           (('ts', 0),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_missing_meta_missing_ts_files(self):
        self._cleanup_test((('data', 0), ('meta', 1), ('ts', 2)),
                           None,
                           None,
                           None,
                           (('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_missing_ts_missing_meta_files(self):
        self._cleanup_test((('data', 0), ('ts', 1), ('meta', 2)),
                           None,
                           None,
                           None,
                           (('ts', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 2 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_ts_missing_data_missing_meta_files(self):
        self._cleanup_test((('ts', 0), ('data', 1), ('meta', 2)),
                           None,
                           None,
                           None,
                           (('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 2 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_missing_data_missing_meta_files(self):
        self._cleanup_test((('data', 0), ('data', 1), ('meta', 2)),
                           None,
                           None,
                           None,
                           (('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 2 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_existing_meta_files(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           (('meta', 1),),
                           None,
                           None,
                           (('data', 0), ('meta', 1)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 1 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_meta_existing_newer_data_files(self):
        self._cleanup_test((('data', 0), ('meta', 2)),
                           (('data', 1),),
                           None,
                           None,
                           (('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_missing_meta_existing_older_meta_files(self):
        self._cleanup_test((('data', 0), ('meta', 2)),
                           (('meta', 1),),
                           None,
                           None,
                           (('data', 0), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 2 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_meta_missing_ts_files(self):
        self._cleanup_test((('data', 0), ('meta', 1), ('ts', 2)),
                           (('data', 0),),
                           None,
                           None,
                           (('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_missing_meta_existing_older_ts_files(self):
        self._cleanup_test((('data', 1), ('meta', 2)),
                           (('ts', 0),),
                           None,
                           None,
                           (('data', 1), ('meta', 2)))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 2 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_data_meta_existing_ts_files(self):
        self._cleanup_test((('data', 0), ('meta', 1), ('ts', 2)),
                           (('ts', 2),),
                           None,
                           None,
                           (('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 0 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_data_meta_existing_newer_ts_files(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           (('ts', 2),),
                           None,
                           None,
                           (('ts', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(0 files, 0 linked, 2 removed, 0 errors)',
                      info_lines)

    def test_cleanup_ts_existing_newer_data_files(self):
        self._cleanup_test((('ts', 0),),
                           (('data', 2),),
                           None,
                           None,
                           (('data', 2),))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(0 files, 0 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_missing_data_file_relink_fails(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           (('meta', 1),),
                           None,
                           (('data', 0), ('meta', 1)),  # nothing is removed
                           (('meta', 1),),
                           exp_ret_code=1,
                           relink_errors={'data': OSError(errno.EPERM, 'oops')}
                           )
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 0 removed, 1 errors)',
                      warning_lines)

    def test_cleanup_missing_meta_file_relink_fails(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           (('data', 0),),
                           None,
                           (('data', 0), ('meta', 1)),  # nothing is removed
                           (('data', 0),),
                           exp_ret_code=1,
                           relink_errors={'meta': OSError(errno.EPERM, 'oops')}
                           )
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 0 removed, 1 errors)',
                      warning_lines)

    def test_cleanup_missing_data_and_meta_file_one_relink_fails(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           None,
                           None,
                           (('data', 0), ('meta', 1)),  # nothing is removed
                           (('data', 0),),
                           exp_ret_code=1,
                           relink_errors={'meta': OSError(errno.EPERM, 'oops')}
                           )
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 1 linked, 0 removed, 1 errors)',
                      warning_lines)

    def test_cleanup_missing_data_and_meta_file_both_relinks_fails(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           None,
                           None,
                           (('data', 0), ('meta', 1)),  # nothing is removed
                           None,
                           exp_ret_code=1,
                           relink_errors={'data': OSError(errno.EPERM, 'oops'),
                                          'meta': OSError(errno.EPERM, 'oops')}
                           )
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 0 removed, 2 errors)',
                      warning_lines)

    def test_cleanup_conflicting_data_file(self):
        self._cleanup_test((('data', 0),),
                           None,
                           (('data', 0),),  # different inode
                           (('data', 0),),
                           (('data', 0),),
                           exp_ret_code=1)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 0 linked, 0 removed, 1 errors)',
                      warning_lines)

    def test_cleanup_conflicting_ts_file(self):
        self._cleanup_test((('ts', 0),),
                           None,
                           (('ts', 0),),  # different inode but same timestamp
                           None,
                           (('ts', 0),),
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 0 linked, 1 removed, 0 errors)',
                      info_lines)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual([], warning_lines)

    def test_cleanup_conflicting_ts_is_linked_to_part_power_minus_1(self):
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()
        # setup a file in the next part power (PART_POWER + 1) location that is
        # linked to a file in an older PART_POWER - 1 location
        filename = '.'.join([self.obj_ts.internal, 'ts'])
        older_filepath, new_filepath = self._make_link(filename,
                                                       PART_POWER - 1)
        self._do_link_test('cleanup',
                           (('ts', 0),),
                           None,
                           None,  # we already made file linked to older part
                           None,
                           (('ts', 0),),
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        # both the PART_POWER and PART_POWER - N partitions are visited, no new
        # links are created, and both the older files are removed
        self.assertIn('2 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 2 removed, 0 errors)',
                      info_lines)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(older_filepath, fd.read())
        self.assertFalse(os.path.exists(older_filepath))

    def test_cleanup_conflicting_ts_is_linked_to_part_power_minus_2_err(self):
        # link from next partition to current-2 partition;
        # different file in current partition
        # by default the relinker will NOT validate the current-2 location
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()
        # setup a file in the next part power (PART_POWER + 1) location that is
        # linked to a file in an older (PART_POWER - 2) location
        filename = '.'.join([self.obj_ts.internal, 'ts'])
        older_filepath, new_filepath = self._make_link(filename,
                                                       PART_POWER - 2)

        self._do_link_test('cleanup',
                           (('ts', 0),),
                           None,
                           None,  # we already made file linked to older part
                           None,  # different inode but same timestamp: removed
                           (('ts', 0),),
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('2 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 2 removed, 0 errors)',
                      info_lines)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual([], warning_lines)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(older_filepath, fd.read())
        # current-2 is linked so can be removed in cleanup
        self.assertFalse(os.path.exists(older_filepath))

    def test_cleanup_conflicting_ts_is_linked_to_part_power_minus_2_ok(self):
        # link from next partition to current-2 partition;
        # different file in current partition
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()
        # setup a file in the next part power (PART_POWER + 1) location that is
        # linked to a file in an older (PART_POWER - 2) location
        filename = '.'.join([self.obj_ts.internal, 'ts'])
        older_filepath, new_filepath = self._make_link(filename,
                                                       PART_POWER - 2)
        self._do_link_test('cleanup',
                           (('ts', 0),),
                           None,
                           None,  # we already made file linked to older part
                           None,
                           (('ts', 0),),
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        # both the PART_POWER and PART_POWER - N partitions are visited, no new
        # links are created, and both the older files are removed
        self.assertIn('2 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 2 removed, 0 errors)',
                      info_lines)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual([], warning_lines)
        with open(new_filepath, 'r') as fd:
            self.assertEqual(older_filepath, fd.read())
        self.assertFalse(os.path.exists(older_filepath))

    def test_cleanup_conflicting_older_data_file(self):
        # older conflicting file isn't relevant so cleanup succeeds
        self._cleanup_test((('data', 0),),
                           (('data', 1),),
                           (('data', 0),),  # different inode
                           None,
                           (('data', 1),),  # cleanup_ondisk_files rm'd 0.data
                           exp_ret_code=0)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(0 files, 0 linked, 1 removed, 0 errors)',
                      info_lines)

    def test_cleanup_conflicting_data_file_conflicting_meta_file(self):
        self._cleanup_test((('data', 0), ('meta', 1)),
                           None,
                           (('data', 0), ('meta', 1)),  # different inodes
                           (('data', 0), ('meta', 1)),
                           (('data', 0), ('meta', 1)),
                           exp_ret_code=1)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 0 removed, 2 errors)',
                      warning_lines)

    def test_cleanup_conflicting_data_file_existing_meta_file(self):
        # if just one link fails to be created then *nothing* is removed from
        # old dir
        self._cleanup_test((('data', 0), ('meta', 1)),
                           (('meta', 1),),
                           (('data', 0),),  # different inode
                           (('data', 0), ('meta', 1)),
                           (('data', 0), ('meta', 1)),
                           exp_ret_code=1)
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 0 linked, 0 removed, 1 errors)',
                      warning_lines)

    def test_cleanup_first_quartile_does_rehash(self):
        # we need object name in lower half of current part
        self._setup_object(lambda part: part < 2 ** (PART_POWER - 1))
        self.assertLess(self.next_part, 2 ** PART_POWER)
        self._common_test_cleanup()

        # don't mock re-hash for variety (and so we can assert side-effects)
        self.assertEqual(0, relinker.main([
            'cleanup',
            '--swift-dir', self.testdir,
            '--devices', self.devices,
            '--skip-mount',
        ]))

        # Old objectname should be removed, new should still exist
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        self.assertFalse(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))
        self.assertFalse(os.path.exists(self.part_dir))

        with open(os.path.join(self.next_part_dir, 'hashes.invalid')) as fp:
            self.assertEqual(fp.read(), '')
        with open(os.path.join(self.next_part_dir, 'hashes.pkl'), 'rb') as fp:
            hashes = pickle.load(fp)
        self.assertIn(self._hash[-3:], hashes)

        # create an object in a first quartile partition and pretend it should
        # be there; check that cleanup does not fail and does not remove the
        # partition!
        self._setup_object(lambda part: part < 2 ** (PART_POWER - 1))
        with mock.patch('swift.cli.relinker.replace_partition_in_path',
                        lambda *args, **kwargs: args[1]):
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        self.assertTrue(os.path.exists(self.objname))

    def test_cleanup_second_quartile_no_rehash(self):
        # we need a part in upper half of current part power
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self.assertGreaterEqual(self.part, 2 ** (PART_POWER - 1))
        self._common_test_cleanup()

        def fake_hash_suffix(suffix_dir, policy):
            # check that the hash dir is empty and remove it just like the
            # real _hash_suffix
            self.assertEqual([self._hash], os.listdir(suffix_dir))
            hash_dir = os.path.join(suffix_dir, self._hash)
            self.assertEqual([], os.listdir(hash_dir))
            os.rmdir(hash_dir)
            os.rmdir(suffix_dir)
            raise PathNotDir()

        with mock.patch('swift.obj.diskfile.DiskFileManager._hash_suffix',
                        side_effect=fake_hash_suffix) as mock_hash_suffix:
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))

        # the old suffix dir is rehashed before the old partition is removed,
        # but the new suffix dir is not rehashed
        self.assertEqual([mock.call(self.suffix_dir, policy=self.policy)],
                         mock_hash_suffix.call_args_list)

        # Old objectname should be removed, new should still exist
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        self.assertFalse(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))
        self.assertFalse(os.path.exists(self.part_dir))

        with open(os.path.join(self.objects, str(self.next_part),
                               'hashes.invalid')) as fp:
            self.assertEqual(fp.read(), '')
        with open(os.path.join(self.objects, str(self.next_part),
                               'hashes.pkl'), 'rb') as fp:
            hashes = pickle.load(fp)
        self.assertIn(self._hash[-3:], hashes)

    def test_cleanup_no_applicable_policy(self):
        # NB do not prepare part power increase
        self._save_ring()
        with self._mock_relinker():
            self.assertEqual(2, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
            ]))
        self.assertEqual(self.logger.get_lines_for_level('warning'),
                         ['No policy found to increase the partition power.'])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_not_mounted(self):
        self._common_test_cleanup()
        with self._mock_relinker():
            self.assertEqual(1, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
            ]))
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            'Skipping sda1 as it is not mounted',
            '1 disks were unmounted',
            '0 hash dirs processed (cleanup=True) '
            '(0 files, 0 linked, 0 removed, 0 errors)',
        ])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_listdir_error(self):
        self._common_test_cleanup()
        with self._mock_relinker():
            with self._mock_listdir():
                self.assertEqual(1, relinker.main([
                    'cleanup',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount-check'
                ]))
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            'Skipping %s because ' % self.objects,
            'There were 1 errors listing partition directories',
            '0 hash dirs processed (cleanup=True) '
            '(0 files, 0 linked, 0 removed, 1 errors)',
        ])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_device_filter(self):
        self._common_test_cleanup()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
                '--device', self.existing_device,
            ]))

        # Old objectname should be removed, new should still exist
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        self.assertFalse(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_device_filter_invalid(self):
        self._common_test_cleanup()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
                '--device', 'none',
            ]))

        # Old objectname should still exist, new should still exist
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        self.assertTrue(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def _time_iter(self, start):
        yield start
        while True:
            yield start + 1

    @patch_policies(
        [StoragePolicy(0, 'platinum', True),
         ECStoragePolicy(
             1, name='ec', is_default=False, ec_type=DEFAULT_TEST_EC_TYPE,
             ec_ndata=4, ec_nparity=2)])
    @mock.patch('os.getpid', return_value=100)
    def test_relink_cleanup(self, mock_getpid):
        # setup a policy-0 object in a part in the second quartile so that its
        # next part *will not* be handled during cleanup
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        # create policy-1 object in a part in the first quartile so that its
        # next part *will* be handled during cleanup
        _hash, pol_1_part, pol_1_next_part, objpath = self._get_object_name(
            lambda part: part < 2 ** (PART_POWER - 1))
        self._create_object(POLICIES[1], pol_1_part, _hash)

        state_files = {
            POLICIES[0]: os.path.join(self.devices, self.existing_device,
                                      'relink.objects.json'),
            POLICIES[1]: os.path.join(self.devices, self.existing_device,
                                      'relink.objects-1.json'),
        }

        self.rb.prepare_increase_partition_power()
        self._save_ring()
        ts1 = time.time()
        with mock.patch('time.time', side_effect=self._time_iter(ts1)):
            self.assertEqual(0, relinker.main([
                'relink',
                self.conf_file,
            ]))

        orig_inodes = {}
        for policy, part in zip(POLICIES,
                                (self.part, pol_1_part)):
            state_file = state_files[policy]
            orig_inodes[policy] = os.stat(state_file).st_ino
            state = {str(part): True}
            with open(state_files[policy], 'rt') as f:
                self.assertEqual(json.load(f), {
                    "part_power": PART_POWER,
                    "next_part_power": PART_POWER + 1,
                    "state": state})
        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data = {
            'devices': {'sda1': {'parts_done': 2,
                                 'policies': {'0': {
                                     'next_part_power': PART_POWER + 1,
                                     'part_power': PART_POWER,
                                     'parts_done': 1,
                                     'start_time': mock.ANY,
                                     'stats': {'errors': 0,
                                               'files': 1,
                                               'hash_dirs': 1,
                                               'linked': 1,
                                               'removed': 0},
                                     'step': 'relink',
                                     'timestamp': mock.ANY,
                                     'total_parts': 1,
                                     'total_time': 0.0},
                                     '1': {
                                         'next_part_power': PART_POWER + 1,
                                         'part_power': PART_POWER,
                                         'parts_done': 1,
                                         'start_time': mock.ANY,
                                         'stats': {
                                             'errors': 0,
                                             'files': 1,
                                             'hash_dirs': 1,
                                             'linked': 1,
                                             'removed': 0},
                                         'step': 'relink',
                                         'timestamp': mock.ANY,
                                         'total_parts': 1,
                                         'total_time': 0.0}},
                                 'start_time': mock.ANY,
                                 'stats': {'errors': 0,
                                           'files': 2,
                                           'hash_dirs': 2,
                                           'linked': 2,
                                           'removed': 0},
                                 'timestamp': mock.ANY,
                                 'total_parts': 2,
                                 'total_time': 0}},
            'workers': {'100': {'devices': ['sda1'],
                                'return_code': 0,
                                'timestamp': mock.ANY}}}
        self.assertEqual(recon_progress, expected_recon_data)

        self.rb.increase_partition_power()
        self.rb._ring = None  # Force builder to reload ring
        self._save_ring()
        with open(state_files[0], 'rt'), open(state_files[1], 'rt'):
            # Keep the state files open during cleanup so the inode can't be
            # released/re-used when it gets unlinked
            self.assertEqual(orig_inodes[0], os.stat(state_files[0]).st_ino)
            self.assertEqual(orig_inodes[1], os.stat(state_files[1]).st_ino)
            ts1 = time.time()
            with mock.patch('time.time', side_effect=self._time_iter(ts1)):
                self.assertEqual(0, relinker.main([
                    'cleanup',
                    self.conf_file,
                ]))
            self.assertNotEqual(orig_inodes[0], os.stat(state_files[0]).st_ino)
            self.assertNotEqual(orig_inodes[1], os.stat(state_files[1]).st_ino)
        for policy, part, next_part in zip(POLICIES,
                                           (self.part, pol_1_part),
                                           (None, pol_1_next_part)):
            state_file = state_files[policy]
            state = {str(part): True}
            if next_part is not None:
                # cleanup will process the new partition as well as the old if
                # old is in first quartile
                state[str(next_part)] = True
            with open(state_file, 'rt') as f:
                # NB: part_power/next_part_power tuple changed, so state was
                # reset (though we track prev_part_power for an efficient clean
                # up)
                self.assertEqual(json.load(f), {
                    "prev_part_power": PART_POWER,
                    "part_power": PART_POWER + 1,
                    "next_part_power": PART_POWER + 1,
                    "state": state})
        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data = {
            'devices': {'sda1': {'parts_done': 3,
                                 'policies': {'0': {
                                     'next_part_power': PART_POWER + 1,
                                     'part_power': PART_POWER + 1,
                                     'parts_done': 1,
                                     'start_time': mock.ANY,
                                     'stats': {'errors': 0,
                                               'files': 1,
                                               'hash_dirs': 1,
                                               'linked': 0,
                                               'removed': 1},
                                     'step': 'cleanup',
                                     'timestamp': mock.ANY,
                                     'total_parts': 1,
                                     'total_time': 0.0},
                                     '1': {
                                         'next_part_power': PART_POWER + 1,
                                         'part_power': PART_POWER + 1,
                                         'parts_done': 2,
                                         'start_time': mock.ANY,
                                         'stats': {
                                             'errors': 0,
                                             'files': 1,
                                             'hash_dirs': 1,
                                             'linked': 0,
                                             'removed': 1},
                                         'step': 'cleanup',
                                         'timestamp': mock.ANY,
                                         'total_parts': 2,
                                         'total_time': 0.0}},
                                 'start_time': mock.ANY,
                                 'stats': {'errors': 0,
                                           'files': 2,
                                           'hash_dirs': 2,
                                           'linked': 0,
                                           'removed': 2},
                                 'timestamp': mock.ANY,
                                 'total_parts': 3,
                                 'total_time': 0}},
            'workers': {'100': {'devices': ['sda1'],
                                'return_code': 0,
                                'timestamp': mock.ANY}}}
        self.assertEqual(recon_progress, expected_recon_data)

    def test_devices_filter_filtering(self):
        # With no filtering, returns all devices
        r = relinker.Relinker(
            {'devices': self.devices,
             'recon_cache_path': self.recon_cache_path},
            self.logger, self.existing_device)
        devices = r.devices_filter("", [self.existing_device])
        self.assertEqual(set([self.existing_device]), devices)

        # With a matching filter, returns what is matching
        devices = r.devices_filter("", [self.existing_device, 'sda2'])
        self.assertEqual(set([self.existing_device]), devices)

        # With a non matching filter, returns nothing
        r.device_list = ['none']
        devices = r.devices_filter("", [self.existing_device])
        self.assertEqual(set(), devices)

    def test_hook_pre_post_device_locking(self):
        r = relinker.Relinker(
            {'devices': self.devices,
             'recon_cache_path': self.recon_cache_path},
            self.logger, self.existing_device)
        device_path = os.path.join(self.devices, self.existing_device)
        r.datadir = 'object'  # would get set in process_policy
        r.states = {"state": {}, "part_power": PART_POWER,
                    "next_part_power": PART_POWER + 1}  # ditto
        lock_file = os.path.join(device_path, '.relink.%s.lock' % r.datadir)
        r.policy = self.policy

        # The first run gets the lock
        r.hook_pre_device(device_path)
        self.assertIsNotNone(r.dev_lock)

        # A following run would block
        with self.assertRaises(IOError) as raised:
            with open(lock_file, 'a') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertEqual(errno.EAGAIN, raised.exception.errno)

        # Another must not get the lock, so it must return an empty list
        r.hook_post_device(device_path)
        self.assertIsNone(r.dev_lock)

        with open(lock_file, 'a') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _test_state_file(self, pol, expected_recon_data):
        r = relinker.Relinker(
            {'devices': self.devices,
             'recon_cache_path': self.recon_cache_path,
             'stats_interval': 0.0},
            self.logger, [self.existing_device])
        device_path = os.path.join(self.devices, self.existing_device)
        r.datadir = 'objects'
        r.part_power = PART_POWER
        r.next_part_power = PART_POWER + 1
        datadir_path = os.path.join(device_path, r.datadir)
        state_file = os.path.join(device_path, 'relink.%s.json' % r.datadir)
        r.policy = pol
        r.pid = 1234  # for recon workers stats

        recon_progress = utils.load_recon_cache(self.recon_cache)
        # the progress for the current policy should be gone. So we should
        # just have anything from any other process polices.. if any.
        self.assertEqual(recon_progress, expected_recon_data)

        # Start relinking
        r.states = {
            "part_power": PART_POWER,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }

        # Load the states: As it starts, it must be empty
        r.hook_pre_device(device_path)
        self.assertEqual({}, r.states["state"])
        os.close(r.dev_lock)  # Release the lock

        # Partition 312 is ignored because it must have been created with the
        # next_part_power, so it does not need to be relinked
        # 96 and 227 are reverse ordered
        # auditor_status_ALL.json is ignored because it's not a partition
        self.assertEqual(['227', '96'], r.partitions_filter(
            "", ['96', '227', '312', 'auditor_status.json']))
        self.assertEqual(r.states["state"], {'96': False, '227': False})

        r.diskfile_mgr = DiskFileRouter({
            'devices': self.devices,
            'mount_check': False,
        }, self.logger)[r.policy]

        # Ack partition 96
        r.hook_pre_partition(os.path.join(datadir_path, '96'))
        r.hook_post_partition(os.path.join(datadir_path, '96'))
        self.assertEqual(r.states["state"], {'96': True, '227': False})
        self.assertEqual(self.logger.get_lines_for_level("info"), [
            "Step: relink Device: sda1 Policy: %s "
            "Partitions: 1/2" % r.policy.name,
        ])
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "part_power": PART_POWER,
                "next_part_power": PART_POWER + 1,
                "state": {'96': True, '227': False}})
        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data.update(
            {'devices': {
                'sda1': {
                    'parts_done': 1,
                    'policies': {
                        str(pol.idx): {
                            'next_part_power': PART_POWER + 1,
                            'part_power': PART_POWER,
                            'parts_done': 1,
                            'start_time': mock.ANY,
                            'stats': {
                                'errors': 0,
                                'files': 0,
                                'hash_dirs': 0,
                                'linked': 0,
                                'removed': 0},
                            'step': 'relink',
                            'timestamp': mock.ANY,
                            'total_parts': 2}},
                    'start_time': mock.ANY,
                    'stats': {
                        'errors': 0,
                        'files': 0,
                        'hash_dirs': 0,
                        'linked': 0,
                        'removed': 0},
                    'timestamp': mock.ANY,
                    'total_parts': 2,
                    'total_time': 0}},
             'workers': {
                '1234': {'timestamp': mock.ANY,
                         'return_code': None,
                         'devices': ['sda1']}}})
        self.assertEqual(recon_progress, expected_recon_data)

        # Restart relinking after only part 96 was done
        self.logger.clear()
        self.assertEqual(['227'],
                         r.partitions_filter("", ['96', '227', '312']))
        self.assertEqual(r.states["state"], {'96': True, '227': False})

        # ...but there's an error
        r.hook_pre_partition(os.path.join(datadir_path, '227'))
        r.stats['errors'] += 1
        r.hook_post_partition(os.path.join(datadir_path, '227'))
        self.assertEqual(self.logger.get_lines_for_level("info"), [
            "Step: relink Device: sda1 Policy: %s "
            "Partitions: 1/2" % r.policy.name,
        ])
        self.assertEqual(r.states["state"], {'96': True, '227': False})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "part_power": PART_POWER,
                "next_part_power": PART_POWER + 1,
                "state": {'96': True, '227': False}})

        # OK, one more try
        self.logger.clear()
        self.assertEqual(['227'],
                         r.partitions_filter("", ['96', '227', '312']))
        self.assertEqual(r.states["state"], {'96': True, '227': False})

        # Ack partition 227
        r.hook_pre_partition(os.path.join(datadir_path, '227'))
        r.hook_post_partition(os.path.join(datadir_path, '227'))
        self.assertEqual(self.logger.get_lines_for_level("info"), [
            "Step: relink Device: sda1 Policy: %s "
            "Partitions: 2/2" % r.policy.name,
        ])
        self.assertEqual(r.states["state"], {'96': True, '227': True})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "part_power": PART_POWER,
                "next_part_power": PART_POWER + 1,
                "state": {'96': True, '227': True}})
        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data.update(
            {'devices': {
                'sda1': {
                    'parts_done': 2,
                    'policies': {
                        str(pol.idx): {
                            'next_part_power': PART_POWER + 1,
                            'part_power': PART_POWER,
                            'parts_done': 2,
                            'start_time': mock.ANY,
                            'stats': {
                                'errors': 1,
                                'files': 0,
                                'hash_dirs': 0,
                                'linked': 0,
                                'removed': 0},
                            'step': 'relink',
                            'timestamp': mock.ANY,
                            'total_parts': 2}},
                    'start_time': mock.ANY,
                    'stats': {
                        'errors': 1,
                        'files': 0,
                        'hash_dirs': 0,
                        'linked': 0,
                        'removed': 0},
                    'timestamp': mock.ANY,
                    'total_parts': 2,
                    'total_time': 0}}})
        self.assertEqual(recon_progress, expected_recon_data)

        # If the process restarts, it reload the state
        r.states = {
            "part_power": PART_POWER,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }
        r.hook_pre_device(device_path)
        self.assertEqual(r.states, {
            "part_power": PART_POWER,
            "next_part_power": PART_POWER + 1,
            "state": {'96': True, '227': True}})
        os.close(r.dev_lock)  # Release the lock

        # Start cleanup -- note that part_power and next_part_power now match!
        r.do_cleanup = True
        r.part_power = PART_POWER + 1
        r.states = {
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }
        # ...which means our state file was ignored
        r.hook_pre_device(device_path)
        self.assertEqual(r.states, {
            "prev_part_power": PART_POWER,
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 1,
            "state": {}})
        os.close(r.dev_lock)  # Release the lock

        self.assertEqual(['227', '96'],
                         r.partitions_filter("", ['96', '227', '312']))
        # Ack partition 227
        r.hook_pre_partition(os.path.join(datadir_path, '227'))
        r.hook_post_partition(os.path.join(datadir_path, '227'))
        self.assertIn("Step: cleanup Device: sda1 Policy: %s "
                      "Partitions: 1/2" % r.policy.name,
                      self.logger.get_lines_for_level("info"))
        self.assertEqual(r.states["state"],
                         {'96': False, '227': True})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "prev_part_power": PART_POWER,
                "part_power": PART_POWER + 1,
                "next_part_power": PART_POWER + 1,
                "state": {'96': False, '227': True}})
        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data.update(
            {'devices': {
                'sda1': {
                    'parts_done': 1,
                    'policies': {
                        str(pol.idx): {
                            'next_part_power': PART_POWER + 1,
                            'part_power': PART_POWER + 1,
                            'parts_done': 1,
                            'start_time': mock.ANY,
                            'stats': {
                                'errors': 0,
                                'files': 0,
                                'hash_dirs': 0,
                                'linked': 0,
                                'removed': 0},
                            'step': 'cleanup',
                            'timestamp': mock.ANY,
                            'total_parts': 2}},
                    'start_time': mock.ANY,
                    'stats': {
                        'errors': 0,
                        'files': 0,
                        'hash_dirs': 0,
                        'linked': 0,
                        'removed': 0},
                    'timestamp': mock.ANY,
                    'total_parts': 2,
                    'total_time': 0}}})
        self.assertEqual(recon_progress, expected_recon_data)

        # Restart cleanup after only part 227 was done
        self.assertEqual(['96'], r.partitions_filter("", ['96', '227', '312']))
        self.assertEqual(r.states["state"],
                         {'96': False, '227': True})

        # Ack partition 96
        r.hook_post_partition(os.path.join(datadir_path, '96'))
        self.assertIn("Step: cleanup Device: sda1 Policy: %s "
                      "Partitions: 2/2" % r.policy.name,
                      self.logger.get_lines_for_level("info"))
        self.assertEqual(r.states["state"],
                         {'96': True, '227': True})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "prev_part_power": PART_POWER,
                "part_power": PART_POWER + 1,
                "next_part_power": PART_POWER + 1,
                "state": {'96': True, '227': True}})

        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data.update(
            {'devices': {
                'sda1': {
                    'parts_done': 2,
                    'policies': {
                        str(pol.idx): {
                            'next_part_power': PART_POWER + 1,
                            'part_power': PART_POWER + 1,
                            'parts_done': 2,
                            'start_time': mock.ANY,
                            'stats': {
                                'errors': 0,
                                'files': 0,
                                'hash_dirs': 0,
                                'linked': 0,
                                'removed': 0},
                            'step': 'cleanup',
                            'timestamp': mock.ANY,
                            'total_parts': 2}},
                    'start_time': mock.ANY,
                    'stats': {
                        'errors': 0,
                        'files': 0,
                        'hash_dirs': 0,
                        'linked': 0,
                        'removed': 0},
                    'timestamp': mock.ANY,
                    'total_parts': 2,
                    'total_time': 0}}})
        self.assertEqual(recon_progress, expected_recon_data)

        # At the end, the state is still accurate
        r.states = {
            "prev_part_power": PART_POWER,
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }
        r.hook_pre_device(device_path)
        self.assertEqual(r.states["state"],
                         {'96': True, '227': True})
        os.close(r.dev_lock)  # Release the lock

        # If the part_power/next_part_power tuple differs, restart from scratch
        r.states = {
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 2,
            "state": {},
        }
        r.hook_pre_device(device_path)
        self.assertEqual(r.states["state"], {})
        self.assertFalse(os.path.exists(state_file))
        # this will also reset the recon stats
        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data.update({
            'devices': {
                'sda1': {
                    'parts_done': 0,
                    'policies': {
                        str(pol.idx): {
                            'next_part_power': PART_POWER + 2,
                            'part_power': PART_POWER + 1,
                            'parts_done': 0,
                            'start_time': mock.ANY,
                            'stats': {
                                'errors': 0,
                                'files': 0,
                                'hash_dirs': 0,
                                'linked': 0,
                                'removed': 0},
                            'step': 'cleanup',
                            'timestamp': mock.ANY,
                            'total_parts': 0}},
                    'start_time': mock.ANY,
                    'stats': {
                        'errors': 0,
                        'files': 0,
                        'hash_dirs': 0,
                        'linked': 0,
                        'removed': 0},
                    'timestamp': mock.ANY,
                    'total_parts': 0,
                    'total_time': 0}}})
        self.assertEqual(recon_progress, expected_recon_data)
        os.close(r.dev_lock)  # Release the lock

        # If the file gets corrupted, restart from scratch
        with open(state_file, 'wt') as f:
            f.write('NOT JSON')
        r.states = {
            "part_power": PART_POWER,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }
        r.hook_pre_device(device_path)
        self.assertEqual(r.states["state"], {})
        self.assertFalse(os.path.exists(state_file))
        recon_progress = utils.load_recon_cache(self.recon_cache)
        expected_recon_data.update({
            'devices': {
                'sda1': {
                    'parts_done': 0,
                    'policies': {
                        str(pol.idx): {
                            'next_part_power': PART_POWER + 1,
                            'part_power': PART_POWER,
                            'parts_done': 0,
                            'start_time': mock.ANY,
                            'stats': {
                                'errors': 0,
                                'files': 0,
                                'hash_dirs': 0,
                                'linked': 0,
                                'removed': 0},
                            'step': 'cleanup',
                            'timestamp': mock.ANY,
                            'total_parts': 0}},
                    'start_time': mock.ANY,
                    'stats': {
                        'errors': 0,
                        'files': 0,
                        'hash_dirs': 0,
                        'linked': 0,
                        'removed': 0},
                    'timestamp': mock.ANY,
                    'total_parts': 0,
                    'total_time': 0}}})
        self.assertEqual(recon_progress, expected_recon_data)
        os.close(r.dev_lock)  # Release the lock
        return expected_recon_data

    @patch_policies(
        [StoragePolicy(0, 'platinum', True),
         ECStoragePolicy(
            1, name='ec', is_default=False, ec_type=DEFAULT_TEST_EC_TYPE,
            ec_ndata=4, ec_nparity=2)])
    def test_state_file(self):
        expected_recon_data = {}
        for policy in POLICIES:
            # because we specifying a device, it should be itself reset
            expected_recon_data = self._test_state_file(
                policy, expected_recon_data)
            self.logger.clear()

    def test_cleanup_relinked_ok(self):
        self._common_test_cleanup()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))

        self.assertTrue(os.path.isfile(self.expected_file))  # link intact
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 0 linked, 1 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_not_yet_relinked(self):
        # force new partition to be above range of partitions visited during
        # cleanup
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self._common_test_cleanup(relink=False)
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))

        self.assertTrue(os.path.isfile(self.expected_file))  # link created
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        self.assertIn(
            'Relinking (cleanup) created link: %s to %s'
            % (self.objname, self.expected_file),
            self.logger.get_lines_for_level('debug'))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)', info_lines)
        # suffix should be invalidated and rehashed in new partition
        hashes_invalid = os.path.join(self.next_part_dir, 'hashes.invalid')
        self.assertTrue(os.path.exists(hashes_invalid))
        with open(hashes_invalid, 'r') as fd:
            self.assertEqual('', fd.read().strip())
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_not_yet_relinked_low(self):
        # force new partition to be in the range of partitions visited during
        # cleanup, but not exist until after cleanup would have visited it
        self._setup_object(lambda part: part < 2 ** (PART_POWER - 1))
        self._common_test_cleanup(relink=False)
        self.assertFalse(os.path.isfile(self.expected_file))
        self.assertFalse(os.path.exists(self.next_part_dir))
        # Relinker processes partitions in reverse order; as a result, the
        # "normal" rehash during cleanup won't hit this, since it doesn't
        # exist yet -- but when we finish processing the old partition,
        # we'll loop back around.
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))

        self.assertTrue(os.path.isfile(self.expected_file))  # link created
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        self.assertIn(
            'Relinking (cleanup) created link: %s to %s'
            % (self.objname, self.expected_file),
            self.logger.get_lines_for_level('debug'))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)', info_lines)
        # suffix should be invalidated and rehashed in new partition
        hashes_invalid = os.path.join(self.next_part_dir, 'hashes.invalid')
        self.assertTrue(os.path.exists(hashes_invalid))
        with open(hashes_invalid, 'r') as fd:
            self.assertEqual('', fd.read().strip())
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_same_object_different_inode_in_new_partition(self):
        # force rehash of new partition to not happen during cleanup
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self._common_test_cleanup(relink=False)
        # new file in the new partition but different inode
        os.makedirs(self.expected_dir)
        with open(self.expected_file, 'w') as fd:
            fd.write('same but different')

        with self._mock_relinker():
            res = relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ])

        self.assertEqual(1, res)
        self.assertTrue(os.path.isfile(self.objname))
        with open(self.objname, 'r') as fd:
            self.assertEqual('Hello World!', fd.read())
        self.assertTrue(os.path.isfile(self.expected_file))
        with open(self.expected_file, 'r') as fd:
            self.assertEqual('same but different', fd.read())
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(2, len(warning_lines), warning_lines)
        self.assertIn('Error relinking (cleanup): failed to relink %s to %s'
                      % (self.objname, self.expected_file), warning_lines[0])
        # suffix should not be invalidated in new partition
        hashes_invalid = os.path.join(self.next_part_dir, 'hashes.invalid')
        self.assertFalse(os.path.exists(hashes_invalid))
        self.assertEqual('1 hash dirs processed (cleanup=True) '
                         '(1 files, 0 linked, 0 removed, 1 errors)',
                         warning_lines[1])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_older_object_in_new_partition(self):
        # relink of the current object failed, but there is an older version of
        # same object in the new partition
        # force rehash of new partition to not happen during cleanup
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self._common_test_cleanup(relink=False)
        os.makedirs(self.expected_dir)
        older_obj_file = os.path.join(
            self.expected_dir,
            utils.Timestamp(int(self.obj_ts) - 1).internal + '.data')
        with open(older_obj_file, "wb") as fd:
            fd.write(b"Hello Olde Worlde!")
            write_metadata(fd, {'name': self.obj_path, 'Content-Length': '18'})

        with self._mock_relinker():
            res = relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ])

        self.assertEqual(0, res)
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        # which is also going to clean up the older file
        self.assertFalse(os.path.isfile(older_obj_file))
        self.assertTrue(os.path.isfile(self.expected_file))  # link created
        self.assertIn(
            'Relinking (cleanup) created link: %s to %s'
            % (self.objname, self.expected_file),
            self.logger.get_lines_for_level('debug'))
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)', info_lines)
        # suffix should be invalidated and rehashed in new partition
        hashes_invalid = os.path.join(self.next_part_dir, 'hashes.invalid')
        self.assertTrue(os.path.exists(hashes_invalid))
        with open(hashes_invalid, 'r') as fd:
            self.assertEqual('', fd.read().strip())
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_deleted(self):
        # force rehash of new partition to not happen during cleanup
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self._common_test_cleanup()
        # rehash during relink creates hashes.invalid...
        hashes_invalid = os.path.join(self.next_part_dir, 'hashes.invalid')
        self.assertTrue(os.path.exists(hashes_invalid))

        # Pretend the object got deleted in between and there is a tombstone
        # note: the tombstone would normally be at a newer timestamp but here
        # we make the tombstone at same timestamp - it  is treated as the
        # 'required' file in the new partition, so the .data is deleted in the
        # old partition
        fname_ts = self.expected_file[:-4] + "ts"
        os.rename(self.expected_file, fname_ts)
        self.assertTrue(os.path.isfile(fname_ts))

        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        self.assertTrue(os.path.isfile(fname_ts))
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        # suffix should not be invalidated in new partition
        self.assertTrue(os.path.exists(hashes_invalid))
        with open(hashes_invalid, 'r') as fd:
            self.assertEqual('', fd.read().strip())
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(0 files, 0 linked, 1 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_old_part_careful_file(self):
        self._common_test_cleanup()
        # make some extra junk file in the part
        extra_file = os.path.join(self.part_dir, 'extra')
        with open(extra_file, 'w'):
            pass
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        # old partition can't be cleaned up
        self.assertTrue(os.path.exists(self.part_dir))
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_old_part_careful_dir(self):
        self._common_test_cleanup()
        # make some extra junk directory in the part
        extra_dir = os.path.join(self.part_dir, 'extra')
        os.mkdir(extra_dir)
        self.assertEqual(0, relinker.main([
            'cleanup',
            '--swift-dir', self.testdir,
            '--devices', self.devices,
            '--skip-mount',
        ]))
        # old partition can't be cleaned up
        self.assertTrue(os.path.exists(self.part_dir))
        self.assertTrue(os.path.exists(extra_dir))

    def test_cleanup_old_part_replication_lock_taken(self):
        # verify that relinker must take the replication lock before deleting
        # it, and handles the LockTimeout when unable to take it
        self._common_test_cleanup()

        config = """
        [DEFAULT]
        swift_dir = %s
        devices = %s
        mount_check = false
        replication_lock_timeout = 1

        [object-relinker]
        """ % (self.testdir, self.devices)
        conf_file = os.path.join(self.testdir, 'relinker.conf')
        with open(conf_file, 'w') as f:
            f.write(dedent(config))

        with utils.lock_path(self.part_dir, name='replication'):
            # lock taken so relinker should be unable to remove the lock file
            with self._mock_relinker():
                self.assertEqual(0, relinker.main(['cleanup', conf_file]))
        # old partition can't be cleaned up
        self.assertTrue(os.path.exists(self.part_dir))
        self.assertTrue(os.path.exists(
            os.path.join(self.part_dir, '.lock-replication')))
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_old_part_partition_lock_taken_during_get_hashes(self):
        # verify that relinker handles LockTimeouts when rehashing
        self._common_test_cleanup()

        config = """
        [DEFAULT]
        swift_dir = %s
        devices = %s
        mount_check = false
        replication_lock_timeout = 1

        [object-relinker]
        """ % (self.testdir, self.devices)
        conf_file = os.path.join(self.testdir, 'relinker.conf')
        with open(conf_file, 'w') as f:
            f.write(dedent(config))

        orig_get_hashes = BaseDiskFileManager.get_hashes

        def new_get_hashes(*args, **kwargs):
            # lock taken so relinker should be unable to rehash
            with utils.lock_path(self.part_dir):
                return orig_get_hashes(*args, **kwargs)

        with self._mock_relinker(), \
                mock.patch('swift.common.utils.DEFAULT_LOCK_TIMEOUT', 0.1), \
                mock.patch.object(BaseDiskFileManager,
                                  'get_hashes', new_get_hashes):
            self.assertEqual(0, relinker.main(['cleanup', conf_file]))
        # old partition can't be cleaned up
        self.assertTrue(os.path.exists(self.part_dir))
        self.assertTrue(os.path.exists(
            os.path.join(self.part_dir, '.lock')))
        self.assertEqual([], self.logger.get_lines_for_level('error'))
        self.assertEqual([], self.logger.get_lines_for_level('warning'))

    def test_cleanup_old_part_lock_taken_between_get_hashes_and_rm(self):
        # verify that relinker must take the partition lock before deleting
        # it, and handles the LockTimeout when unable to take it
        self._common_test_cleanup()

        config = """
        [DEFAULT]
        swift_dir = %s
        devices = %s
        mount_check = false
        replication_lock_timeout = 1

        [object-relinker]
        """ % (self.testdir, self.devices)
        conf_file = os.path.join(self.testdir, 'relinker.conf')
        with open(conf_file, 'w') as f:
            f.write(dedent(config))

        orig_replication_lock = BaseDiskFileManager.replication_lock

        @contextmanager
        def new_lock(*args, **kwargs):
            # lock taken so relinker should be unable to rehash
            with utils.lock_path(self.part_dir):
                with orig_replication_lock(*args, **kwargs) as cm:
                    yield cm

        with self._mock_relinker(), \
                mock.patch('swift.common.utils.DEFAULT_LOCK_TIMEOUT', 0.1), \
                mock.patch.object(BaseDiskFileManager,
                                  'replication_lock', new_lock):
            self.assertEqual(0, relinker.main(['cleanup', conf_file]))
        # old partition can't be cleaned up
        self.assertTrue(os.path.exists(self.part_dir))
        self.assertTrue(os.path.exists(
            os.path.join(self.part_dir, '.lock')))
        self.assertEqual([], self.logger.get_lines_for_level('error'))
        self.assertEqual([], self.logger.get_lines_for_level('warning'))

    def test_cleanup_old_part_robust(self):
        self._common_test_cleanup()

        orig_get_hashes = DiskFileManager.get_hashes
        calls = []

        def mock_get_hashes(mgr, device, part, suffixes, policy):
            orig_resp = orig_get_hashes(mgr, device, part, suffixes, policy)
            if part == self.part:
                expected_files = ['.lock', 'hashes.pkl', 'hashes.invalid']
                self.assertEqual(set(expected_files),
                                 set(os.listdir(self.part_dir)))
                # unlink a random file, should be empty
                os.unlink(os.path.join(self.part_dir, 'hashes.pkl'))
                # create an ssync replication lock, too
                with open(os.path.join(self.part_dir,
                                       '.lock-replication'), 'w'):
                    pass
                calls.append(True)
            elif part == self.next_part:
                # sometimes our random obj needs to rehash the next part too
                pass
            else:
                self.fail('Unexpected call to get_hashes for %r' % part)
            return orig_resp

        with mock.patch.object(DiskFileManager, 'get_hashes', mock_get_hashes):
            with self._mock_relinker():
                self.assertEqual(0, relinker.main([
                    'cleanup',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                ]))
        self.assertEqual([True], calls)
        # old partition can still be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_reapable(self):
        # relink a tombstone
        fname_ts = self.objname[:-4] + "ts"
        os.rename(self.objname, fname_ts)
        self.objname = fname_ts
        self.expected_file = self.expected_file[:-4] + "ts"
        self._common_test_cleanup()
        self.assertTrue(os.path.exists(self.expected_file))  # sanity check

        with self._mock_relinker(), \
                mock.patch('time.time', return_value=1e10 - 1):  # far future
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        self.assertEqual(self.logger.get_lines_for_level('error'), [])
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])
        # reclaimed during relinker cleanup...
        self.assertFalse(os.path.exists(self.objname))
        # reclaimed during relinker relink or relinker cleanup, depending on
        # which quartile the partition is in ...
        self.assertFalse(os.path.exists(self.expected_file))

    def test_cleanup_new_does_not_exist(self):
        self._common_test_cleanup()
        # Pretend the file in the new place got deleted in between relink and
        # cleanup: cleanup should re-create the link
        os.remove(self.expected_file)

        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        self.assertTrue(os.path.isfile(self.expected_file))  # link created
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        self.assertIn(
            'Relinking (cleanup) created link: %s to %s'
            % (self.objname, self.expected_file),
            self.logger.get_lines_for_level('debug'))
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_new_does_not_exist_and_relink_fails(self):
        # force rehash of new partition to not happen during cleanup
        self._setup_object(lambda part: part >= 2 ** (PART_POWER - 1))
        self._common_test_cleanup()
        # rehash during relink creates hashes.invalid...
        hashes_invalid = os.path.join(self.next_part_dir, 'hashes.invalid')
        self.assertTrue(os.path.exists(hashes_invalid))
        # Pretend the file in the new place got deleted in between relink and
        # cleanup: cleanup attempts to re-create the link but fails
        os.remove(self.expected_file)

        with mock.patch('swift.obj.diskfile.os.link', side_effect=OSError):
            with self._mock_relinker():
                self.assertEqual(1, relinker.main([
                    'cleanup',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                ]))
        self.assertFalse(os.path.isfile(self.expected_file))
        self.assertTrue(os.path.isfile(self.objname))  # old file intact
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            'Error relinking (cleanup): failed to relink %s to %s: '
            % (self.objname, self.expected_file),
            '1 hash dirs processed (cleanup=True) '
            '(1 files, 0 linked, 0 removed, 1 errors)',
        ])
        # suffix should not be invalidated in new partition
        self.assertTrue(os.path.exists(hashes_invalid))
        with open(hashes_invalid, 'r') as fd:
            self.assertEqual('', fd.read().strip())
        # nor in the old partition
        old_hashes_invalid = os.path.join(self.part_dir, 'hashes.invalid')
        self.assertFalse(os.path.exists(old_hashes_invalid))
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_remove_fails(self):
        meta_file = utils.Timestamp(int(self.obj_ts) + 1).internal + '.meta'
        old_meta_path = os.path.join(self.objdir, meta_file)
        new_meta_path = os.path.join(self.expected_dir, meta_file)

        with open(old_meta_path, 'w') as fd:
            fd.write('meta file in old partition')
        self._common_test_cleanup()

        calls = []
        orig_remove = os.remove

        def mock_remove(path, *args, **kwargs):
            calls.append(path)
            if len(calls) == 1:
                raise OSError
            return orig_remove(path)

        with mock.patch('swift.obj.diskfile.os.remove', mock_remove):
            with self._mock_relinker():
                self.assertEqual(1, relinker.main([
                    'cleanup',
                    '--swift-dir', self.testdir,
                    '--devices', self.devices,
                    '--skip-mount',
                ]))
        self.assertEqual([old_meta_path, self.objname], calls)
        self.assertTrue(os.path.isfile(self.expected_file))  # new file intact
        self.assertTrue(os.path.isfile(new_meta_path))  # new file intact
        self.assertFalse(os.path.isfile(self.objname))  # old file removed
        self.assertTrue(os.path.isfile(old_meta_path))  # meta file remove fail
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            'Error cleaning up %s: OSError()' % old_meta_path,
            '1 hash dirs processed (cleanup=True) '
            '(2 files, 0 linked, 1 removed, 1 errors)',
        ])
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_cleanup_two_files_need_linking(self):
        meta_file = utils.Timestamp(int(self.obj_ts) + 1).internal + '.meta'
        old_meta_path = os.path.join(self.objdir, meta_file)
        new_meta_path = os.path.join(self.expected_dir, meta_file)

        with open(old_meta_path, 'w') as fd:
            fd.write('unexpected file in old partition')
        self._common_test_cleanup(relink=False)
        self.assertFalse(os.path.isfile(self.expected_file))  # link missing
        self.assertFalse(os.path.isfile(new_meta_path))  # link missing

        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        self.assertTrue(os.path.isfile(self.expected_file))  # new file created
        self.assertTrue(os.path.isfile(new_meta_path))  # new file created
        self.assertFalse(os.path.isfile(self.objname))  # old file removed
        self.assertFalse(os.path.isfile(old_meta_path))  # meta file removed
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(2 files, 2 linked, 2 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    @patch_policies(
        [ECStoragePolicy(
         0, name='platinum', is_default=True, ec_type=DEFAULT_TEST_EC_TYPE,
         ec_ndata=4, ec_nparity=2)])
    def test_cleanup_diskfile_error(self):
        # Switch the policy type so all fragments raise DiskFileError: they
        # are included in the diskfile data as 'unexpected' files and cleanup
        # should include them
        self._common_test_cleanup()
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        log_lines = self.logger.get_lines_for_level('warning')
        # The error is logged six times:
        # during _common_test_cleanup() relink: once for cleanup_ondisk_files
        # in old and once for get_ondisk_files of union of files;
        # during cleanup: once for cleanup_ondisk_files in old and new
        # location, once for get_ondisk_files of union of files;
        # during either relink or cleanup: once for the rehash of the new
        # partition
        self.assertEqual(6, len(log_lines),
                         'Expected 6 log lines, got %r' % log_lines)
        for line in log_lines:
            self.assertIn('Bad fragment index: None', line, log_lines)
        self.assertTrue(os.path.isfile(self.expected_file))  # new file intact
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 0 linked, 1 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    @patch_policies(
        [ECStoragePolicy(
            0, name='platinum', is_default=True, ec_type=DEFAULT_TEST_EC_TYPE,
            ec_ndata=4, ec_nparity=2)])
    def test_cleanup_diskfile_error_new_file_missing(self):
        self._common_test_cleanup(relink=False)
        # Switch the policy type so all fragments raise DiskFileError: they
        # are included in the diskfile data as 'unexpected' files and cleanup
        # should include them
        with self._mock_relinker():
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
        warning_lines = self.logger.get_lines_for_level('warning')
        # once for cleanup_ondisk_files in old, again for the get_ondisk_files
        # of union of files, and one last time when the new partition gets
        # rehashed at the end of processing the old one
        self.assertEqual(3, len(warning_lines),
                         'Expected 3 log lines, got %r' % warning_lines)
        for line in warning_lines:
            self.assertIn('Bad fragment index: None', line, warning_lines)
        self.assertIn(
            'Relinking (cleanup) created link: %s to %s'
            % (self.objname, self.expected_file),
            self.logger.get_lines_for_level('debug'))
        self.assertTrue(os.path.isfile(self.expected_file))  # new file intact
        # old partition should be cleaned up
        self.assertFalse(os.path.exists(self.part_dir))
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn('1 hash dirs processed (cleanup=True) '
                      '(1 files, 1 linked, 1 removed, 0 errors)', info_lines)
        self.assertEqual([], self.logger.get_lines_for_level('error'))

    def test_rehashing(self):
        calls = []

        @contextmanager
        def do_mocks():
            orig_invalidate = relinker.diskfile.invalidate_hash
            orig_get_hashes = DiskFileManager.get_hashes

            def mock_invalidate(suffix_dir):
                calls.append(('invalidate', suffix_dir))
                return orig_invalidate(suffix_dir)

            def mock_get_hashes(self, *args):
                calls.append(('get_hashes', ) + args)
                return orig_get_hashes(self, *args)

            with mock.patch.object(relinker.diskfile, 'invalidate_hash',
                                   mock_invalidate), \
                    mock.patch.object(DiskFileManager, 'get_hashes',
                                      mock_get_hashes):
                with self._mock_relinker():
                    yield

        with do_mocks():
            self.rb.prepare_increase_partition_power()
            self._save_ring()
            self.assertEqual(0, relinker.main([
                'relink',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
            expected = [('invalidate', self.next_suffix_dir)]
            if self.part >= 2 ** (PART_POWER - 1):
                expected.append(('get_hashes', self.existing_device,
                                 self.next_part, [], POLICIES[0]))

            self.assertEqual(calls, expected)
            # Depending on partition, there may or may not be a get_hashes here
            self.rb._ring = None  # Force builder to reload ring
            self.rb.increase_partition_power()
            self._save_ring()
            self.assertEqual(0, relinker.main([
                'cleanup',
                '--swift-dir', self.testdir,
                '--devices', self.devices,
                '--skip-mount',
            ]))
            if self.part < 2 ** (PART_POWER - 1):
                expected.append(('get_hashes', self.existing_device,
                                 self.next_part, [], POLICIES[0]))
            expected.extend([
                ('invalidate', self.suffix_dir),
                ('get_hashes', self.existing_device, self.part, [],
                 POLICIES[0]),
            ])
            self.assertEqual(calls, expected)
            self.assertEqual([], self.logger.get_lines_for_level('error'))


if __name__ == '__main__':
    unittest.main()
