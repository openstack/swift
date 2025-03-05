# Copyright (c) 2022 NVIDIA
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

# Used by get_swift_info and register_swift_info to store information about
# the swift cluster.
import os
import shutil
import time
import unittest
from collections import defaultdict
from tempfile import mkdtemp

from unittest import mock

from swift.common.middleware import backend_ratelimit
from swift.common.middleware.backend_ratelimit import \
    BackendRateLimitMiddleware
from swift.common.swob import Request, HTTPOk
from test.debug_logger import debug_logger
from test.unit.common.middleware.helpers import FakeSwift


class FakeApp(object):
    def __init__(self):
        self.calls = []

    def __call__(self, env, start_response):
        start_response('200 OK', {})
        return ['']


class TestBackendRatelimitMiddleware(unittest.TestCase):
    def setUp(self):
        super(TestBackendRatelimitMiddleware, self).setUp()
        self.swift = FakeSwift()
        self.tempdir = mkdtemp()
        self.default_req_per_dev_per_sec = dict(
            (key, 0.0) for key in
            (None, 'GET', 'HEAD', 'PUT', 'POST', 'DELETE', 'UPDATE',
             'REPLICATE')
        )

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_init(self):
        conf = {'swift_dir': self.tempdir}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual(self.default_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(1.0, rl.requests_per_device_rate_buffer)
        self.assertFalse(rl.is_any_rate_limit_configured)

        conf = {'swift_dir': self.tempdir,
                'requests_per_device_per_second': 1.3,
                'requests_per_device_rate_buffer': 2.4}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 1.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertTrue(rl.is_any_rate_limit_configured)

        conf = {'requests_per_device_per_second': -1}
        factory = backend_ratelimit.filter_factory(conf)
        with self.assertRaises(ValueError) as cm:
            factory(self.swift)
        self.assertEqual(
            'Value must be a non-negative float number, not "-1.0".',
            str(cm.exception))

        conf = {'requests_per_device_rate_buffer': -1}
        factory = backend_ratelimit.filter_factory(conf)
        with self.assertRaises(ValueError):
            factory(self.swift)
        self.assertEqual(
            'Value must be a non-negative float number, not "-1.0".',
            str(cm.exception))

    def test_init_conf_path(self):
        conf = {}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual('/etc/swift/backend-ratelimit.conf', rl.conf_path)
        conf = {'backend_ratelimit_conf_path': '/etc/other/rl.conf'}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual('/etc/other/rl.conf', rl.conf_path)
        conf = {'backend_ratelimit_conf_path': ''}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual('', rl.conf_path)

    def test_init_conf_reload_interval(self):
        conf = {}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual(60, rl.config_reload_interval)
        conf = {'config_reload_interval': 600}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual(600, rl.config_reload_interval)
        conf = {'config_reload_interval': 0}
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        self.assertEqual(0, rl.config_reload_interval)

        def test_bad(value):
            with self.assertRaises(ValueError) as cm:
                conf = {'config_reload_interval': value}
                factory = backend_ratelimit.filter_factory(conf)
                factory(self.swift)
            self.assertIn('Value must be a non-negative float number',
                          str(cm.exception))
        test_bad(-1)
        test_bad('auto')

    def test_init_config_file_set_and_missing(self):
        # warn if missing conf file during init (conf_path set)
        def do_test(conf_path):
            conf = {'backend_ratelimit_conf_path': '%s' % conf_path,
                    'requests_per_device_per_second': "1.3"}
            factory = backend_ratelimit.filter_factory(conf)
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
            exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
            exp_req_per_dev_per_sec.update({None: 1.3})
            self.assertEqual(exp_req_per_dev_per_sec,
                             rl.requests_per_device_per_second)
            self.assertEqual(1.0, rl.requests_per_device_rate_buffer)
            self.assertEqual([], rl.logger.get_lines_for_level('error'))
            self.assertEqual(
                ['Failed to load config file, config unchanged: Unable to '
                 'read config from %s' % conf_path],
                rl.logger.get_lines_for_level('warning'))

        do_test('')
        do_test(os.path.join(self.tempdir, 'backend_rl.conf'))

    def test_init_config_file_unset_and_missing(self):
        # don't warn if missing conf file during init (conf_path not set)
        conf = {'swift_dir': self.tempdir,
                'requests_per_device_per_second': "1.3"}
        factory = backend_ratelimit.filter_factory(conf)
        with mock.patch(
                'swift.common.middleware.backend_ratelimit.get_logger',
                return_value=debug_logger()):
            rl = factory(self.swift)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 1.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(1.0, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('error'))
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))

    def test_init_config_file_no_section(self):
        # warn and ignore conf file without section
        conf_path = os.path.join(self.tempdir, 'backend_rl.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[DEFAULT]\n'
                     'requests_per_device_per_second = 12.3\n')
        conf = {'backend_ratelimit_conf_path': '%s' % conf_path,
                'requests_per_device_per_second': 1.3}
        factory = backend_ratelimit.filter_factory(conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.get_logger',
                        return_value=debug_logger()):
            rl = factory(self.swift)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 1.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(1.0, rl.requests_per_device_rate_buffer)
        lines = rl.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(lines), lines)
        self.assertIn('Invalid config file', lines[0])
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

    def test_read_default_backend_ratelimit_conf(self):
        conf = {'swift_dir': self.tempdir,
                'requests_per_device_per_second': "1.3",
                'requests_per_device_rate_buffer': "2.4",
                # do not set 'backend_ratelimit_conf_path'
                'config_reload_interval': 15}
        # but set it up anyway
        conf_path = os.path.join(self.tempdir, 'backend-ratelimit.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 12.3\n')
        factory = backend_ratelimit.filter_factory(conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.get_logger',
                        return_value=debug_logger()):
            rl = factory(self.swift)
        # backend-ratelimit.conf overrides options
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        # but only the ones that are listed
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertTrue(rl.is_any_rate_limit_configured)
        lines = rl.logger.get_lines_for_level('info')
        self.assertEqual(['Loaded config file %s, config changed' % conf_path],
                         lines)

    def test_config_reload_does_not_override_reload_options(self):
        conf_path = os.path.join(self.tempdir, 'override-ratelimit.conf')
        conf = {'swift_dir': self.tempdir,
                'requests_per_device_per_second': "1.3",
                'requests_per_device_rate_buffer': "2.4",
                'backend_ratelimit_conf_path': conf_path,
                'config_reload_interval': 15}
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 12.3\n'
                     'requests_per_device_rate_buffer = 12.4\n'
                     'backend_ratelimit_conf_path = /etc/swift/ignored.conf\n'
                     'config_reload_interval = 999999\n')
        factory = backend_ratelimit.filter_factory(conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.get_logger',
                        return_value=debug_logger()):
            rl = factory(self.swift)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(12.4, rl.requests_per_device_rate_buffer)
        self.assertTrue(rl.is_any_rate_limit_configured)
        # options related to conf file loading are not loaded from conf file...
        self.assertEqual(conf_path, rl.conf_path)
        self.assertEqual(15, rl.config_reload_interval)
        lines = rl.logger.logger.get_lines_for_level('info')
        self.assertEqual(['Loaded config file %s, config changed' % conf_path],
                         lines)

    def _do_test_init_config_file_overrides_filter_conf(
            self, path_to_actual_conf_file, configured_conf_path):
        # verify that conf file options override filter conf options
        # create the actual file, but no options
        with open(path_to_actual_conf_file, 'w') as fd:
            fd.write('[backend_ratelimit]')
        conf = {'swift_dir': self.tempdir,
                'requests_per_device_per_second': "1.3",
                'requests_per_device_rate_buffer': "2.4",
                'config_reload_interval': 15}
        if configured_conf_path:
            # only configure if given a conf_path
            conf['backend_ratelimit_conf_path'] = configured_conf_path
            exp_configured_conf_path = configured_conf_path
        else:
            # fall back to default
            exp_configured_conf_path = os.path.join(self.tempdir,
                                                    'backend-ratelimit.conf')
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 1.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertTrue(rl.is_any_rate_limit_configured)
        self.assertEqual(exp_configured_conf_path, rl.conf_path)
        self.assertEqual(15, rl.config_reload_interval)

        # create file with option
        with open(path_to_actual_conf_file, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 12.3\n'
                     'backend_ratelimit_conf_path = /etc/swift/ignored.conf\n'
                     'config_reload_interval = 999999\n')
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertTrue(rl.is_any_rate_limit_configured)
        # options related to conf file loading are not loaded from conf file...
        self.assertEqual(exp_configured_conf_path, rl.conf_path)
        self.assertEqual(15, rl.config_reload_interval)

        with open(path_to_actual_conf_file, 'w') as fd:
            fd.write(
                '[backend_ratelimit]\n'
                'requests_per_device_per_second = 5.3\n'
                'requests_per_device_rate_buffer = 0.5\n'
                'delete_requests_per_device_per_second = 1\n'
                'get_requests_per_device_per_second = 2\n'
                'head_requests_per_device_per_second = 3\n'
                'post_requests_per_device_per_second = 4\n'
                'put_requests_per_device_per_second = 5\n'
                'replicate_requests_per_device_per_second = 6\n'
                'update_requests_per_device_per_second = 7\n'
                'backend_ratelimit_conf_path = /etc/swift/ignored.conf\n'
                'config_reload_interval = 999999\n'
            )
        factory = backend_ratelimit.filter_factory(conf)
        rl = factory(self.swift)
        exp_req_per_dev_per_sec.update(
            {
                None: 5.3,
                'DELETE': 1,
                'GET': 2,
                'HEAD': 3,
                'POST': 4,
                'PUT': 5,
                'REPLICATE': 6,
                'UPDATE': 7,
            }
        )
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(0.5, rl.requests_per_device_rate_buffer)
        self.assertTrue(rl.is_any_rate_limit_configured)
        # options related to conf file loading are not loaded from conf file...
        self.assertEqual(exp_configured_conf_path, rl.conf_path)
        self.assertEqual(15, rl.config_reload_interval)

    def test_init_config_file_at_default_path_overrides_filter_conf(self):
        # default conf path is loaded if it exists
        default_conf_path = os.path.join(self.tempdir,
                                         'backend-ratelimit.conf')
        self._do_test_init_config_file_overrides_filter_conf(
            path_to_actual_conf_file=default_conf_path,
            configured_conf_path=None)

        self._do_test_init_config_file_overrides_filter_conf(
            path_to_actual_conf_file=default_conf_path,
            configured_conf_path=default_conf_path)

    def test_init_config_file_at_configured_path_overrides_filter_conf(self):
        # explicitly configured conf path is loaded
        custom_conf_path = os.path.join(self.tempdir, 'backend_rl.conf')
        self._do_test_init_config_file_overrides_filter_conf(
            path_to_actual_conf_file=custom_conf_path,
            configured_conf_path=custom_conf_path)

    def _do_test_config_file_reload(self, reload_interval):
        # verify that conf file options are periodically reloaded
        filter_conf = {'swift_dir': self.tempdir,
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4",
                       'head_requests_per_device_per_second': '6.2'}
        if reload_interval:
            filter_conf['config_reload_interval'] = reload_interval

        now = time.time()
        # create the actual file
        conf_path = os.path.join(filter_conf['swift_dir'],
                                 'backend-ratelimit.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 12.3\n'
                     # conf file cannot re-configure where the conf file is...
                     'backend_ratelimit_conf_path = /etc/ignored\n'
                     'config_reload_interval = also_ignored\n')
        factory = backend_ratelimit.filter_factory(filter_conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            rl = factory(self.swift)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec.update({None: 12.3, 'HEAD': float(
            filter_conf['head_requests_per_device_per_second'])})
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(float(filter_conf['requests_per_device_rate_buffer']),
                         rl.requests_per_device_rate_buffer)
        self.assertEqual(conf_path, rl.conf_path)

        # modify the conf file
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 29.3\n'
                     'requests_per_device_rate_buffer = 12.4\n'
                     'backend_ratelimit_conf_path = /etc/ignored\n'
                     'config_reload_interval = also_ignored\n'
                     'head_requests_per_device_per_second = 5.1\n'
                     'delete_requests_per_device_per_second = 7.3\n'
                     'get_requests_per_device_per_second = 8.4\n')

        # send some requests, but too soon for config file to be reloaded
        req1 = Request.blank('/sda1/99/a/c/o')
        req2 = Request.blank('/sda2/99/a/c/o',
                             environ={'REQUEST_METHOD': 'DELETE'})
        self.swift.register(req1.method, req1.path, HTTPOk, {})
        self.swift.register(req2.method, req2.path, HTTPOk, {})
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + rl.config_reload_interval - 1):
            resp1 = req1.get_response(rl)
            resp2 = req2.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        self.assertEqual(200, resp2.status_int)
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(float(filter_conf['requests_per_device_rate_buffer']),
                         rl.requests_per_device_rate_buffer)
        self.assertEqual(conf_path, rl.conf_path)

        # verify the per dev ratelimiters
        self.assertEqual({('sda1', 'GET'): 0.0,
                          ('sda2', 'DELETE'): 0.0,
                          ('sda1', None): 12.3,
                          ('sda2', None): 12.3},
                         dict((key, val.max_rate)
                              for key, val in rl.rate_limiters.items()))
        for (dev, method), limiter in rl.rate_limiters.items():
            self.assertEqual(2.4 * limiter.clock_accuracy,
                             limiter.rate_buffer_ms, (dev, method))

        # send some requests, time for config file to be reloaded
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + rl.config_reload_interval + 0.01):
            resp1 = req1.get_response(rl)
            resp2 = req2.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        self.assertEqual(200, resp2.status_int)
        exp_req_per_dev_per_sec.update({
            None: 29.3,
            'HEAD': 5.1,
            'DELETE': 7.3,
            'GET': 8.4})
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(12.4, rl.requests_per_device_rate_buffer)
        self.assertEqual(conf_path, rl.conf_path)

        # verify the per dev ratelimiters were updated
        self.assertEqual({('sda1', 'GET'): 8.4,
                          ('sda2', 'DELETE'): 7.3,
                          ('sda1', None): 29.3,
                          ('sda2', None): 29.3},
                         dict((key, val.max_rate)
                              for key, val in rl.rate_limiters.items()))
        for (dev, method), limiter in rl.rate_limiters.items():
            self.assertEqual(12.4 * limiter.clock_accuracy,
                             limiter.rate_buffer_ms, (dev, method))

        # modify the config file again
        # remove requests_per_device_per_second option
        # remove [head|delete]_requests_per_device_per_second options
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'backend_ratelimit_conf_path = /etc/ignored\n'
                     'config_reload_interval = also_ignored\n'
                     'requests_per_device_rate_buffer = 0.5\n'
                     'get_requests_per_device_per_second = 9.5\n')

        # send some requests, not yet time for config file to be reloaded
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 2 * rl.config_reload_interval - 1):
            resp1 = req1.get_response(rl)
            resp2 = req2.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        self.assertEqual(200, resp2.status_int)
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(12.4, rl.requests_per_device_rate_buffer)
        self.assertEqual(conf_path, rl.conf_path)

        # verify the per dev ratelimiters were not updated
        self.assertEqual({('sda1', 'GET'): 8.4,
                          ('sda2', 'DELETE'): 7.3,
                          ('sda1', None): 29.3,
                          ('sda2', None): 29.3},
                         dict((key, val.max_rate)
                              for key, val in rl.rate_limiters.items()))
        for (dev, method), limiter in rl.rate_limiters.items():
            self.assertEqual(12.4 * limiter.clock_accuracy,
                             limiter.rate_buffer_ms, (dev, method))

        # send some requests, time for config file to be reloaded
        with mock.patch(
                'swift.common.middleware.backend_ratelimit.time.time',
                return_value=now + 2 * rl.config_reload_interval + 0.01):
            resp1 = req1.get_response(rl)
            resp2 = req2.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        self.assertEqual(200, resp2.status_int)
        # requests_per_device_per_second option reverts to filter conf
        # delete_requests_per_device_per_second option reverts to default
        # head_requests_per_device_per_second option reverts to filter conf
        exp_req_per_dev_per_sec.update({
            None: 1.3,
            'HEAD': 6.2,
            'DELETE': 0.0,
            'GET': 9.5})
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(0.5, rl.requests_per_device_rate_buffer)
        self.assertEqual(conf_path, rl.conf_path)

        # verify the per dev ratelimiters were updated
        self.assertEqual({('sda1', 'GET'): 9.5,
                          ('sda2', 'DELETE'): 0.0,
                          ('sda1', None): 1.3,
                          ('sda2', None): 1.3},
                         dict((key, val.max_rate)
                              for key, val in rl.rate_limiters.items()))
        for (dev, method), limiter in rl.rate_limiters.items():
            self.assertEqual(0.5 * limiter.clock_accuracy,
                             limiter.rate_buffer_ms, (dev, method))
        return rl

    def test_config_file_reload_default_interval(self):
        rl = self._do_test_config_file_reload(None)
        self.assertEqual(60, rl.config_reload_interval)

    def test_config_file_reload_custom_interval(self):
        rl = self._do_test_config_file_reload(30.1)
        self.assertEqual(30.1, rl.config_reload_interval)

    def test_config_file_reload_clears_all_limits(self):
        # verify that reloaded config file can disable all rate limits
        now = time.time()
        conf_path = os.path.join(self.tempdir, 'missing')
        filter_conf = {'swift_dir': self.tempdir,
                       # path set so expect warning during init
                       'backend_ratelimit_conf_path': conf_path,
                       'requests_per_device_per_second': "1.3",
                       'head_requests_per_device_per_second = 1.1\n'
                       'requests_per_device_rate_buffer': "2.4"}
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 29.3\n'
                     'head_requests_per_device_per_second = 5.1\n'
                     'get_requests_per_device_per_second = 8.4\n')
        factory = backend_ratelimit.filter_factory(filter_conf)

        # expect warning during init
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
        # filter conf has been applied
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec.update({
            None: 29.3,
            'HEAD': 5.1,
            'GET': 8.4})
        self.assertTrue(rl.is_any_rate_limit_configured)
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

        # write zero rate limits to conf file
        # jump into future, send request, config reload attempted
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 0.0\n'
                     'head_requests_per_device_per_second = 0.0\n'
                     'get_requests_per_device_per_second = 0.0\n')
        req1 = Request.blank('/sda1/99/a/c/o')
        self.swift.register(req1.method, req1.path, HTTPOk, {})
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10000):
            resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        self.assertFalse(rl.is_any_rate_limit_configured)
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

    def test_config_file_reload_set_and_missing(self):
        now = time.time()
        conf_path = os.path.join(self.tempdir, 'missing')
        filter_conf = {'swift_dir': self.tempdir,
                       # path set so expect warning during init
                       'backend_ratelimit_conf_path': conf_path,
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4"}
        factory = backend_ratelimit.filter_factory(filter_conf)

        # expect warning during init
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
        # filter conf has been applied
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 1.3
        self.assertTrue(rl.is_any_rate_limit_configured)
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual(
            ['Failed to load config file, config unchanged: Unable to read '
             'config from %s' % conf_path],
            rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

        # jump into future, send request, config reload attempted
        # no ongoing warning
        rl.logger.logger.clear()
        req1 = Request.blank('/sda1/99/a/c/o')
        self.swift.register(req1.method, req1.path, HTTPOk, {})
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10000):
            resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        self.assertTrue(rl.is_any_rate_limit_configured)
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

    def test_config_file_reload_unset_and_missing(self):
        now = time.time()
        filter_conf = {'swift_dir': self.tempdir,
                       # conf path not set so expect no warnings
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4"}
        factory = backend_ratelimit.filter_factory(filter_conf)

        # expect NO warning during init
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
        # filter conf has been applied
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 1.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

        # jump into future, send request, config reload attempted
        # no ongoing warning
        req1 = Request.blank('/sda1/99/a/c/o')
        self.swift.register(req1.method, req1.path, HTTPOk, {})
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10000):
            resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        # previous conf file value has been retained
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

    def test_config_file_reload_empty_section(self):
        # verify that empty section is OK
        now = time.time()
        filter_conf = {'swift_dir': self.tempdir,
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4"}
        # create the actual file
        conf_path = os.path.join(self.tempdir, 'backend-ratelimit.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n')
        factory = backend_ratelimit.filter_factory(filter_conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
        # conf file value has been applied
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 1.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

    def test_config_file_reload_error(self):
        # verify that current config is preserved if reload fails
        now = time.time()
        filter_conf = {'swift_dir': self.tempdir,
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4"}
        # create the actual file
        conf_path = os.path.join(self.tempdir, 'backend-ratelimit.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 12.3\n')
        factory = backend_ratelimit.filter_factory(filter_conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
        # conf file value has been applied
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 29.3\n')

        # jump into future, send request, config reload attempted but fails
        req1 = Request.blank('/sda1/99/a/c/o')
        self.swift.register(req1.method, req1.path, HTTPOk, {})
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10000):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.readconf',
                    side_effect=ValueError('BOOM')
            ) as mock_readconf:
                resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        # previous conf file value has been retained
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        mock_readconf.assert_called_once()
        self.assertEqual(
            ['Invalid config file %s, config unchanged: BOOM' % conf_path],
            rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

        # the reload is not tried again immediately
        rl.logger = debug_logger()
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10059):
            resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        # previous conf file value has been retained
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

        # ..but will be retried after reload interval
        rl.logger = debug_logger()
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10060):
            resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        # previous conf file value has been retained
        exp_req_per_dev_per_sec.update({None: 29.3})
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        self.assertEqual([], rl.logger.get_lines_for_level('warning'))
        self.assertEqual([], rl.logger.get_lines_for_level('error'))

    def test_config_file_reload_logging(self):
        # verify that config reload is logged when config changes
        now = time.time()
        filter_conf = {'swift_dir': self.tempdir,
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4"}
        # create the actual file
        conf_path = os.path.join(self.tempdir, 'backend-ratelimit.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 12.3\n')
        factory = backend_ratelimit.filter_factory(filter_conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
        # conf file value has been applied
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        lines = rl.logger.get_lines_for_level('info')
        self.assertEqual(['Loaded config file %s, config changed' % conf_path],
                         lines)

        # jump into future, send request, config reload attempted, no change
        rl.logger.logger.clear()
        req1 = Request.blank('/sda1/99/a/c/o')
        self.swift.register(req1.method, req1.path, HTTPOk, {})
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10000):
            resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        lines = rl.logger.get_lines_for_level('info')
        self.assertEqual([], lines)

        # modify config file, jump into future, change logged
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 23.4\n')
        rl.logger = debug_logger()
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 10060):
            resp1 = req1.get_response(rl)
        self.assertEqual(200, resp1.status_int)
        # previous conf file value has been retained
        exp_req_per_dev_per_sec.update({None: 23.4})
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        lines = rl.logger.get_lines_for_level('info')
        self.assertEqual(['Loaded config file %s, config changed' % conf_path],
                         lines)

    def test_config_file_disappears_appears_logging(self):
        # verify that config reload is logged when file reappears
        now = time.time()
        filter_conf = {'swift_dir': self.tempdir,
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4"}
        # create the actual file
        conf_path = os.path.join(self.tempdir, 'backend-ratelimit.conf')
        conf_str = ('[backend_ratelimit]\n'
                    'requests_per_device_per_second = 12.3\n')
        with open(conf_path, 'w') as fd:
            fd.write(conf_str)
        factory = backend_ratelimit.filter_factory(filter_conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.get_logger',
                    return_value=debug_logger()):
                rl = factory(self.swift)
        # conf file value has been applied
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)
        lines = rl.logger.get_lines_for_level('info')
        self.assertEqual(
            ['Loaded config file %s, config changed' % conf_path],
            lines)
        lines = rl.logger.get_lines_for_level('warning')
        self.assertFalse(lines)

        def do_request(now):
            rl.logger.logger.clear()
            req1 = Request.blank('/sda1/99/a/c/o')
            self.swift.register(req1.method, req1.path, HTTPOk, {})
            with mock.patch(
                    'swift.common.middleware.backend_ratelimit.time.time',
                    return_value=now):
                resp = req1.get_response(rl)
            self.assertEqual(200, resp.status_int)
            info_lines = rl.logger.get_lines_for_level('info')
            warning_lines = rl.logger.get_lines_for_level('warning')
            return info_lines, warning_lines

        # jump into future, send request, config reload fails - warning
        os.unlink(conf_path)
        now += 100
        info_lines, warning_lines = do_request(now)
        self.assertFalse(info_lines)
        self.assertEqual(
            ['Failed to load config file, config unchanged: Unable to '
             'read config from %s' % conf_path], warning_lines)

        # jump into future, send request, config reload fails - no warning
        now += 100
        info_lines, warning_lines = do_request(now)
        self.assertFalse(info_lines)
        self.assertFalse(warning_lines)

        # reinstate conf file
        with open(conf_path, 'w') as fd:
            fd.write(conf_str)

        # jump into future, send request, config reload succeeds - logged
        now += 100
        info_lines, warning_lines = do_request(now)
        self.assertEqual('Loaded new config file %s, config unchanged'
                         % conf_path, info_lines[0])
        self.assertFalse(warning_lines)

        # jump into future, send request, config reload succeeds - not logged
        now += 100
        info_lines, warning_lines = do_request(now)
        self.assertFalse(info_lines)
        self.assertFalse(warning_lines)

    def test_config_file_reload_disabled(self):
        # verify that conf file options are not periodically reloaded when
        # interval is zero
        now = time.time()
        filter_conf = {'swift_dir': self.tempdir,
                       'requests_per_device_per_second': "1.3",
                       'requests_per_device_rate_buffer': "2.4",
                       'config_reload_interval': 0}
        conf_path = os.path.join(self.tempdir, 'backend-ratelimit.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 12.3\n')
        factory = backend_ratelimit.filter_factory(filter_conf)
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now):
            rl = factory(self.swift)
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)

        with open(conf_path, 'w') as fd:
            fd.write('[backend_ratelimit]\n'
                     'requests_per_device_per_second = 29.3\n')

        req = Request.blank('/sda1/99/a/c/o')
        self.swift.register(req.method, req.path, HTTPOk, {})
        # jump way into the future...
        with mock.patch('swift.common.middleware.backend_ratelimit.time.time',
                        return_value=now + 100000):
            resp = req.get_response(rl)
        self.assertEqual(200, resp.status_int)
        # no change
        exp_req_per_dev_per_sec = dict(self.default_req_per_dev_per_sec)
        exp_req_per_dev_per_sec[None] = 12.3
        self.assertEqual(exp_req_per_dev_per_sec,
                         rl.requests_per_device_per_second)
        self.assertEqual(2.4, rl.requests_per_device_rate_buffer)

    def _do_test_ratelimit(self, method, req_per_sec, rate_buffer,
                           extra_conf=None):
        # send 20 requests, time increments by 0.01 between each request
        start = time.time()
        fake_time = [start]

        def mock_time():
            return fake_time[0]

        app = FakeSwift()
        logger = debug_logger()
        # apply a ratelimit
        conf = {'swift_dir': self.tempdir,
                'requests_per_device_per_second': req_per_sec,
                'requests_per_device_rate_buffer': rate_buffer}
        if extra_conf:
            conf.update(extra_conf)
        rl = BackendRateLimitMiddleware(app, conf, logger)
        success = defaultdict(int)
        ratelimited = 0

        with mock.patch('swift.common.utils.time.time', mock_time):
            for i in range(20):
                for dev in ['sda1', 'sda2', 'sda3']:
                    req = Request.blank('/%s/99/a/c/o' % dev,
                                        environ={'REQUEST_METHOD': method})
                    app.register(method, req.path, HTTPOk, {})
                    resp = req.get_response(rl)
                    if resp.status_int == 200:
                        success[dev] += 1
                    else:
                        self.assertEqual(529, resp.status_int)
                        self.assertTrue(resp.status.startswith(
                            '529 Too Many Backend Requests'))
                        ratelimited += 1
                fake_time[0] += 0.01
        self.assertEqual(
            ratelimited,
            logger.statsd_client.get_increment_counts().get(
                'backend.ratelimit', 0))
        return success

    def test_method_ratelimited(self):
        def do_test_ratelimit(method):
            # no rate-limiting
            success_per_dev = self._do_test_ratelimit(method, 0, 0)
            self.assertEqual([20] * 3, list(success_per_dev.values()))

            # global rate-limited
            success_per_dev = self._do_test_ratelimit(method, 1, 0)
            self.assertEqual([1] * 3, list(success_per_dev.values()))

            success_per_dev = self._do_test_ratelimit(method, 10, 0)
            self.assertEqual([2] * 3, list(success_per_dev.values()))

            success_per_dev = self._do_test_ratelimit(method, 101, 0)
            self.assertEqual([20] * 3, list(success_per_dev.values()))

            # startup burst of 1 seconds allowance plus current allowance...
            success_per_dev = self._do_test_ratelimit(method, 1, 1)
            self.assertEqual([2] * 3, list(success_per_dev.values()))
            success_per_dev = self._do_test_ratelimit(method, 10, 1)
            self.assertEqual([12] * 3, list(success_per_dev.values()))

            # method rate-limited
            extra_conf = {
                '%s_requests_per_device_per_second' % method.lower(): 1
            }
            success_per_dev = self._do_test_ratelimit(method, 0, 0, extra_conf)
            self.assertEqual([1] * 3, list(success_per_dev.values()))

            # method not rate-limited, global rate limited
            extra_conf = {
                '%s_requests_per_device_per_second' % method.lower(): 100
            }
            success_per_dev = self._do_test_ratelimit(method, 1, 0, extra_conf)
            self.assertEqual([1] * 3, list(success_per_dev.values()))

        do_test_ratelimit('GET')
        do_test_ratelimit('HEAD')
        do_test_ratelimit('PUT')
        do_test_ratelimit('POST')
        do_test_ratelimit('DELETE')
        do_test_ratelimit('UPDATE')
        do_test_ratelimit('REPLICATE')

    def test_method_not_ratelimited(self):
        def do_test_no_ratelimit(method):
            # verify no rate-limiting
            success_per_dev = self._do_test_ratelimit(method, 1, 0)
            self.assertEqual([20] * 3, list(success_per_dev.values()))

        do_test_no_ratelimit('OPTIONS')
        do_test_no_ratelimit('SSYNC')

    def test_no_ratelimiting_configured(self):
        # verify shortcut path when no ratelimiting is configured
        with mock.patch(
                'swift.common.middleware.backend_ratelimit.'
                'BackendRateLimitMiddleware._is_allowed') as mock_is_allowed:
            success_per_dev = self._do_test_ratelimit('GET', 0, 0)
        self.assertEqual([20] * 3, list(success_per_dev.values()))
        mock_is_allowed.assert_not_called()

    def test_unhandled_request(self):
        app = FakeSwift()
        logger = debug_logger()
        conf = {'requests_per_device_per_second': 1,
                'requests_per_device_rate_buffer': 1}

        def do_test(path):
            rl = BackendRateLimitMiddleware(app, conf, logger)
            req = Request.blank(path)
            app.register('GET', req.path, HTTPOk, {})
            for i in range(10):
                resp = req.get_response(rl)
                self.assertEqual(200, resp.status_int)
            self.assertEqual(
                0, logger.statsd_client.get_increment_counts().get(
                    'backend.ratelimit', 0))

        do_test('/recon/version')
        do_test('/healthcheck')
        do_test('/v1/a/c/o')
