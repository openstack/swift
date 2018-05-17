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

from __future__ import print_function
import logging
import os
import signal
import sys
import time
from swift import gettext_ as _
from random import random, shuffle
from tempfile import mkstemp

from eventlet import spawn, Timeout

import swift.common.db
from swift.common.constraints import check_drive
from swift.container.backend import ContainerBroker, DATADIR
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger, config_true_value, \
    dump_recon_cache, majority_size, Timestamp, ratelimit_sleep, \
    eventlet_monkey_patch
from swift.common.daemon import Daemon
from swift.common.http import is_success, HTTP_INTERNAL_SERVER_ERROR


class ContainerUpdater(Daemon):
    """Update container information in account listings."""

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='container-updater')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.interval = int(conf.get('interval', 300))
        self.account_ring = None
        self.concurrency = int(conf.get('concurrency', 4))
        if 'slowdown' in conf:
            self.logger.warning(
                'The slowdown option is deprecated in favor of '
                'containers_per_second. This option may be ignored in a '
                'future release.')
            containers_per_second = 1 / (
                float(conf.get('slowdown', '0.01')) + 0.01)
        else:
            containers_per_second = 50
        self.containers_running_time = 0
        self.max_containers_per_second = \
            float(conf.get('containers_per_second',
                           containers_per_second))
        self.node_timeout = float(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.no_changes = 0
        self.successes = 0
        self.failures = 0
        self.account_suppressions = {}
        self.account_suppression_time = \
            float(conf.get('account_suppression_time', 60))
        self.new_account_suppressions = None
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "container.recon")
        self.user_agent = 'container-updater %s' % os.getpid()

    def get_account_ring(self):
        """Get the account ring.  Load it if it hasn't been yet."""
        if not self.account_ring:
            self.account_ring = Ring(self.swift_dir, ring_name='account')
        return self.account_ring

    def _listdir(self, path):
        try:
            return os.listdir(path)
        except OSError as e:
            self.logger.error(_('ERROR:  Failed to get paths to drive '
                                'partitions: %s') % e)
            return []

    def get_paths(self):
        """
        Get paths to all of the partitions on each drive to be processed.

        :returns: a list of paths
        """
        paths = []
        for device in self._listdir(self.devices):
            dev_path = check_drive(self.devices, device, self.mount_check)
            if not dev_path:
                self.logger.warning(_('%s is not mounted'), device)
                continue
            con_path = os.path.join(dev_path, DATADIR)
            if not os.path.exists(con_path):
                continue
            for partition in self._listdir(con_path):
                paths.append(os.path.join(con_path, partition))
        shuffle(paths)
        return paths

    def _load_suppressions(self, filename):
        try:
            with open(filename, 'r') as tmpfile:
                for line in tmpfile:
                    account, until = line.split()
                    until = float(until)
                    self.account_suppressions[account] = until
        except Exception:
            self.logger.exception(
                _('ERROR with loading suppressions from %s: ') % filename)
        finally:
            os.unlink(filename)

    def run_forever(self, *args, **kwargs):
        """
        Run the updater continuously.
        """
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin container update sweep'))
            begin = time.time()
            now = time.time()
            expired_suppressions = \
                [a for a, u in self.account_suppressions.items()
                 if u < now]
            for account in expired_suppressions:
                del self.account_suppressions[account]
            pid2filename = {}
            # read from account ring to ensure it's fresh
            self.get_account_ring().get_nodes('')
            for path in self.get_paths():
                while len(pid2filename) >= self.concurrency:
                    pid = os.wait()[0]
                    try:
                        self._load_suppressions(pid2filename[pid])
                    finally:
                        del pid2filename[pid]
                fd, tmpfilename = mkstemp()
                os.close(fd)
                pid = os.fork()
                if pid:
                    pid2filename[pid] = tmpfilename
                else:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    eventlet_monkey_patch()
                    self.no_changes = 0
                    self.successes = 0
                    self.failures = 0
                    self.new_account_suppressions = open(tmpfilename, 'w')
                    forkbegin = time.time()
                    self.container_sweep(path)
                    elapsed = time.time() - forkbegin
                    self.logger.debug(
                        _('Container update sweep of %(path)s completed: '
                          '%(elapsed).02fs, %(success)s successes, %(fail)s '
                          'failures, %(no_change)s with no changes'),
                        {'path': path, 'elapsed': elapsed,
                         'success': self.successes, 'fail': self.failures,
                         'no_change': self.no_changes})
                    sys.exit()
            while pid2filename:
                pid = os.wait()[0]
                try:
                    self._load_suppressions(pid2filename[pid])
                finally:
                    del pid2filename[pid]
            elapsed = time.time() - begin
            self.logger.info(_('Container update sweep completed: %.02fs'),
                             elapsed)
            dump_recon_cache({'container_updater_sweep': elapsed},
                             self.rcache, self.logger)
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """
        Run the updater once.
        """
        eventlet_monkey_patch()
        self.logger.info(_('Begin container update single threaded sweep'))
        begin = time.time()
        self.no_changes = 0
        self.successes = 0
        self.failures = 0
        for path in self.get_paths():
            self.container_sweep(path)
        elapsed = time.time() - begin
        self.logger.info(_(
            'Container update single threaded sweep completed: '
            '%(elapsed).02fs, %(success)s successes, %(fail)s failures, '
            '%(no_change)s with no changes'),
            {'elapsed': elapsed, 'success': self.successes,
             'fail': self.failures, 'no_change': self.no_changes})
        dump_recon_cache({'container_updater_sweep': elapsed},
                         self.rcache, self.logger)

    def container_sweep(self, path):
        """
        Walk the path looking for container DBs and process them.

        :param path: path to walk
        """
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.db'):
                    self.process_container(os.path.join(root, file))

                    self.containers_running_time = ratelimit_sleep(
                        self.containers_running_time,
                        self.max_containers_per_second)

    def process_container(self, dbfile):
        """
        Process a container, and update the information in the account.

        :param dbfile: container DB to process
        """
        start_time = time.time()
        broker = ContainerBroker(dbfile, logger=self.logger)
        info = broker.get_info()
        # Don't send updates if the container was auto-created since it
        # definitely doesn't have up to date statistics.
        if Timestamp(info['put_timestamp']) <= 0:
            return
        if self.account_suppressions.get(info['account'], 0) > time.time():
            return
        if info['put_timestamp'] > info['reported_put_timestamp'] or \
                info['delete_timestamp'] > info['reported_delete_timestamp'] \
                or info['object_count'] != info['reported_object_count'] or \
                info['bytes_used'] != info['reported_bytes_used']:
            container = '/%s/%s' % (info['account'], info['container'])
            part, nodes = self.get_account_ring().get_nodes(info['account'])
            events = [spawn(self.container_report, node, part, container,
                            info['put_timestamp'], info['delete_timestamp'],
                            info['object_count'], info['bytes_used'],
                            info['storage_policy_index'])
                      for node in nodes]
            successes = 0
            for event in events:
                if is_success(event.wait()):
                    successes += 1
            if successes >= majority_size(len(events)):
                self.logger.increment('successes')
                self.successes += 1
                self.logger.debug(
                    _('Update report sent for %(container)s %(dbfile)s'),
                    {'container': container, 'dbfile': dbfile})
                broker.reported(info['put_timestamp'],
                                info['delete_timestamp'], info['object_count'],
                                info['bytes_used'])
            else:
                self.logger.increment('failures')
                self.failures += 1
                self.logger.debug(
                    _('Update report failed for %(container)s %(dbfile)s'),
                    {'container': container, 'dbfile': dbfile})
                self.account_suppressions[info['account']] = until = \
                    time.time() + self.account_suppression_time
                if self.new_account_suppressions:
                    print(info['account'], until,
                          file=self.new_account_suppressions)
            # Only track timing data for attempted updates:
            self.logger.timing_since('timing', start_time)
        else:
            self.logger.increment('no_changes')
            self.no_changes += 1

    def container_report(self, node, part, container, put_timestamp,
                         delete_timestamp, count, bytes,
                         storage_policy_index):
        """
        Report container info to an account server.

        :param node: node dictionary from the account ring
        :param part: partition the account is on
        :param container: container name
        :param put_timestamp: put timestamp
        :param delete_timestamp: delete timestamp
        :param count: object count in the container
        :param bytes: bytes used in the container
        :param storage_policy_index: the policy index for the container
        """
        with ConnectionTimeout(self.conn_timeout):
            try:
                headers = {
                    'X-Put-Timestamp': put_timestamp,
                    'X-Delete-Timestamp': delete_timestamp,
                    'X-Object-Count': count,
                    'X-Bytes-Used': bytes,
                    'X-Account-Override-Deleted': 'yes',
                    'X-Backend-Storage-Policy-Index': storage_policy_index,
                    'user-agent': self.user_agent}
                conn = http_connect(
                    node['ip'], node['port'], node['device'], part,
                    'PUT', container, headers=headers)
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later): '), node)
                return HTTP_INTERNAL_SERVER_ERROR
        with Timeout(self.node_timeout):
            try:
                resp = conn.getresponse()
                resp.read()
                return resp.status
            except (Exception, Timeout):
                if self.logger.getEffectiveLevel() <= logging.DEBUG:
                    self.logger.exception(
                        _('Exception with %(ip)s:%(port)s/%(device)s'), node)
                return HTTP_INTERNAL_SERVER_ERROR
            finally:
                conn.close()
