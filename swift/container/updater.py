# Copyright (c) 2010-2012 OpenStack, LLC.
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

import logging
import os
import signal
import sys
import time
from random import random, shuffle
from tempfile import mkstemp

from eventlet import spawn, patcher, Timeout

from swift.container.server import DATADIR
from swift.common.bufferedhttp import http_connect
from swift.common.db import ContainerBroker
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger, whataremyips
from swift.common.daemon import Daemon


class ContainerUpdater(Daemon):
    """Update container information in account listings."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='container-updater')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.interval = int(conf.get('interval', 300))
        self.account_ring_path = os.path.join(swift_dir, 'account.ring.gz')
        self.account_ring = None
        self.concurrency = int(conf.get('concurrency', 4))
        self.slowdown = float(conf.get('slowdown', 0.01))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.no_changes = 0
        self.successes = 0
        self.failures = 0
        self.account_suppressions = {}
        self.account_suppression_time = \
            float(conf.get('account_suppression_time', 60))
        self.new_account_suppressions = None

    def get_account_ring(self):
        """Get the account ring.  Load it if it hasn't been yet."""
        if not self.account_ring:
            self.logger.debug(
                _('Loading account ring from %s'), self.account_ring_path)
            self.account_ring = Ring(self.account_ring_path)
        return self.account_ring

    def get_paths(self):
        """
        Get paths to all of the partitions on each drive to be processed.

        :returns: a list of paths
        """
        paths = []
        for device in os.listdir(self.devices):
            dev_path = os.path.join(self.devices, device)
            if self.mount_check and not os.path.ismount(dev_path):
                self.logger.warn(_('%s is not mounted'), device)
                continue
            con_path = os.path.join(dev_path, DATADIR)
            if not os.path.exists(con_path):
                continue
            for partition in os.listdir(con_path):
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
        Run the updator continuously.
        """
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin container update sweep'))
            begin = time.time()
            now = time.time()
            expired_suppressions = \
               [a for a, u in self.account_suppressions.iteritems() if u < now]
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
                    patcher.monkey_patch(all=False, socket=True)
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
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """
        Run the updater once.
        """
        patcher.monkey_patch(all=False, socket=True)
        self.logger.info(_('Begin container update single threaded sweep'))
        begin = time.time()
        self.no_changes = 0
        self.successes = 0
        self.failures = 0
        for path in self.get_paths():
            self.container_sweep(path)
        elapsed = time.time() - begin
        self.logger.info(_('Container update single threaded sweep completed: '
            '%(elapsed).02fs, %(success)s successes, %(fail)s failures, '
            '%(no_change)s with no changes'),
            {'elapsed': elapsed, 'success': self.successes,
             'fail': self.failures, 'no_change': self.no_changes})

    def container_sweep(self, path):
        """
        Walk the path looking for container DBs and process them.

        :param path: path to walk
        """
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.db'):
                    self.process_container(os.path.join(root, file))
                    time.sleep(self.slowdown)

    def process_container(self, dbfile):
        """
        Process a container, and update the information in the account.

        :param dbfile: container DB to process
        """
        broker = ContainerBroker(dbfile, logger=self.logger)
        info = broker.get_info()
        # Don't send updates if the container was auto-created since it
        # definitely doesn't have up to date statistics.
        if float(info['put_timestamp']) <= 0:
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
                            info['object_count'], info['bytes_used'])
                      for node in nodes]
            successes = 0
            failures = 0
            for event in events:
                if 200 <= event.wait() < 300:
                    successes += 1
                else:
                    failures += 1
            if successes > failures:
                self.successes += 1
                self.logger.debug(
                    _('Update report sent for %(container)s %(dbfile)s'),
                    {'container': container, 'dbfile': dbfile})
                broker.reported(info['put_timestamp'],
                                info['delete_timestamp'], info['object_count'],
                                info['bytes_used'])
            else:
                self.failures += 1
                self.logger.debug(
                    _('Update report failed for %(container)s %(dbfile)s'),
                    {'container': container, 'dbfile': dbfile})
                self.account_suppressions[info['account']] = until = \
                    time.time() + self.account_suppression_time
                if self.new_account_suppressions:
                    print >>self.new_account_suppressions, \
                        info['account'], until
        else:
            self.no_changes += 1

    def container_report(self, node, part, container, put_timestamp,
                         delete_timestamp, count, bytes):
        """
        Report container info to an account server.

        :param node: node dictionary from the account ring
        :param part: partition the account is on
        :param container: container name
        :param put_timestamp: put timestamp
        :param delete_timestamp: delete timestamp
        :param count: object count in the container
        :param bytes: bytes used in the container
        """
        with ConnectionTimeout(self.conn_timeout):
            try:
                conn = http_connect(
                    node['ip'], node['port'], node['device'], part,
                    'PUT', container,
                    headers={'X-Put-Timestamp': put_timestamp,
                             'X-Delete-Timestamp': delete_timestamp,
                             'X-Object-Count': count,
                             'X-Bytes-Used': bytes,
                             'X-Account-Override-Deleted': 'yes'})
            except (Exception, Timeout):
                self.logger.exception(_('ERROR account update failed with '
                    '%(ip)s:%(port)s/%(device)s (will retry later): '), node)
                return 500
        with Timeout(self.node_timeout):
            try:
                resp = conn.getresponse()
                resp.read()
                return resp.status
            except (Exception, Timeout):
                if self.logger.getEffectiveLevel() <= logging.DEBUG:
                    self.logger.exception(
                        _('Exception with %(ip)s:%(port)s/%(device)s'), node)
                return 500
