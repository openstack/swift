# Copyright (c) 2010 OpenStack, LLC.
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
import socket
import time
from random import choice, random
from urllib import quote

from eventlet import Timeout

from swift.account import server as account_server
from swift.common.db import AccountBroker
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger


class AuditException(Exception):
    pass


class AccountAuditor(object):
    """Audit accounts."""

    def __init__(self, server_conf, auditor_conf):
        self.logger = get_logger(auditor_conf, 'account-auditor')
        self.devices = server_conf.get('devices', '/srv/node')
        self.mount_check = server_conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.interval = int(auditor_conf.get('interval', 1800))
        swift_dir = server_conf.get('swift_dir', '/etc/swift')
        self.container_ring_path = os.path.join(swift_dir, 'container.ring.gz')
        self.container_ring = None
        self.node_timeout = int(auditor_conf.get('node_timeout', 10))
        self.conn_timeout = float(auditor_conf.get('conn_timeout', 0.5))
        self.max_container_count = \
            int(auditor_conf.get('max_container_count', 100))
        self.container_passes = 0
        self.container_failures = 0
        self.container_errors = 0

    def get_container_ring(self):
        """
        Get the container ring.  Load the ring if neccesary.

        :returns: container ring
        """
        if not self.container_ring:
            self.logger.debug(

                'Loading container ring from %s' % self.container_ring_path)
            self.container_ring = Ring(self.container_ring_path)
        return self.container_ring

    def audit_forever(self):    # pragma: no cover
        """Run the account audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            begin = time.time()
            pids = []
            for device in os.listdir(self.devices):
                if self.mount_check and not \
                        os.path.ismount(os.path.join(self.devices, device)):
                    self.logger.debug(
                        'Skipping %s as it is not mounted' % device)
                    continue
                self.account_audit(device)
            if time.time() - reported >= 3600:  # once an hour
                self.logger.info(
                    'Since %s: Remote audits with containers: %s passed '
                    'audit, %s failed audit, %s errors' %
                    (time.ctime(reported), self.container_passes,
                     self.container_failures, self.container_errors))
                reported = time.time()
                self.container_passes = 0
                self.container_failures = 0
                self.container_errors = 0
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def audit_once(self):
        """Run the account audit once."""
        self.logger.info('Begin account audit "once" mode')
        begin = time.time()
        for device in os.listdir(self.devices):
            if self.mount_check and \
                    not os.path.ismount(os.path.join(self.devices, device)):
                self.logger.debug(
                    'Skipping %s as it is not mounted' % device)
                continue
            self.account_audit(device)
        elapsed = time.time() - begin
        self.logger.info(
            'Account audit "once" mode completed: %.02fs' % elapsed)

    def account_audit(self, device):
        """
        Audit any accounts found on the device.

        :param device: device to audit
        """
        datadir = os.path.join(self.devices, device, account_server.DATADIR)
        if not os.path.exists(datadir):
            return
        broker = None
        partition = None
        attempts = 100
        while not broker and attempts:
            attempts -= 1
            try:
                partition = choice(os.listdir(datadir))
                fpath = os.path.join(datadir, partition)
                if not os.path.isdir(fpath):
                    continue
                suffix = choice(os.listdir(fpath))
                fpath = os.path.join(fpath, suffix)
                if not os.path.isdir(fpath):
                    continue
                hsh = choice(os.listdir(fpath))
                fpath = os.path.join(fpath, hsh)
                if not os.path.isdir(fpath):
                    continue
            except IndexError:
                continue
            for fname in sorted(os.listdir(fpath), reverse=True):
                if fname.endswith('.db'):
                    broker = AccountBroker(os.path.join(fpath, fname))
                    if broker.is_deleted():
                        broker = None
                    break
        if not broker:
            return
        info = broker.get_info()
        for container in broker.get_random_containers(
                max_count=self.max_container_count):
            found = False
            results = []
            part, nodes = \
                self.get_container_ring().get_nodes(info['account'], container)
            for node in nodes:
                try:
                    with ConnectionTimeout(self.conn_timeout):
                        conn = http_connect(node['ip'], node['port'],
                                node['device'], part, 'HEAD',
                                '/%s/%s' % (info['account'], container))
                    with Timeout(self.node_timeout):
                        resp = conn.getresponse()
                        body = resp.read()
                    if 200 <= resp.status <= 299:
                        found = True
                        break
                    else:
                        results.append('%s:%s/%s %s %s' % (node['ip'],
                            node['port'], node['device'], resp.status,
                            resp.reason))
                except socket.error, err:
                    results.append('%s:%s/%s Socket Error: %s' % (node['ip'],
                                   node['port'], node['device'], err))
                except ConnectionTimeout:
                    results.append(
                        '%(ip)s:%(port)s/%(device)s ConnectionTimeout' % node)
                except Timeout:
                    results.append('%(ip)s:%(port)s/%(device)s Timeout' % node)
                except Exception, err:
                    self.logger.exception('ERROR With remote server '
                                          '%(ip)s:%(port)s/%(device)s' % node)
                    results.append('%s:%s/%s Exception: %s' % (node['ip'],
                                   node['port'], node['device'], err))
            if found:
                self.container_passes += 1
                self.logger.debug('Audit passed for /%s %s container %s' %
                    (info['account'], broker.db_file, container))
            else:
                self.container_errors += 1
                self.logger.error('ERROR Could not find container /%s/%s '
                    'referenced by %s on any of the primary container '
                    'servers it should be on: %s' % (info['account'],
                    container, broker.db_file, results))
