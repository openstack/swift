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

from swift.container import server as container_server
from swift.common.db import ContainerBroker
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger


class AuditException(Exception):
    pass


class ContainerAuditor(object):
    """Audit containers."""

    def __init__(self, conf):
        self.logger = get_logger(conf)
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.interval = int(conf.get('interval', 1800))
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.account_ring_path = os.path.join(swift_dir, 'account.ring.gz')
        self.account_ring = None
        self.object_ring_path = os.path.join(swift_dir, 'object.ring.gz')
        self.object_ring = None
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.max_object_count = int(conf.get('max_object_count', 100))
        self.account_passes = 0
        self.account_failures = 0
        self.account_errors = 0
        self.object_passes = 0
        self.object_failures = 0
        self.object_errors = 0

    def get_account_ring(self):
        """
        Get the account ring.  Loads the ring if neccesary.

        :returns: account ring
        """
        if not self.account_ring:
            self.logger.debug(
                'Loading account ring from %s' % self.account_ring_path)
            self.account_ring = Ring(self.account_ring_path)
        return self.account_ring

    def get_object_ring(self):
        """
        Get the object ring.  Loads the ring if neccesary.

        :returns: object ring
        """
        if not self.object_ring:
            self.logger.debug(
                'Loading object ring from %s' % self.object_ring_path)
            self.object_ring = Ring(self.object_ring_path)
        return self.object_ring

    def audit_forever(self):  # pragma: no cover
        """Run the container audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            begin = time.time()
            for device in os.listdir(self.devices):
                if self.mount_check and not\
                        os.path.ismount(os.path.join(self.devices, device)):
                    self.logger.debug(
                        'Skipping %s as it is not mounted' % device)
                    continue
                self.container_audit(device)
            if time.time() - reported >= 3600:  # once an hour
                self.logger.info(
                    'Since %s: Remote audits with accounts: %s passed audit, '
                    '%s failed audit, %s errors  Remote audits with objects: '
                    '%s passed audit, %s failed audit, %s errors' %
                    (time.ctime(reported), self.account_passes,
                     self.account_failures, self.account_errors,
                     self.object_passes, self.object_failures,
                     self.object_errors))
                reported = time.time()
                self.account_passes = 0
                self.account_failures = 0
                self.account_errors = 0
                self.object_passes = 0
                self.object_failures = 0
                self.object_errors = 0
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def audit_once(self):
        """Run the container audit once."""
        self.logger.info('Begin container audit "once" mode')
        begin = time.time()
        for device in os.listdir(self.devices):
            if self.mount_check and \
                    not os.path.ismount(os.path.join(self.devices, device)):
                self.logger.debug(
                    'Skipping %s as it is not mounted' % device)
                continue
            self.container_audit(device)
        elapsed = time.time() - begin
        self.logger.info(
            'Container audit "once" mode completed: %.02fs' % elapsed)

    def container_audit(self, device):
        """
        Audit any containers found on the device

        :param device: device to audit
        """
        datadir = os.path.join(self.devices, device, container_server.DATADIR)
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
                    broker = ContainerBroker(os.path.join(fpath, fname))
                    if broker.is_deleted():
                        broker = None
                    break
        if not broker:
            return
        info = broker.get_info()
        found = False
        good_response = False
        results = []
        part, nodes = self.get_account_ring().get_nodes(info['account'])
        for node in nodes:
            try:
                with ConnectionTimeout(self.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], part, 'GET',
                            '/%s' % info['account'],
                            query_string='prefix=%s' %
                                quote(info['container']))
                with Timeout(self.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
                if 200 <= resp.status <= 299:
                    for cname in body.split('\n'):
                        if cname == info['container']:
                            found = True
                            break
                    if found:
                        break
                    else:
                        results.append('%s:%s/%s %s %s = %s' % (node['ip'],
                            node['port'], node['device'], resp.status,
                            resp.reason, repr(body)))
                else:
                    results.append('%s:%s/%s %s %s' %
                        (node['ip'], node['port'], node['device'],
                        resp.status, resp.reason))
            except socket.error, err:
                results.append('%s:%s/%s Socket Error: %s' % (node['ip'],
                               node['port'], node['device'], err))
            except ConnectionTimeout:
                results.append('%(ip)s:%(port)s/%(device)s ConnectionTimeout' %
                               node)
            except Timeout:
                results.append('%(ip)s:%(port)s/%(device)s Timeout' % node)
            except Exception, err:
                self.logger.exception('ERROR With remote server '
                                      '%(ip)s:%(port)s/%(device)s' % node)
                results.append('%s:%s/%s Exception: %s' % (node['ip'],
                               node['port'], node['device'], err))
        if found:
            self.account_passes += 1
            self.logger.debug('Audit passed for /%s/%s %s' % (info['account'],
                info['container'], broker.db_file))
        else:
            if good_response:
                self.account_failures += 1
            else:
                self.account_errors += 1
            self.logger.error('ERROR Could not find container /%s/%s %s on '
                'any of the primary account servers it should be on: %s' %
                (info['account'], info['container'], broker.db_file, results))
        for obj in broker.get_random_objects(max_count=self.max_object_count):
            found = False
            results = []
            part, nodes = self.get_object_ring().get_nodes(info['account'],
                            info['container'], obj)
            for node in nodes:
                try:
                    with ConnectionTimeout(self.conn_timeout):
                        conn = http_connect(node['ip'], node['port'],
                                node['device'], part, 'HEAD',
                                '/%s/%s/%s' %
                                    (info['account'], info['container'], obj))
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
                self.object_passes += 1
                self.logger.debug('Audit passed for /%s/%s %s object %s' %
                    (info['account'], info['container'], broker.db_file, obj))
            else:
                self.object_errors += 1
                self.logger.error('ERROR Could not find object /%s/%s/%s '
                    'referenced by %s on any of the primary object '
                    'servers it should be on: %s' % (info['account'],
                    info['container'], obj, broker.db_file, results))
