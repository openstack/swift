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

import cPickle as pickle
import os
import socket
import sys
import time
from hashlib import md5
from random import choice, random
from urllib import quote

from eventlet import Timeout

from swift.obj import server as object_server
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger, renamer
from swift.common.exceptions import AuditException


class ObjectAuditor(object):
    """Audit objects."""

    def __init__(self, server_conf, auditor_conf):
        self.logger = get_logger(auditor_conf, 'object-auditor')
        self.devices = server_conf.get('devices', '/srv/node')
        self.mount_check = server_conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.interval = int(auditor_conf.get('interval', 1800))
        swift_dir = server_conf.get('swift_dir', '/etc/swift')
        self.container_ring_path = os.path.join(swift_dir, 'container.ring.gz')
        self.container_ring = None
        self.node_timeout = int(auditor_conf.get('node_timeout', 10))
        self.conn_timeout = float(auditor_conf.get('conn_timeout', 0.5))
        self.passes = 0
        self.quarantines = 0
        self.errors = 0
        self.container_passes = 0
        self.container_failures = 0
        self.container_errors = 0

    def get_container_ring(self):
        """
        Get the container ring, loading it if neccesary.

        :returns: container ring
        """
        if not self.container_ring:
            self.logger.debug(
                'Loading container ring from %s' % self.container_ring_path)
            self.container_ring = Ring(self.container_ring_path)
        return self.container_ring

    def audit_forever(self):    # pragma: no cover
        """Run the object audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            begin = time.time()
            pids = []
            # read from container ring to ensure it's fresh
            self.get_container_ring().get_nodes('')
            for device in os.listdir(self.devices):
                if self.mount_check and not \
                        os.path.ismount(os.path.join(self.devices, device)):
                    self.logger.debug(
                        'Skipping %s as it is not mounted' % device)
                    continue
                self.object_audit(device)
            if time.time() - reported >= 3600:  # once an hour
                self.logger.info(
                    'Since %s: Locally: %d passed audit, %d quarantined, %d '
                    'errors  Remote audits with containers: %s passed audit, '
                    '%s failed audit, %s errors' %
                    (time.ctime(reported), self.passes, self.quarantines,
                     self.errors, self.container_passes,
                     self.container_failures, self.container_errors))
                reported = time.time()
                self.passes = 0
                self.quarantines = 0
                self.errors = 0
                self.container_passes = 0
                self.container_failures = 0
                self.container_errors = 0
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def audit_once(self):
        """Run the object audit once."""
        self.logger.info('Begin object audit "once" mode')
        begin = time.time()
        for device in os.listdir(self.devices):
            if self.mount_check and \
                    not os.path.ismount(os.path.join(self.devices, device)):
                self.logger.debug(
                    'Skipping %s as it is not mounted' % device)
                continue
            self.object_audit(device)
        elapsed = time.time() - begin
        self.logger.info(
            'Object audit "once" mode completed: %.02fs' % elapsed)

    def object_audit(self, device):
        """Walk the device, and audit any objects found."""
        datadir = os.path.join(self.devices, device, object_server.DATADIR)
        if not os.path.exists(datadir):
            return
        name = None
        partition = None
        attempts = 100
        while not name and attempts:
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
                if fname.endswith('.ts'):
                    break
                if fname.endswith('.data'):
                    name = object_server.read_metadata(
                        os.path.join(fpath, fname))['name']
                    break
        if not name:
            return
        _, account, container, obj = name.split('/', 3)
        df = object_server.DiskFile(self.devices, device, partition, account,
                                    container, obj, keep_data_fp=True)
        try:
            if os.path.getsize(df.data_file) != \
                    int(df.metadata['Content-Length']):
                raise AuditException('Content-Length of %s does not match '
                    'file size of %s' % (int(df.metadata['Content-Length']),
                                         os.path.getsize(df.data_file)))
            etag = md5()
            for chunk in df:
                etag.update(chunk)
            etag = etag.hexdigest()
            if etag != df.metadata['ETag']:
                raise AuditException("ETag of %s does not match file's md5 of "
                    "%s" % (df.metadata['ETag'], etag))
        except AuditException, err:
            self.quarantines += 1
            self.logger.error('ERROR Object %s failed audit and will be '
                'quarantined: %s' % (df.datadir, err))
            renamer(df.datadir, os.path.join(self.devices, device,
                'quarantined', 'objects', os.path.basename(df.datadir)))
            return
        except:
            self.errors += 1
            self.logger.exception('ERROR Trying to audit %s' % df.datadir)
            return
        self.passes += 1
        found = False
        good_response = False
        results = []
        part, nodes = self.get_container_ring().get_nodes(account, container)
        for node in nodes:
            try:
                with ConnectionTimeout(self.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], part, 'GET',
                            '/%s/%s' % (account, container),
                            query_string='prefix=%s' % quote(obj))
                with Timeout(self.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
                if 200 <= resp.status <= 299:
                    good_reponse = True
                    for oname in body.split('\n'):
                        if oname == obj:
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
            self.container_passes += 1
            self.logger.debug('Audit passed for %s %s' % (name, df.datadir))
        else:
            if good_response:
                self.container_failures += 1
            else:
                self.container_errors += 1
            self.logger.error('ERROR Could not find object %s %s on any of '
                'the primary container servers it should be on: %s' % (name,
                df.datadir, results))
