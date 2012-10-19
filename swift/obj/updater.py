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

import cPickle as pickle
import os
import signal
import sys
import time
from random import random

from eventlet import patcher, Timeout

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger, renamer, write_pickle, \
    dump_recon_cache, config_true_value
from swift.common.daemon import Daemon
from swift.obj.server import ASYNCDIR
from swift.common.http import is_success, HTTP_NOT_FOUND, \
    HTTP_INTERNAL_SERVER_ERROR


class ObjectUpdater(Daemon):
    """Update object information in container listings."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-updater')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.interval = int(conf.get('interval', 300))
        self.container_ring = None
        self.concurrency = int(conf.get('concurrency', 1))
        self.slowdown = float(conf.get('slowdown', 0.01))
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.successes = 0
        self.failures = 0
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, 'object.recon')

    def get_container_ring(self):
        """Get the container ring.  Load it, if it hasn't been yet."""
        if not self.container_ring:
            self.container_ring = Ring(self.swift_dir, ring_name='container')
        return self.container_ring

    def run_forever(self, *args, **kwargs):
        """Run the updater continuously."""
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin object update sweep'))
            begin = time.time()
            pids = []
            # read from container ring to ensure it's fresh
            self.get_container_ring().get_nodes('')
            for device in os.listdir(self.devices):
                if self.mount_check and not \
                        os.path.ismount(os.path.join(self.devices, device)):
                    self.logger.increment('errors')
                    self.logger.warn(
                        _('Skipping %s as it is not mounted'), device)
                    continue
                while len(pids) >= self.concurrency:
                    pids.remove(os.wait()[0])
                pid = os.fork()
                if pid:
                    pids.append(pid)
                else:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    patcher.monkey_patch(all=False, socket=True)
                    self.successes = 0
                    self.failures = 0
                    forkbegin = time.time()
                    self.object_sweep(os.path.join(self.devices, device))
                    elapsed = time.time() - forkbegin
                    self.logger.info(
                        _('Object update sweep of %(device)s'
                          ' completed: %(elapsed).02fs, %(success)s successes'
                          ', %(fail)s failures'),
                        {'device': device, 'elapsed': elapsed,
                         'success': self.successes, 'fail': self.failures})
                    sys.exit()
            while pids:
                pids.remove(os.wait()[0])
            elapsed = time.time() - begin
            self.logger.info(_('Object update sweep completed: %.02fs'),
                             elapsed)
            dump_recon_cache({'object_updater_sweep': elapsed},
                             self.rcache, self.logger)
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """Run the updater once"""
        self.logger.info(_('Begin object update single threaded sweep'))
        begin = time.time()
        self.successes = 0
        self.failures = 0
        for device in os.listdir(self.devices):
            if self.mount_check and \
                    not os.path.ismount(os.path.join(self.devices, device)):
                self.logger.increment('errors')
                self.logger.warn(
                    _('Skipping %s as it is not mounted'), device)
                continue
            self.object_sweep(os.path.join(self.devices, device))
        elapsed = time.time() - begin
        self.logger.info(
            _('Object update single threaded sweep completed: '
              '%(elapsed).02fs, %(success)s successes, %(fail)s failures'),
            {'elapsed': elapsed, 'success': self.successes,
             'fail': self.failures})
        dump_recon_cache({'object_updater_sweep': elapsed},
                         self.rcache, self.logger)

    def object_sweep(self, device):
        """
        If there are async pendings on the device, walk each one and update.

        :param device: path to device
        """
        start_time = time.time()
        async_pending = os.path.join(device, ASYNCDIR)
        if not os.path.isdir(async_pending):
            return
        for prefix in os.listdir(async_pending):
            prefix_path = os.path.join(async_pending, prefix)
            if not os.path.isdir(prefix_path):
                continue
            last_obj_hash = None
            for update in sorted(os.listdir(prefix_path), reverse=True):
                update_path = os.path.join(prefix_path, update)
                if not os.path.isfile(update_path):
                    continue
                try:
                    obj_hash, timestamp = update.split('-')
                except ValueError:
                    self.logger.increment('errors')
                    self.logger.error(
                        _('ERROR async pending file with unexpected name %s')
                        % (update_path))
                    continue
                if obj_hash == last_obj_hash:
                    self.logger.increment("unlinks")
                    os.unlink(update_path)
                else:
                    self.process_object_update(update_path, device)
                    last_obj_hash = obj_hash
                time.sleep(self.slowdown)
            try:
                os.rmdir(prefix_path)
            except OSError:
                pass
        self.logger.timing_since('timing', start_time)

    def process_object_update(self, update_path, device):
        """
        Process the object information to be updated and update.

        :param update_path: path to pickled object update file
        :param device: path to device
        """
        try:
            update = pickle.load(open(update_path, 'rb'))
        except Exception:
            self.logger.exception(
                _('ERROR Pickle problem, quarantining %s'), update_path)
            self.logger.increment('quarantines')
            renamer(update_path, os.path.join(
                    device, 'quarantined', 'objects',
                    os.path.basename(update_path)))
            return
        successes = update.get('successes', [])
        part, nodes = self.get_container_ring().get_nodes(
            update['account'], update['container'])
        obj = '/%s/%s/%s' % \
              (update['account'], update['container'], update['obj'])
        success = True
        new_successes = False
        for node in nodes:
            if node['id'] not in successes:
                status = self.object_update(node, part, update['op'], obj,
                                            update['headers'])
                if not is_success(status) and status != HTTP_NOT_FOUND:
                    success = False
                else:
                    successes.append(node['id'])
                    new_successes = True
        if success:
            self.successes += 1
            self.logger.increment('successes')
            self.logger.debug(_('Update sent for %(obj)s %(path)s'),
                              {'obj': obj, 'path': update_path})
            self.logger.increment("unlinks")
            os.unlink(update_path)
        else:
            self.failures += 1
            self.logger.increment('failures')
            self.logger.debug(_('Update failed for %(obj)s %(path)s'),
                              {'obj': obj, 'path': update_path})
            if new_successes:
                update['successes'] = successes
                write_pickle(update, update_path, os.path.join(device, 'tmp'))

    def object_update(self, node, part, op, obj, headers):
        """
        Perform the object update to the container

        :param node: node dictionary from the container ring
        :param part: partition that holds the container
        :param op: operation performed (ex: 'POST' or 'DELETE')
        :param obj: object name being updated
        :param headers: headers to send with the update
        """
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(node['ip'], node['port'], node['device'],
                                    part, op, obj, headers)
            with Timeout(self.node_timeout):
                resp = conn.getresponse()
                resp.read()
                return resp.status
        except (Exception, Timeout):
            self.logger.exception(_('ERROR with remote server '
                                    '%(ip)s:%(port)s/%(device)s'), node)
        return HTTP_INTERNAL_SERVER_ERROR
