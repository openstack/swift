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

import six.moves.cPickle as pickle
import os
import signal
import sys
import time
from swift import gettext_ as _
from random import random

from eventlet import spawn, Timeout

from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_drive
from swift.common.exceptions import ConnectionTimeout
from swift.common.ring import Ring
from swift.common.utils import get_logger, renamer, write_pickle, \
    dump_recon_cache, config_true_value, ratelimit_sleep, eventlet_monkey_patch
from swift.common.daemon import Daemon
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.storage_policy import split_policy_string, PolicyError
from swift.obj.diskfile import get_tmp_dir, ASYNCDIR_BASE
from swift.common.http import is_success, HTTP_INTERNAL_SERVER_ERROR


class SweepStats(object):
    """
    Stats bucket for an update sweep
    """
    def __init__(self, errors=0, failures=0, quarantines=0, successes=0,
                 unlinks=0):
        self.errors = errors
        self.failures = failures
        self.quarantines = quarantines
        self.successes = successes
        self.unlinks = unlinks

    def copy(self):
        return type(self)(self.errors, self.failures, self.quarantines,
                          self.successes, self.unlinks)

    def since(self, other):
        return type(self)(self.errors - other.errors,
                          self.failures - other.failures,
                          self.quarantines - other.quarantines,
                          self.successes - other.successes,
                          self.unlinks - other.unlinks)

    def reset(self):
        self.errors = 0
        self.failures = 0
        self.quarantines = 0
        self.successes = 0
        self.unlinks = 0

    def __str__(self):
        keys = (
            (self.successes, 'successes'),
            (self.failures, 'failures'),
            (self.quarantines, 'quarantines'),
            (self.unlinks, 'unlinks'),
            (self.errors, 'errors'),
        )
        return ', '.join('%d %s' % pair for pair in keys)


class ObjectUpdater(Daemon):
    """Update object information in container listings."""

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='object-updater')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.interval = int(conf.get('interval', 300))
        self.container_ring = None
        self.concurrency = int(conf.get('concurrency', 1))
        if 'slowdown' in conf:
            self.logger.warning(
                'The slowdown option is deprecated in favor of '
                'objects_per_second. This option may be ignored in a '
                'future release.')
            objects_per_second = 1 / (
                float(conf.get('slowdown', '0.01')) + 0.01)
        else:
            objects_per_second = 50
        self.objects_running_time = 0
        self.max_objects_per_second = \
            float(conf.get('objects_per_second',
                           objects_per_second))
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.report_interval = float(conf.get('report_interval', 300))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, 'object.recon')
        self.stats = SweepStats()

    def _listdir(self, path):
        try:
            return os.listdir(path)
        except OSError as e:
            self.stats.errors += 1
            self.logger.increment('errors')
            self.logger.error(_('ERROR: Unable to access %(path)s: '
                                '%(error)s') %
                              {'path': path, 'error': e})
            return []

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
            for device in self._listdir(self.devices):
                if not check_drive(self.devices, device, self.mount_check):
                    # We don't count this as an error. The occasional
                    # unmounted drive is part of normal cluster operations,
                    # so a simple warning is sufficient.
                    self.logger.warning(
                        _('Skipping %s as it is not mounted'), device)
                    continue
                while len(pids) >= self.concurrency:
                    pids.remove(os.wait()[0])
                pid = os.fork()
                if pid:
                    pids.append(pid)
                else:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    eventlet_monkey_patch()
                    self.stats.reset()
                    forkbegin = time.time()
                    self.object_sweep(os.path.join(self.devices, device))
                    elapsed = time.time() - forkbegin
                    self.logger.info(
                        ('Object update sweep of %(device)s '
                         'completed: %(elapsed).02fs, %(stats)s'),
                        {'device': device, 'elapsed': elapsed,
                         'stats': self.stats})
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
        """Run the updater once."""
        self.logger.info(_('Begin object update single threaded sweep'))
        begin = time.time()
        self.stats.reset()
        for device in self._listdir(self.devices):
            if not check_drive(self.devices, device, self.mount_check):
                # We don't count this as an error. The occasional unmounted
                # drive is part of normal cluster operations, so a simple
                # warning is sufficient.
                self.logger.warning(
                    _('Skipping %s as it is not mounted'), device)
                continue
            self.object_sweep(os.path.join(self.devices, device))
        elapsed = time.time() - begin
        self.logger.info(
            ('Object update single-threaded sweep completed: '
             '%(elapsed).02fs, %(stats)s'),
            {'elapsed': elapsed, 'stats': self.stats})
        dump_recon_cache({'object_updater_sweep': elapsed},
                         self.rcache, self.logger)

    def object_sweep(self, device):
        """
        If there are async pendings on the device, walk each one and update.

        :param device: path to device
        """
        start_time = time.time()
        last_status_update = start_time
        start_stats = self.stats.copy()
        my_pid = os.getpid()
        self.logger.info("Object update sweep starting on %s (pid: %d)",
                         device, my_pid)

        # loop through async pending dirs for all policies
        for asyncdir in self._listdir(device):
            # we only care about directories
            async_pending = os.path.join(device, asyncdir)
            if not os.path.isdir(async_pending):
                continue
            if not asyncdir.startswith(ASYNCDIR_BASE):
                # skip stuff like "accounts", "containers", etc.
                continue
            try:
                base, policy = split_policy_string(asyncdir)
            except PolicyError as e:
                # This isn't an error, but a misconfiguration. Logging a
                # warning should be sufficient.
                self.logger.warning(_('Directory %(directory)r does not map '
                                      'to a valid policy (%(error)s)') % {
                                    'directory': asyncdir, 'error': e})
                continue
            for prefix in self._listdir(async_pending):
                prefix_path = os.path.join(async_pending, prefix)
                if not os.path.isdir(prefix_path):
                    continue
                last_obj_hash = None
                for update in sorted(self._listdir(prefix_path), reverse=True):
                    update_path = os.path.join(prefix_path, update)
                    if not os.path.isfile(update_path):
                        continue
                    try:
                        obj_hash, timestamp = update.split('-')
                    except ValueError:
                        self.stats.errors += 1
                        self.logger.increment('errors')
                        self.logger.error(
                            _('ERROR async pending file with unexpected '
                              'name %s')
                            % (update_path))
                        continue
                    if obj_hash == last_obj_hash:
                        self.stats.unlinks += 1
                        self.logger.increment('unlinks')
                        os.unlink(update_path)
                    else:
                        self.process_object_update(update_path, device,
                                                   policy)
                        last_obj_hash = obj_hash

                    self.objects_running_time = ratelimit_sleep(
                        self.objects_running_time,
                        self.max_objects_per_second)

                    now = time.time()
                    if now - last_status_update >= self.report_interval:
                        this_sweep = self.stats.since(start_stats)
                        self.logger.info(
                            ('Object update sweep progress on %(device)s: '
                             '%(elapsed).02fs, %(stats)s (pid: %(pid)d)'),
                            {'device': device,
                             'elapsed': now - start_time,
                             'pid': my_pid,
                             'stats': this_sweep})
                        last_status_update = now
                try:
                    os.rmdir(prefix_path)
                except OSError:
                    pass
            self.logger.timing_since('timing', start_time)
            sweep_totals = self.stats.since(start_stats)
            self.logger.info(
                ('Object update sweep completed on %(device)s '
                 'in %(elapsed).02fs seconds:, '
                 '%(successes)d successes, %(failures)d failures, '
                 '%(quarantines)d quarantines, '
                 '%(unlinks)d unlinks, %(errors)d errors '
                 '(pid: %(pid)d)'),
                {'device': device,
                 'elapsed': time.time() - start_time,
                 'pid': my_pid,
                 'successes': sweep_totals.successes,
                 'failures': sweep_totals.failures,
                 'quarantines': sweep_totals.quarantines,
                 'unlinks': sweep_totals.unlinks,
                 'errors': sweep_totals.errors})

    def process_object_update(self, update_path, device, policy):
        """
        Process the object information to be updated and update.

        :param update_path: path to pickled object update file
        :param device: path to device
        :param policy: storage policy of object update
        """
        try:
            update = pickle.load(open(update_path, 'rb'))
        except Exception:
            self.logger.exception(
                _('ERROR Pickle problem, quarantining %s'), update_path)
            self.stats.quarantines += 1
            self.logger.increment('quarantines')
            target_path = os.path.join(device, 'quarantined', 'objects',
                                       os.path.basename(update_path))
            renamer(update_path, target_path, fsync=False)
            return
        successes = update.get('successes', [])
        part, nodes = self.get_container_ring().get_nodes(
            update['account'], update['container'])
        obj = '/%s/%s/%s' % \
              (update['account'], update['container'], update['obj'])
        headers_out = HeaderKeyDict(update['headers'])
        headers_out['user-agent'] = 'object-updater %s' % os.getpid()
        headers_out.setdefault('X-Backend-Storage-Policy-Index',
                               str(int(policy)))
        events = [spawn(self.object_update,
                        node, part, update['op'], obj, headers_out)
                  for node in nodes if node['id'] not in successes]
        success = True
        new_successes = False
        for event in events:
            event_success, node_id = event.wait()
            if event_success is True:
                successes.append(node_id)
                new_successes = True
            else:
                success = False
        if success:
            self.stats.successes += 1
            self.logger.increment('successes')
            self.logger.debug('Update sent for %(obj)s %(path)s',
                              {'obj': obj, 'path': update_path})
            self.stats.unlinks += 1
            self.logger.increment('unlinks')
            os.unlink(update_path)
        else:
            self.stats.failures += 1
            self.logger.increment('failures')
            self.logger.debug('Update failed for %(obj)s %(path)s',
                              {'obj': obj, 'path': update_path})
            if new_successes:
                update['successes'] = successes
                write_pickle(update, update_path, os.path.join(
                    device, get_tmp_dir(policy)))

    def object_update(self, node, part, op, obj, headers_out):
        """
        Perform the object update to the container

        :param node: node dictionary from the container ring
        :param part: partition that holds the container
        :param op: operation performed (ex: 'PUT' or 'DELETE')
        :param obj: object name being updated
        :param headers_out: headers to send with the update
        """
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(node['ip'], node['port'], node['device'],
                                    part, op, obj, headers_out)
            with Timeout(self.node_timeout):
                resp = conn.getresponse()
                resp.read()
                success = is_success(resp.status)
                if not success:
                    self.logger.debug(
                        _('Error code %(status)d is returned from remote '
                          'server %(ip)s: %(port)s / %(device)s'),
                        {'status': resp.status, 'ip': node['ip'],
                         'port': node['port'], 'device': node['device']})
                return (success, node['id'])
        except (Exception, Timeout):
            self.logger.exception(_('ERROR with remote server '
                                    '%(ip)s:%(port)s/%(device)s'), node)
        return HTTP_INTERNAL_SERVER_ERROR, node['id']
