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
    dump_recon_cache, config_true_value, RateLimitedIterator, split_path, \
    eventlet_monkey_patch, get_redirect_data, ContextPool
from swift.common.daemon import Daemon
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.storage_policy import split_policy_string, PolicyError
from swift.obj.diskfile import get_tmp_dir, ASYNCDIR_BASE
from swift.common.http import is_success, HTTP_INTERNAL_SERVER_ERROR, \
    HTTP_MOVED_PERMANENTLY


class SweepStats(object):
    """
    Stats bucket for an update sweep
    """
    def __init__(self, errors=0, failures=0, quarantines=0, successes=0,
                 unlinks=0, redirects=0):
        self.errors = errors
        self.failures = failures
        self.quarantines = quarantines
        self.successes = successes
        self.unlinks = unlinks
        self.redirects = redirects

    def copy(self):
        return type(self)(self.errors, self.failures, self.quarantines,
                          self.successes, self.unlinks)

    def since(self, other):
        return type(self)(self.errors - other.errors,
                          self.failures - other.failures,
                          self.quarantines - other.quarantines,
                          self.successes - other.successes,
                          self.unlinks - other.unlinks,
                          self.redirects - other.redirects)

    def reset(self):
        self.errors = 0
        self.failures = 0
        self.quarantines = 0
        self.successes = 0
        self.unlinks = 0
        self.redirects = 0

    def __str__(self):
        keys = (
            (self.successes, 'successes'),
            (self.failures, 'failures'),
            (self.quarantines, 'quarantines'),
            (self.unlinks, 'unlinks'),
            (self.errors, 'errors'),
            (self.redirects, 'redirects'),
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
        self.concurrency = int(conf.get('concurrency', 8))
        self.updater_workers = int(conf.get('updater_workers', 1))
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
                try:
                    dev_path = check_drive(self.devices, device,
                                           self.mount_check)
                except ValueError as err:
                    # We don't count this as an error. The occasional
                    # unmounted drive is part of normal cluster operations,
                    # so a simple warning is sufficient.
                    self.logger.warning('Skipping: %s', err)
                    continue
                while len(pids) >= self.updater_workers:
                    pids.remove(os.wait()[0])
                pid = os.fork()
                if pid:
                    pids.append(pid)
                else:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    eventlet_monkey_patch()
                    self.stats.reset()
                    forkbegin = time.time()
                    self.object_sweep(dev_path)
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
            try:
                dev_path = check_drive(self.devices, device, self.mount_check)
            except ValueError as err:
                # We don't count this as an error. The occasional unmounted
                # drive is part of normal cluster operations, so a simple
                # warning is sufficient.
                self.logger.warning('Skipping: %s', err)
                continue
            self.object_sweep(dev_path)
        elapsed = time.time() - begin
        self.logger.info(
            ('Object update single-threaded sweep completed: '
             '%(elapsed).02fs, %(stats)s'),
            {'elapsed': elapsed, 'stats': self.stats})
        dump_recon_cache({'object_updater_sweep': elapsed},
                         self.rcache, self.logger)

    def _iter_async_pendings(self, device):
        """
        Locate and yield all the async pendings on the device. Multiple updates
        for the same object will come out in reverse-chronological order
        (i.e. newest first) so that callers can skip stale async_pendings.

        Tries to clean up empty directories as it goes.
        """
        # loop through async pending dirs for all policies
        for asyncdir in self._listdir(device):
            # we only care about directories
            async_pending = os.path.join(device, asyncdir)
            if not asyncdir.startswith(ASYNCDIR_BASE):
                # skip stuff like "accounts", "containers", etc.
                continue
            if not os.path.isdir(async_pending):
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
                    # Async pendings are stored on disk like this:
                    #
                    # <device>/async_pending/<suffix>/<obj_hash>-<timestamp>
                    #
                    # If there are multiple updates for a given object,
                    # they'll look like this:
                    #
                    # <device>/async_pending/<obj_suffix>/<obj_hash>-<timestamp1>
                    # <device>/async_pending/<obj_suffix>/<obj_hash>-<timestamp2>
                    # <device>/async_pending/<obj_suffix>/<obj_hash>-<timestamp3>
                    #
                    # Async updates also have the property that newer
                    # updates contain all the information in older updates.
                    # Since we sorted the directory listing in reverse
                    # order, we'll see timestamp3 first, yield it, and then
                    # unlink timestamp2 and timestamp1 since we know they
                    # are obsolete.
                    #
                    # This way, our caller only gets useful async_pendings.
                    if obj_hash == last_obj_hash:
                        self.stats.unlinks += 1
                        self.logger.increment('unlinks')
                        os.unlink(update_path)
                    else:
                        last_obj_hash = obj_hash
                        yield {'device': device, 'policy': policy,
                               'path': update_path,
                               'obj_hash': obj_hash, 'timestamp': timestamp}

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

        ap_iter = RateLimitedIterator(
            self._iter_async_pendings(device),
            elements_per_second=self.max_objects_per_second)
        with ContextPool(self.concurrency) as pool:
            for update in ap_iter:
                pool.spawn(self.process_object_update,
                           update['path'], update['device'], update['policy'])
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
            pool.waitall()

        self.logger.timing_since('timing', start_time)
        sweep_totals = self.stats.since(start_stats)
        self.logger.info(
            ('Object update sweep completed on %(device)s '
             'in %(elapsed).02fs seconds:, '
             '%(successes)d successes, %(failures)d failures, '
             '%(quarantines)d quarantines, '
             '%(unlinks)d unlinks, %(errors)d errors, '
             '%(redirects)d redirects '
             '(pid: %(pid)d)'),
            {'device': device,
             'elapsed': time.time() - start_time,
             'pid': my_pid,
             'successes': sweep_totals.successes,
             'failures': sweep_totals.failures,
             'quarantines': sweep_totals.quarantines,
             'unlinks': sweep_totals.unlinks,
             'errors': sweep_totals.errors,
             'redirects': sweep_totals.redirects})

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

        def do_update():
            successes = update.get('successes', [])
            headers_out = HeaderKeyDict(update['headers'].copy())
            headers_out['user-agent'] = 'object-updater %s' % os.getpid()
            headers_out.setdefault('X-Backend-Storage-Policy-Index',
                                   str(int(policy)))
            headers_out.setdefault('X-Backend-Accept-Redirect', 'true')
            container_path = update.get('container_path')
            if container_path:
                acct, cont = split_path('/' + container_path, minsegs=2)
            else:
                acct, cont = update['account'], update['container']
            part, nodes = self.get_container_ring().get_nodes(acct, cont)
            obj = '/%s/%s/%s' % (acct, cont, update['obj'])
            events = [spawn(self.object_update,
                            node, part, update['op'], obj, headers_out)
                      for node in nodes if node['id'] not in successes]
            success = True
            new_successes = rewrite_pickle = False
            redirect = None
            redirects = set()
            for event in events:
                event_success, node_id, redirect = event.wait()
                if event_success is True:
                    successes.append(node_id)
                    new_successes = True
                else:
                    success = False
                if redirect:
                    redirects.add(redirect)

            if success:
                self.stats.successes += 1
                self.logger.increment('successes')
                self.logger.debug('Update sent for %(obj)s %(path)s',
                                  {'obj': obj, 'path': update_path})
                self.stats.unlinks += 1
                self.logger.increment('unlinks')
                os.unlink(update_path)
                try:
                    # If this was the last async_pending in the directory,
                    # then this will succeed. Otherwise, it'll fail, and
                    # that's okay.
                    os.rmdir(os.path.dirname(update_path))
                except OSError:
                    pass
            elif redirects:
                # erase any previous successes
                update.pop('successes', None)
                redirect = max(redirects, key=lambda x: x[-1])[0]
                redirect_history = update.setdefault('redirect_history', [])
                if redirect in redirect_history:
                    # force next update to be sent to root, reset history
                    update['container_path'] = None
                    update['redirect_history'] = []
                else:
                    update['container_path'] = redirect
                    redirect_history.append(redirect)
                self.stats.redirects += 1
                self.logger.increment("redirects")
                self.logger.debug(
                    'Update redirected for %(obj)s %(path)s to %(shard)s',
                    {'obj': obj, 'path': update_path,
                     'shard': update['container_path']})
                rewrite_pickle = True
            else:
                self.stats.failures += 1
                self.logger.increment('failures')
                self.logger.debug('Update failed for %(obj)s %(path)s',
                                  {'obj': obj, 'path': update_path})
                if new_successes:
                    update['successes'] = successes
                    rewrite_pickle = True

            return rewrite_pickle, redirect

        rewrite_pickle, redirect = do_update()
        if redirect:
            # make one immediate retry to the redirect location
            rewrite_pickle, redirect = do_update()
        if rewrite_pickle:
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
        :return: a tuple of (``success``, ``node_id``, ``redirect``)
            where ``success`` is True if the update succeeded, ``node_id`` is
            the_id of the node updated and ``redirect`` is either None or a
            tuple of (a path, a timestamp string).
        """
        redirect = None
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(node['ip'], node['port'], node['device'],
                                    part, op, obj, headers_out)
            with Timeout(self.node_timeout):
                resp = conn.getresponse()
                resp.read()

            if resp.status == HTTP_MOVED_PERMANENTLY:
                try:
                    redirect = get_redirect_data(resp)
                except ValueError as err:
                    self.logger.error(
                        'Container update failed for %r; problem with '
                        'redirect location: %s' % (obj, err))

            success = is_success(resp.status)
            if not success:
                self.logger.debug(
                    _('Error code %(status)d is returned from remote '
                      'server %(ip)s: %(port)s / %(device)s'),
                    {'status': resp.status, 'ip': node['ip'],
                     'port': node['port'], 'device': node['device']})
            return success, node['id'], redirect
        except (Exception, Timeout):
            self.logger.exception(_('ERROR with remote server '
                                    '%(ip)s:%(port)s/%(device)s'), node)
        return HTTP_INTERNAL_SERVER_ERROR, node['id'], redirect
