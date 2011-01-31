# Copyright (c) 2010-2011 OpenStack, LLC.
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

from __future__ import with_statement
import sys
import os
import random
import math
import time
import shutil

from eventlet import GreenPool, sleep, Timeout, TimeoutError
from eventlet.green import subprocess
import simplejson
from webob import Response
from webob.exc import HTTPNotFound, HTTPNoContent, HTTPAccepted, \
    HTTPInsufficientStorage, HTTPBadRequest

from swift.common.utils import get_logger, whataremyips, storage_directory, \
    renamer, mkdirs, lock_parent_directory, unlink_older_than, LoggerFileObject
from swift.common import ring
from swift.common.bufferedhttp import BufferedHTTPConnection
from swift.common.exceptions import DriveNotMounted, ConnectionTimeout
from swift.common.daemon import Daemon


def quarantine_db(object_file, server_type):
    """
    In the case that a corrupt file is found, move it to a quarantined area to
    allow replication to fix it.

    :param object_file: path to corrupt file
    :param server_type: type of file that is corrupt
                        ('container' or 'account')
    """
    object_dir = os.path.dirname(object_file)
    quarantine_dir = os.path.abspath(os.path.join(object_dir, '..',
        '..', '..', '..', 'quarantined', server_type + 's',
        os.path.basename(object_dir)))
    renamer(object_dir, quarantine_dir)


class ReplConnection(BufferedHTTPConnection):
    """
    Helper to simplify REPLICATEing to a remote server.
    """

    def __init__(self, node, partition, hash_, logger):
        ""
        self.logger = logger
        self.node = node
        BufferedHTTPConnection.__init__(self, '%(ip)s:%(port)s' % node)
        self.path = '/%s/%s/%s' % (node['device'], partition, hash_)

    def replicate(self, *args):
        """
        Make an HTTP REPLICATE request

        :param args: list of json-encodable objects

        :returns: httplib response object
        """
        try:
            body = simplejson.dumps(args)
            self.request('REPLICATE', self.path, body,
                    {'Content-Type': 'application/json'})
            response = self.getresponse()
            response.data = response.read()
            return response
        except Exception:
            self.logger.exception(
                _('ERROR reading HTTP response from %s'), self.node)
            return None


class Replicator(Daemon):
    """
    Implements the logic for directing db replication.
    """

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf)
        self.root = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.port = int(conf.get('bind_port', self.default_port))
        concurrency = int(conf.get('concurrency', 8))
        self.cpool = GreenPool(size=concurrency)
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring = ring.Ring(os.path.join(swift_dir, self.ring_file))
        self.per_diff = int(conf.get('per_diff', 1000))
        self.run_pause = int(conf.get('run_pause', 30))
        self.vm_test_mode = conf.get(
            'vm_test_mode', 'no').lower() in ('yes', 'true', 'on', '1')
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.reclaim_age = float(conf.get('reclaim_age', 86400 * 7))
        self._zero_stats()

    def _zero_stats(self):
        """Zero out the stats."""
        self.stats = {'attempted': 0, 'success': 0, 'failure': 0, 'ts_repl': 0,
                      'no_change': 0, 'hashmatch': 0, 'rsync': 0, 'diff': 0,
                      'remove': 0, 'empty': 0, 'remote_merge': 0,
                      'start': time.time()}

    def _report_stats(self):
        """Report the current stats to the logs."""
        self.logger.info(
            _('Attempted to replicate %(count)d dbs in %(time).5f seconds '
              '(%(rate).5f/s)'),
            {'count': self.stats['attempted'],
             'time': time.time() - self.stats['start'],
             'rate': self.stats['attempted'] /
                        (time.time() - self.stats['start'] + 0.0000001)})
        self.logger.info(_('Removed %(remove)d dbs') % self.stats)
        self.logger.info(_('%(success)s successes, %(failure)s failures')
            % self.stats)
        self.logger.info(' '.join(['%s:%s' % item for item in
             self.stats.items() if item[0] in
             ('no_change', 'hashmatch', 'rsync', 'diff', 'ts_repl', 'empty')]))

    def _rsync_file(self, db_file, remote_file, whole_file=True):
        """
        Sync a single file using rsync. Used by _rsync_db to handle syncing.

        :param db_file: file to be synced
        :param remote_file: remote location to sync the DB file to
        :param whole-file: if True, uses rsync's --whole-file flag

        :returns: True if the sync was successful, False otherwise
        """
        popen_args = ['rsync', '--quiet', '--no-motd',
                      '--timeout=%s' % int(math.ceil(self.node_timeout)),
                      '--contimeout=%s' % int(math.ceil(self.conn_timeout))]
        if whole_file:
            popen_args.append('--whole-file')
        popen_args.extend([db_file, remote_file])
        proc = subprocess.Popen(popen_args)
        proc.communicate()
        if proc.returncode != 0:
            self.logger.error(_('ERROR rsync failed with %(code)s: %(args)s'),
                              {'code': proc.returncode, 'args': popen_args})
        return proc.returncode == 0

    def _rsync_db(self, broker, device, http, local_id,
            replicate_method='complete_rsync', replicate_timeout=None):
        """
        Sync a whole db using rsync.

        :param broker: DB broker object of DB to be synced
        :param device: device to sync to
        :param http: ReplConnection object
        :param local_id: unique ID of the local database replica
        :param replicate_method: remote operation to perform after rsync
        :param replicate_timeout: timeout to wait in seconds
        """
        if self.vm_test_mode:
            remote_file = '%s::%s%s/%s/tmp/%s' % (device['ip'],
                    self.server_type, device['port'], device['device'],
                    local_id)
        else:
            remote_file = '%s::%s/%s/tmp/%s' % (device['ip'],
                    self.server_type, device['device'], local_id)
        mtime = os.path.getmtime(broker.db_file)
        if not self._rsync_file(broker.db_file, remote_file):
            return False
        # perform block-level sync if the db was modified during the first sync
        if os.path.exists(broker.db_file + '-journal') or \
                    os.path.getmtime(broker.db_file) > mtime:
            # grab a lock so nobody else can modify it
            with broker.lock():
                if not self._rsync_file(broker.db_file, remote_file, False):
                    return False
        with Timeout(replicate_timeout or self.node_timeout):
            response = http.replicate(replicate_method, local_id)
        return response and response.status >= 200 and response.status < 300

    def _usync_db(self, point, broker, http, remote_id, local_id):
        """
        Sync a db by sending all records since the last sync.

        :param point: synchronization high water mark between the replicas
        :param broker: database broker object
        :param http: ReplConnection object for the remote server
        :param remote_id: database id for the remote replica
        :param local_id: database id for the local replica

        :returns: boolean indicating completion and success
        """
        self.stats['diff'] += 1
        self.logger.debug(_('Syncing chunks with %s'), http.host)
        sync_table = broker.get_syncs()
        objects = broker.get_items_since(point, self.per_diff)
        while len(objects):
            with Timeout(self.node_timeout):
                response = http.replicate('merge_items', objects, local_id)
            if not response or response.status >= 300 or response.status < 200:
                if response:
                    self.logger.error(_('ERROR Bad response %(status)s from '
                        '%(host)s'),
                        {'status': response.status, 'host': http.host})
                return False
            point = objects[-1]['ROWID']
            objects = broker.get_items_since(point, self.per_diff)
        with Timeout(self.node_timeout):
            response = http.replicate('merge_syncs', sync_table)
        if response and response.status >= 200 and response.status < 300:
            broker.merge_syncs([{'remote_id': remote_id,
                    'sync_point': point}], incoming=False)
            return True
        return False

    def _in_sync(self, rinfo, info, broker, local_sync):
        """
        Determine whether or not two replicas of a databases are considered
        to be in sync.

        :param rinfo: remote database info
        :param info: local database info
        :param broker: database broker object
        :param local_sync: cached last sync point between replicas

        :returns: boolean indicating whether or not the replicas are in sync
        """
        if max(rinfo['point'], local_sync) >= info['max_row']:
            self.stats['no_change'] += 1
            return True
        if rinfo['hash'] == info['hash']:
            self.stats['hashmatch'] += 1
            broker.merge_syncs([{'remote_id': rinfo['id'],
                'sync_point': rinfo['point']}], incoming=False)
            return True

    def _http_connect(self, node, partition, db_file):
        """
        Make an http_connection using ReplConnection

        :param node: node dictionary from the ring
        :param partition: partition partition to send in the url
        :param db_file: DB file

        :returns: ReplConnection object
        """
        return ReplConnection(node, partition,
                os.path.basename(db_file).split('.', 1)[0], self.logger)

    def _repl_to_node(self, node, broker, partition, info):
        """
        Replicate a database to a node.

        :param node: node dictionary from the ring to be replicated to
        :param broker: DB broker for the DB to be replication
        :param partition: partition on the node to replicate to
        :param info: DB info as a dictionary of {'max_row', 'hash', 'id',
                     'created_at', 'put_timestamp', 'delete_timestamp',
                     'metadata'}

        :returns: True if successful, False otherwise
        """
        with ConnectionTimeout(self.conn_timeout):
            http = self._http_connect(node, partition, broker.db_file)
        if not http:
            self.logger.error(
                _('ERROR Unable to connect to remote server: %s'), node)
            return False
        with Timeout(self.node_timeout):
            response = http.replicate('sync', info['max_row'], info['hash'],
                info['id'], info['created_at'], info['put_timestamp'],
                info['delete_timestamp'], info['metadata'])
        if not response:
            return False
        elif response.status == HTTPNotFound.code:  # completely missing, rsync
            self.stats['rsync'] += 1
            return self._rsync_db(broker, node, http, info['id'])
        elif response.status == HTTPInsufficientStorage.code:
            raise DriveNotMounted()
        elif response.status >= 200 and response.status < 300:
            rinfo = simplejson.loads(response.data)
            local_sync = broker.get_sync(rinfo['id'], incoming=False)
            if self._in_sync(rinfo, info, broker, local_sync):
                return True
            # if the difference in rowids between the two differs by
            # more than 50%, rsync then do a remote merge.
            if rinfo['max_row'] / float(info['max_row']) < 0.5:
                self.stats['remote_merge'] += 1
                return self._rsync_db(broker, node, http, info['id'],
                        replicate_method='rsync_then_merge',
                        replicate_timeout=(info['count'] / 2000))
            # else send diffs over to the remote server
            return self._usync_db(max(rinfo['point'], local_sync),
                        broker, http, rinfo['id'], info['id'])

    def _replicate_object(self, partition, object_file, node_id):
        """
        Replicate the db, choosing method based on whether or not it
        already exists on peers.

        :param partition: partition to be replicated to
        :param object_file: DB file name to be replicated
        :param node_id: node id of the node to be replicated to
        """
        self.logger.debug(_('Replicating db %s'), object_file)
        self.stats['attempted'] += 1
        try:
            broker = self.brokerclass(object_file, pending_timeout=30)
            broker.reclaim(time.time() - self.reclaim_age,
                           time.time() - (self.reclaim_age * 2))
            info = broker.get_replication_info()
        except Exception, e:
            if 'no such table' in str(e):
                self.logger.error(_('Quarantining DB %s'), object_file)
                quarantine_db(broker.db_file, broker.db_type)
            else:
                self.logger.exception(_('ERROR reading db %s'), object_file)
            self.stats['failure'] += 1
            return
        # The db is considered deleted if the delete_timestamp value is greater
        # than the put_timestamp, and there are no objects.
        delete_timestamp = 0
        try:
            delete_timestamp = float(info['delete_timestamp'])
        except ValueError:
            pass
        put_timestamp = 0
        try:
            put_timestamp = float(info['put_timestamp'])
        except ValueError:
            pass
        if delete_timestamp < (time.time() - self.reclaim_age) and \
                delete_timestamp > put_timestamp and \
                info['count'] in (None, '', 0, '0'):
            with lock_parent_directory(object_file):
                shutil.rmtree(os.path.dirname(object_file), True)
                self.stats['remove'] += 1
            return
        responses = []
        nodes = self.ring.get_part_nodes(int(partition))
        shouldbehere = bool([n for n in nodes if n['id'] == node_id])
        repl_nodes = [n for n in nodes if n['id'] != node_id]
        more_nodes = self.ring.get_more_nodes(int(partition))
        for node in repl_nodes:
            success = False
            try:
                success = self._repl_to_node(node, broker, partition, info)
            except DriveNotMounted:
                repl_nodes.append(more_nodes.next())
                self.logger.error(_('ERROR Remote drive not mounted %s'), node)
            except (Exception, TimeoutError):
                self.logger.exception(_('ERROR syncing %(file)s with node'
                        ' %(node)s'), {'file': object_file, 'node': node})
            self.stats['success' if success else 'failure'] += 1
            responses.append(success)
        if not shouldbehere and all(responses):
            # If the db shouldn't be on this node and has been successfully
            # synced to all of its peers, it can be removed.
            with lock_parent_directory(object_file):
                shutil.rmtree(os.path.dirname(object_file), True)
                self.stats['remove'] += 1

    def roundrobin_datadirs(self, datadirs):
        """
        Generator to walk the data dirs in a round robin manner, evenly
        hitting each device on the system.

        :param datadirs: a list of paths to walk
        """

        def walk_datadir(datadir, node_id):
            partitions = os.listdir(datadir)
            random.shuffle(partitions)
            for partition in partitions:
                part_dir = os.path.join(datadir, partition)
                for root, dirs, files in os.walk(part_dir, topdown=False):
                    for fname in (f for f in files if f.endswith('.db')):
                        object_file = os.path.join(root, fname)
                        yield (partition, object_file, node_id)
        its = [walk_datadir(datadir, node_id) for datadir, node_id in datadirs]
        while its:
            for it in its:
                try:
                    yield it.next()
                except StopIteration:
                    its.remove(it)

    def run_once(self):
        """Run a replication pass once."""
        self._zero_stats()
        dirs = []
        ips = whataremyips()
        if not ips:
            self.logger.error(_('ERROR Failed to get my own IPs?'))
            return
        for node in self.ring.devs:
            if node and node['ip'] in ips and node['port'] == self.port:
                if self.mount_check and not os.path.ismount(
                        os.path.join(self.root, node['device'])):
                    self.logger.warn(
                        _('Skipping %(device)s as it is not mounted') % node)
                    continue
                unlink_older_than(
                    os.path.join(self.root, node['device'], 'tmp'),
                    time.time() - self.reclaim_age)
                datadir = os.path.join(self.root, node['device'], self.datadir)
                if os.path.isdir(datadir):
                    dirs.append((datadir, node['id']))
        self.logger.info(_('Beginning replication run'))
        for part, object_file, node_id in self.roundrobin_datadirs(dirs):
            self.cpool.spawn_n(
                self._replicate_object, part, object_file, node_id)
        self.cpool.waitall()
        self.logger.info(_('Replication run OVER'))
        self._report_stats()

    def run_forever(self):
        """
        Replicate dbs under the given root in an infinite loop.
        """
        while True:
            try:
                self.run_once()
            except (Exception, TimeoutError):
                self.logger.exception(_('ERROR trying to replicate'))
            sleep(self.run_pause)


class ReplicatorRpc(object):
    """Handle Replication RPC calls.  TODO(redbo): document please :)"""

    def __init__(self, root, datadir, broker_class, mount_check=True):
        self.root = root
        self.datadir = datadir
        self.broker_class = broker_class
        self.mount_check = mount_check

    def dispatch(self, replicate_args, args):
        if not hasattr(args, 'pop'):
            return HTTPBadRequest(body='Invalid object type')
        op = args.pop(0)
        drive, partition, hsh = replicate_args
        if self.mount_check and \
                not os.path.ismount(os.path.join(self.root, drive)):
            return Response(status='507 %s is not mounted' % drive)
        db_file = os.path.join(self.root, drive,
                storage_directory(self.datadir, partition, hsh), hsh + '.db')
        if op == 'rsync_then_merge':
            return self.rsync_then_merge(drive, db_file, args)
        if op == 'complete_rsync':
            return self.complete_rsync(drive, db_file, args)
        else:
            # someone might be about to rsync a db to us,
            # make sure there's a tmp dir to receive it.
            mkdirs(os.path.join(self.root, drive, 'tmp'))
            if not os.path.exists(db_file):
                return HTTPNotFound()
            return getattr(self, op)(self.broker_class(db_file), args)

    def sync(self, broker, args):
        (remote_sync, hash_, id_, created_at, put_timestamp,
         delete_timestamp, metadata) = args
        try:
            info = broker.get_replication_info()
        except Exception, e:
            if 'no such table' in str(e):
                # TODO(unknown): find a real logger
                print _("Quarantining DB %s") % broker.db_file
                quarantine_db(broker.db_file, broker.db_type)
                return HTTPNotFound()
            raise
        if metadata:
            broker.update_metadata(simplejson.loads(metadata))
        if info['put_timestamp'] != put_timestamp or \
                    info['created_at'] != created_at or \
                    info['delete_timestamp'] != delete_timestamp:
            broker.merge_timestamps(
                created_at, put_timestamp, delete_timestamp)
        info['point'] = broker.get_sync(id_)
        if hash_ == info['hash'] and info['point'] < remote_sync:
            broker.merge_syncs([{'remote_id': id_,
                                 'sync_point': remote_sync}])
            info['point'] = remote_sync
        return Response(simplejson.dumps(info))

    def merge_syncs(self, broker, args):
        broker.merge_syncs(args[0])
        return HTTPAccepted()

    def merge_items(self, broker, args):
        broker.merge_items(args[0], args[1])
        return HTTPAccepted()

    def complete_rsync(self, drive, db_file, args):
        old_filename = os.path.join(self.root, drive, 'tmp', args[0])
        if os.path.exists(db_file):
            return HTTPNotFound()
        if not os.path.exists(old_filename):
            return HTTPNotFound()
        broker = self.broker_class(old_filename)
        broker.newid(args[0])
        renamer(old_filename, db_file)
        return HTTPNoContent()

    def rsync_then_merge(self, drive, db_file, args):
        old_filename = os.path.join(self.root, drive, 'tmp', args[0])
        if not os.path.exists(db_file) or not os.path.exists(old_filename):
            return HTTPNotFound()
        new_broker = self.broker_class(old_filename)
        existing_broker = self.broker_class(db_file)
        point = -1
        objects = existing_broker.get_items_since(point, 1000)
        while len(objects):
            new_broker.merge_items(objects)
            point = objects[-1]['ROWID']
            objects = existing_broker.get_items_since(point, 1000)
            sleep()
        new_broker.newid(args[0])
        renamer(old_filename, db_file)
        return HTTPNoContent()
