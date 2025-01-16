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

import collections
import errno
import os
import uuid
from time import ctime, time
from random import choice, random
from struct import unpack_from

from eventlet import sleep, Timeout
from urllib.parse import urlparse

import swift.common.db
from swift.common.db import DatabaseConnectionError
from swift.container.backend import ContainerBroker
from swift.container.sync_store import ContainerSyncStore
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.daemon import run_daemon
from swift.common.internal_client import (
    delete_object, put_object, head_object,
    InternalClient, UnexpectedResponse)
from swift.common.exceptions import ClientException
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.swob import normalize_etag
from swift.common.utils import (
    clean_content_type, config_true_value,
    FileLikeIter, get_logger, hash_path, quote, validate_sync_to,
    whataremyips, Timestamp, decode_timestamps, parse_options)
from swift.common.daemon import Daemon
from swift.common.http import HTTP_UNAUTHORIZED, HTTP_NOT_FOUND, HTTP_CONFLICT
from swift.common.wsgi import ConfigString
from swift.common.middleware.versioned_writes.object_versioning import (
    SYSMETA_VERSIONS_CONT, SYSMETA_VERSIONS_SYMLINK)


# The default internal client config body is to support upgrades without
# requiring deployment of the new /etc/swift/internal-client.conf
ic_conf_body = """
[DEFAULT]
[pipeline:main]
pipeline = catch_errors proxy-logging cache symlink proxy-server

[app:proxy-server]
use = egg:swift#proxy
account_autocreate = true

[filter:symlink]
use = egg:swift#symlink

[filter:cache]
use = egg:swift#memcache

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:catch_errors]
use = egg:swift#catch_errors
""".lstrip()


class ContainerSync(Daemon):
    """
    Daemon to sync syncable containers.

    This is done by scanning the local devices for container databases and
    checking for x-container-sync-to and x-container-sync-key metadata values.
    If they exist, newer rows since the last sync will trigger PUTs or DELETEs
    to the other container.

    The actual syncing is slightly more complicated to make use of the three
    (or number-of-replicas) main nodes for a container without each trying to
    do the exact same work but also without missing work if one node happens to
    be down.

    Two sync points are kept per container database. All rows between the two
    sync points trigger updates. Any rows newer than both sync points cause
    updates depending on the node's position for the container (primary nodes
    do one third, etc. depending on the replica count of course). After a sync
    run, the first sync point is set to the newest ROWID known and the second
    sync point is set to newest ROWID for which all updates have been sent.

    An example may help. Assume replica count is 3 and perfectly matching
    ROWIDs starting at 1.

       First sync run, database has 6 rows:

       * SyncPoint1 starts as -1.
       * SyncPoint2 starts as -1.
       * No rows between points, so no "all updates" rows.
       * Six rows newer than SyncPoint1, so a third of the rows are sent
         by node 1, another third by node 2, remaining third by node 3.
       * SyncPoint1 is set as 6 (the newest ROWID known).
       * SyncPoint2 is left as -1 since no "all updates" rows were synced.

       Next sync run, database has 12 rows:

       * SyncPoint1 starts as 6.
       * SyncPoint2 starts as -1.
       * The rows between -1 and 6 all trigger updates (most of which
         should short-circuit on the remote end as having already been
         done).
       * Six more rows newer than SyncPoint1, so a third of the rows are
         sent by node 1, another third by node 2, remaining third by node
         3.
       * SyncPoint1 is set as 12 (the newest ROWID known).
       * SyncPoint2 is set as 6 (the newest "all updates" ROWID).

    In this way, under normal circumstances each node sends its share of
    updates each run and just sends a batch of older updates to ensure nothing
    was missed.

    :param conf: The dict of configuration values from the [container-sync]
                 section of the container-server.conf
    :param container_ring: If None, the <swift_dir>/container.ring.gz will be
                           loaded. This is overridden by unit tests.
    """
    log_route = 'container-sync'

    def __init__(self, conf, container_ring=None, logger=None):
        #: The dict of configuration values from the [container-sync] section
        #: of the container-server.conf.
        self.conf = conf
        #: Logger to use for container-sync log lines.
        self.logger = logger or get_logger(conf, log_route=self.log_route)
        #: Path to the local device mount points.
        self.devices = conf.get('devices', '/srv/node')
        #: Indicates whether mount points should be verified as actual mount
        #: points (normally true, false for tests and SAIO).
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        #: Minimum time between full scans. This is to keep the daemon from
        #: running wild on near empty systems.
        self.interval = float(conf.get('interval', 300))
        #: Maximum amount of time to spend syncing a container before moving on
        #: to the next one. If a container sync hasn't finished in this time,
        #: it'll just be resumed next scan.
        self.container_time = int(conf.get('container_time', 60))
        #: ContainerSyncCluster instance for validating sync-to values.
        self.realms_conf = ContainerSyncRealms(
            os.path.join(
                conf.get('swift_dir', '/etc/swift'),
                'container-sync-realms.conf'),
            self.logger)
        #: The list of hosts we're allowed to send syncs to. This can be
        #: overridden by data in self.realms_conf
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.http_proxies = [
            a.strip()
            for a in conf.get('sync_proxy', '').split(',')
            if a.strip()]
        #: ContainerSyncStore instance for iterating over synced containers
        self.sync_store = ContainerSyncStore(self.devices,
                                             self.logger,
                                             self.mount_check)
        #: Number of containers with sync turned on that were successfully
        #: synced.
        self.container_syncs = 0
        #: Number of successful DELETEs triggered.
        self.container_deletes = 0
        #: Number of successful PUTs triggered.
        self.container_puts = 0
        #: Number of containers whose sync has been turned off, but
        #: are not yet cleared from the sync store.
        self.container_skips = 0
        #: Number of containers that had a failure of some type.
        self.container_failures = 0

        #: Per container stats. These are collected per container.
        #: puts - the number of puts that were done for the container
        #: deletes - the number of deletes that were fot the container
        #: bytes - the total number of bytes transferred per the container
        self.container_stats = collections.defaultdict(int)
        self.container_stats.clear()

        #: Time of last stats report.
        self.reported = time()
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        #: swift.common.ring.Ring for locating containers.
        self.container_ring = container_ring or Ring(self.swift_dir,
                                                     ring_name='container')
        bind_ip = conf.get('bind_ip', '0.0.0.0')
        self._myips = whataremyips(bind_ip)
        self._myport = int(conf.get('bind_port', 6201))
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.conn_timeout = float(conf.get('conn_timeout', 5))
        request_tries = int(conf.get('request_tries') or 3)

        internal_client_conf_path = conf.get('internal_client_conf_path')
        if not internal_client_conf_path:
            self.logger.warning(
                'Configuration option internal_client_conf_path not '
                'defined. Using default configuration, See '
                'internal-client.conf-sample for options')
            internal_client_conf = ConfigString(ic_conf_body)
        else:
            internal_client_conf = internal_client_conf_path
        try:
            self.swift = InternalClient(
                internal_client_conf, 'Swift Container Sync', request_tries,
                use_replication_network=True,
                global_conf={'log_name': '%s-ic' % conf.get(
                    'log_name', self.log_route)})
        except (OSError, IOError) as err:
            if err.errno != errno.ENOENT and \
                    not str(err).endswith(' not found'):
                raise
            raise SystemExit(
                'Unable to load internal client from config: '
                '%(conf)r (%(error)s)'
                % {'conf': internal_client_conf_path, 'error': err})

    def run_forever(self, *args, **kwargs):
        """
        Runs container sync scans until stopped.
        """
        sleep(random() * self.interval)
        while True:
            begin = time()
            for path in self.sync_store.synced_containers_generator():
                self.container_stats.clear()
                self.container_sync(path)
                if time() - self.reported >= 3600:  # once an hour
                    self.report()
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """
        Runs a single container sync scan.
        """
        self.logger.info('Begin container sync "once" mode')
        begin = time()
        for path in self.sync_store.synced_containers_generator():
            self.container_sync(path)
            if time() - self.reported >= 3600:  # once an hour
                self.report()
        self.report()
        elapsed = time() - begin
        self.logger.info(
            'Container sync "once" mode completed: %.02fs', elapsed)

    def report(self):
        """
        Writes a report of the stats to the logger and resets the stats for the
        next report.
        """
        self.logger.info(
            'Since %(time)s: %(sync)s synced [%(delete)s deletes, %(put)s '
            'puts], %(skip)s skipped, %(fail)s failed',
            {'time': ctime(self.reported),
             'sync': self.container_syncs,
             'delete': self.container_deletes,
             'put': self.container_puts,
             'skip': self.container_skips,
             'fail': self.container_failures})
        self.reported = time()
        self.container_syncs = 0
        self.container_deletes = 0
        self.container_puts = 0
        self.container_skips = 0
        self.container_failures = 0

    def container_report(self, start, end, sync_point1, sync_point2, info,
                         max_row):
        self.logger.info('Container sync report: %(container)s, '
                         'time window start: %(start)s, '
                         'time window end: %(end)s, '
                         'puts: %(puts)s, '
                         'posts: %(posts)s, '
                         'deletes: %(deletes)s, '
                         'bytes: %(bytes)s, '
                         'sync_point1: %(point1)s, '
                         'sync_point2: %(point2)s, '
                         'total_rows: %(total)s',
                         {'container': '%s/%s' % (info['account'],
                                                  info['container']),
                          'start': start,
                          'end': end,
                          'puts': self.container_stats['puts'],
                          'posts': 0,
                          'deletes': self.container_stats['deletes'],
                          'bytes': self.container_stats['bytes'],
                          'point1': sync_point1,
                          'point2': sync_point2,
                          'total': max_row})

    def container_sync(self, path):
        """
        Checks the given path for a container database, determines if syncing
        is turned on for that database and, if so, sends any updates to the
        other container.

        :param path: the path to a container db
        """
        broker = None
        try:
            broker = ContainerBroker(path, logger=self.logger)
            # The path we pass to the ContainerBroker is a real path of
            # a container DB. If we get here, however, it means that this
            # path is linked from the sync_containers dir. In rare cases
            # of race or processes failures the link can be stale and
            # the get_info below will raise a DB doesn't exist exception
            # In this case we remove the stale link and raise an error
            # since in most cases the db should be there.
            try:
                info = broker.get_info()
            except DatabaseConnectionError as db_err:
                if str(db_err).endswith("DB doesn't exist"):
                    self.sync_store.remove_synced_container(broker)
                raise

            x, nodes = self.container_ring.get_nodes(info['account'],
                                                     info['container'])
            for ordinal, node in enumerate(nodes):
                if is_local_device(self._myips, self._myport,
                                   node['ip'], node['port']):
                    break
            else:
                return
            if broker.metadata.get(SYSMETA_VERSIONS_CONT):
                self.container_skips += 1
                self.logger.increment('skips')
                self.logger.warning('Skipping container %s/%s with '
                                    'object versioning configured' % (
                                        info['account'], info['container']))
                return
            if not broker.is_deleted():
                sync_to = None
                user_key = None
                sync_point1 = info['x_container_sync_point1']
                sync_point2 = info['x_container_sync_point2']
                for key, (value, timestamp) in broker.metadata.items():
                    if key.lower() == 'x-container-sync-to':
                        sync_to = value
                    elif key.lower() == 'x-container-sync-key':
                        user_key = value
                if not sync_to or not user_key:
                    self.container_skips += 1
                    self.logger.increment('skips')
                    return
                err, sync_to, realm, realm_key = validate_sync_to(
                    sync_to, self.allowed_sync_hosts, self.realms_conf)
                if err:
                    self.logger.info(
                        'ERROR %(db_file)s: %(validate_sync_to_err)s',
                        {'db_file': str(broker),
                         'validate_sync_to_err': err})
                    self.container_failures += 1
                    self.logger.increment('failures')
                    return
                start_at = time()
                stop_at = start_at + self.container_time
                next_sync_point = None
                sync_stage_time = start_at
                try:
                    while time() < stop_at and sync_point2 < sync_point1:
                        rows = broker.get_items_since(sync_point2, 1)
                        if not rows:
                            break
                        row = rows[0]
                        if row['ROWID'] > sync_point1:
                            break
                        # This node will only initially sync out one third
                        # of the objects (if 3 replicas, 1/4 if 4, etc.)
                        # and will skip problematic rows as needed in case of
                        # faults.
                        # This section will attempt to sync previously skipped
                        # rows in case the previous attempts by any of the
                        # nodes didn't succeed.
                        if not self.container_sync_row(
                                row, sync_to, user_key, broker, info, realm,
                                realm_key):
                            if not next_sync_point:
                                next_sync_point = sync_point2
                        sync_point2 = row['ROWID']
                        broker.set_x_container_sync_points(None, sync_point2)
                    if next_sync_point:
                        broker.set_x_container_sync_points(None,
                                                           next_sync_point)
                    else:
                        next_sync_point = sync_point2
                    sync_stage_time = time()
                    while sync_stage_time < stop_at:
                        rows = broker.get_items_since(sync_point1, 1)
                        if not rows:
                            break
                        row = rows[0]
                        key = hash_path(info['account'], info['container'],
                                        row['name'], raw_digest=True)
                        # This node will only initially sync out one third of
                        # the objects (if 3 replicas, 1/4 if 4, etc.).
                        # It'll come back around to the section above
                        # and attempt to sync previously skipped rows in case
                        # the other nodes didn't succeed or in case it failed
                        # to do so the first time.
                        if unpack_from('>I', key)[0] % \
                                len(nodes) == ordinal:
                            self.container_sync_row(
                                row, sync_to, user_key, broker, info, realm,
                                realm_key)
                        sync_point1 = row['ROWID']
                        broker.set_x_container_sync_points(sync_point1, None)
                        sync_stage_time = time()
                    self.container_syncs += 1
                    self.logger.increment('syncs')
                finally:
                    self.container_report(start_at, sync_stage_time,
                                          sync_point1,
                                          next_sync_point,
                                          info, broker.get_max_row())
        except (Exception, Timeout):
            self.container_failures += 1
            self.logger.increment('failures')
            self.logger.exception('ERROR Syncing %s',
                                  broker if broker else path)

    def _update_sync_to_headers(self, name, sync_to, user_key,
                                realm, realm_key, method, headers):
        """
        Updates container sync headers

        :param name: The name of the object
        :param sync_to: The URL to the remote container.
        :param user_key: The X-Container-Sync-Key to use when sending requests
                         to the other container.
        :param realm: The realm from self.realms_conf, if there is one.
            If None, fallback to using the older allowed_sync_hosts
            way of syncing.
        :param realm_key: The realm key from self.realms_conf, if there
            is one. If None, fallback to using the older
            allowed_sync_hosts way of syncing.
        :param method: HTTP method to create sig with
        :param headers: headers to update with container sync headers
        """
        if realm and realm_key:
            nonce = uuid.uuid4().hex
            path = urlparse(sync_to).path + '/' + quote(name)
            sig = self.realms_conf.get_sig(method, path,
                                           headers.get('x-timestamp', 0),
                                           nonce, realm_key,
                                           user_key)
            headers['x-container-sync-auth'] = '%s %s %s' % (realm,
                                                             nonce,
                                                             sig)
        else:
            headers['x-container-sync-key'] = user_key

    def _object_in_remote_container(self, name, sync_to, user_key,
                                    realm, realm_key, timestamp):
        """
        Performs head object on remote to eliminate extra remote put and
        local get object calls

        :param name: The name of the object in the updated row in the local
                     database triggering the sync update.
        :param sync_to: The URL to the remote container.
        :param user_key: The X-Container-Sync-Key to use when sending requests
                         to the other container.
        :param realm: The realm from self.realms_conf, if there is one.
            If None, fallback to using the older allowed_sync_hosts
            way of syncing.
        :param realm_key: The realm key from self.realms_conf, if there
            is one. If None, fallback to using the older
            allowed_sync_hosts way of syncing.
        :param timestamp: last modified date of local object
        :returns: True if object already exists in remote
        """
        headers = {'x-timestamp': timestamp.internal}
        self._update_sync_to_headers(name, sync_to, user_key, realm,
                                     realm_key, 'HEAD', headers)
        try:
            metadata, _ = head_object(sync_to, name=name,
                                      headers=headers,
                                      proxy=self.select_http_proxy(),
                                      logger=self.logger,
                                      retries=0)
            remote_ts = Timestamp(metadata.get('x-timestamp', 0))
            self.logger.debug("remote obj timestamp %s local obj %s" %
                              (timestamp.internal, remote_ts.internal))
            if timestamp <= remote_ts:
                return True
            # Object in remote should be updated
            return False
        except ClientException as http_err:
            # Object not in remote
            if http_err.http_status == 404:
                return False
            raise http_err

    def container_sync_row(self, row, sync_to, user_key, broker, info,
                           realm, realm_key):
        """
        Sends the update the row indicates to the sync_to container.
        Update can be either delete or put.

        :param row: The updated row in the local database triggering the sync
                    update.
        :param sync_to: The URL to the remote container.
        :param user_key: The X-Container-Sync-Key to use when sending requests
                         to the other container.
        :param broker: The local container database broker.
        :param info: The get_info result from the local container database
                     broker.
        :param realm: The realm from self.realms_conf, if there is one.
            If None, fallback to using the older allowed_sync_hosts
            way of syncing.
        :param realm_key: The realm key from self.realms_conf, if there
            is one. If None, fallback to using the older
            allowed_sync_hosts way of syncing.
        :returns: True on success
        """
        try:
            start_time = time()
            # extract last modified time from the created_at value
            ts_data, ts_ctype, ts_meta = decode_timestamps(
                row['created_at'])
            if row['deleted']:
                # when sync'ing a deleted object, use ts_data - this is the
                # timestamp of the source tombstone
                try:
                    headers = {'x-timestamp': ts_data.internal}
                    self._update_sync_to_headers(row['name'], sync_to,
                                                 user_key, realm, realm_key,
                                                 'DELETE', headers)
                    delete_object(sync_to, name=row['name'], headers=headers,
                                  proxy=self.select_http_proxy(),
                                  logger=self.logger,
                                  timeout=self.conn_timeout)
                except ClientException as err:
                    if err.http_status not in (
                            HTTP_NOT_FOUND, HTTP_CONFLICT):
                        raise
                self.container_deletes += 1
                self.container_stats['deletes'] += 1
                self.logger.increment('deletes')
                self.logger.timing_since('deletes.timing', start_time)
            else:
                # when sync'ing a live object, use ts_meta - this is the time
                # at which the source object was last modified by a PUT or POST
                if self._object_in_remote_container(row['name'],
                                                    sync_to, user_key, realm,
                                                    realm_key, ts_meta):
                    return True
                exc = None
                # look up for the newest one; the symlink=get query-string has
                # no effect unless symlinks are enabled in the internal client
                # in which case it ensures that symlink objects retain their
                # symlink property when sync'd.
                headers_out = {'X-Newest': True,
                               'X-Backend-Storage-Policy-Index':
                               str(info['storage_policy_index'])}
                try:
                    source_obj_status, headers, body = \
                        self.swift.get_object(info['account'],
                                              info['container'], row['name'],
                                              headers=headers_out,
                                              acceptable_statuses=(2, 4),
                                              params={'symlink': 'get'})

                except (Exception, UnexpectedResponse, Timeout) as err:
                    headers = {}
                    body = None
                    exc = err

                # skip object_versioning links; this is in case the container
                # metadata is out of date
                if headers.get(SYSMETA_VERSIONS_SYMLINK):
                    self.logger.info(
                        'Skipping versioning symlink %s/%s/%s ' % (
                            info['account'], info['container'],
                            row['name']))
                    return True

                timestamp = Timestamp(headers.get('x-timestamp', 0))
                if timestamp < ts_meta:
                    if exc:
                        raise exc
                    raise Exception(
                        'Unknown exception trying to GET: '
                        '%(account)r %(container)r %(object)r' %
                        {'account': info['account'],
                         'container': info['container'],
                         'object': row['name']})
                for key in ('date', 'last-modified'):
                    if key in headers:
                        del headers[key]
                if 'etag' in headers:
                    headers['etag'] = normalize_etag(headers['etag'])
                if 'content-type' in headers:
                    headers['content-type'] = clean_content_type(
                        headers['content-type'])
                self._update_sync_to_headers(row['name'], sync_to, user_key,
                                             realm, realm_key, 'PUT', headers)
                put_object(sync_to, name=row['name'], headers=headers,
                           contents=FileLikeIter(body),
                           proxy=self.select_http_proxy(), logger=self.logger,
                           timeout=self.conn_timeout)
                self.container_puts += 1
                self.container_stats['puts'] += 1
                self.container_stats['bytes'] += row['size']
                self.logger.increment('puts')
                self.logger.timing_since('puts.timing', start_time)
        except ClientException as err:
            if err.http_status == HTTP_UNAUTHORIZED:
                self.logger.info(
                    'Unauth %(sync_from)r => %(sync_to)r',
                    {'sync_from': '%s/%s' %
                        (quote(info['account']), quote(info['container'])),
                     'sync_to': sync_to})
            elif err.http_status == HTTP_NOT_FOUND:
                self.logger.info(
                    'Not found %(sync_from)r => %(sync_to)r \
                    - object %(obj_name)r',
                    {'sync_from': '%s/%s' %
                        (quote(info['account']), quote(info['container'])),
                     'sync_to': sync_to, 'obj_name': row['name']})
            else:
                self.logger.exception(
                    'ERROR Syncing %(db_file)s %(row)s',
                    {'db_file': str(broker), 'row': row})
            self.container_failures += 1
            self.logger.increment('failures')
            return False
        except (Exception, Timeout):
            self.logger.exception(
                'ERROR Syncing %(db_file)s %(row)s',
                {'db_file': str(broker), 'row': row})
            self.container_failures += 1
            self.logger.increment('failures')
            return False
        return True

    def select_http_proxy(self):
        return choice(self.http_proxies) if self.http_proxies else None


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(ContainerSync, conf_file, **options)


if __name__ == '__main__':
    main()
