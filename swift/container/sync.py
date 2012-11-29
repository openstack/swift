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

from time import ctime, time
from random import random, shuffle
from struct import unpack_from

from eventlet import sleep, Timeout

import swift.common.db
from swift.container import server as container_server
from swiftclient import ClientException, delete_object, put_object, \
    quote
from swift.common.direct_client import direct_get_object
from swift.common.ring import Ring
from swift.common.db import ContainerBroker
from swift.common.utils import audit_location_generator, get_logger, \
    hash_path, config_true_value, validate_sync_to, whataremyips
from swift.common.daemon import Daemon
from swift.common.http import HTTP_UNAUTHORIZED, HTTP_NOT_FOUND


class _Iter2FileLikeObject(object):
    """
    Returns an iterator's contents via :func:`read`, making it look like a file
    object.
    """

    def __init__(self, iterator):
        self.iterator = iterator
        self._chunk = ''

    def read(self, size=-1):
        """
        read([size]) -> read at most size bytes, returned as a string.

        If the size argument is negative or omitted, read until EOF is reached.
        Notice that when in non-blocking mode, less data than what was
        requested may be returned, even if no size parameter was given.
        """
        if size < 0:
            chunk = self._chunk
            self._chunk = ''
            return chunk + ''.join(self.iterator)
        chunk = self._chunk
        self._chunk = ''
        if chunk and len(chunk) <= size:
            return chunk
        try:
            chunk += self.iterator.next()
        except StopIteration:
            pass
        if len(chunk) <= size:
            return chunk
        self._chunk = chunk[size:]
        return chunk[:size]


class ContainerSync(Daemon):
    """
    Daemon to sync syncable containers.

    This is done by scanning the local devices for container databases and
    checking for x-container-sync-to and x-container-sync-key metadata values.
    If they exist, newer rows since the last sync will trigger PUTs or DELETEs
    to the other container.

    .. note::

        Container sync will sync object POSTs only if the proxy server is set
        to use "object_post_as_copy = true" which is the default. So-called
        fast object posts, "object_post_as_copy = false" do not update the
        container listings and therefore can't be detected for synchronization.

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
    :param object_ring: If None, the <swift_dir>/object.ring.gz will be loaded.
                        This is overridden by unit tests.
    """

    def __init__(self, conf, container_ring=None, object_ring=None):
        #: The dict of configuration values from the [container-sync] section
        #: of the container-server.conf.
        self.conf = conf
        #: Logger to use for container-sync log lines.
        self.logger = get_logger(conf, log_route='container-sync')
        #: Path to the local device mount points.
        self.devices = conf.get('devices', '/srv/node')
        #: Indicates whether mount points should be verified as actual mount
        #: points (normally true, false for tests and SAIO).
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        #: Minimum time between full scans. This is to keep the daemon from
        #: running wild on near empty systems.
        self.interval = int(conf.get('interval', 300))
        #: Maximum amount of time to spend syncing a container before moving on
        #: to the next one. If a conatiner sync hasn't finished in this time,
        #: it'll just be resumed next scan.
        self.container_time = int(conf.get('container_time', 60))
        #: The list of hosts we're allowed to send syncs to.
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.proxy = conf.get('sync_proxy')
        #: Number of containers with sync turned on that were successfully
        #: synced.
        self.container_syncs = 0
        #: Number of successful DELETEs triggered.
        self.container_deletes = 0
        #: Number of successful PUTs triggered.
        self.container_puts = 0
        #: Number of containers that didn't have sync turned on.
        self.container_skips = 0
        #: Number of containers that had a failure of some type.
        self.container_failures = 0
        #: Time of last stats report.
        self.reported = time()
        swift_dir = conf.get('swift_dir', '/etc/swift')
        #: swift.common.ring.Ring for locating containers.
        self.container_ring = container_ring or Ring(swift_dir,
                                                     ring_name='container')
        #: swift.common.ring.Ring for locating objects.
        self.object_ring = object_ring or Ring(swift_dir, ring_name='object')
        self._myips = whataremyips()
        self._myport = int(conf.get('bind_port', 6001))
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))

    def run_forever(self):
        """
        Runs container sync scans until stopped.
        """
        sleep(random() * self.interval)
        while True:
            begin = time()
            all_locs = audit_location_generator(self.devices,
                                                container_server.DATADIR,
                                                mount_check=self.mount_check,
                                                logger=self.logger)
            for path, device, partition in all_locs:
                self.container_sync(path)
                if time() - self.reported >= 3600:  # once an hour
                    self.report()
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(self.interval - elapsed)

    def run_once(self):
        """
        Runs a single container sync scan.
        """
        self.logger.info(_('Begin container sync "once" mode'))
        begin = time()
        all_locs = audit_location_generator(self.devices,
                                            container_server.DATADIR,
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.container_sync(path)
            if time() - self.reported >= 3600:  # once an hour
                self.report()
        self.report()
        elapsed = time() - begin
        self.logger.info(
            _('Container sync "once" mode completed: %.02fs'), elapsed)

    def report(self):
        """
        Writes a report of the stats to the logger and resets the stats for the
        next report.
        """
        self.logger.info(
            _('Since %(time)s: %(sync)s synced [%(delete)s deletes, %(put)s '
              'puts], %(skip)s skipped, %(fail)s failed'),
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

    def container_sync(self, path):
        """
        Checks the given path for a container database, determines if syncing
        is turned on for that database and, if so, sends any updates to the
        other container.

        :param path: the path to a container db
        """
        try:
            if not path.endswith('.db'):
                return
            broker = ContainerBroker(path)
            info = broker.get_info()
            x, nodes = self.container_ring.get_nodes(info['account'],
                                                     info['container'])
            for ordinal, node in enumerate(nodes):
                if node['ip'] in self._myips and node['port'] == self._myport:
                    break
            else:
                return
            if not broker.is_deleted():
                sync_to = None
                sync_key = None
                sync_point1 = info['x_container_sync_point1']
                sync_point2 = info['x_container_sync_point2']
                for key, (value, timestamp) in broker.metadata.iteritems():
                    if key.lower() == 'x-container-sync-to':
                        sync_to = value
                    elif key.lower() == 'x-container-sync-key':
                        sync_key = value
                if not sync_to or not sync_key:
                    self.container_skips += 1
                    self.logger.increment('skips')
                    return
                sync_to = sync_to.rstrip('/')
                err = validate_sync_to(sync_to, self.allowed_sync_hosts)
                if err:
                    self.logger.info(
                        _('ERROR %(db_file)s: %(validate_sync_to_err)s'),
                        {'db_file': broker.db_file,
                         'validate_sync_to_err': err})
                    self.container_failures += 1
                    self.logger.increment('failures')
                    return
                stop_at = time() + self.container_time
                while time() < stop_at and sync_point2 < sync_point1:
                    rows = broker.get_items_since(sync_point2, 1)
                    if not rows:
                        break
                    row = rows[0]
                    if row['ROWID'] > sync_point1:
                        break
                    key = hash_path(info['account'], info['container'],
                                    row['name'], raw_digest=True)
                    # This node will only intially sync out one third of the
                    # objects (if 3 replicas, 1/4 if 4, etc.). This section
                    # will attempt to sync previously skipped rows in case the
                    # other nodes didn't succeed.
                    if unpack_from('>I', key)[0] % \
                            len(nodes) != ordinal:
                        if not self.container_sync_row(row, sync_to, sync_key,
                                                       broker, info):
                            return
                    sync_point2 = row['ROWID']
                    broker.set_x_container_sync_points(None, sync_point2)
                while time() < stop_at:
                    rows = broker.get_items_since(sync_point1, 1)
                    if not rows:
                        break
                    row = rows[0]
                    key = hash_path(info['account'], info['container'],
                                    row['name'], raw_digest=True)
                    # This node will only intially sync out one third of the
                    # objects (if 3 replicas, 1/4 if 4, etc.). It'll come back
                    # around to the section above and attempt to sync
                    # previously skipped rows in case the other nodes didn't
                    # succeed.
                    if unpack_from('>I', key)[0] % \
                            len(nodes) == ordinal:
                        if not self.container_sync_row(row, sync_to, sync_key,
                                                       broker, info):
                            return
                    sync_point1 = row['ROWID']
                    broker.set_x_container_sync_points(sync_point1, None)
                self.container_syncs += 1
                self.logger.increment('syncs')
        except (Exception, Timeout), err:
            self.container_failures += 1
            self.logger.increment('failures')
            self.logger.exception(_('ERROR Syncing %s'), (broker.db_file))

    def container_sync_row(self, row, sync_to, sync_key, broker, info):
        """
        Sends the update the row indicates to the sync_to container.

        :param row: The updated row in the local database triggering the sync
                    update.
        :param sync_to: The URL to the remote container.
        :param sync_key: The X-Container-Sync-Key to use when sending requests
                         to the other container.
        :param broker: The local container database broker.
        :param info: The get_info result from the local container database
                     broker.
        :returns: True on success
        """
        try:
            start_time = time()
            if row['deleted']:
                try:
                    delete_object(sync_to, name=row['name'],
                                  headers={'x-timestamp': row['created_at'],
                                           'x-container-sync-key': sync_key},
                                  proxy=self.proxy)
                except ClientException, err:
                    if err.http_status != HTTP_NOT_FOUND:
                        raise
                self.container_deletes += 1
                self.logger.increment('deletes')
                self.logger.timing_since('deletes.timing', start_time)
            else:
                part, nodes = self.object_ring.get_nodes(
                    info['account'], info['container'],
                    row['name'])
                shuffle(nodes)
                exc = None
                looking_for_timestamp = float(row['created_at'])
                timestamp = -1
                headers = body = None
                for node in nodes:
                    try:
                        these_headers, this_body = direct_get_object(
                            node, part, info['account'], info['container'],
                            row['name'], resp_chunk_size=65536)
                        this_timestamp = float(these_headers['x-timestamp'])
                        if this_timestamp > timestamp:
                            timestamp = this_timestamp
                            headers = these_headers
                            body = this_body
                    except ClientException, err:
                        # If any errors are not 404, make sure we report the
                        # non-404 one. We don't want to mistakenly assume the
                        # object no longer exists just because one says so and
                        # the others errored for some other reason.
                        if not exc or exc.http_status == HTTP_NOT_FOUND:
                            exc = err
                    except (Exception, Timeout), err:
                        exc = err
                if timestamp < looking_for_timestamp:
                    if exc:
                        raise exc
                    raise Exception(
                        _('Unknown exception trying to GET: %(node)r '
                          '%(account)r %(container)r %(object)r'),
                        {'node': node, 'part': part,
                         'account': info['account'],
                         'container': info['container'],
                         'object': row['name']})
                for key in ('date', 'last-modified'):
                    if key in headers:
                        del headers[key]
                if 'etag' in headers:
                    headers['etag'] = headers['etag'].strip('"')
                headers['x-timestamp'] = row['created_at']
                headers['x-container-sync-key'] = sync_key
                put_object(sync_to, name=row['name'], headers=headers,
                           contents=_Iter2FileLikeObject(body),
                           proxy=self.proxy)
                self.container_puts += 1
                self.logger.increment('puts')
                self.logger.timing_since('puts.timing', start_time)
        except ClientException, err:
            if err.http_status == HTTP_UNAUTHORIZED:
                self.logger.info(
                    _('Unauth %(sync_from)r => %(sync_to)r'),
                    {'sync_from': '%s/%s' %
                        (quote(info['account']), quote(info['container'])),
                     'sync_to': sync_to})
            elif err.http_status == HTTP_NOT_FOUND:
                self.logger.info(
                    _('Not found %(sync_from)r => %(sync_to)r'),
                    {'sync_from': '%s/%s' %
                        (quote(info['account']), quote(info['container'])),
                     'sync_to': sync_to})
            else:
                self.logger.exception(
                    _('ERROR Syncing %(db_file)s %(row)s'),
                    {'db_file': broker.db_file, 'row': row})
            self.container_failures += 1
            self.logger.increment('failures')
            return False
        except (Exception, Timeout), err:
            self.logger.exception(
                _('ERROR Syncing %(db_file)s %(row)s'),
                {'db_file': broker.db_file, 'row': row})
            self.container_failures += 1
            self.logger.increment('failures')
            return False
        return True
