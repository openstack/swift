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

import os
import time
from random import random, shuffle

from swift.container import server as container_server
from swift.common import client, direct_client
from swift.common.ring import Ring
from swift.common.db import ContainerBroker
from swift.common.utils import audit_location_generator, get_logger, \
    normalize_timestamp, TRUE_VALUES, validate_sync_to
from swift.common.daemon import Daemon


class Iter2FileLikeObject(object):

    def __init__(self, iterator):
        self.iterator = iterator
        self._chunk = ''

    def read(self, size=-1):
        if size < 0:
            chunk = self._chunk
            self._chunk = ''
            return chunk + ''.join(self.iterator)
        chunk = ''
        try:
            chunk = self.iterator.next()
        except StopIteration:
            pass
        if len(chunk) <= size:
            return chunk
        self._chunk = chunk[size:]
        return chunk[:size]


class ContainerSync(Daemon):
    """Sync syncable containers."""

    def __init__(self, conf, object_ring=None):
        self.conf = conf
        self.logger = get_logger(conf, log_route='container-sync')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = \
            conf.get('mount_check', 'true').lower() in TRUE_VALUES
        self.interval = int(conf.get('interval', 300))
        self.container_time = int(conf.get('container_time', 60))
        self.allowed_sync_hosts = [h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.container_syncs = 0
        self.container_deletes = 0
        self.container_puts = 0
        self.container_skips = 0
        self.container_failures = 0
        self.reported = time.time()
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.object_ring = object_ring or \
            Ring(os.path.join(swift_dir, 'object.ring.gz'))

    def run_forever(self):
        """Run the container sync until stopped."""
        time.sleep(random() * self.interval)
        while True:
            begin = time.time()
            all_locs = audit_location_generator(self.devices,
                                                container_server.DATADIR,
                                                mount_check=self.mount_check,
                                                logger=self.logger)
            for path, device, partition in all_locs:
                self.container_sync(path)
                if time.time() - self.reported >= 3600:  # once an hour
                    self._report()
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self):
        """Run the container sync once."""
        self.logger.info(_('Begin container sync "once" mode'))
        begin = time.time()
        all_locs = audit_location_generator(self.devices,
                                            container_server.DATADIR,
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.container_sync(path)
            if time.time() - self.reported >= 3600:  # once an hour
                self._report()
        self._report()
        elapsed = time.time() - begin
        self.logger.info(
            _('Container sync "once" mode completed: %.02fs'), elapsed)

    def _report(self):
        self.logger.info(
            _('Since %(time)s: %(sync)s synced [%(delete)s deletes, %(put)s '
              'puts], %(skip)s skipped, %(fail)s failed'),
            {'time': time.ctime(self.reported),
             'sync': self.container_syncs,
             'delete': self.container_deletes,
             'put': self.container_puts,
             'skip': self.container_skips,
             'fail': self.container_failures})
        self.reported = time.time()
        self.container_syncs = 0
        self.container_deletes = 0
        self.container_puts = 0
        self.container_skips = 0
        self.container_failures = 0

    def container_sync(self, path):
        """
        Syncs the given container path

        :param path: the path to a container db
        """
        try:
            if not path.endswith('.db'):
                return
            broker = ContainerBroker(path)
            info = broker.get_info()
            if not broker.is_deleted():
                sync_to = None
                sync_key = None
                sync_row = info['x_container_sync_row']
                for key, (value, timestamp) in broker.metadata.iteritems():
                    if key.lower() == 'x-container-sync-to':
                        sync_to = value
                    elif key.lower() == 'x-container-sync-key':
                        sync_key = value
                if not sync_to or not sync_key:
                    self.container_skips += 1
                    return
                sync_to = sync_to.rstrip('/')
                err = validate_sync_to(sync_to, self.allowed_sync_hosts)
                if err:
                    self.logger.info(
                        _('ERROR %(db_file)s: %(validate_sync_to_err)s'),
                        {'db_file': broker.db_file,
                         'validate_sync_to_err': err})
                    self.container_failures += 1
                    return
                stop_at = time.time() + self.container_time
                while time.time() < stop_at:
                    rows = broker.get_items_since(sync_row, 1)
                    if not rows:
                        break
                    if not self.container_sync_row(rows[0], sync_to, sync_key,
                                                   broker, info):
                        return
                    sync_row = rows[0]['ROWID']
                    broker.set_x_container_sync_row(sync_row)
                self.container_syncs += 1
        except Exception:
            self.container_failures += 1
            self.logger.exception(_('ERROR Syncing %s'), (broker.db_file))

    def container_sync_row(self, row, sync_to, sync_key, broker, info):
        try:
            if row['deleted']:
                try:
                    client.delete_object(sync_to, name=row['name'],
                        headers={'X-Timestamp': row['created_at'],
                                 'X-Container-Sync-Key': sync_key})
                except client.ClientException, err:
                    if err.http_status != 404:
                        raise
                self.container_deletes += 1
            else:
                part, nodes = self.object_ring.get_nodes(
                    info['account'], info['container'],
                    row['name'])
                shuffle(nodes)
                exc = None
                for node in nodes:
                    try:
                        headers, body = \
                            direct_client.direct_get_object(node, part,
                                info['account'], info['container'],
                                row['name'], resp_chunk_size=65536)
                        break
                    except client.ClientException, err:
                        exc = err
                else:
                    if exc:
                        raise exc
                    raise Exception(_('Unknown exception trying to GET: '
                        '%(node)r %(account)r %(container)r %(object)r'),
                        {'node': node, 'part': part,
                         'account': info['account'],
                         'container': info['container'],
                         'object': row['name']})
                for key in ('date', 'last-modified'):
                    if key in headers:
                        del headers[key]
                if 'etag' in headers:
                    headers['etag'] = headers['etag'].strip('"')
                headers['X-Timestamp'] = row['created_at']
                headers['X-Container-Sync-Key'] = sync_key
                client.put_object(sync_to, name=row['name'],
                                headers=headers,
                                contents=Iter2FileLikeObject(body))
                self.container_puts += 1
        except client.ClientException, err:
            if err.http_status == 401:
                self.logger.info(_('Unauth %(sync_from)r '
                    '=> %(sync_to)r key: %(sync_key)r'),
                    {'sync_from': '%s/%s' %
                        (client.quote(info['account']),
                         client.quote(info['container'])),
                     'sync_to': sync_to,
                     'sync_key': sync_key})
            elif err.http_status == 404:
                self.logger.info(_('Not found %(sync_from)r '
                    '=> %(sync_to)r key: %(sync_key)r'),
                    {'sync_from': '%s/%s' %
                        (client.quote(info['account']),
                         client.quote(info['container'])),
                     'sync_to': sync_to,
                     'sync_key': sync_key})
            else:
                self.logger.exception(
                    _('ERROR Syncing %(db_file)s %(row)s'),
                    {'db_file': broker.db_file, 'row': row})
            self.container_failures += 1
            return False
        except Exception:
            self.logger.exception(
                _('ERROR Syncing %(db_file)s %(row)s'),
                {'db_file': broker.db_file, 'row': row})
            self.container_failures += 1
            return False
        return True
