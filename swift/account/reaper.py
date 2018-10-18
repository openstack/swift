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

import os
import random
import socket
from logging import DEBUG
from math import sqrt
from time import time
from hashlib import md5
import itertools

from eventlet import GreenPool, sleep, Timeout
import six

import swift.common.db
from swift.account.backend import AccountBroker, DATADIR
from swift.common.constraints import check_drive
from swift.common.direct_client import direct_delete_container, \
    direct_delete_object, direct_get_container
from swift.common.exceptions import ClientException
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.utils import get_logger, whataremyips, config_true_value, \
    Timestamp
from swift.common.daemon import Daemon
from swift.common.storage_policy import POLICIES, PolicyError


class AccountReaper(Daemon):
    """
    Removes data from status=DELETED accounts. These are accounts that have
    been asked to be removed by the reseller via services
    remove_storage_account XMLRPC call.

    The account is not deleted immediately by the services call, but instead
    the account is simply marked for deletion by setting the status column in
    the account_stat table of the account database. This account reaper scans
    for such accounts and removes the data in the background. The background
    deletion process will occur on the primary account server for the account.

    :param server_conf: The [account-server] dictionary of the account server
                        configuration file
    :param reaper_conf: The [account-reaper] dictionary of the account server
                        configuration file

    See the etc/account-server.conf-sample for information on the possible
    configuration parameters.
    """

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='account-reaper')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 3600))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.account_ring = None
        self.container_ring = None
        self.object_ring = None
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.myips = whataremyips(conf.get('bind_ip', '0.0.0.0'))
        self.bind_port = int(conf.get('bind_port', 6202))
        self.concurrency = int(conf.get('concurrency', 25))
        self.container_concurrency = self.object_concurrency = \
            sqrt(self.concurrency)
        self.container_pool = GreenPool(size=self.container_concurrency)
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.delay_reaping = int(conf.get('delay_reaping') or 0)
        reap_warn_after = float(conf.get('reap_warn_after') or 86400 * 30)
        self.reap_not_done_after = reap_warn_after + self.delay_reaping
        self.start_time = time()
        self.reset_stats()

    def get_account_ring(self):
        """The account :class:`swift.common.ring.Ring` for the cluster."""
        if not self.account_ring:
            self.account_ring = Ring(self.swift_dir, ring_name='account')
        return self.account_ring

    def get_container_ring(self):
        """The container :class:`swift.common.ring.Ring` for the cluster."""
        if not self.container_ring:
            self.container_ring = Ring(self.swift_dir, ring_name='container')
        return self.container_ring

    def get_object_ring(self, policy_idx):
        """
        Get the ring identified by the policy index

        :param policy_idx: Storage policy index
        :returns: A ring matching the storage policy
        """
        return POLICIES.get_object_ring(policy_idx, self.swift_dir)

    def run_forever(self, *args, **kwargs):
        """Main entry point when running the reaper in normal daemon mode.

        This repeatedly calls :func:`run_once` no quicker than the
        configuration interval.
        """
        self.logger.debug('Daemon started.')
        sleep(random.random() * self.interval)
        while True:
            begin = time()
            self.run_once()
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """
        Main entry point when running the reaper in 'once' mode, where it will
        do a single pass over all accounts on the server. This is called
        repeatedly by :func:`run_forever`. This will call :func:`reap_device`
        once for each device on the server.
        """
        self.logger.debug('Begin devices pass: %s', self.devices)
        begin = time()
        try:
            for device in os.listdir(self.devices):
                try:
                    check_drive(self.devices, device, self.mount_check)
                except ValueError as err:
                    self.logger.increment('errors')
                    self.logger.debug('Skipping: %s', err)
                    continue
                self.reap_device(device)
        except (Exception, Timeout):
            self.logger.exception("Exception in top-level account reaper "
                                  "loop")
        elapsed = time() - begin
        self.logger.info('Devices pass completed: %.02fs', elapsed)

    def reap_device(self, device):
        """
        Called once per pass for each device on the server. This will scan the
        accounts directory for the device, looking for partitions this device
        is the primary for, then looking for account databases that are marked
        status=DELETED and still have containers and calling
        :func:`reap_account`. Account databases marked status=DELETED that no
        longer have containers will eventually be permanently removed by the
        reclaim process within the account replicator (see
        :mod:`swift.db_replicator`).

        :param device: The device to look for accounts to be deleted.
        """
        datadir = os.path.join(self.devices, device, DATADIR)
        if not os.path.exists(datadir):
            return
        for partition in os.listdir(datadir):
            partition_path = os.path.join(datadir, partition)
            if not partition.isdigit():
                continue
            nodes = self.get_account_ring().get_part_nodes(int(partition))
            if not os.path.isdir(partition_path):
                continue
            container_shard = None
            for container_shard, node in enumerate(nodes):
                if is_local_device(self.myips, None, node['ip'], None) and \
                        (not self.bind_port or
                         self.bind_port == node['port']) and \
                        (device == node['device']):
                    break
            else:
                continue

            for suffix in os.listdir(partition_path):
                suffix_path = os.path.join(partition_path, suffix)
                if not os.path.isdir(suffix_path):
                    continue
                for hsh in os.listdir(suffix_path):
                    hsh_path = os.path.join(suffix_path, hsh)
                    if not os.path.isdir(hsh_path):
                        continue
                    for fname in sorted(os.listdir(hsh_path), reverse=True):
                        if fname.endswith('.ts'):
                            break
                        elif fname.endswith('.db'):
                            self.start_time = time()
                            broker = \
                                AccountBroker(os.path.join(hsh_path, fname))
                            if broker.is_status_deleted() and \
                                    not broker.empty():
                                self.reap_account(
                                    broker, partition, nodes,
                                    container_shard=container_shard)

    def reset_stats(self):
        self.stats_return_codes = {}
        self.stats_containers_deleted = 0
        self.stats_objects_deleted = 0
        self.stats_containers_remaining = 0
        self.stats_objects_remaining = 0
        self.stats_containers_possibly_remaining = 0
        self.stats_objects_possibly_remaining = 0

    def reap_account(self, broker, partition, nodes, container_shard=None):
        """
        Called once per pass for each account this server is the primary for
        and attempts to delete the data for the given account. The reaper will
        only delete one account at any given time. It will call
        :func:`reap_container` up to sqrt(self.concurrency) times concurrently
        while reaping the account.

        If there is any exception while deleting a single container, the
        process will continue for any other containers and the failed
        containers will be tried again the next time this function is called
        with the same parameters.

        If there is any exception while listing the containers for deletion,
        the process will stop (but will obviously be tried again the next time
        this function is called with the same parameters). This isn't likely
        since the listing comes from the local database.

        After the process completes (successfully or not) statistics about what
        was accomplished will be logged.

        This function returns nothing and should raise no exception but only
        update various self.stats_* values for what occurs.

        :param broker: The AccountBroker for the account to delete.
        :param partition: The partition in the account ring the account is on.
        :param nodes: The primary node dicts for the account to delete.
        :param container_shard: int used to shard containers reaped. If None,
                                will reap all containers.

        .. seealso::

            :class:`swift.account.backend.AccountBroker` for the broker class.

        .. seealso::

            :func:`swift.common.ring.Ring.get_nodes` for a description
            of the node dicts.
        """
        begin = time()
        info = broker.get_info()
        if time() - float(Timestamp(info['delete_timestamp'])) <= \
                self.delay_reaping:
            return False
        account = info['account']
        self.logger.info('Beginning pass on account %s', account)
        self.reset_stats()
        container_limit = 1000
        if container_shard is not None:
            container_limit *= len(nodes)
        try:
            containers = list(broker.list_containers_iter(
                container_limit, '', None, None, None))
            while containers:
                try:
                    for (container, _junk, _junk, _junk, _junk) in containers:
                        if six.PY3:
                            container_ = container.encode('utf-8')
                        else:
                            container_ = container
                        this_shard = int(md5(container_).hexdigest(), 16) % \
                            len(nodes)
                        if container_shard not in (this_shard, None):
                            continue

                        self.container_pool.spawn(self.reap_container, account,
                                                  partition, nodes, container)
                    self.container_pool.waitall()
                except (Exception, Timeout):
                    self.logger.exception(
                        'Exception with containers for account %s', account)
                containers = list(broker.list_containers_iter(
                    container_limit, containers[-1][0], None, None, None))
            log_buf = ['Completed pass on account %s' % account]
        except (Exception, Timeout):
            self.logger.exception('Exception with account %s', account)
            log_buf = ['Incomplete pass on account %s' % account]
        if self.stats_containers_deleted:
            log_buf.append(', %s containers deleted' %
                           self.stats_containers_deleted)
        if self.stats_objects_deleted:
            log_buf.append(', %s objects deleted' % self.stats_objects_deleted)
        if self.stats_containers_remaining:
            log_buf.append(', %s containers remaining' %
                           self.stats_containers_remaining)
        if self.stats_objects_remaining:
            log_buf.append(', %s objects remaining' %
                           self.stats_objects_remaining)
        if self.stats_containers_possibly_remaining:
            log_buf.append(', %s containers possibly remaining' %
                           self.stats_containers_possibly_remaining)
        if self.stats_objects_possibly_remaining:
            log_buf.append(', %s objects possibly remaining' %
                           self.stats_objects_possibly_remaining)
        if self.stats_return_codes:
            log_buf.append(', return codes: ')
            for code in sorted(self.stats_return_codes):
                log_buf.append('%s %sxxs, ' % (self.stats_return_codes[code],
                                               code))
            log_buf[-1] = log_buf[-1][:-2]
        log_buf.append(', elapsed: %.02fs' % (time() - begin))
        self.logger.info(''.join(log_buf))
        self.logger.timing_since('timing', self.start_time)
        delete_timestamp = Timestamp(info['delete_timestamp'])
        if self.stats_containers_remaining and \
           begin - float(delete_timestamp) >= self.reap_not_done_after:
            self.logger.warning(
                'Account %(account)s has not been reaped since %(time)s' %
                {'account': account, 'time': delete_timestamp.isoformat})
        return True

    def reap_container(self, account, account_partition, account_nodes,
                       container):
        """
        Deletes the data and the container itself for the given container. This
        will call :func:`reap_object` up to sqrt(self.concurrency) times
        concurrently for the objects in the container.

        If there is any exception while deleting a single object, the process
        will continue for any other objects in the container and the failed
        objects will be tried again the next time this function is called with
        the same parameters.

        If there is any exception while listing the objects for deletion, the
        process will stop (but will obviously be tried again the next time this
        function is called with the same parameters). This is a possibility
        since the listing comes from querying just the primary remote container
        server.

        Once all objects have been attempted to be deleted, the container
        itself will be attempted to be deleted by sending a delete request to
        all container nodes. The format of the delete request is such that each
        container server will update a corresponding account server, removing
        the container from the account's listing.

        This function returns nothing and should raise no exception but only
        update various self.stats_* values for what occurs.

        :param account: The name of the account for the container.
        :param account_partition: The partition for the account on the account
                                  ring.
        :param account_nodes: The primary node dicts for the account.
        :param container: The name of the container to delete.

        * See also: :func:`swift.common.ring.Ring.get_nodes` for a description
          of the account node dicts.
        """
        account_nodes = list(account_nodes)
        part, nodes = self.get_container_ring().get_nodes(account, container)
        node = nodes[-1]
        pool = GreenPool(size=self.object_concurrency)
        marker = ''
        while True:
            objects = None
            try:
                headers, objects = direct_get_container(
                    node, part, account, container,
                    marker=marker,
                    conn_timeout=self.conn_timeout,
                    response_timeout=self.node_timeout)
                self.stats_return_codes[2] = \
                    self.stats_return_codes.get(2, 0) + 1
                self.logger.increment('return_codes.2')
            except ClientException as err:
                if self.logger.getEffectiveLevel() <= DEBUG:
                    self.logger.exception(
                        'Exception with %(ip)s:%(port)s/%(device)s', node)
                self.stats_return_codes[err.http_status // 100] = \
                    self.stats_return_codes.get(err.http_status // 100, 0) + 1
                self.logger.increment(
                    'return_codes.%d' % (err.http_status // 100,))
            except (Timeout, socket.error) as err:
                self.logger.error(
                    'Timeout Exception with %(ip)s:%(port)s/%(device)s',
                    node)
            if not objects:
                break
            try:
                policy_index = headers.get('X-Backend-Storage-Policy-Index', 0)
                policy = POLICIES.get_by_index(policy_index)
                if not policy:
                    self.logger.error('ERROR: invalid storage policy index: %r'
                                      % policy_index)
                for obj in objects:
                    pool.spawn(self.reap_object, account, container, part,
                               nodes, obj['name'], policy_index)
                pool.waitall()
            except (Exception, Timeout):
                self.logger.exception('Exception with objects for container '
                                      '%(container)s for account %(account)s',
                                      {'container': container,
                                       'account': account})
            marker = objects[-1]['name']
        successes = 0
        failures = 0
        timestamp = Timestamp.now()
        for node in nodes:
            anode = account_nodes.pop()
            try:
                direct_delete_container(
                    node, part, account, container,
                    conn_timeout=self.conn_timeout,
                    response_timeout=self.node_timeout,
                    headers={'X-Account-Host': '%(ip)s:%(port)s' % anode,
                             'X-Account-Partition': str(account_partition),
                             'X-Account-Device': anode['device'],
                             'X-Account-Override-Deleted': 'yes',
                             'X-Timestamp': timestamp.internal})
                successes += 1
                self.stats_return_codes[2] = \
                    self.stats_return_codes.get(2, 0) + 1
                self.logger.increment('return_codes.2')
            except ClientException as err:
                if self.logger.getEffectiveLevel() <= DEBUG:
                    self.logger.exception(
                        'Exception with %(ip)s:%(port)s/%(device)s', node)
                failures += 1
                self.logger.increment('containers_failures')
                self.stats_return_codes[err.http_status // 100] = \
                    self.stats_return_codes.get(err.http_status // 100, 0) + 1
                self.logger.increment(
                    'return_codes.%d' % (err.http_status // 100,))
            except (Timeout, socket.error) as err:
                self.logger.error(
                    'Timeout Exception with %(ip)s:%(port)s/%(device)s',
                    node)
                failures += 1
                self.logger.increment('containers_failures')
        if successes > failures:
            self.stats_containers_deleted += 1
            self.logger.increment('containers_deleted')
        elif not successes:
            self.stats_containers_remaining += 1
            self.logger.increment('containers_remaining')
        else:
            self.stats_containers_possibly_remaining += 1
            self.logger.increment('containers_possibly_remaining')

    def reap_object(self, account, container, container_partition,
                    container_nodes, obj, policy_index):
        """
        Deletes the given object by issuing a delete request to each node for
        the object. The format of the delete request is such that each object
        server will update a corresponding container server, removing the
        object from the container's listing.

        This function returns nothing and should raise no exception but only
        update various self.stats_* values for what occurs.

        :param account: The name of the account for the object.
        :param container: The name of the container for the object.
        :param container_partition: The partition for the container on the
                                    container ring.
        :param container_nodes: The primary node dicts for the container.
        :param obj: The name of the object to delete.
        :param policy_index: The storage policy index of the object's container

        * See also: :func:`swift.common.ring.Ring.get_nodes` for a description
          of the container node dicts.
        """
        cnodes = itertools.cycle(container_nodes)
        try:
            ring = self.get_object_ring(policy_index)
        except PolicyError:
            self.stats_objects_remaining += 1
            self.logger.increment('objects_remaining')
            return
        part, nodes = ring.get_nodes(account, container, obj)
        successes = 0
        failures = 0
        timestamp = Timestamp.now()

        for node in nodes:
            cnode = next(cnodes)
            try:
                direct_delete_object(
                    node, part, account, container, obj,
                    conn_timeout=self.conn_timeout,
                    response_timeout=self.node_timeout,
                    headers={'X-Container-Host': '%(ip)s:%(port)s' % cnode,
                             'X-Container-Partition': str(container_partition),
                             'X-Container-Device': cnode['device'],
                             'X-Backend-Storage-Policy-Index': policy_index,
                             'X-Timestamp': timestamp.internal})
                successes += 1
                self.stats_return_codes[2] = \
                    self.stats_return_codes.get(2, 0) + 1
                self.logger.increment('return_codes.2')
            except ClientException as err:
                if self.logger.getEffectiveLevel() <= DEBUG:
                    self.logger.exception(
                        'Exception with %(ip)s:%(port)s/%(device)s', node)
                failures += 1
                self.logger.increment('objects_failures')
                self.stats_return_codes[err.http_status // 100] = \
                    self.stats_return_codes.get(err.http_status // 100, 0) + 1
                self.logger.increment(
                    'return_codes.%d' % (err.http_status // 100,))
            except (Timeout, socket.error) as err:
                failures += 1
                self.logger.increment('objects_failures')
                self.logger.error(
                    'Timeout Exception with %(ip)s:%(port)s/%(device)s',
                    node)
            if successes > failures:
                self.stats_objects_deleted += 1
                self.logger.increment('objects_deleted')
            elif not successes:
                self.stats_objects_remaining += 1
                self.logger.increment('objects_remaining')
            else:
                self.stats_objects_possibly_remaining += 1
                self.logger.increment('objects_possibly_remaining')
