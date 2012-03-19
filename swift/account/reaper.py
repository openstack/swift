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

import os
import random
from logging import DEBUG
from math import sqrt
from time import time

from eventlet import GreenPool, sleep, Timeout

from swift.account.server import DATADIR
from swift.common.db import AccountBroker
from swift.common.direct_client import ClientException, \
    direct_delete_container, direct_delete_object, direct_get_container
from swift.common.ring import Ring
from swift.common.utils import get_logger, whataremyips
from swift.common.daemon import Daemon


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

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='account-reaper')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.interval = int(conf.get('interval', 3600))
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.account_ring_path = os.path.join(swift_dir, 'account.ring.gz')
        self.container_ring_path = os.path.join(swift_dir, 'container.ring.gz')
        self.object_ring_path = os.path.join(swift_dir, 'object.ring.gz')
        self.account_ring = None
        self.container_ring = None
        self.object_ring = None
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.myips = whataremyips()
        self.concurrency = int(conf.get('concurrency', 25))
        self.container_concurrency = self.object_concurrency = \
            sqrt(self.concurrency)
        self.container_pool = GreenPool(size=self.container_concurrency)

    def get_account_ring(self):
        """ The account :class:`swift.common.ring.Ring` for the cluster. """
        if not self.account_ring:
            self.logger.debug(
                _('Loading account ring from %s'), self.account_ring_path)
            self.account_ring = Ring(self.account_ring_path)
        return self.account_ring

    def get_container_ring(self):
        """ The container :class:`swift.common.ring.Ring` for the cluster. """
        if not self.container_ring:
            self.logger.debug(
                _('Loading container ring from %s'), self.container_ring_path)
            self.container_ring = Ring(self.container_ring_path)
        return self.container_ring

    def get_object_ring(self):
        """ The object :class:`swift.common.ring.Ring` for the cluster. """
        if not self.object_ring:
            self.logger.debug(
                _('Loading object ring from %s'), self.object_ring_path)
            self.object_ring = Ring(self.object_ring_path)
        return self.object_ring

    def run_forever(self, *args, **kwargs):
        """
        Main entry point when running the reaper in its normal daemon mode.
        This repeatedly calls :func:`reap_once` no quicker than the
        configuration interval.
        """
        self.logger.debug(_('Daemon started.'))
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
        self.logger.debug(_('Begin devices pass: %s'), self.devices)
        begin = time()
        for device in os.listdir(self.devices):
            if self.mount_check and \
                    not os.path.ismount(os.path.join(self.devices, device)):
                self.logger.debug(
                    _('Skipping %s as it is not mounted'), device)
                continue
            self.reap_device(device)
        elapsed = time() - begin
        self.logger.info(_('Devices pass completed: %.02fs'), elapsed)

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
            if nodes[0]['ip'] not in self.myips or \
                    not os.path.isdir(partition_path):
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
                            broker = \
                                AccountBroker(os.path.join(hsh_path, fname))
                            if broker.is_status_deleted() and \
                                    not broker.empty():
                                self.reap_account(broker, partition, nodes)

    def reap_account(self, broker, partition, nodes):
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

        .. seealso::

            :class:`swift.common.db.AccountBroker` for the broker class.

        .. seealso::

            :func:`swift.common.ring.Ring.get_nodes` for a description
            of the node dicts.
        """
        begin = time()
        account = broker.get_info()['account']
        self.logger.info(_('Beginning pass on account %s'), account)
        self.stats_return_codes = {}
        self.stats_containers_deleted = 0
        self.stats_objects_deleted = 0
        self.stats_containers_remaining = 0
        self.stats_objects_remaining = 0
        self.stats_containers_possibly_remaining = 0
        self.stats_objects_possibly_remaining = 0
        try:
            marker = ''
            while True:
                containers = \
                    list(broker.list_containers_iter(1000, marker, None, None,
                                                     None))
                if not containers:
                    break
                try:
                    for (container, _junk, _junk, _junk) in containers:
                        self.container_pool.spawn(self.reap_container, account,
                                                  partition, nodes, container)
                    self.container_pool.waitall()
                except (Exception, Timeout):
                    self.logger.exception(
                        _('Exception with containers for account %s'), account)
                marker = containers[-1][0]
            log = 'Completed pass on account %s' % account
        except (Exception, Timeout):
            self.logger.exception(
                _('Exception with account %s'), account)
            log = _('Incomplete pass on account %s') % account
        if self.stats_containers_deleted:
            log += _(', %s containers deleted') % self.stats_containers_deleted
        if self.stats_objects_deleted:
            log += _(', %s objects deleted') % self.stats_objects_deleted
        if self.stats_containers_remaining:
            log += _(', %s containers remaining') % \
                   self.stats_containers_remaining
        if self.stats_objects_remaining:
            log += _(', %s objects remaining') % self.stats_objects_remaining
        if self.stats_containers_possibly_remaining:
            log += _(', %s containers possibly remaining') % \
                   self.stats_containers_possibly_remaining
        if self.stats_objects_possibly_remaining:
            log += _(', %s objects possibly remaining') % \
                   self.stats_objects_possibly_remaining
        if self.stats_return_codes:
            log += _(', return codes: ')
            for code in sorted(self.stats_return_codes.keys()):
                log += '%s %sxxs, ' % (self.stats_return_codes[code], code)
            log = log[:-2]
        log += _(', elapsed: %.02fs') % (time() - begin)
        self.logger.info(log)

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
                objects = direct_get_container(node, part, account, container,
                        marker=marker, conn_timeout=self.conn_timeout,
                        response_timeout=self.node_timeout)[1]
                self.stats_return_codes[2] = \
                    self.stats_return_codes.get(2, 0) + 1
            except ClientException, err:
                if self.logger.getEffectiveLevel() <= DEBUG:
                    self.logger.exception(
                        _('Exception with %(ip)s:%(port)s/%(device)s'), node)
                self.stats_return_codes[err.http_status / 100] = \
                    self.stats_return_codes.get(err.http_status / 100, 0) + 1
            if not objects:
                break
            try:
                for obj in objects:
                    if isinstance(obj['name'], unicode):
                        obj['name'] = obj['name'].encode('utf8')
                    pool.spawn(self.reap_object, account, container, part,
                               nodes, obj['name'])
                pool.waitall()
            except (Exception, Timeout):
                self.logger.exception(_('Exception with objects for container '
                    '%(container)s for account %(account)s'),
                    {'container': container, 'account': account})
            marker = objects[-1]['name']
        successes = 0
        failures = 0
        for node in nodes:
            anode = account_nodes.pop()
            try:
                direct_delete_container(node, part, account, container,
                    conn_timeout=self.conn_timeout,
                    response_timeout=self.node_timeout,
                    headers={'X-Account-Host': '%(ip)s:%(port)s' % anode,
                             'X-Account-Partition': str(account_partition),
                             'X-Account-Device': anode['device'],
                             'X-Account-Override-Deleted': 'yes'})
                successes += 1
                self.stats_return_codes[2] = \
                    self.stats_return_codes.get(2, 0) + 1
            except ClientException, err:
                if self.logger.getEffectiveLevel() <= DEBUG:
                    self.logger.exception(
                        _('Exception with %(ip)s:%(port)s/%(device)s'), node)
                failures += 1
                self.stats_return_codes[err.http_status / 100] = \
                    self.stats_return_codes.get(err.http_status / 100, 0) + 1
        if successes > failures:
            self.stats_containers_deleted += 1
        elif not successes:
            self.stats_containers_remaining += 1
        else:
            self.stats_containers_possibly_remaining += 1

    def reap_object(self, account, container, container_partition,
                    container_nodes, obj):
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

        * See also: :func:`swift.common.ring.Ring.get_nodes` for a description
          of the container node dicts.
        """
        container_nodes = list(container_nodes)
        part, nodes = self.get_object_ring().get_nodes(account, container, obj)
        successes = 0
        failures = 0
        for node in nodes:
            cnode = container_nodes.pop()
            try:
                direct_delete_object(node, part, account, container, obj,
                    conn_timeout=self.conn_timeout,
                    response_timeout=self.node_timeout,
                    headers={'X-Container-Host': '%(ip)s:%(port)s' % cnode,
                             'X-Container-Partition': str(container_partition),
                             'X-Container-Device': cnode['device']})
                successes += 1
                self.stats_return_codes[2] = \
                    self.stats_return_codes.get(2, 0) + 1
            except ClientException, err:
                if self.logger.getEffectiveLevel() <= DEBUG:
                    self.logger.exception(
                        _('Exception with %(ip)s:%(port)s/%(device)s'), node)
                failures += 1
                self.stats_return_codes[err.http_status / 100] = \
                    self.stats_return_codes.get(err.http_status / 100, 0) + 1
            if successes > failures:
                self.stats_objects_deleted += 1
            elif not successes:
                self.stats_objects_remaining += 1
            else:
                self.stats_objects_possibly_remaining += 1
