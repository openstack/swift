# Copyright (c) 2010-2016 OpenStack Foundation
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
import errno

from swift.common.utils import audit_location_generator, mkdirs
from swift.container.backend import DATADIR

SYNC_DATADIR = 'sync_containers'


class ContainerSyncStore(object):
    """
    Filesystem based store for local containers that needs to be synced.

    The store holds a list of containers that need to be synced by the
    container sync daemon. The store is local to the container server node,
    that is, only containers whose databases are kept locally on the node are
    listed.
    """
    def __init__(self, devices, logger, mount_check):
        self.devices = os.path.normpath(os.path.join('/', devices)) + '/'
        self.logger = logger
        self.mount_check = mount_check

    def _container_to_synced_container_path(self, path):
        # path is assumed to be of the form:
        # /srv/node/sdb/containers/part/.../*.db
        # or more generally:
        # devices/device/containers/part/.../*.db
        # Below we split the path to the following parts:
        # devices, device, rest
        devices = self.devices
        path = os.path.normpath(path)
        device = path[len(devices):path.rfind(DATADIR)]
        rest = path[path.rfind(DATADIR) + len(DATADIR) + 1:]

        return os.path.join(devices, device, SYNC_DATADIR, rest)

    def _synced_container_to_container_path(self, path):
        # synced path is assumed to be of the form:
        # /srv/node/sdb/sync_containers/part/.../*.db
        # or more generally:
        # devices/device/sync_containers/part/.../*.db
        # Below we split the path to the following parts:
        # devices, device, rest
        devices = self.devices
        path = os.path.normpath(path)
        device = path[len(devices):path.rfind(SYNC_DATADIR)]
        rest = path[path.rfind(SYNC_DATADIR) + len(SYNC_DATADIR) + 1:]

        return os.path.join(devices, device, DATADIR, rest)

    def add_synced_container(self, broker):
        """
        Adds the container db represented by broker to the list of synced
        containers.

        :param broker: An instance of ContainerBroker representing the
                       container to add.
        """
        sync_file = self._container_to_synced_container_path(broker.db_file)
        stat = None
        try:
            stat = os.stat(sync_file)
        except OSError as oserr:
            if oserr.errno != errno.ENOENT:
                raise oserr

        if stat is not None:
            return

        sync_path = os.path.dirname(sync_file)
        mkdirs(sync_path)

        try:
            os.symlink(broker.db_file, sync_file)
        except OSError as oserr:
            if (oserr.errno != errno.EEXIST or
                    not os.path.islink(sync_file)):
                raise oserr

    def remove_synced_container(self, broker):
        """
        Removes the container db represented by broker from the list of synced
        containers.

        :param broker: An instance of ContainerBroker representing the
                       container to remove.
        """
        sync_file = broker.db_file
        sync_file = self._container_to_synced_container_path(sync_file)
        try:
            os.unlink(sync_file)
            os.removedirs(os.path.dirname(sync_file))
        except OSError as oserr:
            if oserr.errno != errno.ENOENT:
                raise oserr

    def update_sync_store(self, broker):
        """
        Add or remove a symlink to/from the sync-containers directory
        according to the broker's metadata.

        Decide according to the broker x-container-sync-to and
        x-container-sync-key whether a symlink needs to be added or
        removed.

        We mention that if both metadata items do not appear
        at all, the container has never been set for sync in reclaim_age
        in which case we do nothing. This is important as this method is
        called for ALL containers from the container replicator.

        Once we realize that we do need to do something, we check if
        the container is marked for delete, in which case we want to
        remove the symlink

        For adding a symlink we notice that both x-container-sync-to and
        x-container-sync-key exist and are valid, that is, are not empty.

        At this point we know we need to do something, the container
        is not marked for delete and the condition to add a symlink
        is not met. conclusion need to remove the symlink.

        :param broker: An instance of ContainerBroker
        """
        # If the broker metadata does not have both x-container-sync-to
        # and x-container-sync-key it has *never* been set. Make sure
        # we do nothing in this case
        if ('X-Container-Sync-To' not in broker.metadata and
                'X-Container-Sync-Key' not in broker.metadata):
            return

        if broker.is_deleted():
            self.remove_synced_container(broker)
            return

        # If both x-container-sync-to and x-container-sync-key
        # exist and valid, add the symlink
        sync_to = sync_key = None
        if 'X-Container-Sync-To' in broker.metadata:
            sync_to = broker.metadata['X-Container-Sync-To'][0]
        if 'X-Container-Sync-Key' in broker.metadata:
            sync_key = broker.metadata['X-Container-Sync-Key'][0]
        if sync_to and sync_key:
            self.add_synced_container(broker)
            return

        self.remove_synced_container(broker)

    def synced_containers_generator(self):
        """
        Iterates over the list of synced containers
        yielding the path of the container db
        """
        all_locs = audit_location_generator(self.devices, SYNC_DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            # What we want to yield is the real path as its being used for
            # initiating a container broker. The broker would break if not
            # given the db real path, as it e.g. assumes the existence of
            # .pending in the same path
            yield self._synced_container_to_container_path(path)
