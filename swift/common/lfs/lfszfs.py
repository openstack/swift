# Copyright (c) 2010-2011 OpenStack, LLC.
# Copyright (c) Nexenta Systems Inc.
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
import sys

import nspyzfs

import swift.common.ring as ring
from swift.common.lfs import LFS, LFSStatus
from swift.common.utils import mkdirs, whataremyips, storage_directory

def zfs_create(pool, fs_name, mount_point):
    """
    Creates the ZFS filesystem with the given path.
    """
    kwargs = {}
    if mount_point:
        kwargs['mountpoint'] = mount_point

    nspyzfs.create_filesystems('%s/%s' %(pool, fs_name), **kwargs)

class LFSZFS(LFS):

    def __init__(self, conf, srvdir, logger):
        LFS.__init__(self, conf, srvdir, logger)
        self.fs = 'zfs'
        self.topfs = conf.get('topfs')
        self.check_interval = int(conf.get('check_interval', '30'))
        mkdirs(self.root)
        devs = ring.Ring(os.path.join(conf.get('swift_dir', '/etc/swift'),
            'object.ring.gz')).get_devs()
        my_ips = whataremyips()

        # pools is a list of tuple => (<pool name>, <pool mirrorr_count>)
        self.pools = \
        [(dev['device'], dev['mirror_copies']) for dev in devs\
                                                if dev['ip'] in my_ips]

        # Create the Top level ZFS.
        for pool, mr_count in self.pools:
            zfs_create(pool, self.topfs, '%s/%s' %(self.root, pool))

        self.degraded_pools = []
        self.faulted_pools = []
        self.misconfigured_pools = []
        if not self.topfs:
            self.logger.error(
                "Cannot locate ZFS filesystem for the Server. Exiting..\n")
            sys.exit(1)

        self.fs_per_part = False
        self.fs_per_obj = False
        if self.conf.get('fs_per_obj', 'false') == 'true':
            self.fs_per_part = True
            self.fs_per_obj = True
        elif self.conf.get('fs_per_part', 'false') == 'true':
            self.fs_per_part = True

        self.status_checker = LFSStatus(self.check_interval,
            self.check_pools, ())
        self.status_checker.start()

    def check_pools(self, args):
        need_cb = False

        for pool, mr_count in self.pools:
            pool_config = nspyzfs.zpool_status(pool)[0]

            if pool_config.get_mirrorcount() > mr_count:
                # TODO: Actual corrective and recovery measures needs to
                # be implemented.
                self.misconfigured_pools.append(pool)
            else:
                if pool in self.misconfigured_pools:
                    self.misconfigured_pools.remove(pool)

            ret = pool_config.get_state()
            if ret == nspyzfs.ZPOOL_STATE_DEGRADED:
                if not pool in self.degraded_pools:
                    self.degraded_pools.append(pool)
                    need_cb = True
            elif ret == nspyzfs.ZPOOL_STATE_FAULTED:
                if not pool in self.faulted_pools:
                    self.faulted_pools.append(pool)
                    need_cb = True
            elif ret == nspyzfs.ZPOOL_STATE_UNKNOWN:
                need_cb = True
                # TODO: Need to handle ZPOOL_UNKNOWN state.
            else:
                if pool in self.faulted_pools:
                    self.faulted_pools.remove(pool)
                elif pool in self.degraded_pools:
                    self.degraded_pools.remove(pool)

        if need_cb:
            return (self.zfs_error_callback, ())

        return None

    def zfs_error_callback(self, args):
        # TODO: Let Object server know about this situation
        # so that object server can take recovery actions.
        # This function can be worked upon only after recovery
        # semantics for object server is designed and implemented

        self.logger.warning("DEGARDED pools: %s" % degraded_pools)
        self.logger.warning("FAULTED pools: %s" % faulted_pools)

        # We will clear the faults, but maintain the faults
        # internally in degraded_pools and fault_pools.
        # THIS CODE WILL BE REPLACED BY recovery semantics
        # to be implemented in object server.
        self.status_checker.clear_fault()

    def tmp_dir(self, pool, partition, name_hash):
        if self.fs_per_obj:
            return os.path.join(self.root, pool,
                storage_directory(self.srvdir, partition, name_hash), 'tmp')
        elif self.fs_per_part:
            return os.path.join(self.root, pool, self.srvdir, partition, 'tmp')
        return os.path.join(self.root, pool, self.srvdir, 'tmp')

    def setup_partition(self, pool, partition):
        path = os.path.join(self.root, pool, self.srvdir, partition)
        if self.fs_per_part:
            fs = '%s/%s/%s' % (self.topfs, self.srvdir, partition)
            zfs_create(pool, fs, path)
        else:
            mkdirs(path)

    def setup_objdir(self, pool, partition, name_hash):
        path = os.path.join(self.root, pool,
            storage_directory(self.srvdir, partition, name_hash))
        if not os.path.exists(path) and self.fs_per_obj:
            fs = '%s/%s/%s/%s' %(self.topfs, self.srvdir, partition, name_hash)
            zfs_create(pool, fs, path)
