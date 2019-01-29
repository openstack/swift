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
import time

import six
import xattr
import cPickle as pickle
import logging

from swift.common.daemon import Daemon
from swift.common.ring.utils import is_local_device
from swift.common.storage_policy import POLICIES, get_policy_string
from swift.common.utils import get_logger, whataremyips, fallocate, \
    list_from_csv, listdir
from swift.obj import vfile, log
from swift.obj.diskfile import DiskFileRouter, get_data_dir

# Where to write per-disk status file after a run
STATUS_DIR = '/dev/shm/losf-migration'

BATCH_SIZE = 100 * 1024 * 1024
BATCH_MAXCOUNT = 1000

JOURNAL_NAME = 'migration.journal'
IGNORED_FILES = ['.lock', 'hashes.invalid', 'hashes.pkl',
                 'auditor_status_ALL.json']


def read_in_chunks(file, chunk_size=1024 * 1024):
    while True:
        data = file.read(chunk_size)
        if not data:
            break
        yield data


def sync_and_delete(journal_path, logger=logging):
    """
    sync all data and erase processed files
    :param journal_path:
    :return:
    """
    logger.info("object-migrator running sync and delete")
    with open(journal_path) as f:
        for entry in f:
            filepath = entry.rstrip()
            try:
                os.unlink(filepath)
            except Exception as e:
                logger.warn("could not remove {}".format(filepath))
                logger.warn(e)

            opath = os.path.split(filepath)[0]
            spath = os.path.split(opath)[0]
            try:
                os.rmdir(opath)
                os.rmdir(spath)
            except Exception as e:
                pass


def copy_file(filepath, conf, newfilename=None, logger=logging):
    """
    copy the file from the fs to the KV
    returns the number of bytes copied (st_size, does not include metadata)
    """
    datadir, filename = os.path.split(filepath)
    extension = os.path.splitext(filename)[1]
    # read metadata off original file
    metadata = {}
    try:
        meta = xattr.getxattr(filepath, "user.swift.metadata")
        metadata = pickle.loads(meta)
    except IOError:
        pass

    # copy file data
    stats = os.stat(filepath)
    fsize = stats.st_size
    with open(filepath) as orifile:
        try:
            vfw = vfile.VFileWriter.create(datadir, fsize, conf, logging,
                                           extension)
            for piece in read_in_chunks(orifile):
                while piece:
                    written = os.write(vfw.fd, piece)
                    piece = piece[written:]
            if newfilename:
                # logging.info("commit: {}".format(newfilename))
                vfw.commit(newfilename, metadata)
            else:
                # logger.info("commit: {}".format(filename))
                vfw.commit(filename, metadata)
        # TODO catch RPC Errors separately
        except Exception as e:
            logger.exception(e)
        finally:
            if vfw.fd:
                os.close(vfw.fd)
            if vfw.lock_fd:
                os.close(vfw.lock_fd)
    return fsize


def partitions(objroot):
    for partition in listdir(objroot):
        partitionpath = os.path.join(objroot, partition)
        if os.path.isdir(partitionpath):
            yield partitionpath


def suffixes(partitionroot):
    for elem in listdir(partitionroot):
        suffixpath = os.path.join(partitionroot, elem)
        if len(elem) == 3 and os.path.isdir(suffixpath):
            yield suffixpath


def objhashes(suffixroot):
    for elem in listdir(suffixroot):
        objpath = os.path.join(suffixroot, elem)
        if len(elem) == 32 and os.path.isdir(objpath):
            yield objpath


def files(objroot):
    r = []
    for elem in listdir(objroot):
        filepath = os.path.join(objroot, elem)
        if os.path.isfile(filepath):
            r.append(filepath)
    return r


class ObjectMigrator(Daemon):
    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='object-migrator')
        self.bind_ip = conf.get('bind_ip', '0.0.0.0')
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.servers_per_port = int(conf.get('servers_per_port', '0') or 0)
        self.port = None if self.servers_per_port else \
            int(conf.get('bind_port', 6200))
        self.devices_dir = conf.get('devices', '/srv/node')
        self.batch_size = int(conf.get('batch_size', BATCH_SIZE))
        self.batch_maxcount = int(conf.get('batch_maxcount', BATCH_MAXCOUNT))

        policies = conf.get('policies_to_migrate')
        # this needs to be a list of policies, not just indexes
        self.policies = [POLICIES.get_by_index(x) for x in
                         list_from_csv(policies)]
        self._df_router = DiskFileRouter(self.conf, self.logger)
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        # How long to wait between runs when in daemon mode
        self.interval = int(conf.get('interval') or 3600)
        # How long to wait after IO ops (file copy, file deletion)
        self.io_interval = int(conf.get('io_interval') or 0)
        self.all_local_devices = self.get_local_devices()

        # vfile specific config. Change this here and in kvfile?
        self.vfile_conf = {}
        self.vfile_conf['volume_alloc_chunk_size'] = int(
            conf.get('volume_alloc_chunk_size', 16 * 1024))
        self.vfile_conf['volume_low_free_space'] = int(
            conf.get('volume_low_free_space', 8 * 1024))
        self.vfile_conf['metadata_reserve'] = int(
            conf.get('metadata_reserve', 500))
        self.vfile_conf['max_volume_count'] = int(
            conf.get('max_volume_count', 1000))
        self.vfile_conf['max_volume_size'] = int(
            conf.get('max_volume_size', 10 * 1024 * 1024 * 1024))
        # debug logger
        self.vfile_conf['logger'] = log.Syslog(facility=log.Facility.LOCAL2)

    def load_object_ring(self, policy):
        """
        Make sure the policy's rings are loaded.

        :param policy: the StoragePolicy instance
        :returns: appropriate ring object
        """
        policy.load_ring(self.swift_dir)
        return policy.object_ring

    def get_policy2devices(self):
        ips = whataremyips(self.bind_ip)
        policy2devices = {}
        for policy in POLICIES:
            self.load_object_ring(policy)
            local_devices = list(six.moves.filter(
                lambda dev: dev and is_local_device(
                    ips, self.port,
                    dev['replication_ip'], dev['replication_port']),
                policy.object_ring.devs))
            policy2devices[policy] = local_devices
        return policy2devices

    # noinspection PyCompatibility
    def get_local_devices(self):
        """Returns a set of all local devices in all policies."""
        policy2devices = self.get_policy2devices()
        return reduce(set.union, (
            set(d['device'] for d in devices)
            for devices in policy2devices.values()), set())

    def get_worker_args(self, once=False, **kwargs):
        """
        One worker per disk (on per disk per policy would be too much IO)
        """
        self.all_local_devices = self.get_local_devices()
        devices = list(self.all_local_devices)
        if not devices:
            yield dict()
            return

        override_policies = [POLICIES.get_by_index(x) for x in
                             list_from_csv(kwargs.get('policies'))]
        if not override_policies:
            override_policies = None

        for device in devices:
            yield dict(device=device, override_policies=override_policies)

    def run_once(self, *args, **kwargs):
        device = kwargs.get('device')
        override_policies = kwargs.get('override_policies')
        if override_policies:
            self.policies = override_policies
        self.migrate(device)
        self.check_migration_finished(device)

    def run_forever(self, *args, **kwargs):
        device = kwargs.get('device')
        while True:
            try:
                self.migrate(device)
                self.check_migration_finished(device)
            except Exception:
                self.logger.exception('Error during LOSF object migration')
            time.sleep(self.interval)

    def write_status(self, device, policy, migration_complete):
        if not os.path.exists(STATUS_DIR):
            os.makedirs(STATUS_DIR)

        filename = os.path.join(STATUS_DIR, '{}-{}'.format(device, policy.idx))
        with open(filename, 'w') as f:
            if migration_complete:
                f.write('done\n')
            else:
                f.write('incomplete\n')

    def check_migration_finished(self, device):
        for policy in self.policies:
            self._check_migration_finished(device, policy)

    def _check_migration_finished(self, device, policy):
        df_mgr = self._df_router[policy]
        dev_path = df_mgr.get_dev_path(device)
        if not dev_path:
            self.logger.warning('%s is not mounted', device)
            return

        self.logger.warning(
            'checking if losf migration is finished on {}, policy {}'.format(
                dev_path, policy.idx))
        data_dir = get_data_dir(policy)
        obj_root = os.path.join(dev_path, data_dir)
        remaining_files = set()
        remaining_count = 0
        for root, dirs, files in os.walk(obj_root):
            current_files = [f for f in files if f not in IGNORED_FILES]
            if current_files:
                remaining_count += len(current_files)
            remaining_files |= set(current_files)
        if remaining_files:
            unique_filenames = " ".join(list(remaining_files))
            self.logger.warning(
                "{} files remain, names: {}".format(remaining_count,
                                                    unique_filenames))
            self.write_status(device, policy, migration_complete=False)
        else:
            self.write_status(device, policy, migration_complete=True)

    def migrate(self, device):
        for policy in self.policies:
            self._migrate(device, policy)

    def _migrate(self, device, policy):
        copied_bytes = 0
        copied_file_count = 0

        self._df_router = DiskFileRouter(self.conf, self.logger)
        df_mgr = self._df_router[policy]
        dev_path = df_mgr.get_dev_path(device)
        if not dev_path:
            self.logger.warning('%s is not mounted', device)
            return

        if not (policy.diskfile_uri.endswith(
                '.kv') or policy.diskfile_uri.endswith('.hybrid')):
            self.logger.warning(
                'policy is neither kv nor hybrid, cannot migrate')
            return

        self.logger.info('starting losf migration on {}, policy {}'.format(
            dev_path, policy.idx))
        data_dir = get_data_dir(policy)
        losf_dir = get_policy_string('losf', policy)
        obj_root = os.path.join(dev_path, data_dir)
        losf_root = os.path.join(dev_path, losf_dir)
        journal_path = os.path.join(losf_root, JOURNAL_NAME)
        journal = open(journal_path, 'w')
        fallocate(journal.fileno(), 10 * 1024 * 1024, 0)

        for partition in partitions(obj_root):
            self.logger.info('migrating partition %s', partition)
            for suffix in suffixes(partition):
                for obj in objhashes(suffix):
                    objfiles = files(obj)
                    # do we need to turn a data and durable file into one data
                    # file?
                    is_old_durable = False
                    try:
                        setext = set(['.data', '.durable'])
                        if len(objfiles) == 2 and set(os.path.splitext(x)[1]
                                                      for x in
                                                      objfiles) == setext:
                            is_old_durable = True
                    except Exception:
                        pass

                    if is_old_durable:
                        # self.logger.info("OLD STYLE DURABLE DIRECTORY")
                        durable_path = \
                            [x for x in objfiles if x.endswith('.durable')][0]
                        data_path = \
                            [x for x in objfiles if x.endswith('.data')][0]
                        data_filename = os.path.split(data_path)[1]
                        new_data_filename = data_filename.replace('.data',
                                                                  '#d.data')

                        # now, just copy the datafile with the new name.
                        # write both old names to the journal
                        # so they get deleted
                        # self.logger.info(
                        #     "copy: {} with name {}".format(data_path,
                        #                                    new_data_filename))
                        try:
                            copied_bytes += copy_file(data_path,
                                                      self.vfile_conf,
                                                      new_data_filename)
                        except Exception as e:
                            self.logger.increment('losf_skipped_file')
                            self.logger.exception(e)
                            self.logger.warn('skipped')
                            continue
                        # self.logger.debug(
                        #     "copied bytes: {}".format(copied_bytes))
                        journal.write("{}\n".format(data_path))
                        journal.write("{}\n".format(durable_path))
                        if (copied_bytes >= self.batch_size or
                                copied_file_count >= self.batch_maxcount):
                            journal.close()
                            sync_and_delete(journal_path, logger=self.logger)
                            journal = open(journal_path, 'w')
                            copied_bytes = 0
                            copied_file_count = 0
                    else:
                        # no, just copy all files in the directory
                        for filepath in objfiles:
                            # logging.info("copy: {}".format(filepath))
                            try:
                                copied_bytes += copy_file(filepath,
                                                          self.vfile_conf)
                            except Exception as e:
                                self.logger.increment('losf_skipped_file')
                                self.logger.exception(e)
                                self.logger.warn('skipped')
                                continue
                            # logging.debug(
                            #     "copied bytes: {}".format(copied_bytes))
                            journal.write("{}\n".format(filepath))
                            if (copied_bytes >= self.batch_size or
                                    copied_file_count >= self.batch_maxcount):
                                journal.close()
                                sync_and_delete(journal_path,
                                                logger=self.logger)
                                journal = open(journal_path, 'w')
                                copied_bytes = 0
                            copied_file_count = 0
        journal.close()
        sync_and_delete(journal_path)
        os.unlink(journal_path)
