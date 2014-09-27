# Copyright (c) 2010-2013 OpenStack Foundation
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

"""
Disk File Interface for the Swift Object Server

The `DiskFile`, `DiskFileWriter` and `DiskFileReader` classes combined define
the on-disk abstraction layer for supporting the object server REST API
interfaces (excluding `REPLICATE`). Other implementations wishing to provide
an alternative backend for the object server must implement the three
classes. An example alternative implementation can be found in the
`mem_server.py` and `mem_diskfile.py` modules along size this one.

The `DiskFileManager` is a reference implemenation specific class and is not
part of the backend API.

The remaining methods in this module are considered implementation specific and
are also not considered part of the backend API.
"""

import cPickle as pickle
import errno
import fcntl
import os
import time
import uuid
import hashlib
import logging
import traceback
from os.path import basename, dirname, exists, getmtime, join
from random import shuffle
from tempfile import mkstemp
from contextlib import contextmanager
from collections import defaultdict

from xattr import getxattr, setxattr
from eventlet import Timeout
from eventlet.hubs import trampoline

from swift import gettext_ as _
from swift.common.constraints import check_mount
from swift.common.request_helpers import is_sys_meta
from swift.common.utils import mkdirs, Timestamp, \
    storage_directory, hash_path, renamer, fallocate, fsync, \
    fdatasync, drop_buffer_cache, ThreadPool, lock_path, write_pickle, \
    config_true_value, listdir, split_path, ismount, remove_file, \
    get_md5_socket, system_has_splice, splice, tee, SPLICE_F_MORE, \
    F_SETPIPE_SZ
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist, \
    DiskFileCollision, DiskFileNoSpace, DiskFileDeviceUnavailable, \
    DiskFileDeleted, DiskFileError, DiskFileNotOpen, PathNotDir, \
    ReplicationLockTimeout, DiskFileExpired
from swift.common.swob import multi_range_iterator
from swift.common.storage_policy import get_policy_string, POLICIES
from functools import partial


PICKLE_PROTOCOL = 2
ONE_WEEK = 604800
HASH_FILE = 'hashes.pkl'
METADATA_KEY = 'user.swift.metadata'
DROP_CACHE_WINDOW = 1024 * 1024
# These are system-set metadata keys that cannot be changed with a POST.
# They should be lowercase.
DATAFILE_SYSTEM_META = set('content-length content-type deleted etag'.split())
DATADIR_BASE = 'objects'
ASYNCDIR_BASE = 'async_pending'
TMP_BASE = 'tmp'
get_data_dir = partial(get_policy_string, DATADIR_BASE)
get_async_dir = partial(get_policy_string, ASYNCDIR_BASE)
get_tmp_dir = partial(get_policy_string, TMP_BASE)
MD5_OF_EMPTY_STRING = 'd41d8cd98f00b204e9800998ecf8427e'


def read_metadata(fd):
    """
    Helper function to read the pickled metadata from an object file.

    :param fd: file descriptor or filename to load the metadata from

    :returns: dictionary of metadata
    """
    metadata = ''
    key = 0
    try:
        while True:
            metadata += getxattr(fd, '%s%s' % (METADATA_KEY, (key or '')))
            key += 1
    except IOError:
        pass
    return pickle.loads(metadata)


def write_metadata(fd, metadata):
    """
    Helper function to write pickled metadata for an object file.

    :param fd: file descriptor or filename to write the metadata
    :param metadata: metadata to write
    """
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        setxattr(fd, '%s%s' % (METADATA_KEY, key or ''), metastr[:254])
        metastr = metastr[254:]
        key += 1


def extract_policy_index(obj_path):
    """
    Extracts the policy index for an object (based on the name of the objects
    directory) given the device-relative path to the object. Returns 0 in
    the event that the path is malformed in some way.

    The device-relative path is everything after the mount point; for example:

    /srv/node/d42/objects-5/179/
        485dc017205a81df3af616d917c90179/1401811134.873649.data

    would have device-relative path:

    objects-5/179/485dc017205a81df3af616d917c90179/1401811134.873649.data

    :param obj_path: device-relative path of an object
    :returns: storage policy index
    """
    policy_idx = 0
    try:
        obj_portion = obj_path[obj_path.index(DATADIR_BASE):]
        obj_dirname = obj_portion[:obj_portion.index('/')]
    except Exception:
        return policy_idx
    if '-' in obj_dirname:
        base, policy_idx = obj_dirname.split('-', 1)
        if POLICIES.get_by_index(policy_idx) is None:
            policy_idx = 0
    return int(policy_idx)


def quarantine_renamer(device_path, corrupted_file_path):
    """
    In the case that a file is corrupted, move it to a quarantined
    area to allow replication to fix it.

    :params device_path: The path to the device the corrupted file is on.
    :params corrupted_file_path: The path to the file you want quarantined.

    :returns: path (str) of directory the file was moved to
    :raises OSError: re-raises non errno.EEXIST / errno.ENOTEMPTY
                     exceptions from rename
    """
    from_dir = dirname(corrupted_file_path)
    to_dir = join(device_path, 'quarantined',
                  get_data_dir(extract_policy_index(corrupted_file_path)),
                  basename(from_dir))
    invalidate_hash(dirname(from_dir))
    try:
        renamer(from_dir, to_dir)
    except OSError as e:
        if e.errno not in (errno.EEXIST, errno.ENOTEMPTY):
            raise
        to_dir = "%s-%s" % (to_dir, uuid.uuid4().hex)
        renamer(from_dir, to_dir)
    return to_dir


def get_ondisk_files(files, datadir):
    """
    Given a simple list of files names, determine the files to use.

    :params files: simple set of files as a python list
    :params datadir: directory name files are from for convenience
    :returns: a tuple of data, meta and ts (tombstone) files, in one of
              two states:

    * ts_file is not None, data_file is None, meta_file is None

      object is considered deleted

    * data_file is not None, ts_file is None

      object exists, and optionally has fast-POST metadata
    """
    files.sort(reverse=True)
    data_file = meta_file = ts_file = None
    for afile in files:
        assert ts_file is None, "On-disk file search loop" \
            " continuing after tombstone, %s, encountered" % ts_file
        assert data_file is None, "On-disk file search loop" \
            " continuing after data file, %s, encountered" % data_file
        if afile.endswith('.ts'):
            meta_file = None
            ts_file = join(datadir, afile)
            break
        if afile.endswith('.meta') and not meta_file:
            meta_file = join(datadir, afile)
            # NOTE: this does not exit this loop, since a fast-POST
            # operation just updates metadata, writing one or more
            # .meta files, the data file will have an older timestamp,
            # so we keep looking.
            continue
        if afile.endswith('.data'):
            data_file = join(datadir, afile)
            break
    assert ((data_file is None and meta_file is None and ts_file is None)
            or (ts_file is not None and data_file is None
                and meta_file is None)
            or (data_file is not None and ts_file is None)), \
        "On-disk file search algorithm contract is broken: data_file:" \
        " %s, meta_file: %s, ts_file: %s" % (data_file, meta_file, ts_file)
    return data_file, meta_file, ts_file


def hash_cleanup_listdir(hsh_path, reclaim_age=ONE_WEEK):
    """
    List contents of a hash directory and clean up any old files.

    :param hsh_path: object hash path
    :param reclaim_age: age in seconds at which to remove tombstones
    :returns: list of files remaining in the directory, reverse sorted
    """
    files = listdir(hsh_path)
    if len(files) == 1:
        if files[0].endswith('.ts'):
            # remove tombstones older than reclaim_age
            ts = files[0].rsplit('.', 1)[0]
            if (time.time() - float(Timestamp(ts))) > reclaim_age:
                remove_file(join(hsh_path, files[0]))
                files.remove(files[0])
    elif files:
        files.sort(reverse=True)
        data_file, meta_file, ts_file = get_ondisk_files(files, '')
        newest_file = data_file or ts_file
        for filename in list(files):
            if ((filename < newest_file)
                    or (meta_file
                        and filename.endswith('.meta')
                        and filename < meta_file)):
                remove_file(join(hsh_path, filename))
                files.remove(filename)
    return files


def hash_suffix(path, reclaim_age):
    """
    Performs reclamation and returns an md5 of all (remaining) files.

    :param reclaim_age: age in seconds at which to remove tombstones
    :raises PathNotDir: if given path is not a valid directory
    :raises OSError: for non-ENOTDIR errors
    """
    md5 = hashlib.md5()
    try:
        path_contents = sorted(os.listdir(path))
    except OSError as err:
        if err.errno in (errno.ENOTDIR, errno.ENOENT):
            raise PathNotDir()
        raise
    for hsh in path_contents:
        hsh_path = join(path, hsh)
        try:
            files = hash_cleanup_listdir(hsh_path, reclaim_age)
        except OSError as err:
            if err.errno == errno.ENOTDIR:
                partition_path = dirname(path)
                objects_path = dirname(partition_path)
                device_path = dirname(objects_path)
                quar_path = quarantine_renamer(device_path, hsh_path)
                logging.exception(
                    _('Quarantined %(hsh_path)s to %(quar_path)s because '
                      'it is not a directory'), {'hsh_path': hsh_path,
                                                 'quar_path': quar_path})
                continue
            raise
        if not files:
            try:
                os.rmdir(hsh_path)
            except OSError:
                pass
        for filename in files:
            md5.update(filename)
    try:
        os.rmdir(path)
    except OSError:
        pass
    return md5.hexdigest()


def invalidate_hash(suffix_dir):
    """
    Invalidates the hash for a suffix_dir in the partition's hashes file.

    :param suffix_dir: absolute path to suffix dir whose hash needs
                       invalidating
    """

    suffix = basename(suffix_dir)
    partition_dir = dirname(suffix_dir)
    hashes_file = join(partition_dir, HASH_FILE)
    with lock_path(partition_dir):
        try:
            with open(hashes_file, 'rb') as fp:
                hashes = pickle.load(fp)
            if suffix in hashes and not hashes[suffix]:
                return
        except Exception:
            return
        hashes[suffix] = None
        write_pickle(hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)


def get_hashes(partition_dir, recalculate=None, do_listdir=False,
               reclaim_age=ONE_WEEK):
    """
    Get a list of hashes for the suffix dir.  do_listdir causes it to mistrust
    the hash cache for suffix existence at the (unexpectedly high) cost of a
    listdir.  reclaim_age is just passed on to hash_suffix.

    :param partition_dir: absolute path of partition to get hashes for
    :param recalculate: list of suffixes which should be recalculated when got
    :param do_listdir: force existence check for all hashes in the partition
    :param reclaim_age: age at which to remove tombstones

    :returns: tuple of (number of suffix dirs hashed, dictionary of hashes)
    """

    hashed = 0
    hashes_file = join(partition_dir, HASH_FILE)
    modified = False
    force_rewrite = False
    hashes = {}
    mtime = -1

    if recalculate is None:
        recalculate = []

    try:
        with open(hashes_file, 'rb') as fp:
            hashes = pickle.load(fp)
        mtime = getmtime(hashes_file)
    except Exception:
        do_listdir = True
        force_rewrite = True
    if do_listdir:
        for suff in os.listdir(partition_dir):
            if len(suff) == 3:
                hashes.setdefault(suff, None)
        modified = True
    hashes.update((suffix, None) for suffix in recalculate)
    for suffix, hash_ in hashes.items():
        if not hash_:
            suffix_dir = join(partition_dir, suffix)
            try:
                hashes[suffix] = hash_suffix(suffix_dir, reclaim_age)
                hashed += 1
            except PathNotDir:
                del hashes[suffix]
            except OSError:
                logging.exception(_('Error hashing suffix'))
            modified = True
    if modified:
        with lock_path(partition_dir):
            if force_rewrite or not exists(hashes_file) or \
                    getmtime(hashes_file) == mtime:
                write_pickle(
                    hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)
                return hashed, hashes
        return get_hashes(partition_dir, recalculate, do_listdir,
                          reclaim_age)
    else:
        return hashed, hashes


class AuditLocation(object):
    """
    Represents an object location to be audited.

    Other than being a bucket of data, the only useful thing this does is
    stringify to a filesystem path so the auditor's logs look okay.
    """

    def __init__(self, path, device, partition):
        self.path, self.device, self.partition = path, device, partition

    def __str__(self):
        return str(self.path)


def object_audit_location_generator(devices, mount_check=True, logger=None,
                                    device_dirs=None):
    """
    Given a devices path (e.g. "/srv/node"), yield an AuditLocation for all
    objects stored under that directory if device_dirs isn't set.  If
    device_dirs is set, only yield AuditLocation for the objects under the
    entries in device_dirs. The AuditLocation only knows the path to the hash
    directory, not to the .data file therein (if any). This is to avoid a
    double listdir(hash_dir); the DiskFile object will always do one, so
    we don't.

    :param devices: parent directory of the devices to be audited
    :param mount_check: flag to check if a mount check should be performed
                        on devices
    :param logger: a logger object
    :device_dirs: a list of directories under devices to traverse
    """
    if not device_dirs:
        device_dirs = listdir(devices)
    else:
        # remove bogus devices and duplicates from device_dirs
        device_dirs = list(
            set(listdir(devices)).intersection(set(device_dirs)))
    # randomize devices in case of process restart before sweep completed
    shuffle(device_dirs)

    for device in device_dirs:
        if mount_check and not \
                ismount(os.path.join(devices, device)):
            if logger:
                logger.debug(
                    _('Skipping %s as it is not mounted'), device)
            continue
        # loop through object dirs for all policies
        for dir in [dir for dir in os.listdir(os.path.join(devices, device))
                    if dir.startswith(DATADIR_BASE)]:
            datadir_path = os.path.join(devices, device, dir)
            # warn if the object dir doesn't match with a policy
            policy_idx = 0
            if '-' in dir:
                base, policy_idx = dir.split('-', 1)
            try:
                get_data_dir(policy_idx)
            except ValueError:
                if logger:
                    logger.warn(_('Directory %s does not map to a '
                                  'valid policy') % dir)
            partitions = listdir(datadir_path)
            for partition in partitions:
                part_path = os.path.join(datadir_path, partition)
                try:
                    suffixes = listdir(part_path)
                except OSError as e:
                    if e.errno != errno.ENOTDIR:
                        raise
                    continue
                for asuffix in suffixes:
                    suff_path = os.path.join(part_path, asuffix)
                    try:
                        hashes = listdir(suff_path)
                    except OSError as e:
                        if e.errno != errno.ENOTDIR:
                            raise
                        continue
                    for hsh in hashes:
                        hsh_path = os.path.join(suff_path, hsh)
                        yield AuditLocation(hsh_path, device, partition)


class DiskFileManager(object):
    """
    Management class for devices, providing common place for shared parameters
    and methods not provided by the DiskFile class (which primarily services
    the object server REST API layer).

    The `get_diskfile()` method is how this implementation creates a `DiskFile`
    object.

    .. note::

        This class is reference implementation specific and not part of the
        pluggable on-disk backend API.

    .. note::

        TODO(portante): Not sure what the right name to recommend here, as
        "manager" seemed generic enough, though suggestions are welcome.

    :param conf: caller provided configuration object
    :param logger: caller provided logger
    """
    def __init__(self, conf, logger):
        self.logger = logger
        self.devices = conf.get('devices', '/srv/node')
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.keep_cache_size = int(conf.get('keep_cache_size', 5242880))
        self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.reclaim_age = int(conf.get('reclaim_age', ONE_WEEK))
        self.replication_one_per_device = config_true_value(
            conf.get('replication_one_per_device', 'true'))
        self.replication_lock_timeout = int(conf.get(
            'replication_lock_timeout', 15))
        threads_per_disk = int(conf.get('threads_per_disk', '0'))
        self.threadpools = defaultdict(
            lambda: ThreadPool(nthreads=threads_per_disk))

        self.use_splice = False
        self.pipe_size = None

        splice_available = system_has_splice()

        conf_wants_splice = config_true_value(conf.get('splice', 'no'))
        # If the operator wants zero-copy with splice() but we don't have the
        # requisite kernel support, complain so they can go fix it.
        if conf_wants_splice and not splice_available:
            self.logger.warn(
                "Use of splice() requested (config says \"splice = %s\"), "
                "but the system does not support it. "
                "splice() will not be used." % conf.get('splice'))
        elif conf_wants_splice and splice_available:
            try:
                sockfd = get_md5_socket()
                os.close(sockfd)
            except IOError as err:
                # AF_ALG socket support was introduced in kernel 2.6.38; on
                # systems with older kernels (or custom-built kernels lacking
                # AF_ALG support), we can't use zero-copy.
                if err.errno != errno.EAFNOSUPPORT:
                    raise
                self.logger.warn("MD5 sockets not supported. "
                                 "splice() will not be used.")
            else:
                self.use_splice = True
                with open('/proc/sys/fs/pipe-max-size') as f:
                    max_pipe_size = int(f.read())
                self.pipe_size = min(max_pipe_size, self.disk_chunk_size)

    def construct_dev_path(self, device):
        """
        Construct the path to a device without checking if it is mounted.

        :param device: name of target device
        :returns: full path to the device
        """
        return os.path.join(self.devices, device)

    def get_dev_path(self, device, mount_check=None):
        """
        Return the path to a device, checking to see that it is a proper mount
        point based on a configuration parameter.

        :param device: name of target device
        :param mount_check: whether or not to check mountedness of device.
                            Defaults to bool(self.mount_check).
        :returns: full path to the device, None if the path to the device is
                  not a proper mount point.
        """
        should_check = self.mount_check if mount_check is None else mount_check
        if should_check and not check_mount(self.devices, device):
            dev_path = None
        else:
            dev_path = os.path.join(self.devices, device)
        return dev_path

    @contextmanager
    def replication_lock(self, device):
        """
        A context manager that will lock on the device given, if
        configured to do so.

        :raises ReplicationLockTimeout: If the lock on the device
            cannot be granted within the configured timeout.
        """
        if self.replication_one_per_device:
            dev_path = self.get_dev_path(device)
            with lock_path(
                    dev_path,
                    timeout=self.replication_lock_timeout,
                    timeout_class=ReplicationLockTimeout):
                yield True
        else:
            yield True

    def pickle_async_update(self, device, account, container, obj, data,
                            timestamp, policy_idx):
        device_path = self.construct_dev_path(device)
        async_dir = os.path.join(device_path, get_async_dir(policy_idx))
        ohash = hash_path(account, container, obj)
        self.threadpools[device].run_in_thread(
            write_pickle,
            data,
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                         Timestamp(timestamp).internal),
            os.path.join(device_path, get_tmp_dir(policy_idx)))
        self.logger.increment('async_pendings')

    def get_diskfile(self, device, partition, account, container, obj,
                     policy_idx=0, **kwargs):
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        return DiskFile(self, dev_path, self.threadpools[device],
                        partition, account, container, obj,
                        policy_idx=policy_idx,
                        use_splice=self.use_splice, pipe_size=self.pipe_size,
                        **kwargs)

    def object_audit_location_generator(self, device_dirs=None):
        return object_audit_location_generator(self.devices, self.mount_check,
                                               self.logger, device_dirs)

    def get_diskfile_from_audit_location(self, audit_location):
        dev_path = self.get_dev_path(audit_location.device, mount_check=False)
        return DiskFile.from_hash_dir(
            self, audit_location.path, dev_path,
            audit_location.partition)

    def get_diskfile_from_hash(self, device, partition, object_hash,
                               policy_idx, **kwargs):
        """
        Returns a DiskFile instance for an object at the given
        object_hash. Just in case someone thinks of refactoring, be
        sure DiskFileDeleted is *not* raised, but the DiskFile
        instance representing the tombstoned object is returned
        instead.

        :raises DiskFileNotExist: if the object does not exist
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        object_path = os.path.join(
            dev_path, get_data_dir(policy_idx), partition, object_hash[-3:],
            object_hash)
        try:
            filenames = hash_cleanup_listdir(object_path, self.reclaim_age)
        except OSError as err:
            if err.errno == errno.ENOTDIR:
                quar_path = quarantine_renamer(dev_path, object_path)
                logging.exception(
                    _('Quarantined %(object_path)s to %(quar_path)s because '
                      'it is not a directory'), {'object_path': object_path,
                                                 'quar_path': quar_path})
                raise DiskFileNotExist()
            if err.errno != errno.ENOENT:
                raise
            raise DiskFileNotExist()
        if not filenames:
            raise DiskFileNotExist()
        try:
            metadata = read_metadata(os.path.join(object_path, filenames[-1]))
        except EOFError:
            raise DiskFileNotExist()
        try:
            account, container, obj = split_path(
                metadata.get('name', ''), 3, 3, True)
        except ValueError:
            raise DiskFileNotExist()
        return DiskFile(self, dev_path, self.threadpools[device],
                        partition, account, container, obj,
                        policy_idx=policy_idx, **kwargs)

    def get_hashes(self, device, partition, suffix, policy_idx):
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        partition_path = os.path.join(dev_path, get_data_dir(policy_idx),
                                      partition)
        if not os.path.exists(partition_path):
            mkdirs(partition_path)
        suffixes = suffix.split('-') if suffix else []
        _junk, hashes = self.threadpools[device].force_run_in_thread(
            get_hashes, partition_path, recalculate=suffixes)
        return hashes

    def _listdir(self, path):
        try:
            return os.listdir(path)
        except OSError as err:
            if err.errno != errno.ENOENT:
                self.logger.error(
                    'ERROR: Skipping %r due to error with listdir attempt: %s',
                    path, err)
        return []

    def yield_suffixes(self, device, partition, policy_idx):
        """
        Yields tuples of (full_path, suffix_only) for suffixes stored
        on the given device and partition.
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        partition_path = os.path.join(dev_path, get_data_dir(policy_idx),
                                      partition)
        for suffix in self._listdir(partition_path):
            if len(suffix) != 3:
                continue
            try:
                int(suffix, 16)
            except ValueError:
                continue
            yield (os.path.join(partition_path, suffix), suffix)

    def yield_hashes(self, device, partition, policy_idx, suffixes=None):
        """
        Yields tuples of (full_path, hash_only, timestamp) for object
        information stored for the given device, partition, and
        (optionally) suffixes. If suffixes is None, all stored
        suffixes will be searched for object hashes. Note that if
        suffixes is not None but empty, such as [], then nothing will
        be yielded.
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        if suffixes is None:
            suffixes = self.yield_suffixes(device, partition, policy_idx)
        else:
            partition_path = os.path.join(dev_path, get_data_dir(policy_idx),
                                          partition)
            suffixes = (
                (os.path.join(partition_path, suffix), suffix)
                for suffix in suffixes)
        for suffix_path, suffix in suffixes:
            for object_hash in self._listdir(suffix_path):
                object_path = os.path.join(suffix_path, object_hash)
                for name in hash_cleanup_listdir(
                        object_path, self.reclaim_age):
                    ts, ext = name.rsplit('.', 1)
                    yield (object_path, object_hash, ts)
                    break


class DiskFileWriter(object):
    """
    Encapsulation of the write context for servicing PUT REST API
    requests. Serves as the context manager object for the
    :class:`swift.obj.diskfile.DiskFile` class's
    :func:`swift.obj.diskfile.DiskFile.create` method.

    .. note::

        It is the responsibility of the
        :func:`swift.obj.diskfile.DiskFile.create` method context manager to
        close the open file descriptor.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

    :param name: name of object from REST API
    :param datadir: on-disk directory object will end up in on
                    :func:`swift.obj.diskfile.DiskFileWriter.put`
    :param fd: open file descriptor of temporary file to receive data
    :param tmppath: full path name of the opened file descriptor
    :param bytes_per_sync: number bytes written between sync calls
    :param threadpool: internal thread pool to use for disk operations
    """
    def __init__(self, name, datadir, fd, tmppath, bytes_per_sync, threadpool):
        # Parameter tracking
        self._name = name
        self._datadir = datadir
        self._fd = fd
        self._tmppath = tmppath
        self._bytes_per_sync = bytes_per_sync
        self._threadpool = threadpool

        # Internal attributes
        self._upload_size = 0
        self._last_sync = 0
        self._extension = '.data'

    def write(self, chunk):
        """
        Write a chunk of data to disk. All invocations of this method must
        come before invoking the :func:

        For this implementation, the data is written into a temporary file.

        :param chunk: the chunk of data to write as a string object

        :returns: the total number of bytes written to an object
        """

        def _write_entire_chunk(chunk):
            while chunk:
                written = os.write(self._fd, chunk)
                self._upload_size += written
                chunk = chunk[written:]

        self._threadpool.run_in_thread(_write_entire_chunk, chunk)

        # For large files sync every 512MB (by default) written
        diff = self._upload_size - self._last_sync
        if diff >= self._bytes_per_sync:
            self._threadpool.force_run_in_thread(fdatasync, self._fd)
            drop_buffer_cache(self._fd, self._last_sync, diff)
            self._last_sync = self._upload_size

        return self._upload_size

    def _finalize_put(self, metadata, target_path):
        # Write the metadata before calling fsync() so that both data and
        # metadata are flushed to disk.
        write_metadata(self._fd, metadata)
        # We call fsync() before calling drop_cache() to lower the amount of
        # redundant work the drop cache code will perform on the pages (now
        # that after fsync the pages will be all clean).
        fsync(self._fd)
        # From the Department of the Redundancy Department, make sure we call
        # drop_cache() after fsync() to avoid redundant work (pages all
        # clean).
        drop_buffer_cache(self._fd, 0, self._upload_size)
        invalidate_hash(dirname(self._datadir))
        # After the rename completes, this object will be available for other
        # requests to reference.
        renamer(self._tmppath, target_path)
        try:
            hash_cleanup_listdir(self._datadir)
        except OSError:
            logging.exception(_('Problem cleaning up %s'), self._datadir)

    def put(self, metadata):
        """
        Finalize writing the file on disk.

        For this implementation, this method is responsible for renaming the
        temporary file to the final name and directory location.  This method
        should be called after the final call to
        :func:`swift.obj.diskfile.DiskFileWriter.write`.

        :param metadata: dictionary of metadata to be associated with the
                         object
        """
        timestamp = Timestamp(metadata['X-Timestamp']).internal
        metadata['name'] = self._name
        target_path = join(self._datadir, timestamp + self._extension)

        self._threadpool.force_run_in_thread(
            self._finalize_put, metadata, target_path)


class DiskFileReader(object):
    """
    Encapsulation of the WSGI read context for servicing GET REST API
    requests. Serves as the context manager object for the
    :class:`swift.obj.diskfile.DiskFile` class's
    :func:`swift.obj.diskfile.DiskFile.reader` method.

    .. note::

        The quarantining behavior of this method is considered implementation
        specific, and is not required of the API.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

    :param fp: open file object pointer reference
    :param data_file: on-disk data file name for the object
    :param obj_size: verified on-disk size of the object
    :param etag: expected metadata etag value for entire file
    :param threadpool: thread pool to use for read operations
    :param disk_chunk_size: size of reads from disk in bytes
    :param keep_cache_size: maximum object size that will be kept in cache
    :param device_path: on-disk device path, used when quarantining an obj
    :param logger: logger caller wants this object to use
    :param quarantine_hook: 1-arg callable called w/reason when quarantined
    :param use_splice: if true, use zero-copy splice() to send data
    :param pipe_size: size of pipe buffer used in zero-copy operations
    :param keep_cache: should resulting reads be kept in the buffer cache
    """
    def __init__(self, fp, data_file, obj_size, etag, threadpool,
                 disk_chunk_size, keep_cache_size, device_path, logger,
                 quarantine_hook, use_splice, pipe_size, keep_cache=False):
        # Parameter tracking
        self._fp = fp
        self._data_file = data_file
        self._obj_size = obj_size
        self._etag = etag
        self._threadpool = threadpool
        self._disk_chunk_size = disk_chunk_size
        self._device_path = device_path
        self._logger = logger
        self._quarantine_hook = quarantine_hook
        self._use_splice = use_splice
        self._pipe_size = pipe_size
        if keep_cache:
            # Caller suggests we keep this in cache, only do it if the
            # object's size is less than the maximum.
            self._keep_cache = obj_size < keep_cache_size
        else:
            self._keep_cache = False

        # Internal Attributes
        self._iter_etag = None
        self._bytes_read = 0
        self._started_at_0 = False
        self._read_to_eof = False
        self._md5_of_sent_bytes = None
        self._suppress_file_closing = False
        self._quarantined_dir = None

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            dropped_cache = 0
            self._bytes_read = 0
            self._started_at_0 = False
            self._read_to_eof = False
            if self._fp.tell() == 0:
                self._started_at_0 = True
                self._iter_etag = hashlib.md5()
            while True:
                chunk = self._threadpool.run_in_thread(
                    self._fp.read, self._disk_chunk_size)
                if chunk:
                    if self._iter_etag:
                        self._iter_etag.update(chunk)
                    self._bytes_read += len(chunk)
                    if self._bytes_read - dropped_cache > DROP_CACHE_WINDOW:
                        self._drop_cache(self._fp.fileno(), dropped_cache,
                                         self._bytes_read - dropped_cache)
                        dropped_cache = self._bytes_read
                    yield chunk
                else:
                    self._read_to_eof = True
                    self._drop_cache(self._fp.fileno(), dropped_cache,
                                     self._bytes_read - dropped_cache)
                    break
        finally:
            if not self._suppress_file_closing:
                self.close()

    def can_zero_copy_send(self):
        return self._use_splice

    def zero_copy_send(self, wsockfd):
        """
        Does some magic with splice() and tee() to move stuff from disk to
        network without ever touching userspace.

        :param wsockfd: file descriptor (integer) of the socket out which to
                        send data
        """
        # Note: if we ever add support for zero-copy ranged GET responses,
        # we'll have to make this conditional.
        self._started_at_0 = True

        rfd = self._fp.fileno()
        client_rpipe, client_wpipe = os.pipe()
        hash_rpipe, hash_wpipe = os.pipe()
        md5_sockfd = get_md5_socket()

        # The actual amount allocated to the pipe may be rounded up to the
        # nearest multiple of the page size. If we have the memory allocated,
        # we may as well use it.
        #
        # Note: this will raise IOError on failure, so we don't bother
        # checking the return value.
        pipe_size = fcntl.fcntl(client_rpipe, F_SETPIPE_SZ, self._pipe_size)
        fcntl.fcntl(hash_rpipe, F_SETPIPE_SZ, pipe_size)

        dropped_cache = 0
        self._bytes_read = 0
        try:
            while True:
                # Read data from disk to pipe
                bytes_in_pipe = self._threadpool.run_in_thread(
                    splice, rfd, 0, client_wpipe, 0, pipe_size, 0)
                if bytes_in_pipe == 0:
                    self._read_to_eof = True
                    self._drop_cache(rfd, dropped_cache,
                                     self._bytes_read - dropped_cache)
                    break
                self._bytes_read += bytes_in_pipe

                # "Copy" data from pipe A to pipe B (really just some pointer
                # manipulation in the kernel, not actual copying).
                bytes_copied = tee(client_rpipe, hash_wpipe, bytes_in_pipe, 0)
                if bytes_copied != bytes_in_pipe:
                    # We teed data between two pipes of equal size, and the
                    # destination pipe was empty. If, somehow, the destination
                    # pipe was full before all the data was teed, we should
                    # fail here. If we don't raise an exception, then we will
                    # have the incorrect MD5 hash once the object has been
                    # sent out, causing a false-positive quarantine.
                    raise Exception("tee() failed: tried to move %d bytes, "
                                    "but only moved %d" %
                                    (bytes_in_pipe, bytes_copied))
                # Take the data and feed it into an in-kernel MD5 socket. The
                # MD5 socket hashes data that is written to it. Reading from
                # it yields the MD5 checksum of the written data.
                #
                # Note that we don't have to worry about splice() returning
                # None here (which happens on EWOULDBLOCK); we're splicing
                # $bytes_in_pipe bytes from a pipe with exactly that many
                # bytes in it, so read won't block, and we're splicing it into
                # an MD5 socket, which synchronously hashes any data sent to
                # it, so writing won't block either.
                hashed = splice(hash_rpipe, 0, md5_sockfd, 0,
                                bytes_in_pipe, SPLICE_F_MORE)
                if hashed != bytes_in_pipe:
                    raise Exception("md5 socket didn't take all the data? "
                                    "(tried to write %d, but wrote %d)" %
                                    (bytes_in_pipe, hashed))

                while bytes_in_pipe > 0:
                    sent = splice(client_rpipe, 0, wsockfd, 0,
                                  bytes_in_pipe, 0)
                    if sent is None:  # would have blocked
                        trampoline(wsockfd, write=True)
                    else:
                        bytes_in_pipe -= sent

                if self._bytes_read - dropped_cache > DROP_CACHE_WINDOW:
                    self._drop_cache(rfd, dropped_cache,
                                     self._bytes_read - dropped_cache)
                    dropped_cache = self._bytes_read
        finally:
            # Linux MD5 sockets return '00000000000000000000000000000000' for
            # the checksum if you didn't write any bytes to them, instead of
            # returning the correct value.
            if self._bytes_read > 0:
                bin_checksum = os.read(md5_sockfd, 16)
                hex_checksum = ''.join("%02x" % ord(c) for c in bin_checksum)
            else:
                hex_checksum = MD5_OF_EMPTY_STRING
            self._md5_of_sent_bytes = hex_checksum

            os.close(client_rpipe)
            os.close(client_wpipe)
            os.close(hash_rpipe)
            os.close(hash_wpipe)
            os.close(md5_sockfd)
            self.close()

    def app_iter_range(self, start, stop):
        """Returns an iterator over the data file for range (start, stop)"""
        if start or start == 0:
            self._fp.seek(start)
        if stop is not None:
            length = stop - start
        else:
            length = None
        try:
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length < 0:
                        # Chop off the extra:
                        yield chunk[:length]
                        break
                yield chunk
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_ranges(self, ranges, content_type, boundary, size):
        """Returns an iterator over the data file for a set of ranges"""
        if not ranges:
            yield ''
        else:
            try:
                self._suppress_file_closing = True
                for chunk in multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self._suppress_file_closing = False
                self.close()

    def _drop_cache(self, fd, offset, length):
        """Method for no-oping buffer cache drop method."""
        if not self._keep_cache:
            drop_buffer_cache(fd, offset, length)

    def _quarantine(self, msg):
        self._quarantined_dir = self._threadpool.run_in_thread(
            quarantine_renamer, self._device_path, self._data_file)
        self._logger.warn("Quarantined object %s: %s" % (
            self._data_file, msg))
        self._logger.increment('quarantines')
        self._quarantine_hook(msg)

    def _handle_close_quarantine(self):
        """Check if file needs to be quarantined"""
        if self._iter_etag and not self._md5_of_sent_bytes:
            self._md5_of_sent_bytes = self._iter_etag.hexdigest()

        if self._bytes_read != self._obj_size:
            self._quarantine(
                "Bytes read: %s, does not match metadata: %s" % (
                    self._bytes_read, self._obj_size))
        elif self._md5_of_sent_bytes and \
                self._etag != self._md5_of_sent_bytes:
            self._quarantine(
                "ETag %s and file's md5 %s do not match" % (
                    self._etag, self._md5_of_sent_bytes))

    def close(self):
        """
        Close the open file handle if present.

        For this specific implementation, this method will handle quarantining
        the file if necessary.
        """
        if self._fp:
            try:
                if self._started_at_0 and self._read_to_eof:
                    self._handle_close_quarantine()
            except DiskFileQuarantined:
                raise
            except (Exception, Timeout) as e:
                self._logger.error(_(
                    'ERROR DiskFile %(data_file)s'
                    ' close failure: %(exc)s : %(stack)s'),
                    {'exc': e, 'stack': ''.join(traceback.format_stack()),
                     'data_file': self._data_file})
            finally:
                fp, self._fp = self._fp, None
                fp.close()


class DiskFile(object):
    """
    Manage object files.

    This specific implementation manages object files on a disk formatted with
    a POSIX-compliant file system that supports extended attributes as
    metadata on a file or directory.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

    :param mgr: associated DiskFileManager instance
    :param device_path: path to the target device or drive
    :param threadpool: thread pool to use for blocking operations
    :param partition: partition on the device in which the object lives
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param _datadir: override the full datadir otherwise constructed here
    :param policy_idx: used to get the data dir when constructing it here
    :param use_splice: if true, use zero-copy splice() to send data
    :param pipe_size: size of pipe buffer used in zero-copy operations
    """

    def __init__(self, mgr, device_path, threadpool, partition,
                 account=None, container=None, obj=None, _datadir=None,
                 policy_idx=0, use_splice=False, pipe_size=None):
        self._mgr = mgr
        self._device_path = device_path
        self._threadpool = threadpool or ThreadPool(nthreads=0)
        self._logger = mgr.logger
        self._disk_chunk_size = mgr.disk_chunk_size
        self._bytes_per_sync = mgr.bytes_per_sync
        self._use_splice = use_splice
        self._pipe_size = pipe_size
        if account and container and obj:
            self._name = '/' + '/'.join((account, container, obj))
            self._account = account
            self._container = container
            self._obj = obj
            name_hash = hash_path(account, container, obj)
            self._datadir = join(
                device_path, storage_directory(get_data_dir(policy_idx),
                                               partition, name_hash))
        else:
            # gets populated when we read the metadata
            self._name = None
            self._account = None
            self._container = None
            self._obj = None
            self._datadir = None
        self._tmpdir = join(device_path, get_tmp_dir(policy_idx))
        self._metadata = None
        self._data_file = None
        self._fp = None
        self._quarantined_dir = None
        self._content_length = None
        if _datadir:
            self._datadir = _datadir
        else:
            name_hash = hash_path(account, container, obj)
            self._datadir = join(
                device_path, storage_directory(get_data_dir(policy_idx),
                                               partition, name_hash))

    @property
    def account(self):
        return self._account

    @property
    def container(self):
        return self._container

    @property
    def obj(self):
        return self._obj

    @property
    def content_length(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self._content_length

    @property
    def timestamp(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        return Timestamp(self._metadata.get('X-Timestamp'))

    @classmethod
    def from_hash_dir(cls, mgr, hash_dir_path, device_path, partition):
        return cls(mgr, device_path, None, partition, _datadir=hash_dir_path)

    def open(self):
        """
        Open the object.

        This implementation opens the data file representing the object, reads
        the associated metadata in the extended attributes, additionally
        combining metadata from fast-POST `.meta` files.

        .. note::

            An implementation is allowed to raise any of the following
            exceptions, but is only required to raise `DiskFileNotExist` when
            the object representation does not exist.

        :raises DiskFileCollision: on name mis-match with metadata
        :raises DiskFileNotExist: if the object does not exist
        :raises DiskFileDeleted: if the object was previously deleted
        :raises DiskFileQuarantined: if while reading metadata of the file
                                     some data did pass cross checks
        :returns: itself for use as a context manager
        """
        data_file, meta_file, ts_file = self._get_ondisk_file()
        if not data_file:
            raise self._construct_exception_from_ts_file(ts_file)
        self._fp = self._construct_from_data_file(
            data_file, meta_file)
        # This method must populate the internal _metadata attribute.
        self._metadata = self._metadata or {}
        self._data_file = data_file
        return self

    def __enter__(self):
        """
        Context enter.

        .. note::

            An implemenation shall raise `DiskFileNotOpen` when has not
            previously invoked the :func:`swift.obj.diskfile.DiskFile.open`
            method.
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self

    def __exit__(self, t, v, tb):
        """
        Context exit.

        .. note::

            This method will be invoked by the object server while servicing
            the REST API *before* the object has actually been read. It is the
            responsibility of the implementation to properly handle that.
        """
        if self._fp is not None:
            fp, self._fp = self._fp, None
            fp.close()

    def _quarantine(self, data_file, msg):
        """
        Quarantine a file; responsible for incrementing the associated logger's
        count of quarantines.

        :param data_file: full path of data file to quarantine
        :param msg: reason for quarantining to be included in the exception
        :returns: DiskFileQuarantined exception object
        """
        self._quarantined_dir = self._threadpool.run_in_thread(
            quarantine_renamer, self._device_path, data_file)
        self._logger.warn("Quarantined object %s: %s" % (
            data_file, msg))
        self._logger.increment('quarantines')
        return DiskFileQuarantined(msg)

    def _get_ondisk_file(self):
        """
        Do the work to figure out if the data directory exists, and if so,
        determine the on-disk files to use.

        :returns: a tuple of data, meta and ts (tombstone) files, in one of
                  three states:

        * all three are None

          data directory does not exist, or there are no files in
          that directory

        * ts_file is not None, data_file is None, meta_file is None

          object is considered deleted

        * data_file is not None, ts_file is None

          object exists, and optionally has fast-POST metadata
        """
        try:
            files = os.listdir(self._datadir)
        except OSError as err:
            if err.errno == errno.ENOTDIR:
                # If there's a file here instead of a directory, quarantine
                # it; something's gone wrong somewhere.
                raise self._quarantine(
                    # hack: quarantine_renamer actually renames the directory
                    # enclosing the filename you give it, but here we just
                    # want this one file and not its parent.
                    os.path.join(self._datadir, "made-up-filename"),
                    "Expected directory, found file at %s" % self._datadir)
            elif err.errno != errno.ENOENT:
                raise DiskFileError(
                    "Error listing directory %s: %s" % (self._datadir, err))
            # The data directory does not exist, so the object cannot exist.
            fileset = (None, None, None)
        else:
            fileset = get_ondisk_files(files, self._datadir)
        return fileset

    def _construct_exception_from_ts_file(self, ts_file):
        """
        If a tombstone is present it means the object is considered
        deleted. We just need to pull the metadata from the tombstone file
        which has the timestamp to construct the deleted exception. If there
        was no tombstone, just report it does not exist.

        :param ts_file: the tombstone file name found on disk
        :returns: DiskFileDeleted if the ts_file was provided, else
                  DiskFileNotExist
        """
        if not ts_file:
            exc = DiskFileNotExist()
        else:
            try:
                metadata = self._failsafe_read_metadata(ts_file, ts_file)
            except DiskFileQuarantined:
                # If the tombstone's corrupted, quarantine it and pretend it
                # wasn't there
                exc = DiskFileNotExist()
            else:
                # All well and good that we have found a tombstone file, but
                # we don't have a data file so we are just going to raise an
                # exception that we could not find the object, providing the
                # tombstone's timestamp.
                exc = DiskFileDeleted(metadata=metadata)
        return exc

    def _verify_name_matches_hash(self, data_file):
        hash_from_fs = os.path.basename(self._datadir)
        hash_from_name = hash_path(self._name.lstrip('/'))
        if hash_from_fs != hash_from_name:
            raise self._quarantine(
                data_file,
                "Hash of name in metadata does not match directory name")

    def _verify_data_file(self, data_file, fp):
        """
        Verify the metadata's name value matches what we think the object is
        named.

        :param data_file: data file name being consider, used when quarantines
                          occur
        :param fp: open file pointer so that we can `fstat()` the file to
                   verify the on-disk size with Content-Length metadata value
        :raises DiskFileCollision: if the metadata stored name does not match
                                   the referenced name of the file
        :raises DiskFileExpired: if the object has expired
        :raises DiskFileQuarantined: if data inconsistencies were detected
                                     between the metadata and the file-system
                                     metadata
        """
        try:
            mname = self._metadata['name']
        except KeyError:
            raise self._quarantine(data_file, "missing name metadata")
        else:
            if mname != self._name:
                self._logger.error(
                    _('Client path %(client)s does not match '
                      'path stored in object metadata %(meta)s'),
                    {'client': self._name, 'meta': mname})
                raise DiskFileCollision('Client path does not match path '
                                        'stored in object metadata')
        try:
            x_delete_at = int(self._metadata['X-Delete-At'])
        except KeyError:
            pass
        except ValueError:
            # Quarantine, the x-delete-at key is present but not an
            # integer.
            raise self._quarantine(
                data_file, "bad metadata x-delete-at value %s" % (
                    self._metadata['X-Delete-At']))
        else:
            if x_delete_at <= time.time():
                raise DiskFileExpired(metadata=self._metadata)
        try:
            metadata_size = int(self._metadata['Content-Length'])
        except KeyError:
            raise self._quarantine(
                data_file, "missing content-length in metadata")
        except ValueError:
            # Quarantine, the content-length key is present but not an
            # integer.
            raise self._quarantine(
                data_file, "bad metadata content-length value %s" % (
                    self._metadata['Content-Length']))
        fd = fp.fileno()
        try:
            statbuf = os.fstat(fd)
        except OSError as err:
            # Quarantine, we can't successfully stat the file.
            raise self._quarantine(data_file, "not stat-able: %s" % err)
        else:
            obj_size = statbuf.st_size
        if obj_size != metadata_size:
            raise self._quarantine(
                data_file, "metadata content-length %s does"
                " not match actual object size %s" % (
                    metadata_size, statbuf.st_size))
        self._content_length = obj_size
        return obj_size

    def _failsafe_read_metadata(self, source, quarantine_filename=None):
        # Takes source and filename separately so we can read from an open
        # file if we have one
        try:
            return read_metadata(source)
        except Exception as err:
            raise self._quarantine(
                quarantine_filename,
                "Exception reading metadata: %s" % err)

    def _construct_from_data_file(self, data_file, meta_file):
        """
        Open the `.data` file to fetch its metadata, and fetch the metadata
        from the fast-POST `.meta` file as well if it exists, merging them
        properly.

        :param data_file: on-disk `.data` file being considered
        :param meta_file: on-disk fast-POST `.meta` file being considered
        :returns: an opened data file pointer
        :raises DiskFileError: various exceptions from
                    :func:`swift.obj.diskfile.DiskFile._verify_data_file`
        """
        fp = open(data_file, 'rb')
        datafile_metadata = self._failsafe_read_metadata(fp, data_file)
        if meta_file:
            self._metadata = self._failsafe_read_metadata(meta_file, meta_file)
            sys_metadata = dict(
                [(key, val) for key, val in datafile_metadata.iteritems()
                 if key.lower() in DATAFILE_SYSTEM_META
                 or is_sys_meta('object', key)])
            self._metadata.update(sys_metadata)
        else:
            self._metadata = datafile_metadata
        if self._name is None:
            # If we don't know our name, we were just given a hash dir at
            # instantiation, so we'd better validate that the name hashes back
            # to us
            self._name = self._metadata['name']
            self._verify_name_matches_hash(data_file)
        self._verify_data_file(data_file, fp)
        return fp

    def get_metadata(self):
        """
        Provide the metadata for a previously opened object as a dictionary.

        :returns: object's metadata dictionary
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self._metadata

    def read_metadata(self):
        """
        Return the metadata for an object without requiring the caller to open
        the object first.

        :returns: metadata dictionary for an object
        :raises DiskFileError: this implementation will raise the same
                            errors as the `open()` method.
        """
        with self.open():
            return self.get_metadata()

    def reader(self, keep_cache=False,
               _quarantine_hook=lambda m: None):
        """
        Return a :class:`swift.common.swob.Response` class compatible
        "`app_iter`" object as defined by
        :class:`swift.obj.diskfile.DiskFileReader`.

        For this implementation, the responsibility of closing the open file
        is passed to the :class:`swift.obj.diskfile.DiskFileReader` object.

        :param keep_cache: caller's preference for keeping data read in the
                           OS buffer cache
        :param _quarantine_hook: 1-arg callable called when obj quarantined;
                                 the arg is the reason for quarantine.
                                 Default is to ignore it.
                                 Not needed by the REST layer.
        :returns: a :class:`swift.obj.diskfile.DiskFileReader` object
        """
        dr = DiskFileReader(
            self._fp, self._data_file, int(self._metadata['Content-Length']),
            self._metadata['ETag'], self._threadpool, self._disk_chunk_size,
            self._mgr.keep_cache_size, self._device_path, self._logger,
            use_splice=self._use_splice, quarantine_hook=_quarantine_hook,
            pipe_size=self._pipe_size, keep_cache=keep_cache)
        # At this point the reader object is now responsible for closing
        # the file pointer.
        self._fp = None
        return dr

    @contextmanager
    def create(self, size=None):
        """
        Context manager to create a file. We create a temporary file first, and
        then return a DiskFileWriter object to encapsulate the state.

        .. note::

            An implementation is not required to perform on-disk
            preallocations even if the parameter is specified. But if it does
            and it fails, it must raise a `DiskFileNoSpace` exception.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        """
        if not exists(self._tmpdir):
            mkdirs(self._tmpdir)
        fd, tmppath = mkstemp(dir=self._tmpdir)
        try:
            if size is not None and size > 0:
                try:
                    fallocate(fd, size)
                except OSError:
                    raise DiskFileNoSpace()
            yield DiskFileWriter(self._name, self._datadir, fd, tmppath,
                                 self._bytes_per_sync, self._threadpool)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(tmppath)
            except OSError:
                pass

    def write_metadata(self, metadata):
        """
        Write a block of metadata to an object without requiring the caller to
        create the object first. Supports fast-POST behavior semantics.

        :param metadata: dictionary of metadata to be associated with the
                         object
        :raises DiskFileError: this implementation will raise the same
                            errors as the `create()` method.
        """
        with self.create() as writer:
            writer._extension = '.meta'
            writer.put(metadata)

    def delete(self, timestamp):
        """
        Delete the object.

        This implementation creates a tombstone file using the given
        timestamp, and removes any older versions of the object file. Any
        file that has an older timestamp than timestamp will be deleted.

        .. note::

            An implementation is free to use or ignore the timestamp
            parameter.

        :param timestamp: timestamp to compare with each file
        :raises DiskFileError: this implementation will raise the same
                            errors as the `create()` method.
        """
        timestamp = Timestamp(timestamp).internal

        with self.create() as deleter:
            deleter._extension = '.ts'
            deleter.put({'X-Timestamp': timestamp})
