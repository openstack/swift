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

import six.moves.cPickle as pickle
import errno
import fcntl
import json
import os
import time
import uuid
import hashlib
import logging
import traceback
import xattr
from os.path import basename, dirname, exists, getmtime, join, splitext
from random import shuffle
from tempfile import mkstemp
from contextlib import contextmanager
from collections import defaultdict

from eventlet import Timeout
from eventlet.hubs import trampoline

from swift import gettext_ as _
from swift.common.constraints import check_mount, check_dir
from swift.common.request_helpers import is_sys_meta
from swift.common.utils import mkdirs, Timestamp, \
    storage_directory, hash_path, renamer, fallocate, fsync, fdatasync, \
    fsync_dir, drop_buffer_cache, lock_path, write_pickle, \
    config_true_value, listdir, split_path, ismount, remove_file, \
    get_md5_socket, F_SETPIPE_SZ, decode_timestamps, encode_timestamps, \
    tpool_reraise
from swift.common.splice import splice, tee
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist, \
    DiskFileCollision, DiskFileNoSpace, DiskFileDeviceUnavailable, \
    DiskFileDeleted, DiskFileError, DiskFileNotOpen, PathNotDir, \
    ReplicationLockTimeout, DiskFileExpired, DiskFileXattrNotSupported
from swift.common.swob import multi_range_iterator
from swift.common.storage_policy import (
    get_policy_string, split_policy_string, PolicyError, POLICIES,
    REPL_POLICY, EC_POLICY)
from functools import partial


PICKLE_PROTOCOL = 2
ONE_WEEK = 604800
HASH_FILE = 'hashes.pkl'
HASH_INVALIDATIONS_FILE = 'hashes.invalid'
METADATA_KEY = 'user.swift.metadata'
DROP_CACHE_WINDOW = 1024 * 1024
# These are system-set metadata keys that cannot be changed with a POST.
# They should be lowercase.
DATAFILE_SYSTEM_META = set('content-length deleted etag'.split())
DATADIR_BASE = 'objects'
ASYNCDIR_BASE = 'async_pending'
TMP_BASE = 'tmp'
get_data_dir = partial(get_policy_string, DATADIR_BASE)
get_async_dir = partial(get_policy_string, ASYNCDIR_BASE)
get_tmp_dir = partial(get_policy_string, TMP_BASE)
MD5_OF_EMPTY_STRING = 'd41d8cd98f00b204e9800998ecf8427e'


def _get_filename(fd):
    """
    Helper function to get to file name from a file descriptor or filename.

    :param fd: file descriptor or filename.

    :returns: the filename.
    """
    if hasattr(fd, 'name'):
        # fd object
        return fd.name

    # fd is a filename
    return fd


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
            metadata += xattr.getxattr(fd, '%s%s' % (METADATA_KEY,
                                                     (key or '')))
            key += 1
    except (IOError, OSError) as e:
        for err in 'ENOTSUP', 'EOPNOTSUPP':
            if hasattr(errno, err) and e.errno == getattr(errno, err):
                msg = "Filesystem at %s does not support xattr" % \
                      _get_filename(fd)
                logging.exception(msg)
                raise DiskFileXattrNotSupported(e)
        if e.errno == errno.ENOENT:
            raise DiskFileNotExist()
        # TODO: we might want to re-raise errors that don't denote a missing
        # xattr here.  Seems to be ENODATA on linux and ENOATTR on BSD/OSX.
    return pickle.loads(metadata)


def write_metadata(fd, metadata, xattr_size=65536):
    """
    Helper function to write pickled metadata for an object file.

    :param fd: file descriptor or filename to write the metadata
    :param metadata: metadata to write
    """
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        try:
            xattr.setxattr(fd, '%s%s' % (METADATA_KEY, key or ''),
                           metastr[:xattr_size])
            metastr = metastr[xattr_size:]
            key += 1
        except IOError as e:
            for err in 'ENOTSUP', 'EOPNOTSUPP':
                if hasattr(errno, err) and e.errno == getattr(errno, err):
                    msg = "Filesystem at %s does not support xattr" % \
                          _get_filename(fd)
                    logging.exception(msg)
                    raise DiskFileXattrNotSupported(e)
            if e.errno in (errno.ENOSPC, errno.EDQUOT):
                msg = "No space left on device for %s" % _get_filename(fd)
                logging.exception(msg)
                raise DiskFileNoSpace()
            raise


def extract_policy(obj_path):
    """
    Extracts the policy for an object (based on the name of the objects
    directory) given the device-relative path to the object. Returns None in
    the event that the path is malformed in some way.

    The device-relative path is everything after the mount point; for example:

    /srv/node/d42/objects-5/179/
        485dc017205a81df3af616d917c90179/1401811134.873649.data

    would have device-relative path:

    objects-5/179/485dc017205a81df3af616d917c90179/1401811134.873649.data

    :param obj_path: device-relative path of an object, or the full path
    :returns: a :class:`~swift.common.storage_policy.BaseStoragePolicy` or None
    """
    try:
        obj_portion = obj_path[obj_path.rindex(DATADIR_BASE):]
        obj_dirname = obj_portion[:obj_portion.index('/')]
    except Exception:
        return None
    try:
        base, policy = split_policy_string(obj_dirname)
    except PolicyError:
        return None
    return policy


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
    policy = extract_policy(corrupted_file_path)
    if policy is None:
        # TODO: support a quarantine-unknown location
        policy = POLICIES.legacy
    from_dir = dirname(corrupted_file_path)
    to_dir = join(device_path, 'quarantined',
                  get_data_dir(policy),
                  basename(from_dir))
    invalidate_hash(dirname(from_dir))
    try:
        renamer(from_dir, to_dir, fsync=False)
    except OSError as e:
        if e.errno not in (errno.EEXIST, errno.ENOTEMPTY):
            raise
        to_dir = "%s-%s" % (to_dir, uuid.uuid4().hex)
        renamer(from_dir, to_dir, fsync=False)
    return to_dir


def consolidate_hashes(partition_dir):
    """
    Take what's in hashes.pkl and hashes.invalid, combine them, write the
    result back to hashes.pkl, and clear out hashes.invalid.

    :param suffix_dir: absolute path to partition dir containing hashes.pkl
                       and hashes.invalid

    :returns: the hashes, or None if there's no hashes.pkl.
    """
    hashes_file = join(partition_dir, HASH_FILE)
    invalidations_file = join(partition_dir, HASH_INVALIDATIONS_FILE)

    if not os.path.exists(hashes_file):
        if os.path.exists(invalidations_file):
            # no hashes at all -> everything's invalid, so empty the file with
            # the invalid suffixes in it, if it exists
            try:
                with open(invalidations_file, 'wb'):
                    pass
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
        return None

    with lock_path(partition_dir):
        try:
            with open(hashes_file, 'rb') as hashes_fp:
                pickled_hashes = hashes_fp.read()
        except (IOError, OSError):
            hashes = {}
        else:
            try:
                hashes = pickle.loads(pickled_hashes)
            except Exception:
                # pickle.loads() can raise a wide variety of exceptions when
                # given invalid input depending on the way in which the
                # input is invalid.
                hashes = None

        modified = False
        try:
            with open(invalidations_file, 'rb') as inv_fh:
                for line in inv_fh:
                    suffix = line.strip()
                    if hashes is not None and hashes.get(suffix) is not None:
                        hashes[suffix] = None
                        modified = True
        except (IOError, OSError) as e:
            if e.errno != errno.ENOENT:
                raise

        if modified:
            write_pickle(hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)

        # Now that all the invalidations are reflected in hashes.pkl, it's
        # safe to clear out the invalidations file.
        try:
            with open(invalidations_file, 'w') as inv_fh:
                pass
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

        return hashes


def invalidate_hash(suffix_dir):
    """
    Invalidates the hash for a suffix_dir in the partition's hashes file.

    :param suffix_dir: absolute path to suffix dir whose hash needs
                       invalidating
    """

    suffix = basename(suffix_dir)
    partition_dir = dirname(suffix_dir)
    hashes_file = join(partition_dir, HASH_FILE)
    if not os.path.exists(hashes_file):
        return

    invalidations_file = join(partition_dir, HASH_INVALIDATIONS_FILE)
    with lock_path(partition_dir):
        with open(invalidations_file, 'ab') as inv_fh:
            inv_fh.write(suffix + "\n")


class AuditLocation(object):
    """
    Represents an object location to be audited.

    Other than being a bucket of data, the only useful thing this does is
    stringify to a filesystem path so the auditor's logs look okay.
    """

    def __init__(self, path, device, partition, policy):
        self.path, self.device, self.partition, self.policy = (
            path, device, partition, policy)

    def __str__(self):
        return str(self.path)


def object_audit_location_generator(devices, mount_check=True, logger=None,
                                    device_dirs=None, auditor_type="ALL"):
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
    :param device_dirs: a list of directories under devices to traverse
    :param auditor_type: either ALL or ZBF
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
        device_dir = os.path.join(devices, device)
        try:
            dirs = os.listdir(device_dir)
        except OSError as e:
            if logger:
                logger.debug(
                    _('Skipping %(dir)s: %(err)s') % {'dir': device_dir,
                                                      'err': e.strerror})
            continue
        for dir_ in dirs:
            if not dir_.startswith(DATADIR_BASE):
                continue
            try:
                base, policy = split_policy_string(dir_)
            except PolicyError as e:
                if logger:
                    logger.warning(_('Directory %(directory)r does not map '
                                     'to a valid policy (%(error)s)') % {
                                   'directory': dir_, 'error': e})
                continue
            datadir_path = os.path.join(devices, device, dir_)

            partitions = get_auditor_status(datadir_path, logger, auditor_type)

            for pos, partition in enumerate(partitions):
                update_auditor_status(datadir_path, logger,
                                      partitions[pos:], auditor_type)
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
                        yield AuditLocation(hsh_path, device, partition,
                                            policy)

            update_auditor_status(datadir_path, logger, [], auditor_type)


def get_auditor_status(datadir_path, logger, auditor_type):
    auditor_status = os.path.join(
        datadir_path, "auditor_status_%s.json" % auditor_type)
    status = {}
    try:
        with open(auditor_status) as statusfile:
            status = statusfile.read()
    except (OSError, IOError) as e:
        if e.errno != errno.ENOENT and logger:
            logger.warning(_('Cannot read %(auditor_status)s (%(err)s)') %
                           {'auditor_status': auditor_status, 'err': e})
        return listdir(datadir_path)
    try:
        status = json.loads(status)
    except ValueError as e:
        logger.warning(_('Loading JSON from %(auditor_status)s failed'
                         ' (%(err)s)') %
                       {'auditor_status': auditor_status, 'err': e})
        return listdir(datadir_path)
    return status['partitions']


def update_auditor_status(datadir_path, logger, partitions, auditor_type):
    status = json.dumps({'partitions': partitions})
    auditor_status = os.path.join(
        datadir_path, "auditor_status_%s.json" % auditor_type)
    try:
        with open(auditor_status, "wb") as statusfile:
            statusfile.write(status)
    except (OSError, IOError) as e:
        if logger:
            logger.warning(_('Cannot write %(auditor_status)s (%(err)s)') %
                           {'auditor_status': auditor_status, 'err': e})


def clear_auditor_status(devices, auditor_type="ALL"):
    for device in os.listdir(devices):
        for dir_ in os.listdir(os.path.join(devices, device)):
            if not dir_.startswith("objects"):
                continue
            datadir_path = os.path.join(devices, device, dir_)
            auditor_status = os.path.join(
                datadir_path, "auditor_status_%s.json" % auditor_type)
            remove_file(auditor_status)


def strip_self(f):
    """
    Wrapper to attach module level functions to base class.
    """
    def wrapper(self, *args, **kwargs):
        return f(*args, **kwargs)
    return wrapper


class DiskFileRouter(object):

    policy_type_to_manager_cls = {}

    @classmethod
    def register(cls, policy_type):
        """
        Decorator for Storage Policy implementations to register
        their DiskFile implementation.
        """
        def register_wrapper(diskfile_cls):
            if policy_type in cls.policy_type_to_manager_cls:
                raise PolicyError(
                    '%r is already registered for the policy_type %r' % (
                        cls.policy_type_to_manager_cls[policy_type],
                        policy_type))
            cls.policy_type_to_manager_cls[policy_type] = diskfile_cls
            return diskfile_cls
        return register_wrapper

    def __init__(self, *args, **kwargs):
        self.policy_to_manager = {}
        for policy in POLICIES:
            manager_cls = self.policy_type_to_manager_cls[policy.policy_type]
            self.policy_to_manager[policy] = manager_cls(*args, **kwargs)

    def __getitem__(self, policy):
        return self.policy_to_manager[policy]


class BaseDiskFileManager(object):
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

    diskfile_cls = None  # must be set by subclasses

    invalidate_hash = strip_self(invalidate_hash)
    consolidate_hashes = strip_self(consolidate_hashes)
    quarantine_renamer = strip_self(quarantine_renamer)

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

        self.use_splice = False
        self.pipe_size = None

        conf_wants_splice = config_true_value(conf.get('splice', 'no'))
        # If the operator wants zero-copy with splice() but we don't have the
        # requisite kernel support, complain so they can go fix it.
        if conf_wants_splice and not splice.available:
            self.logger.warning(
                "Use of splice() requested (config says \"splice = %s\"), "
                "but the system does not support it. "
                "splice() will not be used." % conf.get('splice'))
        elif conf_wants_splice and splice.available:
            try:
                sockfd = get_md5_socket()
                os.close(sockfd)
            except IOError as err:
                # AF_ALG socket support was introduced in kernel 2.6.38; on
                # systems with older kernels (or custom-built kernels lacking
                # AF_ALG support), we can't use zero-copy.
                if err.errno != errno.EAFNOSUPPORT:
                    raise
                self.logger.warning("MD5 sockets not supported. "
                                    "splice() will not be used.")
            else:
                self.use_splice = True
                with open('/proc/sys/fs/pipe-max-size') as f:
                    max_pipe_size = int(f.read())
                self.pipe_size = min(max_pipe_size, self.disk_chunk_size)

    def make_on_disk_filename(self, timestamp, ext=None,
                              ctype_timestamp=None, *a, **kw):
        """
        Returns filename for given timestamp.

        :param timestamp: the object timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        :param ext: an optional string representing a file extension to be
                    appended to the returned file name
        :param ctype_timestamp: an optional content-type timestamp, an instance
                                of :class:`~swift.common.utils.Timestamp`
        :returns: a file name
        """
        rv = timestamp.internal
        if ext == '.meta' and ctype_timestamp:
            # If ctype_timestamp is None then the filename is simply the
            # internal form of the timestamp. If ctype_timestamp is not None
            # then the difference between the raw values of the two timestamps
            # is appended as a hex number, with its sign.
            #
            # There are two reasons for encoding the content-type timestamp
            # in the filename in this way. First, it means that two .meta files
            # having the same timestamp but different content-type timestamps
            # (and potentially different content-type values) will be distinct
            # and therefore will be independently replicated when rsync
            # replication is used. That ensures that all nodes end up having
            # all content-type values after replication (with the most recent
            # value being selected when the diskfile is opened). Second, having
            # the content-type encoded in timestamp in the filename makes it
            # possible for the  on disk file search code to determine that
            # timestamp by inspecting only the filename, and not needing to
            # open the file and read its xattrs.
            rv = encode_timestamps(timestamp, ctype_timestamp, explicit=True)
        if ext:
            rv = '%s%s' % (rv, ext)
        return rv

    def parse_on_disk_filename(self, filename):
        """
        Parse an on disk file name.

        :param filename: the file name including extension
        :returns: a dict, with keys for timestamp, ext and ctype_timestamp:

            * timestamp is a :class:`~swift.common.utils.Timestamp`
            * ctype_timestamp is a :class:`~swift.common.utils.Timestamp` or
              None for .meta files, otherwise None
            * ext is a string, the file extension including the leading dot or
              the empty string if the filename has no extension.

            Subclasses may override this method to add further keys to the
            returned dict.

        :raises DiskFileError: if any part of the filename is not able to be
                               validated.
        """
        ts_ctype = None
        fname, ext = splitext(filename)
        try:
            if ext == '.meta':
                timestamp, ts_ctype = decode_timestamps(
                    fname, explicit=True)[:2]
            else:
                timestamp = Timestamp(fname)
        except ValueError:
            raise DiskFileError('Invalid Timestamp value in filename %r'
                                % filename)
        return {
            'timestamp': timestamp,
            'ext': ext,
            'ctype_timestamp': ts_ctype
        }

    def _process_ondisk_files(self, exts, results, **kwargs):
        """
        Called by get_ondisk_files(). Should be over-ridden to implement
        subclass specific handling of files.

        :param exts: dict of lists of file info, keyed by extension
        :param results: a dict that may be updated with results
        """
        raise NotImplementedError

    def _verify_ondisk_files(self, results, **kwargs):
        """
        Verify that the final combination of on disk files complies with the
        diskfile contract.

        :param results: files that have been found and accepted
        :returns: True if the file combination is compliant, False otherwise
        """
        data_file, meta_file, ts_file = tuple(
            [results[key]
             for key in ('data_file', 'meta_file', 'ts_file')])

        return ((data_file is None and meta_file is None and ts_file is None)
                or (ts_file is not None and data_file is None
                    and meta_file is None)
                or (data_file is not None and ts_file is None))

    def _split_list(self, original_list, condition):
        """
        Split a list into two lists. The first list contains the first N items
        of the original list, in their original order,  where 0 < N <=
        len(original list). The second list contains the remaining items of the
        original list, in their original order.

        The index, N, at which the original list is split is the index of the
        first item in the list that does not satisfy the given condition. Note
        that the original list should be appropriately sorted if the second
        list is to contain no items that satisfy the given condition.

        :param original_list: the list to be split.
        :param condition: a single argument function that will be used to test
                          for the list item to split on.
        :return: a tuple of two lists.
        """
        for i, item in enumerate(original_list):
            if not condition(item):
                return original_list[:i], original_list[i:]
        return original_list, []

    def _split_gt_timestamp(self, file_info_list, timestamp):
        """
        Given a list of file info dicts, reverse sorted by timestamp, split the
        list into two: items newer than timestamp, and items at same time or
        older than timestamp.

        :param file_info_list: a list of file_info dicts.
        :param timestamp: a Timestamp.
        :return: a tuple of two lists.
        """
        return self._split_list(
            file_info_list, lambda x: x['timestamp'] > timestamp)

    def _split_gte_timestamp(self, file_info_list, timestamp):
        """
        Given a list of file info dicts, reverse sorted by timestamp, split the
        list into two: items newer than or at same time as the timestamp, and
        items older than timestamp.

        :param file_info_list: a list of file_info dicts.
        :param timestamp: a Timestamp.
        :return: a tuple of two lists.
        """
        return self._split_list(
            file_info_list, lambda x: x['timestamp'] >= timestamp)

    def get_ondisk_files(self, files, datadir, verify=True, **kwargs):
        """
        Given a simple list of files names, determine the files that constitute
        a valid fileset i.e. a set of files that defines the state of an
        object, and determine the files that are obsolete and could be deleted.
        Note that some files may fall into neither category.

        If a file is considered part of a valid fileset then its info dict will
        be added to the results dict, keyed by <extension>_info. Any files that
        are no longer required will have their info dicts added to a list
        stored under the key 'obsolete'.

        The results dict will always contain entries with keys 'ts_file',
        'data_file' and 'meta_file'. Their values will be the fully qualified
        path to a file of the corresponding type if there is such a file in the
        valid fileset, or None.

        :param files: a list of file names.
        :param datadir: directory name files are from.
        :param verify: if True verify that the ondisk file contract has not
                       been violated, otherwise do not verify.
        :returns: a dict that will contain keys:
                    ts_file   -> path to a .ts file or None
                    data_file -> path to a .data file or None
                    meta_file -> path to a .meta file or None
                  and may contain keys:
                    ts_info   -> a file info dict for a .ts file
                    data_info -> a file info dict for a .data file
                    meta_info -> a file info dict for a .meta file
                    obsolete  -> a list of file info dicts for obsolete files
        """
        # Build the exts data structure:
        # exts is a dict that maps file extensions to a list of file_info
        # dicts for the files having that extension. The file_info dicts are of
        # the form returned by parse_on_disk_filename, with the filename added.
        # Each list is sorted in reverse timestamp order.

        # the results dict is used to collect results of file filtering
        results = {}

        # The exts dict will be modified during subsequent processing as files
        # are removed to be discarded or ignored.
        exts = defaultdict(list)
        for afile in files:
            # Categorize files by extension
            try:
                file_info = self.parse_on_disk_filename(afile)
                file_info['filename'] = afile
                exts[file_info['ext']].append(file_info)
            except DiskFileError as e:
                file_path = os.path.join(datadir or '', afile)
                self.logger.warning('Unexpected file %s: %s',
                                    file_path, e)
                results.setdefault('unexpected', []).append(file_path)
        for ext in exts:
            # For each extension sort files into reverse chronological order.
            exts[ext] = sorted(
                exts[ext], key=lambda info: info['timestamp'], reverse=True)

        if exts.get('.ts'):
            # non-tombstones older than or equal to latest tombstone are
            # obsolete
            for ext in filter(lambda ext: ext != '.ts', exts.keys()):
                exts[ext], older = self._split_gt_timestamp(
                    exts[ext], exts['.ts'][0]['timestamp'])
                results.setdefault('obsolete', []).extend(older)
            # all but most recent .ts are obsolete
            results.setdefault('obsolete', []).extend(exts['.ts'][1:])
            exts['.ts'] = exts['.ts'][:1]

        if exts.get('.meta'):
            # retain the newest meta file
            retain = 1
            if exts['.meta'][1:]:
                # there are other meta files so find the one with newest
                # ctype_timestamp...
                exts['.meta'][1:] = sorted(
                    exts['.meta'][1:],
                    key=lambda info: info['ctype_timestamp'],
                    reverse=True)
                # ...and retain this IFF its ctype_timestamp is greater than
                # newest meta file
                if (exts['.meta'][1]['ctype_timestamp'] >
                        exts['.meta'][0]['ctype_timestamp']):
                    if (exts['.meta'][1]['timestamp'] ==
                            exts['.meta'][0]['timestamp']):
                        # both at same timestamp so retain only the one with
                        # newest ctype
                        exts['.meta'][:2] = [exts['.meta'][1],
                                             exts['.meta'][0]]
                        retain = 1
                    else:
                        # retain both - first has newest metadata, second has
                        # newest ctype
                        retain = 2
            # discard all meta files not being retained...
            results.setdefault('obsolete', []).extend(exts['.meta'][retain:])
            exts['.meta'] = exts['.meta'][:retain]

        # delegate to subclass handler
        self._process_ondisk_files(exts, results, **kwargs)

        # set final choice of files
        if exts.get('.ts'):
            results['ts_info'] = exts['.ts'][0]
        if 'data_info' in results and exts.get('.meta'):
            # only report a meta file if a data file has been chosen
            results['meta_info'] = exts['.meta'][0]
            ctype_info = exts['.meta'].pop()
            if (ctype_info['ctype_timestamp']
                    > results['data_info']['timestamp']):
                results['ctype_info'] = ctype_info

        # set ts_file, data_file, meta_file and ctype_file with path to
        # chosen file or None
        for info_key in ('data_info', 'meta_info', 'ts_info', 'ctype_info'):
            info = results.get(info_key)
            key = info_key[:-5] + '_file'
            results[key] = join(datadir, info['filename']) if info else None

        if verify:
            assert self._verify_ondisk_files(
                results, **kwargs), \
                "On-disk file search algorithm contract is broken: %s" \
                % str(results)

        return results

    def cleanup_ondisk_files(self, hsh_path, reclaim_age=ONE_WEEK, **kwargs):
        """
        Clean up on-disk files that are obsolete and gather the set of valid
        on-disk files for an object.

        :param hsh_path: object hash path
        :param reclaim_age: age in seconds at which to remove tombstones
        :param frag_index: if set, search for a specific fragment index .data
                           file, otherwise accept the first valid .data file
        :returns: a dict that may contain: valid on disk files keyed by their
                  filename extension; a list of obsolete files stored under the
                  key 'obsolete'; a list of files remaining in the directory,
                  reverse sorted, stored under the key 'files'.
        """
        def is_reclaimable(timestamp):
            return (time.time() - float(timestamp)) > reclaim_age

        files = listdir(hsh_path)
        files.sort(reverse=True)
        results = self.get_ondisk_files(
            files, hsh_path, verify=False, **kwargs)
        if 'ts_info' in results and is_reclaimable(
                results['ts_info']['timestamp']):
            remove_file(join(hsh_path, results['ts_info']['filename']))
            files.remove(results.pop('ts_info')['filename'])
        for file_info in results.get('possible_reclaim', []):
            # stray files are not deleted until reclaim-age
            if is_reclaimable(file_info['timestamp']):
                results.setdefault('obsolete', []).append(file_info)
        for file_info in results.get('obsolete', []):
            remove_file(join(hsh_path, file_info['filename']))
            files.remove(file_info['filename'])
        results['files'] = files
        return results

    def _update_suffix_hashes(self, hashes, ondisk_info):
        """
        Applies policy specific updates to the given dict of md5 hashes for
        the given ondisk_info.

        :param hashes: a dict of md5 hashes to be updated
        :param ondisk_info: a dict describing the state of ondisk files, as
                            returned by get_ondisk_files
        """
        raise NotImplementedError

    def _hash_suffix_dir(self, path, reclaim_age):
        """

        :param path: full path to directory
        :param reclaim_age: age in seconds at which to remove tombstones
        """
        hashes = defaultdict(hashlib.md5)
        try:
            path_contents = sorted(os.listdir(path))
        except OSError as err:
            if err.errno in (errno.ENOTDIR, errno.ENOENT):
                raise PathNotDir()
            raise
        for hsh in path_contents:
            hsh_path = join(path, hsh)
            try:
                ondisk_info = self.cleanup_ondisk_files(hsh_path, reclaim_age)
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
            if not ondisk_info['files']:
                try:
                    os.rmdir(hsh_path)
                except OSError:
                    pass
                continue

            # ondisk_info has info dicts containing timestamps for those
            # files that could determine the state of the diskfile if it were
            # to be opened. We update the suffix hash with the concatenation of
            # each file's timestamp and extension. The extension is added to
            # guarantee distinct hash values from two object dirs that have
            # different file types at the same timestamp(s).
            #
            # Files that may be in the object dir but would have no effect on
            # the state of the diskfile are not used to update the hash.
            for key in (k for k in ('meta_info', 'ts_info')
                        if k in ondisk_info):
                info = ondisk_info[key]
                hashes[None].update(info['timestamp'].internal + info['ext'])

            # delegate to subclass for data file related updates...
            self._update_suffix_hashes(hashes, ondisk_info)

            if 'ctype_info' in ondisk_info:
                # We have a distinct content-type timestamp so update the
                # hash. As a precaution, append '_ctype' to differentiate this
                # value from any other timestamp value that might included in
                # the hash in future. There is no .ctype file so use _ctype to
                # avoid any confusion.
                info = ondisk_info['ctype_info']
                hashes[None].update(info['ctype_timestamp'].internal
                                    + '_ctype')

        try:
            os.rmdir(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise PathNotDir()
        else:
            # if we remove it, pretend like it wasn't there to begin with so
            # that the suffix key gets removed
            raise PathNotDir()
        return hashes

    def _hash_suffix(self, path, reclaim_age):
        """
        Performs reclamation and returns an md5 of all (remaining) files.

        :param path: full path to directory
        :param reclaim_age: age in seconds at which to remove tombstones
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        """
        raise NotImplementedError

    def _get_hashes(self, partition_path, recalculate=None, do_listdir=False,
                    reclaim_age=None):
        """
        Get a list of hashes for the suffix dir.  do_listdir causes it to
        mistrust the hash cache for suffix existence at the (unexpectedly high)
        cost of a listdir.  reclaim_age is just passed on to hash_suffix.

        :param partition_path: absolute path of partition to get hashes for
        :param recalculate: list of suffixes which should be recalculated when
                            got
        :param do_listdir: force existence check for all hashes in the
                           partition
        :param reclaim_age: age at which to remove tombstones

        :returns: tuple of (number of suffix dirs hashed, dictionary of hashes)
        """
        reclaim_age = reclaim_age or self.reclaim_age
        hashed = 0
        hashes_file = join(partition_path, HASH_FILE)
        modified = False
        force_rewrite = False
        hashes = {}
        mtime = -1

        if recalculate is None:
            recalculate = []

        try:
            mtime = getmtime(hashes_file)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

        try:
            hashes = self.consolidate_hashes(partition_path)
        except Exception:
            do_listdir = True
            force_rewrite = True
        else:
            if hashes is None:  # no hashes.pkl file; let's build it
                do_listdir = True
                force_rewrite = True
                hashes = {}

        if do_listdir:
            for suff in os.listdir(partition_path):
                if len(suff) == 3:
                    hashes.setdefault(suff, None)
            modified = True
        hashes.update((suffix, None) for suffix in recalculate)
        for suffix, hash_ in hashes.items():
            if not hash_:
                suffix_dir = join(partition_path, suffix)
                try:
                    hashes[suffix] = self._hash_suffix(suffix_dir, reclaim_age)
                    hashed += 1
                except PathNotDir:
                    del hashes[suffix]
                except OSError:
                    logging.exception(_('Error hashing suffix'))
                modified = True
        if modified:
            with lock_path(partition_path):
                if force_rewrite or not exists(hashes_file) or \
                        getmtime(hashes_file) == mtime:
                    write_pickle(
                        hashes, hashes_file, partition_path, PICKLE_PROTOCOL)
                    return hashed, hashes
            return self._get_hashes(partition_path, recalculate, do_listdir,
                                    reclaim_age)
        else:
            return hashed, hashes

    def construct_dev_path(self, device):
        """
        Construct the path to a device without checking if it is mounted.

        :param device: name of target device
        :returns: full path to the device
        """
        return os.path.join(self.devices, device)

    def get_dev_path(self, device, mount_check=None):
        """
        Return the path to a device, first checking to see if either it
        is a proper mount point, or at least a directory depending on
        the mount_check configuration option.

        :param device: name of target device
        :param mount_check: whether or not to check mountedness of device.
                            Defaults to bool(self.mount_check).
        :returns: full path to the device, None if the path to the device is
                  not a proper mount point or directory.
        """
        # we'll do some kind of check unless explicitly forbidden
        if mount_check is not False:
            if mount_check or self.mount_check:
                check = check_mount
            else:
                check = check_dir
            if not check(self.devices, device):
                return None
        return os.path.join(self.devices, device)

    @contextmanager
    def replication_lock(self, device):
        """
        A context manager that will lock on the device given, if
        configured to do so.

        :param device: name of target device
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
                            timestamp, policy):
        """
        Write data describing a container update notification to a pickle file
        in the async_pending directory.

        :param device: name of target device
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name for the object
        :param data: update data to be written to pickle file
        :param timestamp: a Timestamp
        :param policy: the StoragePolicy instance
        """
        device_path = self.construct_dev_path(device)
        async_dir = os.path.join(device_path, get_async_dir(policy))
        ohash = hash_path(account, container, obj)
        write_pickle(
            data,
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                         Timestamp(timestamp).internal),
            os.path.join(device_path, get_tmp_dir(policy)))
        self.logger.increment('async_pendings')

    def get_diskfile(self, device, partition, account, container, obj,
                     policy, **kwargs):
        """
        Returns a BaseDiskFile instance for an object based on the object's
        partition, path parts and policy.

        :param device: name of target device
        :param partition: partition on device in which the object lives
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name for the object
        :param policy: the StoragePolicy instance
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        return self.diskfile_cls(self, dev_path,
                                 partition, account, container, obj,
                                 policy=policy, use_splice=self.use_splice,
                                 pipe_size=self.pipe_size, **kwargs)

    def object_audit_location_generator(self, device_dirs=None,
                                        auditor_type="ALL"):
        """
        Yield an AuditLocation for all objects stored under device_dirs.

        :param device_dirs: directory of target device
        :param auditor_type: either ALL or ZBF
        """
        return object_audit_location_generator(self.devices, self.mount_check,
                                               self.logger, device_dirs,
                                               auditor_type)

    def get_diskfile_from_audit_location(self, audit_location):
        """
        Returns a BaseDiskFile instance for an object at the given
        AuditLocation.

        :param audit_location: object location to be audited
        """
        dev_path = self.get_dev_path(audit_location.device, mount_check=False)
        return self.diskfile_cls.from_hash_dir(
            self, audit_location.path, dev_path,
            audit_location.partition, policy=audit_location.policy)

    def get_diskfile_from_hash(self, device, partition, object_hash,
                               policy, **kwargs):
        """
        Returns a DiskFile instance for an object at the given
        object_hash. Just in case someone thinks of refactoring, be
        sure DiskFileDeleted is *not* raised, but the DiskFile
        instance representing the tombstoned object is returned
        instead.

        :param device: name of target device
        :param partition: partition on the device in which the object lives
        :param object_hash: the hash of an object path
        :param policy: the StoragePolicy instance
        :raises DiskFileNotExist: if the object does not exist
        :returns: an instance of BaseDiskFile
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        object_path = os.path.join(
            dev_path, get_data_dir(policy), str(partition), object_hash[-3:],
            object_hash)
        try:
            filenames = self.cleanup_ondisk_files(object_path,
                                                  self.reclaim_age)['files']
        except OSError as err:
            if err.errno == errno.ENOTDIR:
                quar_path = self.quarantine_renamer(dev_path, object_path)
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
        return self.diskfile_cls(self, dev_path,
                                 partition, account, container, obj,
                                 policy=policy, **kwargs)

    def get_hashes(self, device, partition, suffixes, policy):
        """

        :param device: name of target device
        :param partition: partition name
        :param suffixes: a list of suffix directories to be recalculated
        :param policy: the StoragePolicy instance
        :returns: a dictionary that maps suffix directories
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        partition_path = os.path.join(dev_path, get_data_dir(policy),
                                      partition)
        if not os.path.exists(partition_path):
            mkdirs(partition_path)
        _junk, hashes = tpool_reraise(
            self._get_hashes, partition_path, recalculate=suffixes)
        return hashes

    def _listdir(self, path):
        """
        :param path: full path to directory
        """
        try:
            return os.listdir(path)
        except OSError as err:
            if err.errno != errno.ENOENT:
                self.logger.error(
                    'ERROR: Skipping %r due to error with listdir attempt: %s',
                    path, err)
        return []

    def yield_suffixes(self, device, partition, policy):
        """
        Yields tuples of (full_path, suffix_only) for suffixes stored
        on the given device and partition.

        :param device: name of target device
        :param partition: partition name
        :param policy: the StoragePolicy instance
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        partition_path = os.path.join(dev_path, get_data_dir(policy),
                                      partition)
        for suffix in self._listdir(partition_path):
            if len(suffix) != 3:
                continue
            try:
                int(suffix, 16)
            except ValueError:
                continue
            yield (os.path.join(partition_path, suffix), suffix)

    def yield_hashes(self, device, partition, policy,
                     suffixes=None, **kwargs):
        """
        Yields tuples of (full_path, hash_only, timestamps) for object
        information stored for the given device, partition, and
        (optionally) suffixes. If suffixes is None, all stored
        suffixes will be searched for object hashes. Note that if
        suffixes is not None but empty, such as [], then nothing will
        be yielded.

        timestamps is a dict which may contain items mapping:

        - ts_data -> timestamp of data or tombstone file,
        - ts_meta -> timestamp of meta file, if one exists
        - ts_ctype -> timestamp of meta file containing most recent
                      content-type value, if one exists

        where timestamps are instances of
        :class:`~swift.common.utils.Timestamp`

        :param device: name of target device
        :param partition: partition name
        :param policy: the StoragePolicy instance
        :param suffixes: optional list of suffix directories to be searched
        """
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        if suffixes is None:
            suffixes = self.yield_suffixes(device, partition, policy)
        else:
            partition_path = os.path.join(dev_path,
                                          get_data_dir(policy),
                                          str(partition))
            suffixes = (
                (os.path.join(partition_path, suffix), suffix)
                for suffix in suffixes)
        key_preference = (
            ('ts_meta', 'meta_info', 'timestamp'),
            ('ts_data', 'data_info', 'timestamp'),
            ('ts_data', 'ts_info', 'timestamp'),
            ('ts_ctype', 'ctype_info', 'ctype_timestamp'),
        )
        for suffix_path, suffix in suffixes:
            for object_hash in self._listdir(suffix_path):
                object_path = os.path.join(suffix_path, object_hash)
                try:
                    results = self.cleanup_ondisk_files(
                        object_path, self.reclaim_age, **kwargs)
                    timestamps = {}
                    for ts_key, info_key, info_ts_key in key_preference:
                        if info_key not in results:
                            continue
                        timestamps[ts_key] = results[info_key][info_ts_key]
                    if 'ts_data' not in timestamps:
                        # file sets that do not include a .data or .ts
                        # file cannot be opened and therefore cannot
                        # be ssync'd
                        continue
                    yield (object_path, object_hash, timestamps)
                except AssertionError as err:
                    self.logger.debug('Invalid file set in %s (%s)' % (
                        object_path, err))
                except DiskFileError as err:
                    self.logger.debug(
                        'Invalid diskfile filename in %r (%s)' % (
                            object_path, err))


class BaseDiskFileWriter(object):
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
    :param diskfile: the diskfile creating this DiskFileWriter instance
    """

    def __init__(self, name, datadir, fd, tmppath, bytes_per_sync, diskfile):
        # Parameter tracking
        self._name = name
        self._datadir = datadir
        self._fd = fd
        self._tmppath = tmppath
        self._bytes_per_sync = bytes_per_sync
        self._diskfile = diskfile

        # Internal attributes
        self._upload_size = 0
        self._last_sync = 0
        self._extension = '.data'
        self._put_succeeded = False

    @property
    def manager(self):
        return self._diskfile.manager

    @property
    def put_succeeded(self):
        return self._put_succeeded

    def write(self, chunk):
        """
        Write a chunk of data to disk. All invocations of this method must
        come before invoking the :func:

        For this implementation, the data is written into a temporary file.

        :param chunk: the chunk of data to write as a string object

        :returns: the total number of bytes written to an object
        """

        while chunk:
            written = os.write(self._fd, chunk)
            self._upload_size += written
            chunk = chunk[written:]

        # For large files sync every 512MB (by default) written
        diff = self._upload_size - self._last_sync
        if diff >= self._bytes_per_sync:
            tpool_reraise(fdatasync, self._fd)
            drop_buffer_cache(self._fd, self._last_sync, diff)
            self._last_sync = self._upload_size

        return self._upload_size

    def _finalize_put(self, metadata, target_path, cleanup):
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
        self.manager.invalidate_hash(dirname(self._datadir))
        # After the rename completes, this object will be available for other
        # requests to reference.
        renamer(self._tmppath, target_path)
        # If rename is successful, flag put as succeeded. This is done to avoid
        # unnecessary os.unlink() of tempfile later. As renamer() has
        # succeeded, the tempfile would no longer exist at its original path.
        self._put_succeeded = True
        if cleanup:
            try:
                self.manager.cleanup_ondisk_files(self._datadir)['files']
            except OSError:
                logging.exception(_('Problem cleaning up %s'), self._datadir)

    def _put(self, metadata, cleanup=True, *a, **kw):
        """
        Helper method for subclasses.

        For this implementation, this method is responsible for renaming the
        temporary file to the final name and directory location.  This method
        should be called after the final call to
        :func:`swift.obj.diskfile.DiskFileWriter.write`.

        :param metadata: dictionary of metadata to be associated with the
                         object
        :param cleanup: a Boolean. If True then obsolete files will be removed
                        from the object dir after the put completes, otherwise
                        obsolete files are left in place.
        """
        timestamp = Timestamp(metadata['X-Timestamp'])
        ctype_timestamp = metadata.get('Content-Type-Timestamp')
        if ctype_timestamp:
            ctype_timestamp = Timestamp(ctype_timestamp)
        filename = self.manager.make_on_disk_filename(
            timestamp, self._extension, ctype_timestamp=ctype_timestamp,
            *a, **kw)
        metadata['name'] = self._name
        target_path = join(self._datadir, filename)

        tpool_reraise(self._finalize_put, metadata, target_path, cleanup)

    def put(self, metadata):
        """
        Finalize writing the file on disk.

        :param metadata: dictionary of metadata to be associated with the
                         object
        """
        raise NotImplementedError

    def commit(self, timestamp):
        """
        Perform any operations necessary to mark the object as durable. For
        replication policy type this is a no-op.

        :param timestamp: object put timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        """
        pass


class BaseDiskFileReader(object):
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
    :param disk_chunk_size: size of reads from disk in bytes
    :param keep_cache_size: maximum object size that will be kept in cache
    :param device_path: on-disk device path, used when quarantining an obj
    :param logger: logger caller wants this object to use
    :param quarantine_hook: 1-arg callable called w/reason when quarantined
    :param use_splice: if true, use zero-copy splice() to send data
    :param pipe_size: size of pipe buffer used in zero-copy operations
    :param diskfile: the diskfile creating this DiskFileReader instance
    :param keep_cache: should resulting reads be kept in the buffer cache
    """
    def __init__(self, fp, data_file, obj_size, etag,
                 disk_chunk_size, keep_cache_size, device_path, logger,
                 quarantine_hook, use_splice, pipe_size, diskfile,
                 keep_cache=False):
        # Parameter tracking
        self._fp = fp
        self._data_file = data_file
        self._obj_size = obj_size
        self._etag = etag
        self._diskfile = diskfile
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

    @property
    def manager(self):
        return self._diskfile.manager

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
                chunk = self._fp.read(self._disk_chunk_size)
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
                (bytes_in_pipe, _1, _2) = splice(
                    rfd, None, client_wpipe, None, pipe_size, 0)
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
                (hashed, _1, _2) = splice(hash_rpipe, None, md5_sockfd, None,
                                          bytes_in_pipe, splice.SPLICE_F_MORE)
                if hashed != bytes_in_pipe:
                    raise Exception("md5 socket didn't take all the data? "
                                    "(tried to write %d, but wrote %d)" %
                                    (bytes_in_pipe, hashed))

                while bytes_in_pipe > 0:
                    try:
                        res = splice(client_rpipe, None, wsockfd, None,
                                     bytes_in_pipe, 0)
                        bytes_in_pipe -= res[0]
                    except IOError as exc:
                        if exc.errno == errno.EWOULDBLOCK:
                            trampoline(wsockfd, write=True)
                        else:
                            raise

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
        """
        Returns an iterator over the data file for range (start, stop)

        """
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
        """
        Returns an iterator over the data file for a set of ranges

        """
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
        """
        Method for no-oping buffer cache drop method.

        :param fd: file descriptor or filename
        """
        if not self._keep_cache:
            drop_buffer_cache(fd, offset, length)

    def _quarantine(self, msg):
        self._quarantined_dir = self.manager.quarantine_renamer(
            self._device_path, self._data_file)
        self._logger.warning("Quarantined object %s: %s" % (
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


class BaseDiskFile(object):
    """
    Manage object files.

    This specific implementation manages object files on a disk formatted with
    a POSIX-compliant file system that supports extended attributes as
    metadata on a file or directory.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

        The following path format is used for data file locations:
        <devices_path/<device_dir>/<datadir>/<partdir>/<suffixdir>/<hashdir>/
        <datafile>.<ext>

    :param mgr: associated DiskFileManager instance
    :param device_path: path to the target device or drive
    :param partition: partition on the device in which the object lives
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param _datadir: override the full datadir otherwise constructed here
    :param policy: the StoragePolicy instance
    :param use_splice: if true, use zero-copy splice() to send data
    :param pipe_size: size of pipe buffer used in zero-copy operations
    """
    reader_cls = None  # must be set by subclasses
    writer_cls = None  # must be set by subclasses

    def __init__(self, mgr, device_path, partition,
                 account=None, container=None, obj=None, _datadir=None,
                 policy=None, use_splice=False, pipe_size=None, **kwargs):
        self._manager = mgr
        self._device_path = device_path
        self._logger = mgr.logger
        self._disk_chunk_size = mgr.disk_chunk_size
        self._bytes_per_sync = mgr.bytes_per_sync
        self._use_splice = use_splice
        self._pipe_size = pipe_size
        self.policy = policy
        if account and container and obj:
            self._name = '/' + '/'.join((account, container, obj))
            self._account = account
            self._container = container
            self._obj = obj
            name_hash = hash_path(account, container, obj)
            self._datadir = join(
                device_path, storage_directory(get_data_dir(policy),
                                               partition, name_hash))
        else:
            # gets populated when we read the metadata
            self._name = None
            self._account = None
            self._container = None
            self._obj = None
            self._datadir = None
        self._tmpdir = join(device_path, get_tmp_dir(policy))
        self._ondisk_info = None
        self._metadata = None
        self._datafile_metadata = None
        self._metafile_metadata = None
        self._data_file = None
        self._fp = None
        self._quarantined_dir = None
        self._content_length = None
        if _datadir:
            self._datadir = _datadir
        else:
            name_hash = hash_path(account, container, obj)
            self._datadir = join(
                device_path, storage_directory(get_data_dir(policy),
                                               partition, name_hash))

    @property
    def manager(self):
        return self._manager

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

    @property
    def data_timestamp(self):
        if self._datafile_metadata is None:
            raise DiskFileNotOpen()
        return Timestamp(self._datafile_metadata.get('X-Timestamp'))

    @property
    def durable_timestamp(self):
        """
        Provides the timestamp of the newest data file found in the object
        directory.

        :return: A Timestamp instance, or None if no data file was found.
        :raises DiskFileNotOpen: if the open() method has not been previously
                                 called on this instance.
        """
        if self._ondisk_info is None:
            raise DiskFileNotOpen()
        if self._datafile_metadata:
            return Timestamp(self._datafile_metadata.get('X-Timestamp'))
        return None

    @property
    def fragments(self):
        return None

    @property
    def content_type(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self._metadata.get('Content-Type')

    @property
    def content_type_timestamp(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        t = self._metadata.get('Content-Type-Timestamp',
                               self._datafile_metadata.get('X-Timestamp'))
        return Timestamp(t)

    @classmethod
    def from_hash_dir(cls, mgr, hash_dir_path, device_path, partition, policy):
        return cls(mgr, device_path, None, partition, _datadir=hash_dir_path,
                   policy=policy)

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
        # First figure out if the data directory exists
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
            files = []

        # gather info about the valid files to use to open the DiskFile
        file_info = self._get_ondisk_files(files)

        self._data_file = file_info.get('data_file')
        if not self._data_file:
            raise self._construct_exception_from_ts_file(**file_info)
        self._fp = self._construct_from_data_file(**file_info)
        # This method must populate the internal _metadata attribute.
        self._metadata = self._metadata or {}
        return self

    def __enter__(self):
        """
        Context enter.

        .. note::

            An implementation shall raise `DiskFileNotOpen` when has not
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
        self._quarantined_dir = self.manager.quarantine_renamer(
            self._device_path, data_file)
        self._logger.warning("Quarantined object %s: %s" % (
            data_file, msg))
        self._logger.increment('quarantines')
        return DiskFileQuarantined(msg)

    def _get_ondisk_files(self, files):
        """
        Determine the on-disk files to use.

        :param files: a list of files in the object's dir
        :returns: dict of files to use having keys 'data_file', 'ts_file',
                 'meta_file'
        """
        raise NotImplementedError

    def _construct_exception_from_ts_file(self, ts_file, **kwargs):
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
        """

        :param data_file: data file name, used when quarantines occur
        """
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
        """
        Read metadata from source object file. In case of failure, quarantine
        the file.

        Takes source and filename separately so we can read from an open
        file if we have one.

        :param source: file descriptor or filename to load the metadata from
        :param quarantine_filename: full path of file to load the metadata from
        """
        try:
            return read_metadata(source)
        except (DiskFileXattrNotSupported, DiskFileNotExist):
            raise
        except Exception as err:
            raise self._quarantine(
                quarantine_filename,
                "Exception reading metadata: %s" % err)

    def _merge_content_type_metadata(self, ctype_file):
        """
        When a second .meta file is providing the most recent Content-Type
        metadata then merge it into the metafile_metadata.

        :param ctype_file: An on-disk .meta file
        """
        ctypefile_metadata = self._failsafe_read_metadata(
            ctype_file, ctype_file)
        if ('Content-Type' in ctypefile_metadata
            and (ctypefile_metadata.get('Content-Type-Timestamp') >
                 self._metafile_metadata.get('Content-Type-Timestamp'))
            and (ctypefile_metadata.get('Content-Type-Timestamp') >
                 self.data_timestamp)):
            self._metafile_metadata['Content-Type'] = \
                ctypefile_metadata['Content-Type']
            self._metafile_metadata['Content-Type-Timestamp'] = \
                ctypefile_metadata.get('Content-Type-Timestamp')

    def _construct_from_data_file(self, data_file, meta_file, ctype_file,
                                  **kwargs):
        """
        Open the `.data` file to fetch its metadata, and fetch the metadata
        from fast-POST `.meta` files as well if any exist, merging them
        properly.

        :param data_file: on-disk `.data` file being considered
        :param meta_file: on-disk fast-POST `.meta` file being considered
        :param ctype_file: on-disk fast-POST `.meta` file being considered that
                           contains content-type and content-type timestamp
        :returns: an opened data file pointer
        :raises DiskFileError: various exceptions from
                    :func:`swift.obj.diskfile.DiskFile._verify_data_file`
        """
        fp = open(data_file, 'rb')
        self._datafile_metadata = self._failsafe_read_metadata(fp, data_file)
        self._metadata = {}
        if meta_file:
            self._metafile_metadata = self._failsafe_read_metadata(
                meta_file, meta_file)
            if ctype_file and ctype_file != meta_file:
                self._merge_content_type_metadata(ctype_file)
            sys_metadata = dict(
                [(key, val) for key, val in self._datafile_metadata.items()
                 if key.lower() in DATAFILE_SYSTEM_META
                 or is_sys_meta('object', key)])
            self._metadata.update(self._metafile_metadata)
            self._metadata.update(sys_metadata)
            # diskfile writer added 'name' to metafile, so remove it here
            self._metafile_metadata.pop('name', None)
            # TODO: the check for Content-Type is only here for tests that
            # create .data files without Content-Type
            if ('Content-Type' in self._datafile_metadata and
                    (self.data_timestamp >
                     self._metafile_metadata.get('Content-Type-Timestamp'))):
                self._metadata['Content-Type'] = \
                    self._datafile_metadata['Content-Type']
                self._metadata.pop('Content-Type-Timestamp', None)
        else:
            self._metadata.update(self._datafile_metadata)
        if self._name is None:
            # If we don't know our name, we were just given a hash dir at
            # instantiation, so we'd better validate that the name hashes back
            # to us
            self._name = self._metadata['name']
            self._verify_name_matches_hash(data_file)
        self._verify_data_file(data_file, fp)
        return fp

    def get_metafile_metadata(self):
        """
        Provide the metafile metadata for a previously opened object as a
        dictionary. This is metadata that was written by a POST and does not
        include any persistent metadata that was set by the original PUT.

        :returns: object's .meta file metadata dictionary, or None if there is
                  no .meta file
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self._metafile_metadata

    def get_datafile_metadata(self):
        """
        Provide the datafile metadata for a previously opened object as a
        dictionary. This is metadata that was included when the object was
        first PUT, and does not include metadata set by any subsequent POST.

        :returns: object's datafile metadata dictionary
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        """
        if self._datafile_metadata is None:
            raise DiskFileNotOpen()
        return self._datafile_metadata

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
        dr = self.reader_cls(
            self._fp, self._data_file, int(self._metadata['Content-Length']),
            self._metadata['ETag'], self._disk_chunk_size,
            self._manager.keep_cache_size, self._device_path, self._logger,
            use_splice=self._use_splice, quarantine_hook=_quarantine_hook,
            pipe_size=self._pipe_size, diskfile=self, keep_cache=keep_cache)
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
        try:
            fd, tmppath = mkstemp(dir=self._tmpdir)
        except OSError as err:
            if err.errno in (errno.ENOSPC, errno.EDQUOT):
                # No more inodes in filesystem
                raise DiskFileNoSpace()
            raise
        dfw = None
        try:
            if size is not None and size > 0:
                try:
                    fallocate(fd, size)
                except OSError as err:
                    if err.errno in (errno.ENOSPC, errno.EDQUOT):
                        raise DiskFileNoSpace()
                    raise
            dfw = self.writer_cls(self._name, self._datadir, fd, tmppath,
                                  bytes_per_sync=self._bytes_per_sync,
                                  diskfile=self)
            yield dfw
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            if (dfw is None) or (not dfw.put_succeeded):
                # Try removing the temp file only if put did NOT succeed.
                #
                # dfw.put_succeeded is set to True after renamer() succeeds in
                # DiskFileWriter._finalize_put()
                try:
                    os.unlink(tmppath)
                except OSError:
                    self._logger.exception('Error removing tempfile: %s' %
                                           tmppath)

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
        # this is dumb, only tests send in strings
        timestamp = Timestamp(timestamp)
        with self.create() as deleter:
            deleter._extension = '.ts'
            deleter.put({'X-Timestamp': timestamp.internal})


class DiskFileReader(BaseDiskFileReader):
    pass


class DiskFileWriter(BaseDiskFileWriter):
    def put(self, metadata):
        """
        Finalize writing the file on disk.

        :param metadata: dictionary of metadata to be associated with the
                         object
        """
        super(DiskFileWriter, self)._put(metadata, True)


class DiskFile(BaseDiskFile):
    reader_cls = DiskFileReader
    writer_cls = DiskFileWriter

    def _get_ondisk_files(self, files):
        self._ondisk_info = self.manager.get_ondisk_files(files, self._datadir)
        return self._ondisk_info


@DiskFileRouter.register(REPL_POLICY)
class DiskFileManager(BaseDiskFileManager):
    diskfile_cls = DiskFile

    def _process_ondisk_files(self, exts, results, **kwargs):
        """
        Implement replication policy specific handling of .data files.

        :param exts: dict of lists of file info, keyed by extension
        :param results: a dict that may be updated with results
        """
        if exts.get('.data'):
            for ext in exts.keys():
                if ext == '.data':
                    # older .data's are obsolete
                    exts[ext], obsolete = self._split_gte_timestamp(
                        exts[ext], exts['.data'][0]['timestamp'])
                else:
                    # other files at same or older timestamp as most recent
                    # data are obsolete
                    exts[ext], obsolete = self._split_gt_timestamp(
                        exts[ext], exts['.data'][0]['timestamp'])
                results.setdefault('obsolete', []).extend(obsolete)

            # set results
            results['data_info'] = exts['.data'][0]

        # .meta files *may* be ready for reclaim if there is no data
        if exts.get('.meta') and not exts.get('.data'):
            results.setdefault('possible_reclaim', []).extend(
                exts.get('.meta'))

    def _update_suffix_hashes(self, hashes, ondisk_info):
        """
        Applies policy specific updates to the given dict of md5 hashes for
        the given ondisk_info.

        :param hashes: a dict of md5 hashes to be updated
        :param ondisk_info: a dict describing the state of ondisk files, as
                            returned by get_ondisk_files
        """
        if 'data_info' in ondisk_info:
            file_info = ondisk_info['data_info']
            hashes[None].update(
                file_info['timestamp'].internal + file_info['ext'])

    def _hash_suffix(self, path, reclaim_age):
        """
        Performs reclamation and returns an md5 of all (remaining) files.

        :param path: full path to directory
        :param reclaim_age: age in seconds at which to remove tombstones
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        :returns: md5 of files in suffix
        """
        hashes = self._hash_suffix_dir(path, reclaim_age)
        return hashes[None].hexdigest()


class ECDiskFileReader(BaseDiskFileReader):
    pass


class ECDiskFileWriter(BaseDiskFileWriter):

    def _finalize_durable(self, durable_file_path):
        exc = None
        try:
            try:
                with open(durable_file_path, 'w') as _fp:
                    fsync(_fp.fileno())
                fsync_dir(self._datadir)
            except (OSError, IOError) as err:
                if err.errno not in (errno.ENOSPC, errno.EDQUOT):
                    # re-raise to catch all handler
                    raise
                msg = (_('No space left on device for %(file)s (%(err)s)') %
                       {'file': durable_file_path, 'err': err})
                self.manager.logger.error(msg)
                exc = DiskFileNoSpace(str(err))
            else:
                try:
                    self.manager.cleanup_ondisk_files(self._datadir)['files']
                except OSError as os_err:
                    self.manager.logger.exception(
                        _('Problem cleaning up %(datadir)s (%(err)s)') %
                        {'datadir': self._datadir, 'err': os_err})
        except Exception as err:
            msg = (_('Problem writing durable state file %(file)s (%(err)s)') %
                   {'file': durable_file_path, 'err': err})
            self.manager.logger.exception(msg)
            exc = DiskFileError(msg)
        if exc:
            raise exc

    def commit(self, timestamp):
        """
        Finalize put by writing a timestamp.durable file for the object. We
        do this for EC policy because it requires a 2-phase put commit
        confirmation.

        :param timestamp: object put timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        """
        durable_file_path = os.path.join(
            self._datadir, timestamp.internal + '.durable')
        tpool_reraise(self._finalize_durable, durable_file_path)

    def put(self, metadata):
        """
        The only difference between this method and the replication policy
        DiskFileWriter method is adding the frag index to the metadata.

        :param metadata: dictionary of metadata to be associated with object
        """
        fi = None
        cleanup = True
        if self._extension == '.data':
            # generally we treat the fragment index provided in metadata as
            # canon, but if it's unavailable (e.g. tests) it's reasonable to
            # use the frag_index provided at instantiation. Either way make
            # sure that the fragment index is included in object sysmeta.
            fi = metadata.setdefault('X-Object-Sysmeta-Ec-Frag-Index',
                                     self._diskfile._frag_index)
            # defer cleanup until commit() writes .durable
            cleanup = False
        super(ECDiskFileWriter, self)._put(metadata, cleanup, frag_index=fi)


class ECDiskFile(BaseDiskFile):

    reader_cls = ECDiskFileReader
    writer_cls = ECDiskFileWriter

    def __init__(self, *args, **kwargs):
        super(ECDiskFile, self).__init__(*args, **kwargs)
        frag_index = kwargs.get('frag_index')
        self._frag_index = None
        if frag_index is not None:
            self._frag_index = self.manager.validate_fragment_index(frag_index)

    @property
    def durable_timestamp(self):
        """
        Provides the timestamp of the newest durable file found in the object
        directory.

        :return: A Timestamp instance, or None if no durable file was found.
        :raises DiskFileNotOpen: if the open() method has not been previously
                                 called on this instance.
        """
        if self._ondisk_info is None:
            raise DiskFileNotOpen()
        if self._ondisk_info.get('durable_frag_set'):
            return self._ondisk_info['durable_frag_set'][0]['timestamp']
        return None

    @property
    def fragments(self):
        """
        Provides information about all fragments that were found in the object
        directory, including fragments without a matching durable file, and
        including any fragment chosen to construct the opened diskfile.

        :return: A dict mapping <Timestamp instance> -> <list of frag indexes>,
                 or None if the diskfile has not been opened or no fragments
                 were found.
        """
        if self._ondisk_info:
            frag_sets = self._ondisk_info['frag_sets']
            return dict([(ts, [info['frag_index'] for info in frag_set])
                         for ts, frag_set in frag_sets.items()])

    def _get_ondisk_files(self, files):
        """
        The only difference between this method and the replication policy
        DiskFile method is passing in the frag_index kwarg to our manager's
        get_ondisk_files method.

        :param files: list of file names
        """
        self._ondisk_info = self.manager.get_ondisk_files(
            files, self._datadir, frag_index=self._frag_index)
        return self._ondisk_info

    def purge(self, timestamp, frag_index):
        """
        Remove a tombstone file matching the specified timestamp or
        datafile matching the specified timestamp and fragment index
        from the object directory.

        This provides the EC reconstructor/ssync process with a way to
        remove a tombstone or fragment from a handoff node after
        reverting it to its primary node.

        The hash will be invalidated, and if empty or invalid the
        hsh_path will be removed on next cleanup_ondisk_files.

        :param timestamp: the object timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        :param frag_index: fragment archive index, must be
                           a whole number or None.
        """
        exts = ['.ts']
        # when frag_index is None it's not possible to build a data file name
        if frag_index is not None:
            exts.append('.data')
        for ext in exts:
            purge_file = self.manager.make_on_disk_filename(
                timestamp, ext=ext, frag_index=frag_index)
            remove_file(os.path.join(self._datadir, purge_file))
        self.manager.invalidate_hash(dirname(self._datadir))


@DiskFileRouter.register(EC_POLICY)
class ECDiskFileManager(BaseDiskFileManager):
    diskfile_cls = ECDiskFile

    def validate_fragment_index(self, frag_index):
        """
        Return int representation of frag_index, or raise a DiskFileError if
        frag_index is not a whole number.

        :param frag_index: a fragment archive index
        """
        try:
            frag_index = int(str(frag_index))
        except (ValueError, TypeError) as e:
            raise DiskFileError(
                'Bad fragment index: %s: %s' % (frag_index, e))
        if frag_index < 0:
            raise DiskFileError(
                'Fragment index must not be negative: %s' % frag_index)
        return frag_index

    def make_on_disk_filename(self, timestamp, ext=None, frag_index=None,
                              ctype_timestamp=None, *a, **kw):
        """
        Returns the EC specific filename for given timestamp.

        :param timestamp: the object timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        :param ext: an optional string representing a file extension to be
                    appended to the returned file name
        :param frag_index: a fragment archive index, used with .data extension
                           only, must be a whole number.
        :param ctype_timestamp: an optional content-type timestamp, an instance
                                of :class:`~swift.common.utils.Timestamp`
        :returns: a file name
        :raises DiskFileError: if ext=='.data' and the kwarg frag_index is not
                               a whole number
        """
        if ext == '.data':
            # for datafiles only we encode the fragment index in the filename
            # to allow archives of different indexes to temporarily be stored
            # on the same node in certain situations
            frag_index = self.validate_fragment_index(frag_index)
            rv = timestamp.internal + '#' + str(frag_index)
            return '%s%s' % (rv, ext or '')
        return super(ECDiskFileManager, self).make_on_disk_filename(
            timestamp, ext, ctype_timestamp, *a, **kw)

    def parse_on_disk_filename(self, filename):
        """
        Returns timestamp(s) and other info extracted from a policy specific
        file name. For EC policy the data file name includes a fragment index
        which must be stripped off to retrieve the timestamp.

        :param filename: the file name including extension
        :returns: a dict, with keys for timestamp, frag_index, ext and
                  ctype_timestamp:

            * timestamp is a :class:`~swift.common.utils.Timestamp`
            * frag_index is an int or None
            * ctype_timestamp is a :class:`~swift.common.utils.Timestamp` or
              None for .meta files, otherwise None
            * ext is a string, the file extension including the leading dot or
              the empty string if the filename has no extension.

        :raises DiskFileError: if any part of the filename is not able to be
                               validated.
        """
        frag_index = None
        float_frag, ext = splitext(filename)
        if ext == '.data':
            parts = float_frag.split('#', 1)
            try:
                timestamp = Timestamp(parts[0])
            except ValueError:
                raise DiskFileError('Invalid Timestamp value in filename %r'
                                    % filename)
            # it is an error for an EC data file to not have a valid
            # fragment index
            try:
                frag_index = parts[1]
            except IndexError:
                # expect validate_fragment_index raise DiskFileError
                pass
            frag_index = self.validate_fragment_index(frag_index)
            return {
                'timestamp': timestamp,
                'frag_index': frag_index,
                'ext': ext,
                'ctype_timestamp': None
            }
        rv = super(ECDiskFileManager, self).parse_on_disk_filename(filename)
        rv['frag_index'] = None
        return rv

    def _process_ondisk_files(self, exts, results, frag_index=None, **kwargs):
        """
        Implement EC policy specific handling of .data and .durable files.

        :param exts: dict of lists of file info, keyed by extension
        :param results: a dict that may be updated with results
        :param frag_index: if set, search for a specific fragment index .data
                           file, otherwise accept the first valid .data file.
        """
        durable_info = None
        if exts.get('.durable'):
            durable_info = exts['.durable'][0]
            # Mark everything older than most recent .durable as obsolete
            # and remove from the exts dict.
            for ext in exts.keys():
                exts[ext], older = self._split_gte_timestamp(
                    exts[ext], durable_info['timestamp'])
                results.setdefault('obsolete', []).extend(older)

        # Split the list of .data files into sets of frags having the same
        # timestamp, identifying the durable and newest sets (if any) as we go.
        # To do this we can take advantage of the list of .data files being
        # reverse-time ordered. Keep the resulting per-timestamp frag sets in
        # a frag_sets dict mapping a Timestamp instance -> frag_set.
        all_frags = exts.get('.data')
        frag_sets = {}
        durable_frag_set = None
        while all_frags:
            frag_set, all_frags = self._split_gte_timestamp(
                all_frags, all_frags[0]['timestamp'])
            # sort the frag set into ascending frag_index order
            frag_set.sort(key=lambda info: info['frag_index'])
            timestamp = frag_set[0]['timestamp']
            frag_sets[timestamp] = frag_set
            if durable_info and durable_info['timestamp'] == timestamp:
                durable_frag_set = frag_set

        # Select a single chosen frag from the chosen frag_set, by either
        # matching against a specified frag_index or taking the highest index.
        chosen_frag = None
        if durable_frag_set:
            if frag_index is not None:
                # search the frag set to find the exact frag_index
                for info in durable_frag_set:
                    if info['frag_index'] == frag_index:
                        chosen_frag = info
                        break
            else:
                chosen_frag = durable_frag_set[-1]

        # If we successfully found a frag then set results
        if chosen_frag:
            results['data_info'] = chosen_frag
            results['durable_frag_set'] = durable_frag_set
        results['frag_sets'] = frag_sets

        # Mark any isolated .durable as obsolete
        if exts.get('.durable') and not durable_frag_set:
            results.setdefault('obsolete', []).extend(exts['.durable'])
            exts.pop('.durable')

        # Fragments *may* be ready for reclaim, unless they are durable
        for frag_set in frag_sets.values():
            if frag_set == durable_frag_set:
                continue
            results.setdefault('possible_reclaim', []).extend(frag_set)

        # .meta files *may* be ready for reclaim if there is no durable data
        if exts.get('.meta') and not durable_frag_set:
            results.setdefault('possible_reclaim', []).extend(
                exts.get('.meta'))

    def _verify_ondisk_files(self, results, frag_index=None, **kwargs):
        """
        Verify that the final combination of on disk files complies with the
        erasure-coded diskfile contract.

        :param results: files that have been found and accepted
        :param frag_index: specifies a specific fragment index .data file
        :returns: True if the file combination is compliant, False otherwise
        """
        if super(ECDiskFileManager, self)._verify_ondisk_files(
                results, **kwargs):
            have_data_file = results['data_file'] is not None
            have_durable = results.get('durable_frag_set') is not None
            return have_data_file == have_durable
        return False

    def _update_suffix_hashes(self, hashes, ondisk_info):
        """
        Applies policy specific updates to the given dict of md5 hashes for
        the given ondisk_info.

        The only difference between this method and the replication policy
        function is the way that data files update hashes dict. Instead of all
        filenames hashed into a single hasher, each data file name will fall
        into a bucket keyed by its fragment index.

        :param hashes: a dict of md5 hashes to be updated
        :param ondisk_info: a dict describing the state of ondisk files, as
                            returned by get_ondisk_files
        """
        for frag_set in ondisk_info['frag_sets'].values():
            for file_info in frag_set:
                fi = file_info['frag_index']
                hashes[fi].update(file_info['timestamp'].internal)
        if 'durable_frag_set' in ondisk_info:
            file_info = ondisk_info['durable_frag_set'][0]
            hashes[None].update(file_info['timestamp'].internal + '.durable')

    def _hash_suffix(self, path, reclaim_age):
        """
        Performs reclamation and returns an md5 of all (remaining) files.

        :param path: full path to directory
        :param reclaim_age: age in seconds at which to remove tombstones
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        :returns: dict of md5 hex digests
        """
        # hash_per_fi instead of single hash for whole suffix
        # here we flatten out the hashers hexdigest into a dictionary instead
        # of just returning the one hexdigest for the whole suffix

        hash_per_fi = self._hash_suffix_dir(path, reclaim_age)
        return dict((fi, md5.hexdigest()) for fi, md5 in hash_per_fi.items())
