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
import binascii
import copy
import errno
import fcntl
import json
import os
import re
import time
import uuid
from hashlib import md5
import logging
import traceback
import xattr
from os.path import basename, dirname, exists, join, splitext
from random import shuffle
from tempfile import mkstemp
from contextlib import contextmanager
from collections import defaultdict
from datetime import timedelta

from eventlet import Timeout, tpool
from eventlet.hubs import trampoline
import six
from pyeclib.ec_iface import ECDriverError, ECInvalidFragmentMetadata, \
    ECBadFragmentChecksum, ECInvalidParameter

from swift import gettext_ as _
from swift.common.constraints import check_drive
from swift.common.request_helpers import is_sys_meta
from swift.common.utils import mkdirs, Timestamp, \
    storage_directory, hash_path, renamer, fallocate, fsync, fdatasync, \
    fsync_dir, drop_buffer_cache, lock_path, write_pickle, \
    config_true_value, listdir, split_path, remove_file, \
    get_md5_socket, F_SETPIPE_SZ, decode_timestamps, encode_timestamps, \
    MD5_OF_EMPTY_STRING, link_fd_to_path, \
    O_TMPFILE, makedirs_count, replace_partition_in_path, remove_directory
from swift.common.splice import splice, tee
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist, \
    DiskFileCollision, DiskFileNoSpace, DiskFileDeviceUnavailable, \
    DiskFileDeleted, DiskFileError, DiskFileNotOpen, PathNotDir, \
    ReplicationLockTimeout, DiskFileExpired, DiskFileXattrNotSupported, \
    DiskFileBadMetadataChecksum
from swift.common.swob import multi_range_iterator
from swift.common.storage_policy import (
    get_policy_string, split_policy_string, PolicyError, POLICIES,
    REPL_POLICY, EC_POLICY)


PICKLE_PROTOCOL = 2
DEFAULT_RECLAIM_AGE = timedelta(weeks=1).total_seconds()
HASH_FILE = 'hashes.pkl'
HASH_INVALIDATIONS_FILE = 'hashes.invalid'
METADATA_KEY = b'user.swift.metadata'
METADATA_CHECKSUM_KEY = b'user.swift.metadata_checksum'
DROP_CACHE_WINDOW = 1024 * 1024
# These are system-set metadata keys that cannot be changed with a POST.
# They should be lowercase.
RESERVED_DATAFILE_META = {'content-length', 'deleted', 'etag'}
DATAFILE_SYSTEM_META = {'x-static-large-object'}
DATADIR_BASE = 'objects'
ASYNCDIR_BASE = 'async_pending'
TMP_BASE = 'tmp'
MIN_TIME_UPDATE_AUDITOR_STATUS = 60
# This matches rsync tempfiles, like ".<timestamp>.data.Xy095a"
RE_RSYNC_TEMPFILE = re.compile(r'^\..*\.([a-zA-Z0-9_]){6}$')


def get_data_dir(policy_or_index):
    '''
    Get the data dir for the given policy.

    :param policy_or_index: ``StoragePolicy`` instance, or an index (string or
                            int); if None, the legacy Policy-0 is assumed.
    :returns: ``objects`` or ``objects-<N>`` as appropriate
    '''
    return get_policy_string(DATADIR_BASE, policy_or_index)


def get_async_dir(policy_or_index):
    '''
    Get the async dir for the given policy.

    :param policy_or_index: ``StoragePolicy`` instance, or an index (string or
                            int); if None, the legacy Policy-0 is assumed.
    :returns: ``async_pending`` or ``async_pending-<N>`` as appropriate
    '''
    return get_policy_string(ASYNCDIR_BASE, policy_or_index)


def get_tmp_dir(policy_or_index):
    '''
    Get the temp dir for the given policy.

    :param policy_or_index: ``StoragePolicy`` instance, or an index (string or
                            int); if None, the legacy Policy-0 is assumed.
    :returns: ``tmp`` or ``tmp-<N>`` as appropriate
    '''
    return get_policy_string(TMP_BASE, policy_or_index)


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


def _encode_metadata(metadata):
    """
    UTF8 encode any unicode keys or values in given metadata dict.

    :param metadata: a dict
    """
    def encode_str(item):
        if isinstance(item, six.text_type):
            return item.encode('utf8')
        return item

    return dict(((encode_str(k), encode_str(v)) for k, v in metadata.items()))


def _decode_metadata(metadata):
    """
    Given a metadata dict from disk, convert keys and values to native strings.

    :param metadata: a dict
    """
    if six.PY2:
        def to_str(item):
            if isinstance(item, six.text_type):
                return item.encode('utf8')
            return item
    else:
        def to_str(item):
            if isinstance(item, six.binary_type):
                return item.decode('utf8', 'surrogateescape')
            return item

    return dict(((to_str(k), to_str(v)) for k, v in metadata.items()))


def read_metadata(fd, add_missing_checksum=False):
    """
    Helper function to read the pickled metadata from an object file.

    :param fd: file descriptor or filename to load the metadata from
    :param add_missing_checksum: if set and checksum is missing, add it

    :returns: dictionary of metadata
    """
    metadata = b''
    key = 0
    try:
        while True:
            metadata += xattr.getxattr(
                fd, METADATA_KEY + str(key or '').encode('ascii'))
            key += 1
    except (IOError, OSError) as e:
        if errno.errorcode.get(e.errno) in ('ENOTSUP', 'EOPNOTSUPP'):
            msg = "Filesystem at %s does not support xattr"
            logging.exception(msg, _get_filename(fd))
            raise DiskFileXattrNotSupported(e)
        if e.errno == errno.ENOENT:
            raise DiskFileNotExist()
        # TODO: we might want to re-raise errors that don't denote a missing
        # xattr here.  Seems to be ENODATA on linux and ENOATTR on BSD/OSX.

    metadata_checksum = None
    try:
        metadata_checksum = xattr.getxattr(fd, METADATA_CHECKSUM_KEY)
    except (IOError, OSError):
        # All the interesting errors were handled above; the only thing left
        # here is ENODATA / ENOATTR to indicate that this attribute doesn't
        # exist. This is fine; it just means that this object predates the
        # introduction of metadata checksums.
        if add_missing_checksum:
            new_checksum = md5(metadata).hexdigest().encode('ascii')
            try:
                xattr.setxattr(fd, METADATA_CHECKSUM_KEY, new_checksum)
            except (IOError, OSError) as e:
                logging.error("Error adding metadata: %s" % e)

    if metadata_checksum:
        computed_checksum = md5(metadata).hexdigest().encode('ascii')
        if metadata_checksum != computed_checksum:
            raise DiskFileBadMetadataChecksum(
                "Metadata checksum mismatch for %s: "
                "stored checksum='%s', computed='%s'" % (
                    fd, metadata_checksum, computed_checksum))

    # strings are utf-8 encoded when written, but have not always been
    # (see https://bugs.launchpad.net/swift/+bug/1678018) so encode them again
    # when read
    if six.PY2:
        metadata = pickle.loads(metadata)
    else:
        metadata = pickle.loads(metadata, encoding='bytes')
    return _decode_metadata(metadata)


def write_metadata(fd, metadata, xattr_size=65536):
    """
    Helper function to write pickled metadata for an object file.

    :param fd: file descriptor or filename to write the metadata
    :param metadata: metadata to write
    """
    metastr = pickle.dumps(_encode_metadata(metadata), PICKLE_PROTOCOL)
    metastr_md5 = md5(metastr).hexdigest().encode('ascii')
    key = 0
    try:
        while metastr:
            xattr.setxattr(fd, METADATA_KEY + str(key or '').encode('ascii'),
                           metastr[:xattr_size])
            metastr = metastr[xattr_size:]
            key += 1
        xattr.setxattr(fd, METADATA_CHECKSUM_KEY, metastr_md5)
    except IOError as e:
        # errno module doesn't always have both of these, hence the ugly
        # check
        if errno.errorcode.get(e.errno) in ('ENOTSUP', 'EOPNOTSUPP'):
            msg = "Filesystem at %s does not support xattr"
            logging.exception(msg, _get_filename(fd))
            raise DiskFileXattrNotSupported(e)
        elif e.errno in (errno.ENOSPC, errno.EDQUOT):
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

    /srv/node/d42/objects-5/30/179/
        485dc017205a81df3af616d917c90179/1401811134.873649.data

    would have device-relative path:

    objects-5/30/179/485dc017205a81df3af616d917c90179/1401811134.873649.data

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


def read_hashes(partition_dir):
    """
    Read the existing hashes.pkl

    :returns: a dict, the suffix hashes (if any), the key 'valid' will be False
              if hashes.pkl is corrupt, cannot be read or does not exist
    """
    hashes_file = join(partition_dir, HASH_FILE)
    hashes = {'valid': False}
    try:
        with open(hashes_file, 'rb') as hashes_fp:
            pickled_hashes = hashes_fp.read()
    except (IOError, OSError):
        pass
    else:
        try:
            hashes = pickle.loads(pickled_hashes)
        except Exception:
            # pickle.loads() can raise a wide variety of exceptions when
            # given invalid input depending on the way in which the
            # input is invalid.
            pass
    # hashes.pkl w/o valid updated key is "valid" but "forever old"
    hashes.setdefault('valid', True)
    hashes.setdefault('updated', -1)
    return hashes


def write_hashes(partition_dir, hashes):
    """
    Write hashes to hashes.pkl

    The updated key is added to hashes before it is written.
    """
    hashes_file = join(partition_dir, HASH_FILE)
    # 'valid' key should always be set by the caller; however, if there's a bug
    # setting invalid is most safe
    hashes.setdefault('valid', False)
    hashes['updated'] = time.time()
    write_pickle(hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)


def consolidate_hashes(partition_dir):
    """
    Take what's in hashes.pkl and hashes.invalid, combine them, write the
    result back to hashes.pkl, and clear out hashes.invalid.

    :param partition_dir: absolute path to partition dir containing hashes.pkl
                          and hashes.invalid

    :returns: a dict, the suffix hashes (if any), the key 'valid' will be False
              if hashes.pkl is corrupt, cannot be read or does not exist
    """
    invalidations_file = join(partition_dir, HASH_INVALIDATIONS_FILE)

    with lock_path(partition_dir):
        hashes = read_hashes(partition_dir)

        found_invalidation_entry = False
        try:
            with open(invalidations_file, 'rb') as inv_fh:
                for line in inv_fh:
                    found_invalidation_entry = True
                    suffix = line.strip()
                    hashes[suffix] = None
        except (IOError, OSError) as e:
            if e.errno != errno.ENOENT:
                raise

        if found_invalidation_entry:
            write_hashes(partition_dir, hashes)
            # Now that all the invalidations are reflected in hashes.pkl, it's
            # safe to clear out the invalidations file.
            with open(invalidations_file, 'wb') as inv_fh:
                pass

        return hashes


def invalidate_hash(suffix_dir):
    """
    Invalidates the hash for a suffix_dir in the partition's hashes file.

    :param suffix_dir: absolute path to suffix dir whose hash needs
                       invalidating
    """

    suffix = basename(suffix_dir)
    partition_dir = dirname(suffix_dir)
    invalidations_file = join(partition_dir, HASH_INVALIDATIONS_FILE)
    if not isinstance(suffix, bytes):
        suffix = suffix.encode('utf-8')
    with lock_path(partition_dir), open(invalidations_file, 'ab') as inv_fh:
        inv_fh.write(suffix + b"\n")


def relink_paths(target_path, new_target_path, check_existing=False):
    """
    Hard-links a file located in target_path using the second path
    new_target_path. Creates intermediate directories if required.

    :param target_path: current absolute filename
    :param new_target_path: new absolute filename for the hardlink
    :param check_existing: if True, check whether the link is already present
                           before attempting to create a new one
    """

    if target_path != new_target_path:
        logging.debug('Relinking %s to %s due to next_part_power set',
                      target_path, new_target_path)
        new_target_dir = os.path.dirname(new_target_path)
        if not os.path.isdir(new_target_dir):
            os.makedirs(new_target_dir)

        link_exists = False
        if check_existing:
            try:
                new_stat = os.stat(new_target_path)
                orig_stat = os.stat(target_path)
                link_exists = (new_stat.st_ino == orig_stat.st_ino)
            except OSError:
                pass  # if anything goes wrong, try anyway

        if not link_exists:
            os.link(target_path, new_target_path)


def get_part_path(dev_path, policy, partition):
    """
    Given the device path, policy, and partition, returns the full
    path to the partition
    """
    return os.path.join(dev_path, get_data_dir(policy), str(partition))


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


def object_audit_location_generator(devices, datadir, mount_check=True,
                                    logger=None, device_dirs=None,
                                    auditor_type="ALL"):
    """
    Given a devices path (e.g. "/srv/node"), yield an AuditLocation for all
    objects stored under that directory for the given datadir (policy),
    if device_dirs isn't set.  If device_dirs is set, only yield AuditLocation
    for the objects under the entries in device_dirs. The AuditLocation only
    knows the path to the hash directory, not to the .data file therein
    (if any). This is to avoid a double listdir(hash_dir); the DiskFile object
    will always do one, so we don't.

    :param devices: parent directory of the devices to be audited
    :param datadir: objects directory
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

    base, policy = split_policy_string(datadir)
    for device in device_dirs:
        try:
            check_drive(devices, device, mount_check)
        except ValueError as err:
            if logger:
                logger.debug('Skipping: %s', err)
            continue

        datadir_path = os.path.join(devices, device, datadir)
        if not os.path.exists(datadir_path):
            continue

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
        if six.PY3:
            statusfile = open(auditor_status, encoding='utf8')
        else:
            statusfile = open(auditor_status, 'rb')
        with statusfile:
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
    if six.PY3:
        status = status.encode('utf8')
    auditor_status = os.path.join(
        datadir_path, "auditor_status_%s.json" % auditor_type)
    try:
        mtime = os.stat(auditor_status).st_mtime
    except OSError:
        mtime = 0
    recently_updated = (mtime + MIN_TIME_UPDATE_AUDITOR_STATUS) > time.time()
    if recently_updated and len(partitions) > 0:
        if logger:
            logger.debug(
                'Skipping the update of recently changed %s' % auditor_status)
        return
    try:
        with open(auditor_status, "wb") as statusfile:
            statusfile.write(status)
    except (OSError, IOError) as e:
        if logger:
            logger.warning(_('Cannot write %(auditor_status)s (%(err)s)') %
                           {'auditor_status': auditor_status, 'err': e})


def clear_auditor_status(devices, datadir, auditor_type="ALL"):
    device_dirs = listdir(devices)
    for device in device_dirs:
        datadir_path = os.path.join(devices, device, datadir)
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

    def __init__(self, *args, **kwargs):
        self.policy_to_manager = {}
        for policy in POLICIES:
            # create diskfile managers now to provoke any errors
            self.policy_to_manager[int(policy)] = \
                policy.get_diskfile_manager(*args, **kwargs)

    def __getitem__(self, policy):
        return self.policy_to_manager[int(policy)]


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
    policy = None  # must be set by subclasses

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
        self.reclaim_age = int(conf.get('reclaim_age', DEFAULT_RECLAIM_AGE))
        replication_concurrency_per_device = conf.get(
            'replication_concurrency_per_device')
        replication_one_per_device = conf.get('replication_one_per_device')
        if replication_concurrency_per_device is None \
                and replication_one_per_device is not None:
            self.logger.warning('Option replication_one_per_device is '
                                'deprecated and will be removed in a future '
                                'version. Update your configuration to use '
                                'option replication_concurrency_per_device.')
            if config_true_value(replication_one_per_device):
                replication_concurrency_per_device = 1
            else:
                replication_concurrency_per_device = 0
        elif replication_one_per_device is not None:
            self.logger.warning('Option replication_one_per_device ignored as '
                                'replication_concurrency_per_device is '
                                'defined.')
        if replication_concurrency_per_device is None:
            self.replication_concurrency_per_device = 1
        else:
            self.replication_concurrency_per_device = int(
                replication_concurrency_per_device)
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
        self.use_linkat = True

    @classmethod
    def check_policy(cls, policy):
        if policy.policy_type != cls.policy:
            raise ValueError('Invalid policy_type: %s' % policy.policy_type)

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

    def parse_on_disk_filename(self, filename, policy):
        """
        Parse an on disk file name.

        :param filename: the file name including extension
        :param policy: storage policy used to store the file
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

    def get_ondisk_files(self, files, datadir, verify=True, policy=None,
                         **kwargs):
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
        :param policy: storage policy used to store the files. Used to
                       validate fragment indexes for EC policies.
        :returns: a dict that will contain keys:
                    ts_file   -> path to a .ts file or None
                    data_file -> path to a .data file or None
                    meta_file -> path to a .meta file or None
                    ctype_file -> path to a .meta file or None
                  and may contain keys:
                    ts_info   -> a file info dict for a .ts file
                    data_info -> a file info dict for a .data file
                    meta_info -> a file info dict for a .meta file
                    ctype_info -> a file info dict for a .meta file which
                    contains the content-type value
                    unexpected -> a list of file paths for unexpected
                    files
                    possible_reclaim -> a list of file info dicts for possible
                    reclaimable files
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
                file_info = self.parse_on_disk_filename(afile, policy)
                file_info['filename'] = afile
                exts[file_info['ext']].append(file_info)
            except DiskFileError as e:
                file_path = os.path.join(datadir or '', afile)
                results.setdefault('unexpected', []).append(file_path)
                # log warnings if it's not a rsync temp file
                if RE_RSYNC_TEMPFILE.match(afile):
                    self.logger.debug('Rsync tempfile: %s', file_path)
                else:
                    self.logger.warning('Unexpected file %s: %s',
                                        file_path, e)
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
        if 'data_info' in results:
            if exts.get('.meta'):
                # only report a meta file if a data file has been chosen
                results['meta_info'] = exts['.meta'][0]
                ctype_info = exts['.meta'].pop()
                if (ctype_info['ctype_timestamp']
                        > results['data_info']['timestamp']):
                    results['ctype_info'] = ctype_info
        elif exts.get('.ts'):
            # only report a ts file if a data file has not been chosen
            # (ts files will commonly already have been removed from exts if
            # a data file was chosen, but that may not be the case if
            # non-durable EC fragment(s) were chosen, hence the elif here)
            results['ts_info'] = exts['.ts'][0]

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

    def cleanup_ondisk_files(self, hsh_path, **kwargs):
        """
        Clean up on-disk files that are obsolete and gather the set of valid
        on-disk files for an object.

        :param hsh_path: object hash path
        :param frag_index: if set, search for a specific fragment index .data
                           file, otherwise accept the first valid .data file
        :returns: a dict that may contain: valid on disk files keyed by their
                  filename extension; a list of obsolete files stored under the
                  key 'obsolete'; a list of files remaining in the directory,
                  reverse sorted, stored under the key 'files'.
        """
        def is_reclaimable(timestamp):
            return (time.time() - float(timestamp)) > self.reclaim_age

        try:
            files = os.listdir(hsh_path)
        except OSError as err:
            if err.errno == errno.ENOENT:
                results = self.get_ondisk_files(
                    [], hsh_path, verify=False, **kwargs)
                results['files'] = []
                return results
            else:
                raise

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
        if not files:  # everything got unlinked
            try:
                os.rmdir(hsh_path)
            except OSError as err:
                if err.errno not in (errno.ENOENT, errno.ENOTEMPTY):
                    self.logger.debug(
                        'Error cleaning up empty hash directory %s: %s',
                        hsh_path, err)
                # else, no real harm; pass
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

    def _hash_suffix_dir(self, path, policy):
        """

        :param path: full path to directory
        :param policy: storage policy used
        """
        if six.PY2:
            hashes = defaultdict(md5)
        else:
            class shim(object):
                def __init__(self):
                    self.md5 = md5()

                def update(self, s):
                    if isinstance(s, str):
                        self.md5.update(s.encode('utf-8'))
                    else:
                        self.md5.update(s)

                def hexdigest(self):
                    return self.md5.hexdigest()
            hashes = defaultdict(shim)
        try:
            path_contents = sorted(os.listdir(path))
        except OSError as err:
            if err.errno in (errno.ENOTDIR, errno.ENOENT):
                raise PathNotDir()
            raise
        for hsh in path_contents:
            hsh_path = join(path, hsh)
            try:
                ondisk_info = self.cleanup_ondisk_files(
                    hsh_path, policy=policy)
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

    def _hash_suffix(self, path, policy=None):
        """
        Performs reclamation and returns an md5 of all (remaining) files.

        :param path: full path to directory
        :param policy: storage policy used to store the files
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        """
        raise NotImplementedError

    def _get_hashes(self, *args, **kwargs):
        hashed, hashes = self.__get_hashes(*args, **kwargs)
        hashes.pop('updated', None)
        hashes.pop('valid', None)
        return hashed, hashes

    def __get_hashes(self, device, partition, policy, recalculate=None,
                     do_listdir=False):
        """
        Get hashes for each suffix dir in a partition.  do_listdir causes it to
        mistrust the hash cache for suffix existence at the (unexpectedly high)
        cost of a listdir.

        :param device: name of target device
        :param partition: partition on the device in which the object lives
        :param policy: the StoragePolicy instance
        :param recalculate: list of suffixes which should be recalculated when
                            got
        :param do_listdir: force existence check for all hashes in the
                           partition

        :returns: tuple of (number of suffix dirs hashed, dictionary of hashes)
        """
        hashed = 0
        dev_path = self.get_dev_path(device)
        partition_path = get_part_path(dev_path, policy, partition)
        hashes_file = join(partition_path, HASH_FILE)
        modified = False
        orig_hashes = {'valid': False}

        if recalculate is None:
            recalculate = []

        try:
            orig_hashes = self.consolidate_hashes(partition_path)
        except Exception:
            self.logger.warning('Unable to read %r', hashes_file,
                                exc_info=True)

        if not orig_hashes['valid']:
            # This is the only path to a valid hashes from invalid read (e.g.
            # does not exist, corrupt, etc.).  Moreover, in order to write this
            # valid hashes we must read *the exact same* invalid state or we'll
            # trigger race detection.
            do_listdir = True
            hashes = {'valid': True}
            # If the exception handling around consolidate_hashes fired we're
            # going to do a full rehash regardless; but we need to avoid
            # needless recursion if the on-disk hashes.pkl is actually readable
            # (worst case is consolidate_hashes keeps raising exceptions and we
            # eventually run out of stack).
            # N.B. orig_hashes invalid only effects new parts and error/edge
            # conditions - so try not to get overly caught up trying to
            # optimize it out unless you manage to convince yourself there's a
            # bad behavior.
            orig_hashes = read_hashes(partition_path)
        else:
            hashes = copy.deepcopy(orig_hashes)

        if do_listdir:
            for suff in os.listdir(partition_path):
                if len(suff) == 3:
                    hashes.setdefault(suff, None)
            modified = True
            self.logger.debug('Run listdir on %s', partition_path)
        hashes.update((suffix, None) for suffix in recalculate)
        for suffix, hash_ in list(hashes.items()):
            if not hash_:
                suffix_dir = join(partition_path, suffix)
                try:
                    hashes[suffix] = self._hash_suffix(
                        suffix_dir, policy=policy)
                    hashed += 1
                except PathNotDir:
                    del hashes[suffix]
                except OSError:
                    logging.exception(_('Error hashing suffix'))
                modified = True
        if modified:
            with lock_path(partition_path):
                if read_hashes(partition_path) == orig_hashes:
                    write_hashes(partition_path, hashes)
                    return hashed, hashes
            return self.__get_hashes(device, partition, policy,
                                     recalculate=recalculate,
                                     do_listdir=do_listdir)
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
        if mount_check is False:
            # explicitly forbidden from syscall, just return path
            return join(self.devices, device)
        # we'll do some kind of check if not explicitly forbidden
        try:
            return check_drive(self.devices, device,
                               mount_check or self.mount_check)
        except ValueError:
            return None

    @contextmanager
    def replication_lock(self, device, policy, partition):
        """
        A context manager that will lock on the device given, if
        configured to do so.

        :param device: name of target device
        :param policy: policy targeted by the replication request
        :param partition: partition targeted by the replication request
        :raises ReplicationLockTimeout: If the lock on the device
            cannot be granted within the configured timeout.
        """
        if self.replication_concurrency_per_device:
            dev_path = self.get_dev_path(device)
            part_path = os.path.join(dev_path, get_data_dir(policy),
                                     str(partition))
            limit_time = time.time() + self.replication_lock_timeout
            with lock_path(
                    dev_path,
                    timeout=self.replication_lock_timeout,
                    timeout_class=ReplicationLockTimeout,
                    limit=self.replication_concurrency_per_device):
                with lock_path(
                        part_path,
                        timeout=limit_time - time.time(),
                        timeout_class=ReplicationLockTimeout,
                        limit=1,
                        name='replication'):
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
        tmp_dir = os.path.join(device_path, get_tmp_dir(policy))
        mkdirs(tmp_dir)
        ohash = hash_path(account, container, obj)
        write_pickle(
            data,
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                         Timestamp(timestamp).internal),
            tmp_dir)
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

    def clear_auditor_status(self, policy, auditor_type="ALL"):
        datadir = get_data_dir(policy)
        clear_auditor_status(self.devices, datadir, auditor_type)

    def object_audit_location_generator(self, policy, device_dirs=None,
                                        auditor_type="ALL"):
        """
        Yield an AuditLocation for all objects stored under device_dirs.

        :param policy: the StoragePolicy instance
        :param device_dirs: directory of target device
        :param auditor_type: either ALL or ZBF
        """
        datadir = get_data_dir(policy)
        return object_audit_location_generator(self.devices, datadir,
                                               self.mount_check,
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
            filenames = self.cleanup_ondisk_files(object_path)['files']
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
        partition_path = get_part_path(dev_path, policy, partition)
        if not os.path.exists(partition_path):
            mkdirs(partition_path)
        _junk, hashes = tpool.execute(
            self._get_hashes, device, partition, policy, recalculate=suffixes)
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
        partition_path = get_part_path(dev_path, policy, partition)
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
        Yields tuples of (hash_only, timestamps) for object
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

        partition_path = get_part_path(dev_path, policy, partition)
        if suffixes is None:
            suffixes = self.yield_suffixes(device, partition, policy)
        else:
            suffixes = (
                (os.path.join(partition_path, suffix), suffix)
                for suffix in suffixes)

        key_preference = (
            ('ts_meta', 'meta_info', 'timestamp'),
            ('ts_data', 'data_info', 'timestamp'),
            ('ts_data', 'ts_info', 'timestamp'),
            ('ts_ctype', 'ctype_info', 'ctype_timestamp'),
        )

        # cleanup_ondisk_files() will remove empty hash dirs, and we'll
        # invalidate any empty suffix dirs so they'll get cleaned up on
        # the next rehash
        for suffix_path, suffix in suffixes:
            found_files = False
            for object_hash in self._listdir(suffix_path):
                object_path = os.path.join(suffix_path, object_hash)
                try:
                    results = self.cleanup_ondisk_files(
                        object_path, **kwargs)
                    if results['files']:
                        found_files = True
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
                    yield (object_hash, timestamps)
                except AssertionError as err:
                    self.logger.debug('Invalid file set in %s (%s)' % (
                        object_path, err))
                except DiskFileError as err:
                    self.logger.debug(
                        'Invalid diskfile filename in %r (%s)' % (
                            object_path, err))

            if not found_files:
                self.invalidate_hash(suffix_path)


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
    :param next_part_power: the next partition power to be used
    """

    def __init__(self, name, datadir, size, bytes_per_sync, diskfile,
                 next_part_power):
        # Parameter tracking
        self._name = name
        self._datadir = datadir
        self._fd = None
        self._tmppath = None
        self._size = size
        self._chunks_etag = md5()
        self._bytes_per_sync = bytes_per_sync
        self._diskfile = diskfile
        self.next_part_power = next_part_power

        # Internal attributes
        self._upload_size = 0
        self._last_sync = 0
        self._extension = '.data'
        self._put_succeeded = False

    @property
    def manager(self):
        return self._diskfile.manager

    @property
    def logger(self):
        return self.manager.logger

    def _get_tempfile(self):
        tmppath = None
        if self.manager.use_linkat:
            self._dirs_created = makedirs_count(self._datadir)
            try:
                fd = os.open(self._datadir, O_TMPFILE | os.O_WRONLY)
            except OSError as err:
                if err.errno in (errno.EOPNOTSUPP, errno.EISDIR, errno.EINVAL):
                    msg = 'open(%s, O_TMPFILE | O_WRONLY) failed: %s \
                           Falling back to using mkstemp()' \
                           % (self._datadir, os.strerror(err.errno))
                    self.logger.warning(msg)
                    self.manager.use_linkat = False
                else:
                    raise
        if not self.manager.use_linkat:
            tmpdir = join(self._diskfile._device_path,
                          get_tmp_dir(self._diskfile.policy))
            if not exists(tmpdir):
                mkdirs(tmpdir)
            fd, tmppath = mkstemp(dir=tmpdir)
        return fd, tmppath

    def open(self):
        if self._fd is not None:
            raise ValueError('DiskFileWriter is already open')

        try:
            self._fd, self._tmppath = self._get_tempfile()
        except OSError as err:
            if err.errno in (errno.ENOSPC, errno.EDQUOT):
                # No more inodes in filesystem
                raise DiskFileNoSpace()
            raise
        if self._size is not None and self._size > 0:
            try:
                fallocate(self._fd, self._size)
            except OSError as err:
                if err.errno in (errno.ENOSPC, errno.EDQUOT):
                    raise DiskFileNoSpace()
                raise
        return self

    def close(self):
        if self._fd:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        if self._tmppath and not self._put_succeeded:
            # Try removing the temp file only if put did NOT succeed.
            #
            # dfw.put_succeeded is set to True after renamer() succeeds in
            # DiskFileWriter._finalize_put()
            try:
                # when mkstemp() was used
                os.unlink(self._tmppath)
            except OSError:
                self.logger.exception('Error removing tempfile: %s' %
                                      self._tmppath)
            self._tmppath = None

    def write(self, chunk):
        """
        Write a chunk of data to disk. All invocations of this method must
        come before invoking the :func:

        For this implementation, the data is written into a temporary file.

        :param chunk: the chunk of data to write as a string object
        """
        if not self._fd:
            raise ValueError('Writer is not open')
        self._chunks_etag.update(chunk)
        while chunk:
            written = os.write(self._fd, chunk)
            self._upload_size += written
            chunk = chunk[written:]

        # For large files sync every 512MB (by default) written
        diff = self._upload_size - self._last_sync
        if diff >= self._bytes_per_sync:
            tpool.execute(fdatasync, self._fd)
            drop_buffer_cache(self._fd, self._last_sync, diff)
            self._last_sync = self._upload_size

    def chunks_finished(self):
        """
        Expose internal stats about written chunks.

        :returns: a tuple, (upload_size, etag)
        """
        return self._upload_size, self._chunks_etag.hexdigest()

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
        # After the rename/linkat completes, this object will be available for
        # requests to reference.
        if self._tmppath:
            # It was a named temp file created by mkstemp()
            renamer(self._tmppath, target_path)
        else:
            # It was an unnamed temp file created by open() with O_TMPFILE
            link_fd_to_path(self._fd, target_path,
                            self._diskfile._dirs_created)

        # Check if the partition power will/has been increased
        new_target_path = None
        if self.next_part_power:
            new_target_path = replace_partition_in_path(
                target_path, self.next_part_power)
            if target_path != new_target_path:
                try:
                    fsync_dir(os.path.dirname(target_path))
                    relink_paths(target_path, new_target_path)
                except OSError as exc:
                    self.manager.logger.exception(
                        'Relinking %s to %s failed: %s',
                        target_path, new_target_path, exc)

        # If rename is successful, flag put as succeeded. This is done to avoid
        # unnecessary os.unlink() of tempfile later. As renamer() has
        # succeeded, the tempfile would no longer exist at its original path.
        self._put_succeeded = True
        if cleanup:
            try:
                self.manager.cleanup_ondisk_files(self._datadir)
            except OSError:
                logging.exception(_('Problem cleaning up %s'), self._datadir)

            self._part_power_cleanup(target_path, new_target_path)

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

        tpool.execute(self._finalize_put, metadata, target_path, cleanup)

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

    def _part_power_cleanup(self, cur_path, new_path):
        """
        Cleanup relative DiskFile directories.

        If the partition power is increased soon or has just been increased but
        the relinker didn't yet cleanup the old files, an additional cleanup of
        the relative dirs has to be done. Otherwise there might be some unused
        files left if a PUT or DELETE is done in the meantime
        :param cur_path: current full path to an object file
        :param new_path: recomputed path to an object file, based on the
                         next_part_power set in the ring

        """
        if new_path is None:
            return

        # Partition power will be increased soon
        if new_path != cur_path:
            new_target_dir = os.path.dirname(new_path)
            try:
                self.manager.cleanup_ondisk_files(new_target_dir)
            except OSError:
                logging.exception(
                    _('Problem cleaning up %s'), new_target_dir)

        # Partition power has been increased, cleanup not yet finished
        else:
            prev_part_power = int(self.next_part_power) - 1
            old_target_path = replace_partition_in_path(
                cur_path, prev_part_power)
            old_target_dir = os.path.dirname(old_target_path)
            try:
                self.manager.cleanup_ondisk_files(old_target_dir)
            except OSError:
                logging.exception(
                    _('Problem cleaning up %s'), old_target_dir)


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

    def _init_checks(self):
        if self._fp.tell() == 0:
            self._started_at_0 = True
            self._iter_etag = md5()

    def _update_checks(self, chunk):
        if self._iter_etag:
            self._iter_etag.update(chunk)

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            dropped_cache = 0
            self._bytes_read = 0
            self._started_at_0 = False
            self._read_to_eof = False
            self._init_checks()
            while True:
                chunk = self._fp.read(self._disk_chunk_size)
                if chunk:
                    self._update_checks(chunk)
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
                hex_checksum = binascii.hexlify(bin_checksum).decode('ascii')
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
    :param open_expired: if True, open() will not raise a DiskFileExpired if
                         object is expired
    :param next_part_power: the next partition power to be used
    """
    reader_cls = None  # must be set by subclasses
    writer_cls = None  # must be set by subclasses

    def __init__(self, mgr, device_path, partition,
                 account=None, container=None, obj=None, _datadir=None,
                 policy=None, use_splice=False, pipe_size=None,
                 open_expired=False, next_part_power=None, **kwargs):
        self._manager = mgr
        self._device_path = device_path
        self._logger = mgr.logger
        self._disk_chunk_size = mgr.disk_chunk_size
        self._bytes_per_sync = mgr.bytes_per_sync
        self._use_splice = use_splice
        self._pipe_size = pipe_size
        self._open_expired = open_expired
        # This might look a lttle hacky i.e tracking number of newly created
        # dirs to fsync only those many later. If there is a better way,
        # please suggest.
        # Or one could consider getting rid of doing fsyncs on dirs altogether
        # and mounting XFS with the 'dirsync' mount option which should result
        # in all entry fops being carried out synchronously.
        self._dirs_created = 0
        self.policy = policy
        self.next_part_power = next_part_power
        if account and container and obj:
            self._name = '/' + '/'.join((account, container, obj))
            self._account = account
            self._container = container
            self._obj = obj
        else:
            # gets populated when we read the metadata
            self._name = None
            self._account = None
            self._container = None
            self._obj = None
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

    def open(self, modernize=False, current_time=None):
        """
        Open the object.

        This implementation opens the data file representing the object, reads
        the associated metadata in the extended attributes, additionally
        combining metadata from fast-POST `.meta` files.

        :param modernize: if set, update this diskfile to the latest format.
             Currently, this means adding metadata checksums if none are
             present.

        :param current_time: Unix time used in checking expiration. If not
             present, the current time will be used.

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
        file_info = self._get_ondisk_files(files, self.policy)

        self._data_file = file_info.get('data_file')
        if not self._data_file:
            raise self._construct_exception_from_ts_file(**file_info)
        self._fp = self._construct_from_data_file(
            current_time=current_time, modernize=modernize, **file_info)
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

    def _get_ondisk_files(self, files, policy=None):
        """
        Determine the on-disk files to use.

        :param files: a list of files in the object's dir
        :param policy: storage policy used to store the files
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

    def _verify_data_file(self, data_file, fp, current_time):
        """
        Verify the metadata's name value matches what we think the object is
        named.

        :param data_file: data file name being consider, used when quarantines
                          occur
        :param fp: open file pointer so that we can `fstat()` the file to
                   verify the on-disk size with Content-Length metadata value
        :param current_time: Unix time used in checking expiration
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
            if current_time is None:
                current_time = time.time()
            if x_delete_at <= current_time and not self._open_expired:
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

    def _failsafe_read_metadata(self, source, quarantine_filename=None,
                                add_missing_checksum=False):
        """
        Read metadata from source object file. In case of failure, quarantine
        the file.

        Takes source and filename separately so we can read from an open
        file if we have one.

        :param source: file descriptor or filename to load the metadata from
        :param quarantine_filename: full path of file to load the metadata from
        :param add_missing_checksum: if True and no metadata checksum is
            present, generate one and write it down
        """
        try:
            return read_metadata(source, add_missing_checksum)
        except (DiskFileXattrNotSupported, DiskFileNotExist):
            raise
        except DiskFileBadMetadataChecksum as err:
            raise self._quarantine(quarantine_filename, str(err))
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
                                  current_time, modernize=False,
                                  **kwargs):
        """
        Open the `.data` file to fetch its metadata, and fetch the metadata
        from fast-POST `.meta` files as well if any exist, merging them
        properly.

        :param data_file: on-disk `.data` file being considered
        :param meta_file: on-disk fast-POST `.meta` file being considered
        :param ctype_file: on-disk fast-POST `.meta` file being considered that
                           contains content-type and content-type timestamp
        :param current_time: Unix time used in checking expiration
        :param modernize: whether to update the on-disk files to the newest
                          format
        :returns: an opened data file pointer
        :raises DiskFileError: various exceptions from
                    :func:`swift.obj.diskfile.DiskFile._verify_data_file`
        """
        try:
            fp = open(data_file, 'rb')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise DiskFileNotExist()
            raise
        self._datafile_metadata = self._failsafe_read_metadata(
            fp, data_file,
            add_missing_checksum=modernize)
        self._metadata = {}
        if meta_file:
            self._metafile_metadata = self._failsafe_read_metadata(
                meta_file, meta_file,
                add_missing_checksum=modernize)
            if ctype_file and ctype_file != meta_file:
                self._merge_content_type_metadata(ctype_file)
            sys_metadata = dict(
                [(key, val) for key, val in self._datafile_metadata.items()
                 if key.lower() in (RESERVED_DATAFILE_META |
                                    DATAFILE_SYSTEM_META)
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
        self._verify_data_file(data_file, fp, current_time)
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

    def read_metadata(self, current_time=None):
        """
        Return the metadata for an object without requiring the caller to open
        the object first.

        :param current_time: Unix time used in checking expiration. If not
             present, the current time will be used.
        :returns: metadata dictionary for an object
        :raises DiskFileError: this implementation will raise the same
                            errors as the `open()` method.
        """
        with self.open(current_time=current_time):
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

    def writer(self, size=None):
        return self.writer_cls(self._name, self._datadir, size,
                               self._bytes_per_sync, self,
                               self.next_part_power)

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
        dfw = self.writer(size)
        try:
            yield dfw.open()
        finally:
            dfw.close()

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

    def _get_ondisk_files(self, files, policy=None):
        self._ondisk_info = self.manager.get_ondisk_files(
            files, self._datadir, policy=policy)
        return self._ondisk_info


class DiskFileManager(BaseDiskFileManager):
    diskfile_cls = DiskFile
    policy = REPL_POLICY

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

    def _hash_suffix(self, path, policy=None):
        """
        Performs reclamation and returns an md5 of all (remaining) files.

        :param path: full path to directory
        :param policy: storage policy used to store the files
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        :returns: md5 of files in suffix
        """
        hashes = self._hash_suffix_dir(path, policy)
        return hashes[None].hexdigest()


class ECDiskFileReader(BaseDiskFileReader):
    def __init__(self, fp, data_file, obj_size, etag,
                 disk_chunk_size, keep_cache_size, device_path, logger,
                 quarantine_hook, use_splice, pipe_size, diskfile,
                 keep_cache=False):
        super(ECDiskFileReader, self).__init__(
            fp, data_file, obj_size, etag,
            disk_chunk_size, keep_cache_size, device_path, logger,
            quarantine_hook, use_splice, pipe_size, diskfile, keep_cache)
        self.frag_buf = None
        self.frag_offset = 0
        self.frag_size = self._diskfile.policy.fragment_size

    def _init_checks(self):
        super(ECDiskFileReader, self)._init_checks()
        # for a multi-range GET this will be called at the start of each range;
        # only initialise the frag_buf for reads starting at 0.
        # TODO: reset frag buf to '' if tell() shows that start is on a frag
        # boundary so that we check frags selected by a range not starting at 0
        if self._started_at_0:
            self.frag_buf = b''
        else:
            self.frag_buf = None

    def _check_frag(self, frag):
        if not frag:
            return
        if not isinstance(frag, six.binary_type):
            # ECInvalidParameter can be returned if the frag violates the input
            # format so for safety, check the input chunk if it's binary to
            # avoid quarantining a valid fragment archive.
            self._diskfile._logger.warn(
                _('Unexpected fragment data type (not quarantined) '
                  '%(datadir)s: %(type)s at offset 0x%(offset)x'),
                {'datadir': self._diskfile._datadir,
                 'type': type(frag),
                 'offset': self.frag_offset})
            return

        try:
            self._diskfile.policy.pyeclib_driver.get_metadata(frag)
        except (ECInvalidFragmentMetadata, ECBadFragmentChecksum,
                ECInvalidParameter):
            # Any of these exceptions may be returned from ECDriver with a
            # corrupted fragment.
            msg = 'Invalid EC metadata at offset 0x%x' % self.frag_offset
            self._quarantine(msg)
            # We have to terminate the response iter with an exception but it
            # can't be StopIteration, this will produce a STDERR traceback in
            # eventlet.wsgi if you have eventlet_debug turned on; but any
            # attempt to finish the iterator cleanly won't trigger the needful
            # error handling cleanup - failing to do so, and yet still failing
            # to deliver all promised bytes will hang the HTTP connection
            raise DiskFileQuarantined(msg)
        except ECDriverError as err:
            self._diskfile._logger.warn(
                _('Problem checking EC fragment %(datadir)s: %(err)s'),
                {'datadir': self._diskfile._datadir, 'err': err})

    def _update_checks(self, chunk):
        super(ECDiskFileReader, self)._update_checks(chunk)
        if self.frag_buf is not None:
            self.frag_buf += chunk
            cursor = 0
            while len(self.frag_buf) >= cursor + self.frag_size:
                self._check_frag(self.frag_buf[cursor:cursor + self.frag_size])
                cursor += self.frag_size
                self.frag_offset += self.frag_size
            if cursor:
                self.frag_buf = self.frag_buf[cursor:]

    def _handle_close_quarantine(self):
        super(ECDiskFileReader, self)._handle_close_quarantine()
        self._check_frag(self.frag_buf)


class ECDiskFileWriter(BaseDiskFileWriter):

    def _finalize_durable(self, data_file_path, durable_data_file_path):
        exc = None
        new_data_file_path = new_durable_data_file_path = None
        if self.next_part_power:
            new_data_file_path = replace_partition_in_path(
                data_file_path, self.next_part_power)
            new_durable_data_file_path = replace_partition_in_path(
                durable_data_file_path, self.next_part_power)
        try:
            try:
                os.rename(data_file_path, durable_data_file_path)
                fsync_dir(self._datadir)
                if self.next_part_power and \
                        data_file_path != new_data_file_path:
                    try:
                        os.rename(new_data_file_path,
                                  new_durable_data_file_path)
                    except OSError as exc:
                        self.manager.logger.exception(
                            'Renaming new path %s to %s failed: %s',
                            new_data_file_path, new_durable_data_file_path,
                            exc)

            except (OSError, IOError) as err:
                if err.errno not in (errno.ENOSPC, errno.EDQUOT):
                    # re-raise to catch all handler
                    raise
                params = {'file': durable_data_file_path, 'err': err}
                self.manager.logger.exception(
                    _('No space left on device for %(file)s (%(err)s)'),
                    params)
                exc = DiskFileNoSpace(
                    'No space left on device for %(file)s (%(err)s)' % params)
            else:
                try:
                    self.manager.cleanup_ondisk_files(self._datadir)
                except OSError as os_err:
                    self.manager.logger.exception(
                        _('Problem cleaning up %(datadir)s (%(err)s)'),
                        {'datadir': self._datadir, 'err': os_err})
                self._part_power_cleanup(
                    durable_data_file_path, new_durable_data_file_path)

        except Exception as err:
            params = {'file': durable_data_file_path, 'err': err}
            self.manager.logger.exception(
                _('Problem making data file durable %(file)s (%(err)s)'),
                params)
            exc = DiskFileError(
                'Problem making data file durable %(file)s (%(err)s)' % params)
        if exc:
            raise exc

    def commit(self, timestamp):
        """
        Finalize put by renaming the object data file to include a durable
        marker. We do this for EC policy because it requires a 2-phase put
        commit confirmation.

        :param timestamp: object put timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        :raises DiskFileError: if the diskfile frag_index has not been set
                              (either during initialisation or a call to put())
        """
        data_file_path = join(
            self._datadir, self.manager.make_on_disk_filename(
                timestamp, '.data', self._diskfile._frag_index))
        durable_data_file_path = os.path.join(
            self._datadir, self.manager.make_on_disk_filename(
                timestamp, '.data', self._diskfile._frag_index, durable=True))
        tpool.execute(
            self._finalize_durable, data_file_path, durable_data_file_path)

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
            fi = self.manager.validate_fragment_index(
                fi, self._diskfile.policy)
            self._diskfile._frag_index = fi
            # defer cleanup until commit() writes makes diskfile durable
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
            self._frag_index = self.manager.validate_fragment_index(
                frag_index, self.policy)
        self._frag_prefs = self._validate_frag_prefs(kwargs.get('frag_prefs'))
        self._durable_frag_set = None

    def _validate_frag_prefs(self, frag_prefs):
        """
        Validate that frag_prefs is a list of dicts containing expected keys
        'timestamp' and 'exclude'. Convert timestamp values to Timestamp
        instances and validate that exclude values are valid fragment indexes.

        :param frag_prefs: data to validate, should be a list of dicts.
        :raise DiskFileError: if the frag_prefs data is invalid.
        :return: a list of dicts with converted and validated values.
        """
        # We *do* want to preserve an empty frag_prefs list because it
        # indicates that a durable file is not required.
        if frag_prefs is None:
            return None

        try:
            return [
                {'timestamp': Timestamp(pref['timestamp']),
                 'exclude': [self.manager.validate_fragment_index(fi)
                             for fi in pref['exclude']]}
                for pref in frag_prefs]
        except ValueError as e:
            raise DiskFileError(
                'Bad timestamp in frag_prefs: %r: %s'
                % (frag_prefs, e))
        except DiskFileError as e:
            raise DiskFileError(
                'Bad fragment index in frag_prefs: %r: %s'
                % (frag_prefs, e))
        except (KeyError, TypeError) as e:
            raise DiskFileError(
                'Bad frag_prefs: %r: %s' % (frag_prefs, e))

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

    def _get_ondisk_files(self, files, policy=None):
        """
        The only difference between this method and the replication policy
        DiskFile method is passing in the frag_index and frag_prefs kwargs to
        our manager's get_ondisk_files method.

        :param files: list of file names
        :param policy: storage policy used to store the files
        """
        self._ondisk_info = self.manager.get_ondisk_files(
            files, self._datadir, frag_index=self._frag_index,
            frag_prefs=self._frag_prefs, policy=policy)
        return self._ondisk_info

    def purge(self, timestamp, frag_index):
        """
        Remove a tombstone file matching the specified timestamp or
        datafile matching the specified timestamp and fragment index
        from the object directory.

        This provides the EC reconstructor/ssync process with a way to
        remove a tombstone or fragment from a handoff node after
        reverting it to its primary node.

        The hash will be invalidated, and if empty the hsh_path will
        be removed immediately.

        :param timestamp: the object timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        :param frag_index: fragment archive index, must be
                           a whole number or None.
        """
        purge_file = self.manager.make_on_disk_filename(
            timestamp, ext='.ts')
        remove_file(os.path.join(self._datadir, purge_file))
        if frag_index is not None:
            # data file may or may not be durable so try removing both filename
            # possibilities
            purge_file = self.manager.make_on_disk_filename(
                timestamp, ext='.data', frag_index=frag_index)
            remove_file(os.path.join(self._datadir, purge_file))
            purge_file = self.manager.make_on_disk_filename(
                timestamp, ext='.data', frag_index=frag_index, durable=True)
            remove_file(os.path.join(self._datadir, purge_file))
            remove_directory(self._datadir)
        self.manager.invalidate_hash(dirname(self._datadir))


class ECDiskFileManager(BaseDiskFileManager):
    diskfile_cls = ECDiskFile
    policy = EC_POLICY

    def validate_fragment_index(self, frag_index, policy=None):
        """
        Return int representation of frag_index, or raise a DiskFileError if
        frag_index is not a whole number.

        :param frag_index: a fragment archive index
        :param policy: storage policy used to validate the index against
        """
        try:
            frag_index = int(str(frag_index))
        except (ValueError, TypeError) as e:
            raise DiskFileError(
                'Bad fragment index: %s: %s' % (frag_index, e))
        if frag_index < 0:
            raise DiskFileError(
                'Fragment index must not be negative: %s' % frag_index)
        if policy and frag_index >= policy.ec_ndata + policy.ec_nparity:
            msg = 'Fragment index must be less than %d for a %d+%d policy: %s'
            raise DiskFileError(msg % (
                policy.ec_ndata + policy.ec_nparity,
                policy.ec_ndata, policy.ec_nparity, frag_index))
        return frag_index

    def make_on_disk_filename(self, timestamp, ext=None, frag_index=None,
                              ctype_timestamp=None, durable=False, *a, **kw):
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
        :param durable: if True then include a durable marker in data filename.
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
            if durable:
                rv += '#d'
            return '%s%s' % (rv, ext)
        return super(ECDiskFileManager, self).make_on_disk_filename(
            timestamp, ext, ctype_timestamp, *a, **kw)

    def parse_on_disk_filename(self, filename, policy):
        """
        Returns timestamp(s) and other info extracted from a policy specific
        file name. For EC policy the data file name includes a fragment index
        and possibly a durable marker, both of which which must be stripped off
        to retrieve the timestamp.

        :param filename: the file name including extension
        :returns: a dict, with keys for timestamp, frag_index, durable, ext and
                  ctype_timestamp:

            * timestamp is a :class:`~swift.common.utils.Timestamp`
            * frag_index is an int or None
            * ctype_timestamp is a :class:`~swift.common.utils.Timestamp` or
              None for .meta files, otherwise None
            * ext is a string, the file extension including the leading dot or
              the empty string if the filename has no extension
            * durable is a boolean that is True if the filename is a data file
              that includes a durable marker

        :raises DiskFileError: if any part of the filename is not able to be
                               validated.
        """
        frag_index = None
        float_frag, ext = splitext(filename)
        if ext == '.data':
            parts = float_frag.split('#')
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
            frag_index = self.validate_fragment_index(frag_index, policy)
            try:
                durable = parts[2] == 'd'
            except IndexError:
                durable = False
            return {
                'timestamp': timestamp,
                'frag_index': frag_index,
                'ext': ext,
                'ctype_timestamp': None,
                'durable': durable
            }
        rv = super(ECDiskFileManager, self).parse_on_disk_filename(
            filename, policy)
        rv['frag_index'] = None
        return rv

    def _process_ondisk_files(self, exts, results, frag_index=None,
                              frag_prefs=None, **kwargs):
        """
        Implement EC policy specific handling of .data and legacy .durable
        files.

        If a frag_prefs keyword arg is provided then its value may determine
        which fragment index at which timestamp is used to construct the
        diskfile. The value of frag_prefs should be a list. Each item in the
        frag_prefs list should be a dict that describes per-timestamp
        preferences using the following items:

            * timestamp: An instance of :class:`~swift.common.utils.Timestamp`.
            * exclude: A list of valid fragment indexes (i.e. whole numbers)
              that should be EXCLUDED when choosing a fragment at the
              timestamp. This list may be empty.

        For example::

            [
              {'timestamp': <Timestamp instance>, 'exclude': [1,3]},
              {'timestamp': <Timestamp instance>, 'exclude': []}
            ]

        The order of per-timestamp dicts in the frag_prefs list is significant
        and indicates descending preference for fragments from each timestamp
        i.e. a fragment that satisfies the first per-timestamp preference in
        the frag_prefs will be preferred over a fragment that satisfies a
        subsequent per-timestamp preferred, and so on.

        If a timestamp is not cited in any per-timestamp preference dict then
        it is assumed that any fragment index at that timestamp may be used to
        construct the diskfile.

        When a frag_prefs arg is provided, including an empty list, there is no
        requirement for there to be a durable file at the same timestamp as a
        data file that is chosen to construct the disk file

        :param exts: dict of lists of file info, keyed by extension
        :param results: a dict that may be updated with results
        :param frag_index: if set, search for a specific fragment index .data
                           file, otherwise accept the first valid .data file.
        :param frag_prefs: if set, search for any fragment index .data file
                           that satisfies the frag_prefs.
        """
        durable_info = None
        if exts.get('.durable'):
            # in older versions, separate .durable files were used to indicate
            # the durability of data files having the same timestamp
            durable_info = exts['.durable'][0]

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
            for frag in frag_set:
                # a data file marked as durable may supersede a legacy durable
                # file if it is newer
                if frag['durable']:
                    if (not durable_info or
                            durable_info['timestamp'] < timestamp):
                        # this frag defines the durable timestamp
                        durable_info = frag
                    break
            if durable_info and durable_info['timestamp'] == timestamp:
                durable_frag_set = frag_set
                break  # ignore frags that are older than durable timestamp

        # Choose which frag set to use
        chosen_frag_set = None
        if frag_prefs is not None:
            candidate_frag_sets = dict(frag_sets)
            # For each per-timestamp frag preference dict, do we have any frag
            # indexes at that timestamp that are not in the exclusion list for
            # that timestamp? If so choose the highest of those frag_indexes.
            for ts, exclude_indexes in [
                    (ts_pref['timestamp'], ts_pref['exclude'])
                    for ts_pref in frag_prefs
                    if ts_pref['timestamp'] in candidate_frag_sets]:
                available_indexes = [info['frag_index']
                                     for info in candidate_frag_sets[ts]]
                acceptable_indexes = list(set(available_indexes) -
                                          set(exclude_indexes))
                if acceptable_indexes:
                    chosen_frag_set = candidate_frag_sets[ts]
                    # override any frag_index passed in as method param with
                    # the last (highest) acceptable_index
                    frag_index = acceptable_indexes[-1]
                    break
                else:
                    # this frag_set has no acceptable frag index so
                    # remove it from the candidate frag_sets
                    candidate_frag_sets.pop(ts)
            else:
                # No acceptable frag index was found at any timestamp mentioned
                # in the frag_prefs. Choose the newest remaining candidate
                # frag_set - the proxy can decide if it wants the returned
                # fragment with that time.
                if candidate_frag_sets:
                    ts_newest = sorted(candidate_frag_sets.keys())[-1]
                    chosen_frag_set = candidate_frag_sets[ts_newest]
        else:
            chosen_frag_set = durable_frag_set

        # Select a single chosen frag from the chosen frag_set, by either
        # matching against a specified frag_index or taking the highest index.
        chosen_frag = None
        if chosen_frag_set:
            if frag_index is not None:
                # search the frag set to find the exact frag_index
                for info in chosen_frag_set:
                    if info['frag_index'] == frag_index:
                        chosen_frag = info
                        break
            else:
                chosen_frag = chosen_frag_set[-1]

        # If we successfully found a frag then set results
        if chosen_frag:
            results['data_info'] = chosen_frag
            results['durable_frag_set'] = durable_frag_set
            results['chosen_frag_set'] = chosen_frag_set
            if chosen_frag_set != durable_frag_set:
                # hide meta files older than data file but newer than durable
                # file so they don't get marked as obsolete (we already threw
                # out .meta's that are older than a .durable)
                exts['.meta'], _older = self._split_gt_timestamp(
                    exts['.meta'], chosen_frag['timestamp'])
        results['frag_sets'] = frag_sets

        # Mark everything older than most recent durable data as obsolete
        # and remove from the exts dict.
        if durable_info:
            for ext in exts.keys():
                exts[ext], older = self._split_gte_timestamp(
                    exts[ext], durable_info['timestamp'])
                results.setdefault('obsolete', []).extend(older)

        # Mark any isolated legacy .durable as obsolete
        if exts.get('.durable') and not durable_frag_set:
            results.setdefault('obsolete', []).extend(exts['.durable'])
            exts.pop('.durable')

        # Fragments *may* be ready for reclaim, unless they are durable
        for frag_set in frag_sets.values():
            if frag_set in (durable_frag_set, chosen_frag_set):
                continue
            results.setdefault('possible_reclaim', []).extend(frag_set)

        # .meta files *may* be ready for reclaim if there is no durable data
        if exts.get('.meta') and not durable_frag_set:
            results.setdefault('possible_reclaim', []).extend(
                exts.get('.meta'))

    def _verify_ondisk_files(self, results, frag_index=None,
                             frag_prefs=None, **kwargs):
        """
        Verify that the final combination of on disk files complies with the
        erasure-coded diskfile contract.

        :param results: files that have been found and accepted
        :param frag_index: specifies a specific fragment index .data file
        :param frag_prefs: if set, indicates that fragment preferences have
            been specified and therefore that a selected fragment is not
            required to be durable.
        :returns: True if the file combination is compliant, False otherwise
        """
        if super(ECDiskFileManager, self)._verify_ondisk_files(
                results, **kwargs):
            have_data_file = results['data_file'] is not None
            have_durable = (results.get('durable_frag_set') is not None or
                            (have_data_file and frag_prefs is not None))
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
            # The durable_frag_set may be indicated by a legacy
            # <timestamp>.durable file or by a durable <timestamp>#fi#d.data
            # file. Either way we update hashes[None] with the string
            # <timestamp>.durable which is a consistent representation of the
            # abstract state of the object regardless of the actual file set.
            # That way if we use a local combination of a legacy t1.durable and
            # t1#0.data to reconstruct a remote t1#0#d.data then, when next
            # hashed, the local and remote will make identical updates to their
            # suffix hashes.
            file_info = ondisk_info['durable_frag_set'][0]
            hashes[None].update(file_info['timestamp'].internal + '.durable')

    def _hash_suffix(self, path, policy=None):
        """
        Performs reclamation and returns an md5 of all (remaining) files.

        :param path: full path to directory
        :param policy: storage policy used to store the files
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        :returns: dict of md5 hex digests
        """
        # hash_per_fi instead of single hash for whole suffix
        # here we flatten out the hashers hexdigest into a dictionary instead
        # of just returning the one hexdigest for the whole suffix

        hash_per_fi = self._hash_suffix_dir(path, policy)
        return dict((fi, md5.hexdigest()) for fi, md5 in hash_per_fi.items())
