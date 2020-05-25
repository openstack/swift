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

import copy
import errno
import os
import time
import json
from hashlib import md5
import logging
import traceback
from os.path import basename, dirname, join
from random import shuffle
from contextlib import contextmanager
from collections import defaultdict

from eventlet import Timeout, tpool
import six

from swift import gettext_ as _
from swift.common.constraints import check_drive
from swift.common.request_helpers import is_sys_meta
from swift.common.utils import fdatasync, \
    config_true_value, listdir, split_path, lock_path
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist, \
    DiskFileCollision, DiskFileNoSpace, DiskFileDeviceUnavailable, \
    DiskFileError, PathNotDir, \
    DiskFileExpired, DiskFileXattrNotSupported, \
    DiskFileBadMetadataChecksum
from swift.common.storage_policy import (
    split_policy_string, POLICIES,
    REPL_POLICY, EC_POLICY)

from swift.obj import vfile
from swift.obj.diskfile import BaseDiskFileManager, DiskFileManager, \
    ECDiskFileManager, BaseDiskFile, DiskFile, DiskFileReader, DiskFileWriter,\
    BaseDiskFileReader, BaseDiskFileWriter, ECDiskFile, ECDiskFileReader, \
    ECDiskFileWriter, AuditLocation, RESERVED_DATAFILE_META, \
    DATAFILE_SYSTEM_META, strip_self, DEFAULT_RECLAIM_AGE, _encode_metadata, \
    get_part_path, get_data_dir, update_auditor_status, extract_policy, \
    HASH_FILE, read_hashes, write_hashes, consolidate_hashes, invalidate_hash


def quarantine_vrenamer(device_path, corrupted_file_path):
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

    # rename key in KV
    return vfile.quarantine_ohash(dirname(corrupted_file_path), policy)


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
        if not check_drive(devices, device, mount_check):
            if logger:
                logger.debug(
                    'Skipping %s as it is not %s', device,
                    'mounted' if mount_check else 'a dir')
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
                suffixes = vfile.listdir(part_path)
            except OSError as e:
                if e.errno != errno.ENOTDIR:
                    raise
                continue
            for asuffix in suffixes:
                suff_path = os.path.join(part_path, asuffix)
                try:
                    hashes = vfile.listdir(suff_path)
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
        return vfile.listdir(datadir_path)
    try:
        status = json.loads(status)
    except ValueError as e:
        logger.warning(_('Loading JSON from %(auditor_status)s failed'
                         ' (%(err)s)') %
                       {'auditor_status': auditor_status, 'err': e})
        return vfile.listdir(datadir_path)
    return status['partitions']


class BaseKVFile(BaseDiskFile):
    # Todo: we may want a separate __init__ to define KV specific attribute
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
        try:
            files = vfile.listdir(self._datadir)
        except (OSError, vfile.VFileException) as err:
            raise DiskFileError(
                "Error listing directory %s: %s" % (self._datadir, err))

        # gather info about the valid files to use to open the DiskFile
        file_info = self._get_ondisk_files(files)

        self._data_file = file_info.get('data_file')
        if not self._data_file:
            raise self._construct_exception_from_ts_file(**file_info)
        self._vfr = self._construct_from_data_file(
            current_time=current_time, modernize=modernize, **file_info)
        # This method must populate the internal _metadata attribute.
        self._metadata = self._metadata or {}
        return self

    def _verify_data_file(self, data_file, data_vfr, current_time):
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

        obj_size = data_vfr.data_size
        if obj_size != metadata_size:
            raise self._quarantine(
                data_file, "metadata content-length %s does"
                           " not match actual object size %s" % (
                               metadata_size, obj_size))
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
        :param add_missing_checksum: ignored
        """
        try:
            vfr = vfile.VFileReader.get_vfile(source, self._logger)
            vfr_metadata = vfr.metadata
            vfr.close()
            return vfr_metadata
        except (DiskFileXattrNotSupported, DiskFileNotExist):
            raise
        except DiskFileBadMetadataChecksum as err:
            raise self._quarantine(quarantine_filename, str(err))
        except Exception as err:
            raise self._quarantine(
                quarantine_filename,
                "Exception reading metadata: %s" % err)

    # This could be left unchanged now that _failsafe_read_metadata() is
    # patched. Test it.
    def _merge_content_type_metadata(self, ctype_file):
        """
        When a second .meta file is providing the most recent Content-Type
        metadata then merge it into the metafile_metadata.

        :param ctype_file: An on-disk .meta file
        """
        try:
            ctype_vfr = vfile.VFileReader.get_vfile(ctype_file, self._logger)
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise DiskFileNotExist()
            raise
        ctypefile_metadata = ctype_vfr.metadata
        ctype_vfr.close()
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
        :param modernize: ignored
        :returns: an opened data file pointer
        :raises DiskFileError: various exceptions from
                    :func:`swift.obj.diskfile.DiskFile._verify_data_file`
        """
        # TODO: need to catch exception, check if ENOENT (see in diskfile)
        try:
            data_vfr = vfile.VFileReader.get_vfile(data_file, self._logger)
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise DiskFileNotExist()
            raise

        self._datafile_metadata = data_vfr.metadata

        self._metadata = {}
        if meta_file:
            self._metafile_metadata = self._failsafe_read_metadata(
                meta_file, meta_file)

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
        self._verify_data_file(data_file, data_vfr, current_time)
        return data_vfr

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
            self._vfr, self._data_file, int(self._metadata['Content-Length']),
            self._metadata['ETag'], self._disk_chunk_size,
            self._manager.keep_cache_size, self._device_path, self._logger,
            use_splice=self._use_splice, quarantine_hook=_quarantine_hook,
            pipe_size=self._pipe_size, diskfile=self, keep_cache=keep_cache)
        # At this point the reader object is now responsible for closing
        # the file pointer.
        # self._fp = None
        self._vfr = None
        return dr

    def writer(self, size=None):
        return self.writer_cls(self._manager, self._name, self._datadir, size,
                               self._bytes_per_sync, self, self._logger)

    def _get_tempfile(self):
        raise Exception("_get_tempfile called, shouldn't happen")

    @contextmanager
    def create(self, size=None, extension=None):
        """
        Context manager to create a vfile.
        It could create separate volumes based on the extension.

        Currently no caller will set the extension. The easiest would be to
        patch server.py, in DELETE(), add an extension=".ts" parameter to the
        self.get_diskfile() call, as kwargs is passed all the way down to here.

        It's also possible to have the writer_cls handle the volume creation
        later: at the first write() call, if any, assume it's not a tombstone,
        and in put(), check self._extension.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :param extension: file extension, with dot ('.ts')
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        """
        dfw = self.writer(size)
        try:
            yield dfw.open()
        finally:
            dfw.close()


class BaseKVFileReader(BaseDiskFileReader):
    def __init__(self, vfr, data_file, obj_size, etag,
                 disk_chunk_size, keep_cache_size, device_path, logger,
                 quarantine_hook, use_splice, pipe_size, diskfile,
                 keep_cache=False):
        # Parameter tracking
        self._vfr = vfr
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

    def _init_checks(self):
        if self._vfr.tell() == 0:
            self._started_at_0 = True
            self._iter_etag = md5()

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            self._bytes_read = 0
            self._started_at_0 = False
            self._read_to_eof = False
            self._init_checks()
            while True:
                chunk = self._vfr.read(self._disk_chunk_size)
                if chunk:
                    self._update_checks(chunk)
                    self._bytes_read += len(chunk)
                    yield chunk
                else:
                    self._read_to_eof = True
                    break
        finally:
            if not self._suppress_file_closing:
                self.close()

    def can_zero_copy_send(self):
        return False

    def app_iter_range(self, start, stop):
        """
        Returns an iterator over the data file for range (start, stop)

        """
        if start or start == 0:
            self._vfr.seek(start)
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

    def close(self):
        """
        Close the open file handle if present.

        For this specific implementation, this method will handle quarantining
        the file if necessary.
        """
        if self._vfr:
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
                vfr, self._vfr = self._vfr, None
                vfr.close()


class BaseKVFileWriter(BaseDiskFileWriter):
    def __init__(self, mgr, name, datadir, size, bytes_per_sync, diskfile,
                 logger):
        # Parameter tracking
        self._manager = mgr
        self._name = name
        self._datadir = datadir
        self._vfile_writer = None
        self._tmppath = None
        self._size = size
        self._chunks_etag = md5()
        self._bytes_per_sync = bytes_per_sync
        self._diskfile = diskfile
        self._logger = logger

        # Internal attributes
        self._upload_size = 0
        self._last_sync = 0
        self._extension = '.data'
        self._put_succeeded = False

    def open(self):
        if self._vfile_writer is not None:
            raise ValueError('DiskFileWriter is already open')

        try:
            # TODO: support extension
            self._vfile_writer = vfile.VFileWriter.create(
                self._datadir, self._size, self._manager.vfile_conf,
                self._logger, extension=None)
        except OSError as err:
            if err.errno in (errno.ENOSPC, errno.EDQUOT):
                # No more inodes in filesystem
                raise DiskFileNoSpace(err.strerror)
            raise
        return self

    def close(self):
        if self._vfile_writer:
            try:
                os.close(self._vfile_writer.fd)
                os.close(self._vfile_writer.lock_fd)
            except OSError:
                pass
            self._vfile_writer = None

    def write(self, chunk):
        """
        Write a chunk of data to disk. All invocations of this method must
        come before invoking the :func:

        :param chunk: the chunk of data to write as a string object

        :returns: the total number of bytes written to an object
        """

        if not self._vfile_writer:
            raise ValueError('Writer is not open')
        self._chunks_etag.update(chunk)
        while chunk:
            written = os.write(self._vfile_writer.fd, chunk)
            self._upload_size += written
            chunk = chunk[written:]

        # For large files sync every 512MB (by default) written
        diff = self._upload_size - self._last_sync
        if diff >= self._bytes_per_sync:
            tpool.execute(fdatasync, self._vfile_writer.fd)
            # drop_buffer_cache(self._vfile_writer.fd, self._last_sync, diff)
            self._last_sync = self._upload_size

        return self._upload_size

    def _finalize_put(self, metadata, target_path, cleanup):
        filename = basename(target_path)
        # write metadata and sync
        self._vfile_writer.commit(filename, _encode_metadata(metadata))
        self._put_succeeded = True
        if cleanup:
            try:
                self.manager.cleanup_ondisk_files(self._datadir)['files']
            except OSError:
                logging.exception(_('Problem cleaning up %s'), self._datadir)


class KVFileReader(BaseKVFileReader, DiskFileReader):
    pass


class KVFileWriter(BaseKVFileWriter, DiskFileWriter):
    def put(self, metadata):
        """
        Finalize writing the file on disk.

        :param metadata: dictionary of metadata to be associated with the
                         object
        """
        super(KVFileWriter, self)._put(metadata, True)


class KVFile(BaseKVFile, DiskFile):
    reader_cls = KVFileReader
    writer_cls = KVFileWriter


class BaseKVFileManager(BaseDiskFileManager):
    diskfile_cls = None  # must be set by subclasses

    invalidate_hash = strip_self(invalidate_hash)
    consolidate_hashes = strip_self(consolidate_hashes)
    quarantine_renamer = strip_self(quarantine_vrenamer)

    def __init__(self, conf, logger):
        self.logger = logger
        self.devices = conf.get('devices', '/srv/node')
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.keep_cache_size = int(conf.get('keep_cache_size', 5242880))
        self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024
        # vfile specific config
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
        # end vfile specific config
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

        files = vfile.listdir(hsh_path)

        files.sort(reverse=True)
        results = self.get_ondisk_files(
            files, hsh_path, verify=False, **kwargs)
        if 'ts_info' in results and is_reclaimable(
                results['ts_info']['timestamp']):
            remove_vfile(join(hsh_path, results['ts_info']['filename']))
            files.remove(results.pop('ts_info')['filename'])
        for file_info in results.get('possible_reclaim', []):
            # stray files are not deleted until reclaim-age
            if is_reclaimable(file_info['timestamp']):
                results.setdefault('obsolete', []).append(file_info)
        for file_info in results.get('obsolete', []):
            remove_vfile(join(hsh_path, file_info['filename']))
            files.remove(file_info['filename'])
        results['files'] = files

        return results

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
            path_contents = sorted(vfile.listdir(path))
        except OSError as err:
            if err.errno in (errno.ENOTDIR, errno.ENOENT):
                raise PathNotDir()
            raise
        for hsh in path_contents:
            hsh_path = os.path.join(path, hsh)
            try:
                ondisk_info = self.cleanup_ondisk_files(
                    hsh_path, policy=policy)
            except OSError as err:
                if err.errno == errno.ENOTDIR:
                    partition_path = os.path.dirname(path)
                    objects_path = os.path.dirname(partition_path)
                    device_path = os.path.dirname(objects_path)
                    # The made-up filename is so that the eventual dirpath()
                    # will result in this object directory that we care about.
                    # Some failures will result in an object directory
                    # becoming a file, thus causing the parent directory to
                    # be qarantined.
                    quar_path = quarantine_vrenamer(
                        device_path, os.path.join(
                            hsh_path, "made-up-filename"))
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

        return hashes

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
        hashes_file = os.path.join(partition_path, HASH_FILE)
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
            for suff in vfile.listdir(partition_path):
                if len(suff) == 3:
                    hashes.setdefault(suff, None)
            modified = True
        hashes.update((suffix, None) for suffix in recalculate)
        for suffix, hash_ in list(hashes.items()):
            if not hash_:
                suffix_dir = os.path.join(partition_path, suffix)
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
        object_path = join(
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
            vf = vfile.VFileReader.get_vfile(join(object_path, filenames[-1]),
                                             self.logger)
            metadata = vf.metadata
            vf.close()
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

    def _listdir(self, path):
        """
        :param path: full path to directory
        """
        try:
            return vfile.listdir(path)
        except OSError as err:
            if err.errno != errno.ENOENT:
                self.logger.error(
                    'ERROR: Skipping %r due to error with listdir attempt: %s',
                    path, err)
        return []

    def exists(self, path):
        """
        :param path: full path to directory
        """
        return vfile.exists(path)

    def mkdirs(self, path):
        """
        :param path: full path to directory
        """
        return vfile.mkdirs(path)

    def listdir(self, path):
        """
        :param path: full path to directory
        """
        return vfile.listdir(path)

    def rmtree(self, path, ignore_errors=False):
        vfile.rmtree(path)

    def remove_file(self, path):
        """
        similar to utils.remove_file
        :param path: full path to directory
        """
        try:
            return vfile.delete_vfile_from_path(path)
        except (OSError, vfile.VFileException):
            pass

    def remove(self, path):
        """
        :param path: full path to directory
        """
        return vfile.delete_vfile_from_path(path)

    def isdir(self, path):
        """
        :param path: full path to directory
        """
        return vfile.isdir(path)

    def isfile(self, path):
        """
        :param path: full path to directory
        """
        return vfile.isfile(path)

    def rmdir(self, path):
        """
        :param path: full path to directory
        """
        pass


class KVFileManager(BaseKVFileManager, DiskFileManager):
    diskfile_cls = KVFile
    policy_type = REPL_POLICY


class ECKVFileReader(BaseKVFileReader, ECDiskFileReader):
    def __init__(self, vfr, data_file, obj_size, etag,
                 disk_chunk_size, keep_cache_size, device_path, logger,
                 quarantine_hook, use_splice, pipe_size, diskfile,
                 keep_cache=False):
        super(ECKVFileReader, self).__init__(
            vfr, data_file, obj_size, etag,
            disk_chunk_size, keep_cache_size, device_path, logger,
            quarantine_hook, use_splice, pipe_size, diskfile, keep_cache)
        self.frag_buf = None
        self.frag_offset = 0
        self.frag_size = self._diskfile.policy.fragment_size

    def _init_checks(self):
        super(ECKVFileReader, self)._init_checks()
        # for a multi-range GET this will be called at the start of each range;
        # only initialise the frag_buf for reads starting at 0.
        # TODO: reset frag buf to '' if tell() shows that start is on a frag
        # boundary so that we check frags selected by a range not starting at 0
        # ECDOING - check _started_at_0 is defined correctly
        if self._started_at_0:
            self.frag_buf = ''
        else:
            self.frag_buf = None

    def _update_checks(self, chunk):
        # super(ECKVFileReader, self)._update_checks(chunk)

        # Because of python's MRO, this will call
        # ECDiskFileReader._update_checks, and blow up.
        # rather than mess with the class's mro() function, explicitely call
        # the one we want.
        BaseDiskFileReader._update_checks(self, chunk)
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
        # super(ECKVFileReader, self)._handle_close_quarantine()
        BaseDiskFileReader._handle_close_quarantine(self)
        self._check_frag(self.frag_buf)


class ECKVFileWriter(BaseKVFileWriter, ECDiskFileWriter):
    # TODO: this needs to be updated wrt. next_part_power, and other changes
    # in diskfile.py
    def _finalize_durable(self, data_file_path, durable_data_file_path,
                          timestamp):
        exc = None
        try:
            try:
                vfile.rename_vfile(data_file_path, durable_data_file_path,
                                   self._diskfile._logger)
            except (OSError, IOError) as err:
                if err.errno == errno.ENOENT:
                    files = vfile.listdir(self._datadir)
                    results = self.manager.get_ondisk_files(
                        files, self._datadir,
                        frag_index=self._diskfile._frag_index,
                        policy=self._diskfile.policy)
                    # We "succeeded" if another writer cleaned up our data
                    ts_info = results.get('ts_info')
                    durables = results.get('durable_frag_set', [])
                    if ts_info and ts_info['timestamp'] > timestamp:
                        return
                    elif any(frag_set['timestamp'] > timestamp
                             for frag_set in durables):
                        return

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
                    self.manager.cleanup_ondisk_files(self._datadir)['files']
                except OSError as os_err:
                    self.manager.logger.exception(
                        _('Problem cleaning up %(datadir)s (%(err)s)'),
                        {'datadir': self._datadir, 'err': os_err})
        except Exception as err:
            params = {'file': durable_data_file_path, 'err': err}
            self.manager.logger.exception(
                _('Problem making data file durable %(file)s (%(err)s)'),
                params)
            exc = DiskFileError(
                'Problem making data file durable %(file)s (%(err)s)' % params)
        if exc:
            raise exc

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
            fi = self.manager.validate_fragment_index(fi)
            self._diskfile._frag_index = fi
            # defer cleanup until commit() writes makes diskfile durable
            cleanup = False
        super(ECKVFileWriter, self)._put(metadata, cleanup, frag_index=fi)


class ECKVFile(BaseKVFile, ECDiskFile):

    reader_cls = ECKVFileReader
    writer_cls = ECKVFileWriter

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
        purge_file = self.manager.make_on_disk_filename(
            timestamp, ext='.ts')
        remove_vfile(os.path.join(self._datadir, purge_file))
        if frag_index is not None:
            # data file may or may not be durable so try removing both filename
            # possibilities
            purge_file = self.manager.make_on_disk_filename(
                timestamp, ext='.data', frag_index=frag_index)
            remove_vfile(os.path.join(self._datadir, purge_file))
            purge_file = self.manager.make_on_disk_filename(
                timestamp, ext='.data', frag_index=frag_index, durable=True)
            remove_vfile(os.path.join(self._datadir, purge_file))
            # we don't use hashes.pkl files
            # self.manager.invalidate_hash(dirname(self._datadir))


class ECKVFileManager(BaseKVFileManager, ECDiskFileManager):
    diskfile_cls = ECKVFile
    policy_type = EC_POLICY


def remove_vfile(filepath):
    try:
        vfile.delete_vfile_from_path(filepath)
    except OSError:
        pass
