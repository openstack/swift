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

""" Disk File Interface for Swift Object Server"""

from __future__ import with_statement
import cPickle as pickle
import errno
import os
import time
import traceback
from hashlib import md5
from tempfile import mkstemp
from contextlib import contextmanager

from xattr import getxattr, setxattr
from eventlet import Timeout

from swift.common.constraints import check_mount
from swift.common.utils import mkdirs, normalize_timestamp, \
    storage_directory, hash_path, renamer, fallocate, fsync, \
    fdatasync, drop_buffer_cache, ThreadPool
from swift.common.exceptions import DiskFileError, DiskFileNotExist, \
    DiskFileCollision, DiskFileNoSpace, DiskFileDeviceUnavailable
from swift.obj.base import invalidate_hash, \
    quarantine_renamer
from swift.common.swob import multi_range_iterator


PICKLE_PROTOCOL = 2
METADATA_KEY = 'user.swift.metadata'


def read_metadata(fd):
    """
    Helper function to read the pickled metadata from an object file.

    :param fd: file descriptor to load the metadata from

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

    :param fd: file descriptor to write the metadata
    :param metadata: metadata to write
    """
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        setxattr(fd, '%s%s' % (METADATA_KEY, key or ''), metastr[:254])
        metastr = metastr[254:]
        key += 1


class DiskWriter(object):
    """
    Encapsulation of the write context for servicing PUT REST API
    requests. Serves as the context manager object for DiskFile's writer()
    method.
    """
    def __init__(self, disk_file, fd, tmppath, threadpool):
        self.disk_file = disk_file
        self.fd = fd
        self.tmppath = tmppath
        self.upload_size = 0
        self.last_sync = 0
        self.threadpool = threadpool

    def write(self, chunk):
        """
        Write a chunk of data into the temporary file.

        :param chunk: the chunk of data to write as a string object
        """

        def _write_entire_chunk(chunk):
            while chunk:
                written = os.write(self.fd, chunk)
                self.upload_size += written
                chunk = chunk[written:]

        self.threadpool.run_in_thread(_write_entire_chunk, chunk)

        # For large files sync every 512MB (by default) written
        diff = self.upload_size - self.last_sync
        if diff >= self.disk_file.bytes_per_sync:
            self.threadpool.force_run_in_thread(fdatasync, self.fd)
            drop_buffer_cache(self.fd, self.last_sync, diff)
            self.last_sync = self.upload_size

    def put(self, metadata, extension='.data'):
        """
        Finalize writing the file on disk, and renames it from the temp file
        to the real location.  This should be called after the data has been
        written to the temp file.

        :param metadata: dictionary of metadata to be written
        :param extension: extension to be used when making the file
        """
        if not self.tmppath:
            raise ValueError("tmppath is unusable.")
        timestamp = normalize_timestamp(metadata['X-Timestamp'])
        metadata['name'] = self.disk_file.name

        def finalize_put():
            # Write the metadata before calling fsync() so that both data and
            # metadata are flushed to disk.
            write_metadata(self.fd, metadata)
            # We call fsync() before calling drop_cache() to lower the amount
            # of redundant work the drop cache code will perform on the pages
            # (now that after fsync the pages will be all clean).
            fsync(self.fd)
            # From the Department of the Redundancy Department, make sure
            # we call drop_cache() after fsync() to avoid redundant work
            # (pages all clean).
            drop_buffer_cache(self.fd, 0, self.upload_size)
            invalidate_hash(os.path.dirname(self.disk_file.datadir))
            # After the rename completes, this object will be available for
            # other requests to reference.
            renamer(self.tmppath,
                    os.path.join(self.disk_file.datadir,
                                 timestamp + extension))

        self.threadpool.force_run_in_thread(finalize_put)
        self.disk_file.metadata = metadata


class DiskFile(object):
    """
    Manage object files on disk.

    :param path: path to devices on the node
    :param device: device name
    :param partition: partition on the device the object lives in
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param keep_data_fp: if True, don't close the fp, otherwise close it
    :param disk_chunk_size: size of chunks on file reads
    :param bytes_per_sync: number of bytes between fdatasync calls
    :param iter_hook: called when __iter__ returns a chunk
    :param threadpool: thread pool in which to do blocking operations

    :raises DiskFileCollision: on md5 collision
    """

    def __init__(self, path, device, partition, account, container, obj,
                 logger, keep_data_fp=False, disk_chunk_size=65536,
                 bytes_per_sync=(512 * 1024 * 1024), iter_hook=None,
                 threadpool=None, obj_dir='objects', mount_check=False,
                 disallowed_metadata_keys=None):
        if mount_check and not check_mount(path, device):
            raise DiskFileDeviceUnavailable()
        self.disk_chunk_size = disk_chunk_size
        self.bytes_per_sync = bytes_per_sync
        self.iter_hook = iter_hook
        self.name = '/' + '/'.join((account, container, obj))
        name_hash = hash_path(account, container, obj)
        self.datadir = os.path.join(
            path, device, storage_directory(obj_dir, partition, name_hash))
        self.device_path = os.path.join(path, device)
        self.tmpdir = os.path.join(path, device, 'tmp')
        self.logger = logger
        self.disallowed_metadata_keys = disallowed_metadata_keys
        self.metadata = {}
        self.meta_file = None
        self.data_file = None
        self.fp = None
        self.iter_etag = None
        self.started_at_0 = False
        self.read_to_eof = False
        self.quarantined_dir = None
        self.keep_cache = False
        self.suppress_file_closing = False
        self.threadpool = threadpool or ThreadPool(nthreads=0)
        if not os.path.exists(self.datadir):
            return
        files = sorted(os.listdir(self.datadir), reverse=True)
        for afile in files:
            if afile.endswith('.ts'):
                self.data_file = self.meta_file = None
                self.metadata = {'deleted': True}
                return
            if afile.endswith('.meta') and not self.meta_file:
                self.meta_file = os.path.join(self.datadir, afile)
            if afile.endswith('.data') and not self.data_file:
                self.data_file = os.path.join(self.datadir, afile)
                break
        if not self.data_file:
            return
        self.fp = open(self.data_file, 'rb')
        self.metadata = read_metadata(self.fp)
        if not keep_data_fp:
            self.close(verify_file=False)
        if self.meta_file:
            with open(self.meta_file) as mfp:
                for key in self.metadata.keys():
                    if key.lower() not in self.disallowed_metadata_keys:
                        del self.metadata[key]
                self.metadata.update(read_metadata(mfp))
        if 'name' in self.metadata:
            if self.metadata['name'] != self.name:
                self.logger.error(_('Client path %(client)s does not match '
                                    'path stored in object metadata %(meta)s'),
                                  {'client': self.name,
                                   'meta': self.metadata['name']})
                raise DiskFileCollision('Client path does not match path '
                                        'stored in object metadata')

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            dropped_cache = 0
            read = 0
            self.started_at_0 = False
            self.read_to_eof = False
            if self.fp.tell() == 0:
                self.started_at_0 = True
                self.iter_etag = md5()
            while True:
                chunk = self.threadpool.run_in_thread(
                    self.fp.read, self.disk_chunk_size)
                if chunk:
                    if self.iter_etag:
                        self.iter_etag.update(chunk)
                    read += len(chunk)
                    if read - dropped_cache > (1024 * 1024):
                        self._drop_cache(self.fp.fileno(), dropped_cache,
                                         read - dropped_cache)
                        dropped_cache = read
                    yield chunk
                    if self.iter_hook:
                        self.iter_hook()
                else:
                    self.read_to_eof = True
                    self._drop_cache(self.fp.fileno(), dropped_cache,
                                     read - dropped_cache)
                    break
        finally:
            if not self.suppress_file_closing:
                self.close()

    def app_iter_range(self, start, stop):
        """Returns an iterator over the data file for range (start, stop)"""
        if start or start == 0:
            self.fp.seek(start)
        if stop is not None:
            length = stop - start
        else:
            length = None
        for chunk in self:
            if length is not None:
                length -= len(chunk)
                if length < 0:
                    # Chop off the extra:
                    yield chunk[:length]
                    break
            yield chunk

    def app_iter_ranges(self, ranges, content_type, boundary, size):
        """Returns an iterator over the data file for a set of ranges"""
        if not ranges:
            yield ''
        else:
            try:
                self.suppress_file_closing = True
                for chunk in multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self.suppress_file_closing = False
                self.close()

    def _handle_close_quarantine(self):
        """Check if file needs to be quarantined"""
        try:
            self.get_data_file_size()
        except DiskFileNotExist:
            return
        except DiskFileError:
            self.quarantine()
            return

        if self.iter_etag and self.started_at_0 and self.read_to_eof and \
                'ETag' in self.metadata and \
                self.iter_etag.hexdigest() != self.metadata.get('ETag'):
            self.quarantine()

    def close(self, verify_file=True):
        """
        Close the file. Will handle quarantining file if necessary.

        :param verify_file: Defaults to True. If false, will not check
                            file to see if it needs quarantining.
        """
        if self.fp:
            try:
                if verify_file:
                    self._handle_close_quarantine()
            except (Exception, Timeout), e:
                self.logger.error(_(
                    'ERROR DiskFile %(data_file)s in '
                    '%(data_dir)s close failure: %(exc)s : %(stack)'),
                    {'exc': e, 'stack': ''.join(traceback.format_stack()),
                     'data_file': self.data_file, 'data_dir': self.datadir})
            finally:
                self.fp.close()
                self.fp = None

    def is_deleted(self):
        """
        Check if the file is deleted.

        :returns: True if the file doesn't exist or has been flagged as
                  deleted.
        """
        return not self.data_file or 'deleted' in self.metadata

    def is_expired(self):
        """
        Check if the file is expired.

        :returns: True if the file has an X-Delete-At in the past
        """
        return ('X-Delete-At' in self.metadata and
                int(self.metadata['X-Delete-At']) <= time.time())

    @contextmanager
    def writer(self, size=None):
        """
        Context manager to write a file. We create a temporary file first, and
        then return a DiskWriter object to encapsulate the state.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        """
        if not os.path.exists(self.tmpdir):
            mkdirs(self.tmpdir)
        fd, tmppath = mkstemp(dir=self.tmpdir)
        try:
            if size is not None and size > 0:
                try:
                    fallocate(fd, size)
                except OSError:
                    raise DiskFileNoSpace()
            yield DiskWriter(self, fd, tmppath, self.threadpool)
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(tmppath)
            except OSError:
                pass

    def put_metadata(self, metadata, tombstone=False):
        """
        Short hand for putting metadata to .meta and .ts files.

        :param metadata: dictionary of metadata to be written
        :param tombstone: whether or not we are writing a tombstone
        """
        extension = '.ts' if tombstone else '.meta'
        with self.writer() as writer:
            writer.put(metadata, extension=extension)

    def unlinkold(self, timestamp):
        """
        Remove any older versions of the object file.  Any file that has an
        older timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        timestamp = normalize_timestamp(timestamp)

        def _unlinkold():
            for fname in os.listdir(self.datadir):
                if fname < timestamp:
                    try:
                        os.unlink(os.path.join(self.datadir, fname))
                    except OSError, err:    # pragma: no cover
                        if err.errno != errno.ENOENT:
                            raise
        self.threadpool.run_in_thread(_unlinkold)

    def _drop_cache(self, fd, offset, length):
        """Method for no-oping buffer cache drop method."""
        if not self.keep_cache:
            drop_buffer_cache(fd, offset, length)

    def quarantine(self):
        """
        In the case that a file is corrupted, move it to a quarantined
        area to allow replication to fix it.

        :returns: if quarantine is successful, path to quarantined
                  directory otherwise None
        """
        if not (self.is_deleted() or self.quarantined_dir):
            self.quarantined_dir = self.threadpool.run_in_thread(
                quarantine_renamer, self.device_path, self.data_file)
            self.logger.increment('quarantines')
            return self.quarantined_dir

    def get_data_file_size(self):
        """
        Returns the os.path.getsize for the file.  Raises an exception if this
        file does not match the Content-Length stored in the metadata. Or if
        self.data_file does not exist.

        :returns: file size as an int
        :raises DiskFileError: on file size mismatch.
        :raises DiskFileNotExist: on file not existing (including deleted)
        """
        try:
            file_size = 0
            if self.data_file:
                file_size = self.threadpool.run_in_thread(
                    os.path.getsize, self.data_file)
                if 'Content-Length' in self.metadata:
                    metadata_size = int(self.metadata['Content-Length'])
                    if file_size != metadata_size:
                        raise DiskFileError(
                            'Content-Length of %s does not match file size '
                            'of %s' % (metadata_size, file_size))
                return file_size
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
        raise DiskFileNotExist('Data File does not exist.')
