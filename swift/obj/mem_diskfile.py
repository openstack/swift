# Copyright (c) 2010-2013 OpenStack, LLC.
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

""" In-Memory Disk File Interface for Swift Object Server"""

import cStringIO
import time
import hashlib
from contextlib import contextmanager

from eventlet import Timeout

from swift.common.utils import Timestamp
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist, \
    DiskFileCollision, DiskFileDeleted, DiskFileNotOpen
from swift.common.swob import multi_range_iterator


class InMemoryFileSystem(object):
    """
    A very simplistic in-memory file system scheme.

    There is one dictionary mapping a given object name to a tuple. The first
    entry in the tuble is the cStringIO buffer representing the file contents,
    the second entry is the metadata dictionary.
    """

    def __init__(self):
        self._filesystem = {}

    def get_object(self, name):
        val = self._filesystem.get(name)
        if val is None:
            data, metadata = None, None
        else:
            data, metadata = val
        return data, metadata

    def put_object(self, name, data, metadata):
        self._filesystem[name] = (data, metadata)

    def del_object(self, name):
        del self._filesystem[name]

    def get_diskfile(self, account, container, obj, **kwargs):
        return DiskFile(self, account, container, obj)

    def pickle_async_update(self, *args, **kwargs):
        """
        For now don't handle async updates.
        """
        pass


class DiskFileWriter(object):
    """
    .. note::
        Sample alternative pluggable on-disk backend implementation.

    Encapsulation of the write context for servicing PUT REST API
    requests. Serves as the context manager object for DiskFile's create()
    method.

    :param fs: internal file system object to use
    :param name: standard object name
    :param fp: `StringIO` in-memory representation object
    """
    def __init__(self, fs, name, fp):
        self._filesystem = fs
        self._name = name
        self._fp = fp
        self._upload_size = 0

    def write(self, chunk):
        """
        Write a chunk of data into the `StringIO` object.

        :param chunk: the chunk of data to write as a string object
        """
        self._fp.write(chunk)
        self._upload_size += len(chunk)
        return self._upload_size

    def put(self, metadata):
        """
        Make the final association in the in-memory file system for this name
        with the `StringIO` object.

        :param metadata: dictionary of metadata to be written
        :param extension: extension to be used when making the file
        """
        metadata['name'] = self._name
        self._filesystem.put_object(self._name, self._fp, metadata)

    def commit(self, timestamp):
        """
        Perform any operations necessary to mark the object as durable. For
        mem_diskfile type this is a no-op.

        :param timestamp: object put timestamp, an instance of
                          :class:`~swift.common.utils.Timestamp`
        """
        pass


class DiskFileReader(object):
    """
    .. note::
        Sample alternative pluggable on-disk backend implementation.

    Encapsulation of the read context for servicing GET REST API
    requests. Serves as the context manager object for DiskFile's reader()
    method.

    :param name: object name
    :param fp: open file object pointer reference
    :param obj_size: on-disk size of object in bytes
    :param etag: MD5 hash of object from metadata
    """
    def __init__(self, name, fp, obj_size, etag):
        self._name = name
        self._fp = fp
        self._obj_size = obj_size
        self._etag = etag
        #
        self._iter_etag = None
        self._bytes_read = 0
        self._started_at_0 = False
        self._read_to_eof = False
        self._suppress_file_closing = False
        #
        self.was_quarantined = ''

    def __iter__(self):
        try:
            self._bytes_read = 0
            self._started_at_0 = False
            self._read_to_eof = False
            if self._fp.tell() == 0:
                self._started_at_0 = True
                self._iter_etag = hashlib.md5()
            while True:
                chunk = self._fp.read()
                if chunk:
                    if self._iter_etag:
                        self._iter_etag.update(chunk)
                    self._bytes_read += len(chunk)
                    yield chunk
                else:
                    self._read_to_eof = True
                    break
        finally:
            if not self._suppress_file_closing:
                self.close()

    def app_iter_range(self, start, stop):
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
                try:
                    self.close()
                except DiskFileQuarantined:
                    pass

    def _quarantine(self, msg):
        self.was_quarantined = msg

    def _handle_close_quarantine(self):
        if self._bytes_read != self._obj_size:
            self._quarantine(
                "Bytes read: %s, does not match metadata: %s" % (
                    self.bytes_read, self._obj_size))
        elif self._iter_etag and \
                self._etag != self._iter_etag.hexdigest():
            self._quarantine(
                "ETag %s and file's md5 %s do not match" % (
                    self._etag, self._iter_etag.hexdigest()))

    def close(self):
        """
        Close the file. Will handle quarantining file if necessary.
        """
        if self._fp:
            try:
                if self._started_at_0 and self._read_to_eof:
                    self._handle_close_quarantine()
            except (Exception, Timeout):
                pass
            finally:
                self._fp = None


class DiskFile(object):
    """
    .. note::

        Sample alternative pluggable on-disk backend implementation. This
        example duck-types the reference implementation DiskFile class.

    Manage object files in-memory.

    :param mgr: DiskFileManager
    :param device_path: path to the target device or drive
    :param threadpool: thread pool to use for blocking operations
    :param partition: partition on the device in which the object lives
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param keep_cache: caller's preference for keeping data read in the cache
    """

    def __init__(self, fs, account, container, obj):
        self._name = '/' + '/'.join((account, container, obj))
        self._metadata = None
        self._fp = None
        self._filesystem = fs

    def open(self):
        """
        Open the file and read the metadata.

        This method must populate the _metadata attribute.
        :raises DiskFileCollision: on name mis-match with metadata
        :raises DiskFileDeleted: if it does not exist, or a tombstone is
                                 present
        :raises DiskFileQuarantined: if while reading metadata of the file
                                     some data did pass cross checks
        """
        fp, self._metadata = self._filesystem.get_object(self._name)
        if fp is None:
            raise DiskFileDeleted()
        self._fp = self._verify_data_file(fp)
        self._metadata = self._metadata or {}
        return self

    def __enter__(self):
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self

    def __exit__(self, t, v, tb):
        if self._fp is not None:
            self._fp = None

    def _verify_data_file(self, fp):
        """
        Verify the metadata's name value matches what we think the object is
        named.

        :raises DiskFileCollision: if the metadata stored name does not match
                                   the referenced name of the file
        :raises DiskFileNotExist: if the object has expired
        :raises DiskFileQuarantined: if data inconsistencies were detected
                                     between the metadata and the file-system
                                     metadata
        """
        try:
            mname = self._metadata['name']
        except KeyError:
            raise self._quarantine(self._name, "missing name metadata")
        else:
            if mname != self._name:
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
                self._name, "bad metadata x-delete-at value %s" % (
                    self._metadata['X-Delete-At']))
        else:
            if x_delete_at <= time.time():
                raise DiskFileNotExist('Expired')
        try:
            metadata_size = int(self._metadata['Content-Length'])
        except KeyError:
            raise self._quarantine(
                self._name, "missing content-length in metadata")
        except ValueError:
            # Quarantine, the content-length key is present but not an
            # integer.
            raise self._quarantine(
                self._name, "bad metadata content-length value %s" % (
                    self._metadata['Content-Length']))
        try:
            fp.seek(0, 2)
            obj_size = fp.tell()
            fp.seek(0, 0)
        except OSError as err:
            # Quarantine, we can't successfully stat the file.
            raise self._quarantine(self._name, "not stat-able: %s" % err)
        if obj_size != metadata_size:
            raise self._quarantine(
                self._name, "metadata content-length %s does"
                " not match actual object size %s" % (
                    metadata_size, obj_size))
        return fp

    def get_metadata(self):
        """
        Provide the metadata for an object as a dictionary.

        :returns: object's metadata dictionary
        """
        if self._metadata is None:
            raise DiskFileNotOpen()
        return self._metadata

    def read_metadata(self):
        """
        Return the metadata for an object.

        :returns: metadata dictionary for an object
        """
        with self.open():
            return self.get_metadata()

    def reader(self, keep_cache=False):
        """
        Return a swift.common.swob.Response class compatible "app_iter"
        object. The responsibility of closing the open file is passed to the
        DiskFileReader object.

        :param keep_cache:
        """
        dr = DiskFileReader(self._name, self._fp,
                            int(self._metadata['Content-Length']),
                            self._metadata['ETag'])
        # At this point the reader object is now responsible for
        # the file pointer.
        self._fp = None
        return dr

    @contextmanager
    def create(self, size=None):
        """
        Context manager to create a file. We create a temporary file first, and
        then return a DiskFileWriter object to encapsulate the state.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        """
        fp = cStringIO.StringIO()
        try:
            yield DiskFileWriter(self._filesystem, self._name, fp)
        finally:
            del fp

    def write_metadata(self, metadata):
        """
        Write a block of metadata to an object.
        """
        cur_fp = self._filesystem.get(self._name)
        if cur_fp is not None:
            self._filesystem[self._name] = (cur_fp, metadata)

    def delete(self, timestamp):
        """
        Perform a delete for the given object in the given container under the
        given account.

        This creates a tombstone file with the given timestamp, and removes
        any older versions of the object file.  Any file that has an older
        timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        fp, md = self._filesystem.get_object(self._name)
        if md and md['X-Timestamp'] < Timestamp(timestamp):
            self._filesystem.del_object(self._name)
