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

""" Object Server for Swift """

from __future__ import with_statement
import cPickle as pickle
import errno
import os
import time
import traceback
from datetime import datetime
from hashlib import md5
from tempfile import mkstemp
from urllib import unquote
from contextlib import contextmanager

from xattr import getxattr, setxattr
from eventlet import sleep, Timeout, tpool

from swift.common.utils import mkdirs, normalize_timestamp, public, \
    storage_directory, hash_path, renamer, fallocate, fsync, \
    split_path, drop_buffer_cache, get_logger, write_pickle, \
    config_true_value, validate_device_partition, timing_stats
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_object_creation, check_mount, \
    check_float, check_utf8
from swift.common.exceptions import ConnectionTimeout, DiskFileError, \
    DiskFileNotExist
from swift.obj.replicator import tpool_reraise, invalidate_hash, \
    quarantine_renamer, get_hashes
from swift.common.http import is_success
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPCreated, \
    HTTPInternalServerError, HTTPNoContent, HTTPNotFound, HTTPNotModified, \
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity, \
    HTTPClientDisconnect, HTTPMethodNotAllowed, Request, Response, UTC, \
    HTTPInsufficientStorage, multi_range_iterator


DATADIR = 'objects'
ASYNCDIR = 'async_pending'
PICKLE_PROTOCOL = 2
METADATA_KEY = 'user.swift.metadata'
MAX_OBJECT_NAME_LENGTH = 1024
# keep these lower-case
DISALLOWED_HEADERS = set('content-length content-type deleted etag'.split())


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
    :param iter_hook: called when __iter__ returns a chunk
    """

    def __init__(self, path, device, partition, account, container, obj,
                 logger, keep_data_fp=False, disk_chunk_size=65536,
                 iter_hook=None):
        self.disk_chunk_size = disk_chunk_size
        self.iter_hook = iter_hook
        self.name = '/' + '/'.join((account, container, obj))
        name_hash = hash_path(account, container, obj)
        self.datadir = os.path.join(
            path, device, storage_directory(DATADIR, partition, name_hash))
        self.device_path = os.path.join(path, device)
        self.tmpdir = os.path.join(path, device, 'tmp')
        self.tmppath = None
        self.logger = logger
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
        if not os.path.exists(self.datadir):
            return
        files = sorted(os.listdir(self.datadir), reverse=True)
        for file in files:
            if file.endswith('.ts'):
                self.data_file = self.meta_file = None
                self.metadata = {'deleted': True}
                return
            if file.endswith('.meta') and not self.meta_file:
                self.meta_file = os.path.join(self.datadir, file)
            if file.endswith('.data') and not self.data_file:
                self.data_file = os.path.join(self.datadir, file)
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
                    if key.lower() not in DISALLOWED_HEADERS:
                        del self.metadata[key]
                self.metadata.update(read_metadata(mfp))

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
                chunk = self.fp.read(self.disk_chunk_size)
                if chunk:
                    if self.iter_etag:
                        self.iter_etag.update(chunk)
                    read += len(chunk)
                    if read - dropped_cache > (1024 * 1024):
                        self.drop_cache(self.fp.fileno(), dropped_cache,
                                        read - dropped_cache)
                        dropped_cache = read
                    yield chunk
                    if self.iter_hook:
                        self.iter_hook()
                else:
                    self.read_to_eof = True
                    self.drop_cache(self.fp.fileno(), dropped_cache,
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
        if (not ranges or len(ranges) == 0):
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
        except DiskFileError:
            self.quarantine()
            return
        except DiskFileNotExist:
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
    def mkstemp(self):
        """Contextmanager to make a temporary file."""
        if not os.path.exists(self.tmpdir):
            mkdirs(self.tmpdir)
        fd, self.tmppath = mkstemp(dir=self.tmpdir)
        try:
            yield fd
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            tmppath, self.tmppath = self.tmppath, None
            try:
                os.unlink(tmppath)
            except OSError:
                pass

    def put(self, fd, metadata, extension='.data'):
        """
        Finalize writing the file on disk, and renames it from the temp file to
        the real location.  This should be called after the data has been
        written to the temp file.

        :param fd: file descriptor of the temp file
        :param metadata: dictionary of metadata to be written
        :param extension: extension to be used when making the file
        """
        assert self.tmppath is not None
        metadata['name'] = self.name
        timestamp = normalize_timestamp(metadata['X-Timestamp'])
        write_metadata(fd, metadata)
        if 'Content-Length' in metadata:
            self.drop_cache(fd, 0, int(metadata['Content-Length']))
        tpool.execute(fsync, fd)
        invalidate_hash(os.path.dirname(self.datadir))
        renamer(self.tmppath,
                os.path.join(self.datadir, timestamp + extension))
        self.metadata = metadata

    def put_metadata(self, metadata, tombstone=False):
        """
        Short hand for putting metadata to .meta and .ts files.

        :param metadata: dictionary of metadata to be written
        :param tombstone: whether or not we are writing a tombstone
        """
        extension = '.ts' if tombstone else '.meta'
        with self.mkstemp() as fd:
            self.put(fd, metadata, extension=extension)

    def unlinkold(self, timestamp):
        """
        Remove any older versions of the object file.  Any file that has an
        older timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        timestamp = normalize_timestamp(timestamp)
        for fname in os.listdir(self.datadir):
            if fname < timestamp:
                try:
                    os.unlink(os.path.join(self.datadir, fname))
                except OSError, err:    # pragma: no cover
                    if err.errno != errno.ENOENT:
                        raise

    def drop_cache(self, fd, offset, length):
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
            self.quarantined_dir = quarantine_renamer(self.device_path,
                                                      self.data_file)
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
                file_size = os.path.getsize(self.data_file)
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


class ObjectController(object):
    """Implements the WSGI application for the Swift Object Server."""

    def __init__(self, conf):
        """
        Creates a new WSGI application for the Swift Object Server. An
        example configuration is given at
        <source-dir>/etc/object-server.conf-sample or
        /etc/swift/object-server.conf-sample.
        """
        self.logger = get_logger(conf, log_route='object-server')
        self.devices = conf.get('devices', '/srv/node/')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.keep_cache_size = int(conf.get('keep_cache_size', 5242880))
        self.keep_cache_private = \
            config_true_value(conf.get('keep_cache_private', 'false'))
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.slow = int(conf.get('slow', 0))
        self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024
        default_allowed_headers = '''
            content-disposition,
            content-encoding,
            x-delete-at,
            x-object-manifest,
        '''
        self.allowed_headers = set(
            i.strip().lower() for i in
            conf.get('allowed_headers', default_allowed_headers).split(',')
            if i.strip() and i.strip().lower() not in DISALLOWED_HEADERS)
        self.expiring_objects_account = \
            (conf.get('auto_create_account_prefix') or '.') + \
            'expiring_objects'
        self.expiring_objects_container_divisor = \
            int(conf.get('expiring_objects_container_divisor') or 86400)

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice):
        """
        Sends or saves an async update.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param host: host that the container is on
        :param partition: partition that the container is on
        :param contdevice: device name that the container is on
        :param headers_out: dictionary of headers to send in the container
                            request
        :param objdevice: device name that the object is in
        """
        full_path = '/%s/%s/%s' % (account, container, obj)
        if all([host, partition, contdevice]):
            try:
                with ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(':', 1)
                    conn = http_connect(ip, port, contdevice, partition, op,
                                        full_path, headers_out)
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    response.read()
                    if is_success(response.status):
                        return
                    else:
                        self.logger.error(_(
                            'ERROR Container update failed '
                            '(saving for async update later): %(status)d '
                            'response from %(ip)s:%(port)s/%(dev)s'),
                            {'status': response.status, 'ip': ip, 'port': port,
                             'dev': contdevice})
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR container update failed with '
                    '%(ip)s:%(port)s/%(dev)s (saving for async update later)'),
                    {'ip': ip, 'port': port, 'dev': contdevice})
        async_dir = os.path.join(self.devices, objdevice, ASYNCDIR)
        ohash = hash_path(account, container, obj)
        self.logger.increment('async_pendings')
        write_pickle(
            {'op': op, 'account': account, 'container': container,
             'obj': obj, 'headers': headers_out},
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                         normalize_timestamp(headers_out['x-timestamp'])),
            os.path.join(self.devices, objdevice, 'tmp'))

    def container_update(self, op, account, container, obj, headers_in,
                         headers_out, objdevice):
        """
        Update the container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param headers_in: dictionary of headers from the original request
        :param headers_out: dictionary of headers to send in the container
                            request(s)
        :param objdevice: device name that the object is in
        """
        conthosts = [h.strip() for h in
                     headers_in.get('X-Container-Host', '').split(',')]
        contdevices = [d.strip() for d in
                       headers_in.get('X-Container-Device', '').split(',')]
        contpartition = headers_in.get('X-Container-Partition', '')

        if len(conthosts) != len(contdevices):
            # This shouldn't happen unless there's a bug in the proxy,
            # but if there is, we want to know about it.
            self.logger.error(_('ERROR Container update failed: different  '
                                'numbers of hosts and devices in request: '
                                '"%s" vs "%s"' %
                                (headers_in.get('X-Container-Host', ''),
                                 headers_in.get('X-Container-Device', ''))))
            return

        if contpartition:
            updates = zip(conthosts, contdevices)
        else:
            updates = []

        for conthost, contdevice in updates:
            self.async_update(op, account, container, obj, conthost,
                              contpartition, contdevice, headers_out,
                              objdevice)

    def delete_at_update(self, op, delete_at, account, container, obj,
                         headers_in, objdevice):
        """
        Update the expiring objects container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param headers_in: dictionary of headers from the original request
        :param objdevice: device name that the object is in
        """
        # Quick cap that will work from now until Sat Nov 20 17:46:39 2286
        # At that time, Swift will be so popular and pervasive I will have
        # created income for thousands of future programmers.
        delete_at = max(min(delete_at, 9999999999), 0)
        updates = [(None, None)]

        partition = None
        hosts = contdevices = [None]
        headers_out = {'x-timestamp': headers_in['x-timestamp'],
                       'x-trans-id': headers_in.get('x-trans-id', '-')}
        if op != 'DELETE':
            partition = headers_in.get('X-Delete-At-Partition', None)
            hosts = headers_in.get('X-Delete-At-Host', '')
            contdevices = headers_in.get('X-Delete-At-Device', '')
            updates = [upd for upd in
                       zip((h.strip() for h in hosts.split(',')),
                           (c.strip() for c in contdevices.split(',')))
                       if all(upd) and partition]
            if not updates:
                updates = [(None, None)]
            headers_out['x-size'] = '0'
            headers_out['x-content-type'] = 'text/plain'
            headers_out['x-etag'] = 'd41d8cd98f00b204e9800998ecf8427e'

        for host, contdevice in updates:
            self.async_update(
                op, self.expiring_objects_account,
                str(delete_at / self.expiring_objects_container_divisor *
                    self.expiring_objects_container_divisor),
                '%s-%s/%s/%s' % (delete_at, account, container, obj),
                host, partition, contdevice, headers_out, objdevice)

    @public
    @timing_stats
    def POST(self, request):
        """Handle HTTP POST requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                                  content_type='text/plain')
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)

        if file.is_deleted() or file.is_expired():
            return HTTPNotFound(request=request)
        try:
            file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            file.quarantine()
            return HTTPNotFound(request=request)
        metadata = {'X-Timestamp': request.headers['x-timestamp']}
        metadata.update(val for val in request.headers.iteritems()
                        if val[0].lower().startswith('x-object-meta-'))
        for header_key in self.allowed_headers:
            if header_key in request.headers:
                header_caps = header_key.title()
                metadata[header_caps] = request.headers[header_key]
        old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
        if old_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update('PUT', new_delete_at, account, container,
                                      obj, request.headers, device)
            if old_delete_at:
                self.delete_at_update('DELETE', old_delete_at, account,
                                      container, obj, request.headers, device)
        file.put_metadata(metadata)
        return HTTPAccepted(request=request)

    @public
    @timing_stats
    def PUT(self, request):
        """Handle HTTP PUT requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        error_response = check_object_creation(request, obj)
        if error_response:
            return error_response
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)
        orig_timestamp = file.metadata.get('X-Timestamp')
        upload_expiration = time.time() + self.max_upload_time
        etag = md5()
        upload_size = 0
        last_sync = 0
        with file.mkstemp() as fd:
            try:
                fallocate(fd, int(request.headers.get('content-length', 0)))
            except OSError:
                return HTTPInsufficientStorage(drive=device, request=request)
            reader = request.environ['wsgi.input'].read
            for chunk in iter(lambda: reader(self.network_chunk_size), ''):
                upload_size += len(chunk)
                if time.time() > upload_expiration:
                    self.logger.increment('PUT.timeouts')
                    return HTTPRequestTimeout(request=request)
                etag.update(chunk)
                while chunk:
                    written = os.write(fd, chunk)
                    chunk = chunk[written:]
                # For large files sync every 512MB (by default) written
                if upload_size - last_sync >= self.bytes_per_sync:
                    tpool.execute(fsync, fd)
                    drop_buffer_cache(fd, last_sync, upload_size - last_sync)
                    last_sync = upload_size
                sleep()

            if 'content-length' in request.headers and \
                    int(request.headers['content-length']) != upload_size:
                return HTTPClientDisconnect(request=request)
            etag = etag.hexdigest()
            if 'etag' in request.headers and \
                    request.headers['etag'].lower() != etag:
                return HTTPUnprocessableEntity(request=request)
            metadata = {
                'X-Timestamp': request.headers['x-timestamp'],
                'Content-Type': request.headers['content-type'],
                'ETag': etag,
                'Content-Length': str(upload_size),
            }
            metadata.update(val for val in request.headers.iteritems()
                            if val[0].lower().startswith('x-object-meta-') and
                            len(val[0]) > 14)
            for header_key in self.allowed_headers:
                if header_key in request.headers:
                    header_caps = header_key.title()
                    metadata[header_caps] = request.headers[header_key]
            old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
            if old_delete_at != new_delete_at:
                if new_delete_at:
                    self.delete_at_update(
                        'PUT', new_delete_at, account, container, obj,
                        request.headers, device)
                if old_delete_at:
                    self.delete_at_update(
                        'DELETE', old_delete_at, account, container, obj,
                        request.headers, device)
            file.put(fd, metadata)
        file.unlinkold(metadata['X-Timestamp'])
        if not orig_timestamp or \
                orig_timestamp < request.headers['x-timestamp']:
            self.container_update(
                'PUT', account, container, obj, request.headers,
                {'x-size': file.metadata['Content-Length'],
                 'x-content-type': file.metadata['Content-Type'],
                 'x-timestamp': file.metadata['X-Timestamp'],
                 'x-etag': file.metadata['ETag'],
                 'x-trans-id': request.headers.get('x-trans-id', '-')},
                device)
        resp = HTTPCreated(request=request, etag=etag)
        return resp

    @public
    @timing_stats
    def GET(self, request):
        """Handle HTTP GET requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, keep_data_fp=True,
                        disk_chunk_size=self.disk_chunk_size,
                        iter_hook=sleep)
        if file.is_deleted() or file.is_expired():
            if request.headers.get('if-match') == '*':
                return HTTPPreconditionFailed(request=request)
            else:
                return HTTPNotFound(request=request)
        try:
            file_size = file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            file.quarantine()
            return HTTPNotFound(request=request)
        if request.headers.get('if-match') not in (None, '*') and \
                file.metadata['ETag'] not in request.if_match:
            file.close()
            return HTTPPreconditionFailed(request=request)
        if request.headers.get('if-none-match') is not None:
            if file.metadata['ETag'] in request.if_none_match:
                resp = HTTPNotModified(request=request)
                resp.etag = file.metadata['ETag']
                file.close()
                return resp
        try:
            if_unmodified_since = request.if_unmodified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_unmodified_since and \
                datetime.fromtimestamp(
                    float(file.metadata['X-Timestamp']), UTC) > \
                if_unmodified_since:
            file.close()
            return HTTPPreconditionFailed(request=request)
        try:
            if_modified_since = request.if_modified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_modified_since and \
                datetime.fromtimestamp(
                    float(file.metadata['X-Timestamp']), UTC) < \
                if_modified_since:
            file.close()
            return HTTPNotModified(request=request)
        response = Response(app_iter=file,
                            request=request, conditional_response=True)
        response.headers['Content-Type'] = file.metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in file.metadata.iteritems():
            if key.lower().startswith('x-object-meta-') or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = file.metadata['ETag']
        response.last_modified = float(file.metadata['X-Timestamp'])
        response.content_length = file_size
        if response.content_length < self.keep_cache_size and \
                (self.keep_cache_private or
                 ('X-Auth-Token' not in request.headers and
                  'X-Storage-Token' not in request.headers)):
            file.keep_cache = True
        if 'Content-Encoding' in file.metadata:
            response.content_encoding = file.metadata['Content-Encoding']
        response.headers['X-Timestamp'] = file.metadata['X-Timestamp']
        return request.get_response(response)

    @public
    @timing_stats
    def HEAD(self, request):
        """Handle HTTP HEAD requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            resp = HTTPBadRequest(request=request)
            resp.content_type = 'text/plain'
            resp.body = str(err)
            return resp
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)
        if file.is_deleted() or file.is_expired():
            return HTTPNotFound(request=request)
        try:
            file_size = file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            file.quarantine()
            return HTTPNotFound(request=request)
        response = Response(request=request, conditional_response=True)
        response.headers['Content-Type'] = file.metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in file.metadata.iteritems():
            if key.lower().startswith('x-object-meta-') or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = file.metadata['ETag']
        response.last_modified = float(file.metadata['X-Timestamp'])
        # Needed for container sync feature
        response.headers['X-Timestamp'] = file.metadata['X-Timestamp']
        response.content_length = file_size
        if 'Content-Encoding' in file.metadata:
            response.content_encoding = file.metadata['Content-Encoding']
        return response

    @public
    @timing_stats
    def DELETE(self, request):
        """Handle HTTP DELETE requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, e:
            return HTTPBadRequest(body=str(e), request=request,
                                  content_type='text/plain')
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        response_class = HTTPNoContent
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)
        if 'x-if-delete-at' in request.headers and \
                int(request.headers['x-if-delete-at']) != \
                int(file.metadata.get('X-Delete-At') or 0):
            return HTTPPreconditionFailed(
                request=request,
                body='X-If-Delete-At and X-Delete-At do not match')
        orig_timestamp = file.metadata.get('X-Timestamp')
        if file.is_deleted() or file.is_expired():
            response_class = HTTPNotFound
        metadata = {
            'X-Timestamp': request.headers['X-Timestamp'], 'deleted': True,
        }
        old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
        if old_delete_at:
            self.delete_at_update('DELETE', old_delete_at, account,
                                  container, obj, request.headers, device)
        file.put_metadata(metadata, tombstone=True)
        file.unlinkold(metadata['X-Timestamp'])
        if not orig_timestamp or \
                orig_timestamp < request.headers['x-timestamp']:
            self.container_update(
                'DELETE', account, container, obj, request.headers,
                {'x-timestamp': metadata['X-Timestamp'],
                 'x-trans-id': request.headers.get('x-trans-id', '-')},
                device)
        resp = response_class(request=request)
        return resp

    @public
    @timing_stats
    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        try:
            device, partition, suffix = split_path(
                unquote(request.path), 2, 3, True)
            validate_device_partition(device, partition)
        except ValueError, e:
            return HTTPBadRequest(body=str(e), request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        path = os.path.join(self.devices, device, DATADIR, partition)
        if not os.path.exists(path):
            mkdirs(path)
        suffixes = suffix.split('-') if suffix else []
        _junk, hashes = tpool_reraise(get_hashes, path, recalculate=suffixes)
        return Response(body=pickle.dumps(hashes))

    def __call__(self, env, start_response):
        """WSGI Application entry point for the Swift Object Server."""
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)

        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8 or contains NULL')
        else:
            try:
                # disallow methods which have not been marked 'public'
                try:
                    method = getattr(self, req.method)
                    getattr(method, 'publicly_accessible')
                except AttributeError:
                    res = HTTPMethodNotAllowed()
                else:
                    res = method(req)
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s'
                    ' %(path)s '), {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = time.time() - start_time
        if self.log_requests:
            log_line = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %.4f' % (
                req.remote_addr,
                time.strftime('%d/%b/%Y:%H:%M:%S +0000',
                              time.gmtime()),
                req.method, req.path, res.status.split()[0],
                res.content_length or '-', req.referer or '-',
                req.headers.get('x-trans-id', '-'),
                req.user_agent or '-',
                trans_time)
            if req.method == 'REPLICATE':
                self.logger.debug(log_line)
            else:
                self.logger.info(log_line)
        if req.method in ('PUT', 'DELETE'):
            slow = self.slow - trans_time
            if slow > 0:
                sleep(slow)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
