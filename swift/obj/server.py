# Copyright (c) 2010-2011 OpenStack, LLC.
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

from webob import Request, Response, UTC
from webob.exc import HTTPAccepted, HTTPBadRequest, HTTPCreated, \
    HTTPInternalServerError, HTTPNoContent, HTTPNotFound, \
    HTTPNotModified, HTTPPreconditionFailed, \
    HTTPRequestTimeout, HTTPUnprocessableEntity, HTTPMethodNotAllowed
from xattr import getxattr, setxattr
from eventlet import sleep, Timeout, TimeoutError, tpool

from swift.common.utils import mkdirs, normalize_timestamp, \
    storage_directory, hash_path, renamer, fallocate, \
    split_path, drop_buffer_cache, get_logger, write_pickle
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_object_creation, check_mount, \
    check_float, check_utf8
from swift.common.exceptions import ConnectionTimeout
from swift.obj.replicator import get_hashes, invalidate_hash, \
    recalculate_hashes


DATADIR = 'objects'
ASYNCDIR = 'async_pending'
PICKLE_PROTOCOL = 2
METADATA_KEY = 'user.swift.metadata'
MAX_OBJECT_NAME_LENGTH = 1024
KEEP_CACHE_SIZE = (5 * 1024 * 1024)


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
    :param disk_chunk_Size: size of chunks on file reads
    """

    def __init__(self, path, device, partition, account, container, obj,
                    keep_data_fp=False, disk_chunk_size=65536):
        self.disk_chunk_size = disk_chunk_size
        self.name = '/' + '/'.join((account, container, obj))
        name_hash = hash_path(account, container, obj)
        self.datadir = os.path.join(path, device,
                    storage_directory(DATADIR, partition, name_hash))
        self.tmpdir = os.path.join(path, device, 'tmp')
        self.metadata = {}
        self.meta_file = None
        self.data_file = None
        self.fp = None
        self.keep_cache = False
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
            self.close()
        if self.meta_file:
            with open(self.meta_file) as mfp:
                for key in self.metadata.keys():
                    if key.lower() not in ('content-type', 'content-encoding',
                                'deleted', 'content-length', 'etag'):
                        del self.metadata[key]
                self.metadata.update(read_metadata(mfp))

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            dropped_cache = 0
            read = 0
            while True:
                chunk = self.fp.read(self.disk_chunk_size)
                if chunk:
                    read += len(chunk)
                    if read - dropped_cache > (1024 * 1024):
                        self.drop_cache(self.fp.fileno(), dropped_cache,
                            read - dropped_cache)
                        dropped_cache = read
                    yield chunk
                else:
                    self.drop_cache(self.fp.fileno(), dropped_cache,
                        read - dropped_cache)
                    break
        finally:
            self.close()

    def app_iter_range(self, start, stop):
        """Returns an iterator over the data file for range (start, stop)"""
        if start:
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

    def close(self):
        """Close the file."""
        if self.fp:
            self.fp.close()
            self.fp = None

    def is_deleted(self):
        """
        Check if the file is deleted.

        :returns: True if the file doesn't exist or has been flagged as
                  deleted.
        """
        return not self.data_file or 'deleted' in self.metadata

    @contextmanager
    def mkstemp(self):
        """Contextmanager to make a temporary file."""
        if not os.path.exists(self.tmpdir):
            mkdirs(self.tmpdir)
        fd, tmppath = mkstemp(dir=self.tmpdir)
        try:
            yield fd, tmppath
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(tmppath)
            except OSError:
                pass

    def put(self, fd, tmppath, metadata, extension='.data'):
        """
        Finalize writing the file on disk, and renames it from the temp file to
        the real location.  This should be called after the data has been
        written to the temp file.

        :params fd: file descriptor of the temp file
        :param tmppath: path to the temporary file being used
        :param metadata: dictionary of metadata to be written
        :param extention: extension to be used when making the file
        """
        metadata['name'] = self.name
        timestamp = normalize_timestamp(metadata['X-Timestamp'])
        write_metadata(fd, metadata)
        if 'Content-Length' in metadata:
            self.drop_cache(fd, 0, int(metadata['Content-Length']))
        tpool.execute(os.fsync, fd)
        invalidate_hash(os.path.dirname(self.datadir))
        renamer(tmppath, os.path.join(self.datadir, timestamp + extension))
        self.metadata = metadata

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


class ObjectController(object):
    """Implements the WSGI application for the Swift Object Server."""

    def __init__(self, conf):
        """
        Creates a new WSGI application for the Swift Object Server. An
        example configuration is given at
        <source-dir>/etc/object-server.conf-sample or
        /etc/swift/object-server.conf-sample.
        """
        self.logger = get_logger(conf)
        self.devices = conf.get('devices', '/srv/node/')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.log_requests = conf.get('log_requests', 't')[:1].lower() == 't'
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.slow = int(conf.get('slow', 0))
        self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024

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
                            request
        :param objdevice: device name that the object is in
        """
        host = headers_in.get('X-Container-Host', None)
        partition = headers_in.get('X-Container-Partition', None)
        contdevice = headers_in.get('X-Container-Device', None)
        if not all([host, partition, contdevice]):
            return
        full_path = '/%s/%s/%s' % (account, container, obj)
        try:
            with ConnectionTimeout(self.conn_timeout):
                ip, port = host.split(':')
                conn = http_connect(ip, port, contdevice, partition, op,
                        full_path, headers_out)
            with Timeout(self.node_timeout):
                response = conn.getresponse()
                response.read()
                if 200 <= response.status < 300:
                    return
                else:
                    self.logger.error(_('ERROR Container update failed '
                        '(saving for async update later): %(status)d '
                        'response from %(ip)s:%(port)s/%(dev)s'),
                        {'status': response.status, 'ip': ip, 'port': port,
                         'dev': contdevice})
        except (Exception, TimeoutError):
            self.logger.exception(_('ERROR container update failed with '
                '%(ip)s:%(port)s/%(dev)s (saving for async update later)'),
                {'ip': ip, 'port': port, 'dev': contdevice})
        async_dir = os.path.join(self.devices, objdevice, ASYNCDIR)
        ohash = hash_path(account, container, obj)
        write_pickle(
            {'op': op, 'account': account, 'container': container,
                'obj': obj, 'headers': headers_out},
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                normalize_timestamp(headers_out['x-timestamp'])),
            os.path.join(self.devices, objdevice, 'tmp'))

    def POST(self, request):
        """Handle HTTP POST requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                        content_type='text/plain')
        if 'x-timestamp' not in request.headers or \
                    not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                        content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return Response(status='507 %s is not mounted' % device)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, disk_chunk_size=self.disk_chunk_size)

        if file.is_deleted():
            response_class = HTTPNotFound
        else:
            response_class = HTTPAccepted

        metadata = {'X-Timestamp': request.headers['x-timestamp']}
        metadata.update(val for val in request.headers.iteritems()
                if val[0].lower().startswith('x-object-meta-'))
        with file.mkstemp() as (fd, tmppath):
            file.put(fd, tmppath, metadata, extension='.meta')
        return response_class(request=request)

    def PUT(self, request):
        """Handle HTTP PUT requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                        content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return Response(status='507 %s is not mounted' % device)
        if 'x-timestamp' not in request.headers or \
                    not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                        content_type='text/plain')
        error_response = check_object_creation(request, obj)
        if error_response:
            return error_response
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, disk_chunk_size=self.disk_chunk_size)
        upload_expiration = time.time() + self.max_upload_time
        etag = md5()
        upload_size = 0
        last_sync = 0
        with file.mkstemp() as (fd, tmppath):
            if 'content-length' in request.headers:
                fallocate(fd, int(request.headers['content-length']))
            for chunk in iter(lambda: request.body_file.read(
                    self.network_chunk_size), ''):
                upload_size += len(chunk)
                if time.time() > upload_expiration:
                    return HTTPRequestTimeout(request=request)
                etag.update(chunk)
                while chunk:
                    written = os.write(fd, chunk)
                    chunk = chunk[written:]
                # For large files sync every 512MB (by default) written
                if upload_size - last_sync >= self.bytes_per_sync:
                    tpool.execute(os.fdatasync, fd)
                    drop_buffer_cache(fd, last_sync, upload_size - last_sync)
                    last_sync = upload_size

            if 'content-length' in request.headers and \
                    int(request.headers['content-length']) != upload_size:
                return Response(status='499 Client Disconnect')
            etag = etag.hexdigest()
            if 'etag' in request.headers and \
                            request.headers['etag'].lower() != etag:
                return HTTPUnprocessableEntity(request=request)
            metadata = {
                'X-Timestamp': request.headers['x-timestamp'],
                'Content-Type': request.headers['content-type'],
                'ETag': etag,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            if 'x-object-manifest' in request.headers:
                metadata['X-Object-Manifest'] = \
                    request.headers['x-object-manifest']
            metadata.update(val for val in request.headers.iteritems()
                    if val[0].lower().startswith('x-object-meta-') and
                    len(val[0]) > 14)
            if 'content-encoding' in request.headers:
                metadata['Content-Encoding'] = \
                    request.headers['Content-Encoding']
            file.put(fd, tmppath, metadata)
        file.unlinkold(metadata['X-Timestamp'])
        self.container_update('PUT', account, container, obj, request.headers,
            {'x-size': file.metadata['Content-Length'],
             'x-content-type': file.metadata['Content-Type'],
             'x-timestamp': file.metadata['X-Timestamp'],
             'x-etag': file.metadata['ETag'],
             'x-cf-trans-id': request.headers.get('x-cf-trans-id', '-')},
            device)
        resp = HTTPCreated(request=request, etag=etag)
        return resp

    def GET(self, request):
        """Handle HTTP GET requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                        content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return Response(status='507 %s is not mounted' % device)
        file = DiskFile(self.devices, device, partition, account, container,
                obj, keep_data_fp=True, disk_chunk_size=self.disk_chunk_size)
        if file.is_deleted():
            if request.headers.get('if-match') == '*':
                return HTTPPreconditionFailed(request=request)
            else:
                return HTTPNotFound(request=request)
        if request.headers.get('if-match') not in (None, '*') and \
                file.metadata['ETag'] not in request.if_match:
            file.close()
            return HTTPPreconditionFailed(request=request)
        if request.headers.get('if-none-match') != None:
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
           datetime.fromtimestamp(float(file.metadata['X-Timestamp']), UTC) > \
           if_unmodified_since:
            file.close()
            return HTTPPreconditionFailed(request=request)
        try:
            if_modified_since = request.if_modified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_modified_since and \
           datetime.fromtimestamp(float(file.metadata['X-Timestamp']), UTC) < \
           if_modified_since:
            file.close()
            return HTTPNotModified(request=request)
        response = Response(content_type=file.metadata.get('Content-Type',
                        'application/octet-stream'), app_iter=file,
                        request=request, conditional_response=True)
        for key, value in file.metadata.iteritems():
            if key == 'X-Object-Manifest' or \
                    key.lower().startswith('x-object-meta-'):
                response.headers[key] = value
        response.etag = file.metadata['ETag']
        response.last_modified = float(file.metadata['X-Timestamp'])
        response.content_length = int(file.metadata['Content-Length'])
        if response.content_length < KEEP_CACHE_SIZE and \
                'X-Auth-Token' not in request.headers and \
                'X-Storage-Token' not in request.headers:
            file.keep_cache = True
        if 'Content-Encoding' in file.metadata:
            response.content_encoding = file.metadata['Content-Encoding']
        return request.get_response(response)

    def HEAD(self, request):
        """Handle HTTP HEAD requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
        except ValueError, err:
            resp = HTTPBadRequest(request=request)
            resp.content_type = 'text/plain'
            resp.body = str(err)
            return resp
        if self.mount_check and not check_mount(self.devices, device):
            return Response(status='507 %s is not mounted' % device)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, disk_chunk_size=self.disk_chunk_size)
        if file.is_deleted():
            return HTTPNotFound(request=request)
        response = Response(content_type=file.metadata['Content-Type'],
                            request=request, conditional_response=True)
        for key, value in file.metadata.iteritems():
            if key == 'X-Object-Manifest' or \
                    key.lower().startswith('x-object-meta-'):
                response.headers[key] = value
        response.etag = file.metadata['ETag']
        response.last_modified = float(file.metadata['X-Timestamp'])
        response.content_length = int(file.metadata['Content-Length'])
        if 'Content-Encoding' in file.metadata:
            response.content_encoding = file.metadata['Content-Encoding']
        return response

    def DELETE(self, request):
        """Handle HTTP DELETE requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
        except ValueError, e:
            return HTTPBadRequest(body=str(e), request=request,
                        content_type='text/plain')
        if 'x-timestamp' not in request.headers or \
                    not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                        content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return Response(status='507 %s is not mounted' % device)
        response_class = HTTPNoContent
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, disk_chunk_size=self.disk_chunk_size)
        if file.is_deleted():
            response_class = HTTPNotFound
        metadata = {
            'X-Timestamp': request.headers['X-Timestamp'], 'deleted': True,
        }
        with file.mkstemp() as (fd, tmppath):
            file.put(fd, tmppath, metadata, extension='.ts')
        file.unlinkold(metadata['X-Timestamp'])
        self.container_update('DELETE', account, container, obj,
            request.headers, {'x-timestamp': metadata['X-Timestamp'],
            'x-cf-trans-id': request.headers.get('x-cf-trans-id', '-')},
            device)
        resp = response_class(request=request)
        return resp

    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        try:
            device, partition, suffix = split_path(
                unquote(request.path), 2, 3, True)
        except ValueError, e:
            return HTTPBadRequest(body=str(e), request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return Response(status='507 %s is not mounted' % device)
        path = os.path.join(self.devices, device, DATADIR, partition)
        if not os.path.exists(path):
            mkdirs(path)
        if suffix:
            recalculate_hashes(path, suffix.split('-'))
            return Response()
        _junk, hashes = get_hashes(path, do_listdir=False)
        return Response(body=pickle.dumps(hashes))

    def __call__(self, env, start_response):
        """WSGI Application entry point for the Swift Object Server."""
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-cf-trans-id', None)
        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8')
        else:
            try:
                if hasattr(self, req.method):
                    res = getattr(self, req.method)(req)
                else:
                    res = HTTPMethodNotAllowed()
            except Exception:
                self.logger.exception(_('ERROR __call__ error with %(method)s'
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
                req.headers.get('x-cf-trans-id', '-'),
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
