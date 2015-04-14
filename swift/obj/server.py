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

""" Object Server for Swift """

import cPickle as pickle
import json
import os
import multiprocessing
import time
import traceback
import rfc822
import socket
import math
from swift import gettext_ as _
from hashlib import md5

from eventlet import sleep, wsgi, Timeout

from swift.common.utils import public, get_logger, \
    config_true_value, timing_stats, replication, \
    normalize_delete_at_timestamp, get_log_line, Timestamp, \
    get_expirer_container, iter_multipart_mime_documents
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_object_creation, \
    valid_timestamp, check_utf8
from swift.common.exceptions import ConnectionTimeout, DiskFileQuarantined, \
    DiskFileNotExist, DiskFileCollision, DiskFileNoSpace, DiskFileDeleted, \
    DiskFileDeviceUnavailable, DiskFileExpired, ChunkReadTimeout, \
    DiskFileXattrNotSupported
from swift.obj import ssync_receiver
from swift.common.http import is_success
from swift.common.base_storage_server import BaseStorageServer
from swift.common.request_helpers import get_name_and_placement, \
    is_user_meta, is_sys_or_user_meta
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPCreated, \
    HTTPInternalServerError, HTTPNoContent, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity, \
    HTTPClientDisconnect, HTTPMethodNotAllowed, Request, Response, \
    HTTPInsufficientStorage, HTTPForbidden, HTTPException, HeaderKeyDict, \
    HTTPConflict, HTTPServerError
from swift.obj.diskfile import DATAFILE_SYSTEM_META, DiskFileRouter


def iter_mime_headers_and_bodies(wsgi_input, mime_boundary, read_chunk_size):
    mime_documents_iter = iter_multipart_mime_documents(
        wsgi_input, mime_boundary, read_chunk_size)

    for file_like in mime_documents_iter:
        hdrs = HeaderKeyDict(rfc822.Message(file_like, 0))
        yield (hdrs, file_like)


def drain(file_like, read_size, timeout):
    """
    Read and discard any bytes from file_like.

    :param file_like: file-like object to read from
    :param read_size: how big a chunk to read at a time
    :param timeout: how long to wait for a read (use None for no timeout)

    :raises ChunkReadTimeout: if no chunk was read in time
    """

    while True:
        with ChunkReadTimeout(timeout):
            chunk = file_like.read(read_size)
            if not chunk:
                break


class EventletPlungerString(str):
    """
    Eventlet won't send headers until it's accumulated at least
    eventlet.wsgi.MINIMUM_CHUNK_SIZE bytes or the app iter is exhausted. If we
    want to send the response body behind Eventlet's back, perhaps with some
    zero-copy wizardry, then we have to unclog the plumbing in eventlet.wsgi
    to force the headers out, so we use an EventletPlungerString to empty out
    all of Eventlet's buffers.
    """
    def __len__(self):
        return wsgi.MINIMUM_CHUNK_SIZE + 1


class ObjectController(BaseStorageServer):
    """Implements the WSGI application for the Swift Object Server."""

    server_type = 'object-server'

    def __init__(self, conf, logger=None):
        """
        Creates a new WSGI application for the Swift Object Server. An
        example configuration is given at
        <source-dir>/etc/object-server.conf-sample or
        /etc/swift/object-server.conf-sample.
        """
        super(ObjectController, self).__init__(conf)
        self.logger = logger or get_logger(conf, log_route='object-server')
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.client_timeout = int(conf.get('client_timeout', 60))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.slow = int(conf.get('slow', 0))
        self.keep_cache_private = \
            config_true_value(conf.get('keep_cache_private', 'false'))

        default_allowed_headers = '''
            content-disposition,
            content-encoding,
            x-delete-at,
            x-object-manifest,
            x-static-large-object,
        '''
        extra_allowed_headers = [
            header.strip().lower() for header in conf.get(
                'allowed_headers', default_allowed_headers).split(',')
            if header.strip()
        ]
        self.allowed_headers = set()
        for header in extra_allowed_headers:
            if header not in DATAFILE_SYSTEM_META:
                self.allowed_headers.add(header)
        self.auto_create_account_prefix = \
            conf.get('auto_create_account_prefix') or '.'
        self.expiring_objects_account = self.auto_create_account_prefix + \
            (conf.get('expiring_objects_account_name') or 'expiring_objects')
        self.expiring_objects_container_divisor = \
            int(conf.get('expiring_objects_container_divisor') or 86400)
        # Initialization was successful, so now apply the network chunk size
        # parameter as the default read / write buffer size for the network
        # sockets.
        #
        # NOTE WELL: This is a class setting, so until we get set this on a
        # per-connection basis, this affects reading and writing on ALL
        # sockets, those between the proxy servers and external clients, and
        # those between the proxy servers and the other internal servers.
        #
        # ** Because the primary motivation for this is to optimize how data
        # is written back to the proxy server, we could use the value from the
        # disk_chunk_size parameter. However, it affects all created sockets
        # using this class so we have chosen to tie it to the
        # network_chunk_size parameter value instead.
        socket._fileobject.default_bufsize = self.network_chunk_size

        # Provide further setup specific to an object server implementation.
        self.setup(conf)

    def setup(self, conf):
        """
        Implementation specific setup. This method is called at the very end
        by the constructor to allow a specific implementation to modify
        existing attributes or add its own attributes.

        :param conf: WSGI configuration parameter
        """

        # Common on-disk hierarchy shared across account, container and object
        # servers.
        self._diskfile_router = DiskFileRouter(conf, self.logger)
        # This is populated by global_conf_callback way below as the semaphore
        # is shared by all workers.
        if 'replication_semaphore' in conf:
            # The value was put in a list so it could get past paste
            self.replication_semaphore = conf['replication_semaphore'][0]
        else:
            self.replication_semaphore = None
        self.replication_failure_threshold = int(
            conf.get('replication_failure_threshold') or 100)
        self.replication_failure_ratio = float(
            conf.get('replication_failure_ratio') or 1.0)

    def get_diskfile(self, device, partition, account, container, obj,
                     policy, **kwargs):
        """
        Utility method for instantiating a DiskFile object supporting a given
        REST API.

        An implementation of the object server that wants to use a different
        DiskFile class would simply over-ride this method to provide that
        behavior.
        """
        return self._diskfile_router[policy].get_diskfile(
            device, partition, account, container, obj, policy, **kwargs)

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice, policy):
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
        :param policy: the associated BaseStoragePolicy instance
        """
        headers_out['user-agent'] = 'object-server %s' % os.getpid()
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
        data = {'op': op, 'account': account, 'container': container,
                'obj': obj, 'headers': headers_out}
        timestamp = headers_out['x-timestamp']
        self._diskfile_router[policy].pickle_async_update(
            objdevice, account, container, obj, data, timestamp, policy)

    def container_update(self, op, account, container, obj, request,
                         headers_out, objdevice, policy):
        """
        Update the container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param request: the original request object driving the update
        :param headers_out: dictionary of headers to send in the container
                            request(s)
        :param objdevice: device name that the object is in
        :param policy:  the BaseStoragePolicy instance
        """
        headers_in = request.headers
        conthosts = [h.strip() for h in
                     headers_in.get('X-Container-Host', '').split(',')]
        contdevices = [d.strip() for d in
                       headers_in.get('X-Container-Device', '').split(',')]
        contpartition = headers_in.get('X-Container-Partition', '')

        if len(conthosts) != len(contdevices):
            # This shouldn't happen unless there's a bug in the proxy,
            # but if there is, we want to know about it.
            self.logger.error(_('ERROR Container update failed: different '
                                'numbers of hosts and devices in request: '
                                '"%s" vs "%s"') %
                               (headers_in.get('X-Container-Host', ''),
                                headers_in.get('X-Container-Device', '')))
            return

        if contpartition:
            updates = zip(conthosts, contdevices)
        else:
            updates = []

        headers_out['x-trans-id'] = headers_in.get('x-trans-id', '-')
        headers_out['referer'] = request.as_referer()
        headers_out['X-Backend-Storage-Policy-Index'] = int(policy)
        for conthost, contdevice in updates:
            self.async_update(op, account, container, obj, conthost,
                              contpartition, contdevice, headers_out,
                              objdevice, policy)

    def delete_at_update(self, op, delete_at, account, container, obj,
                         request, objdevice, policy):
        """
        Update the expiring objects container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param delete_at: scheduled delete in UNIX seconds, int
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param request: the original request driving the update
        :param objdevice: device name that the object is in
        :param policy: the BaseStoragePolicy instance (used for tmp dir)
        """
        if config_true_value(
                request.headers.get('x-backend-replication', 'f')):
            return
        delete_at = normalize_delete_at_timestamp(delete_at)
        updates = [(None, None)]

        partition = None
        hosts = contdevices = [None]
        headers_in = request.headers
        headers_out = HeaderKeyDict({
            # system accounts are always Policy-0
            'X-Backend-Storage-Policy-Index': 0,
            'x-timestamp': request.timestamp.internal,
            'x-trans-id': headers_in.get('x-trans-id', '-'),
            'referer': request.as_referer()})
        if op != 'DELETE':
            delete_at_container = headers_in.get('X-Delete-At-Container', None)
            if not delete_at_container:
                self.logger.warning(
                    'X-Delete-At-Container header must be specified for '
                    'expiring objects background %s to work properly. Making '
                    'best guess as to the container name for now.' % op)
                # TODO(gholt): In a future release, change the above warning to
                # a raised exception and remove the guess code below.
                delete_at_container = get_expirer_container(
                    delete_at, self.expiring_objects_container_divisor,
                    account, container, obj)
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
        else:
            # DELETEs of old expiration data have no way of knowing what the
            # old X-Delete-At-Container was at the time of the initial setting
            # of the data, so a best guess is made here.
            # Worst case is a DELETE is issued now for something that doesn't
            # exist there and the original data is left where it is, where
            # it will be ignored when the expirer eventually tries to issue the
            # object DELETE later since the X-Delete-At value won't match up.
            delete_at_container = get_expirer_container(
                delete_at, self.expiring_objects_container_divisor,
                account, container, obj)
        delete_at_container = normalize_delete_at_timestamp(
            delete_at_container)

        for host, contdevice in updates:
            self.async_update(
                op, self.expiring_objects_account, delete_at_container,
                '%s-%s/%s/%s' % (delete_at, account, container, obj),
                host, partition, contdevice, headers_out, objdevice,
                policy)

    def _make_timeout_reader(self, file_like):
        def timeout_reader():
            with ChunkReadTimeout(self.client_timeout):
                return file_like.read(self.network_chunk_size)
        return timeout_reader

    def _read_put_commit_message(self, mime_documents_iter):
        rcvd_commit = False
        try:
            with ChunkReadTimeout(self.client_timeout):
                commit_hdrs, commit_iter = next(mime_documents_iter)
                if commit_hdrs.get('X-Document', None) == "put commit":
                    rcvd_commit = True
            drain(commit_iter, self.network_chunk_size, self.client_timeout)
        except ChunkReadTimeout:
            raise HTTPClientDisconnect()
        except StopIteration:
            raise HTTPBadRequest(body="couldn't find PUT commit MIME doc")
        return rcvd_commit

    def _read_metadata_footer(self, mime_documents_iter):
        try:
            with ChunkReadTimeout(self.client_timeout):
                footer_hdrs, footer_iter = next(mime_documents_iter)
        except ChunkReadTimeout:
            raise HTTPClientDisconnect()
        except StopIteration:
            raise HTTPBadRequest(body="couldn't find footer MIME doc")

        timeout_reader = self._make_timeout_reader(footer_iter)
        try:
            footer_body = ''.join(iter(timeout_reader, ''))
        except ChunkReadTimeout:
            raise HTTPClientDisconnect()

        footer_md5 = footer_hdrs.get('Content-MD5')
        if not footer_md5:
            raise HTTPBadRequest(body="no Content-MD5 in footer")
        if footer_md5 != md5(footer_body).hexdigest():
            raise HTTPUnprocessableEntity(body="footer MD5 mismatch")

        try:
            return HeaderKeyDict(json.loads(footer_body))
        except ValueError:
            raise HTTPBadRequest("invalid JSON for footer doc")

    def _check_container_override(self, update_headers, metadata):
        for key, val in metadata.iteritems():
            override_prefix = 'x-backend-container-update-override-'
            if key.lower().startswith(override_prefix):
                override = key.lower().replace(override_prefix, 'x-')
                update_headers[override] = val

    @public
    @timing_stats()
    def POST(self, request):
        """Handle HTTP POST requests for the Swift Object Server."""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        req_timestamp = valid_timestamp(request)
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy=policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            orig_metadata = disk_file.read_metadata()
        except DiskFileXattrNotSupported:
            return HTTPInsufficientStorage(drive=device, request=request)
        except (DiskFileNotExist, DiskFileQuarantined):
            return HTTPNotFound(request=request)
        orig_timestamp = Timestamp(orig_metadata.get('X-Timestamp', 0))
        if orig_timestamp >= req_timestamp:
            return HTTPConflict(
                request=request,
                headers={'X-Backend-Timestamp': orig_timestamp.internal})
        metadata = {'X-Timestamp': req_timestamp.internal}
        metadata.update(val for val in request.headers.iteritems()
                        if is_user_meta('object', val[0]))
        for header_key in self.allowed_headers:
            if header_key in request.headers:
                header_caps = header_key.title()
                metadata[header_caps] = request.headers[header_key]
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        if orig_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update('PUT', new_delete_at, account, container,
                                      obj, request, device, policy)
            if orig_delete_at:
                self.delete_at_update('DELETE', orig_delete_at, account,
                                      container, obj, request, device,
                                      policy)
        try:
            disk_file.write_metadata(metadata)
        except (DiskFileXattrNotSupported, DiskFileNoSpace):
            return HTTPInsufficientStorage(drive=device, request=request)
        return HTTPAccepted(request=request)

    @public
    @timing_stats()
    def PUT(self, request):
        """Handle HTTP PUT requests for the Swift Object Server."""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        req_timestamp = valid_timestamp(request)
        error_response = check_object_creation(request, obj)
        if error_response:
            return error_response
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        try:
            fsize = request.message_length()
        except ValueError as e:
            return HTTPBadRequest(body=str(e), request=request,
                                  content_type='text/plain')

        # In case of multipart-MIME put, the proxy sends a chunked request,
        # but may let us know the real content length so we can verify that
        # we have enough disk space to hold the object.
        if fsize is None:
            fsize = request.headers.get('X-Backend-Obj-Content-Length')
            if fsize is not None:
                try:
                    fsize = int(fsize)
                except ValueError as e:
                    return HTTPBadRequest(body=str(e), request=request,
                                          content_type='text/plain')
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy=policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            orig_metadata = disk_file.read_metadata()
        except DiskFileXattrNotSupported:
            return HTTPInsufficientStorage(drive=device, request=request)
        except (DiskFileNotExist, DiskFileQuarantined):
            orig_metadata = {}

        # Checks for If-None-Match
        if request.if_none_match is not None and orig_metadata:
            if '*' in request.if_none_match:
                # File exists already so return 412
                return HTTPPreconditionFailed(request=request)
            if orig_metadata.get('ETag') in request.if_none_match:
                # The current ETag matches, so return 412
                return HTTPPreconditionFailed(request=request)

        orig_timestamp = Timestamp(orig_metadata.get('X-Timestamp', 0))
        if orig_timestamp >= req_timestamp:
            return HTTPConflict(
                request=request,
                headers={'X-Backend-Timestamp': orig_timestamp.internal})
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        upload_expiration = time.time() + self.max_upload_time
        etag = md5()
        elapsed_time = 0
        try:
            with disk_file.create(size=fsize) as writer:
                upload_size = 0

                # If the proxy wants to send us object metadata after the
                # object body, it sets some headers. We have to tell the
                # proxy, in the 100 Continue response, that we're able to
                # parse a multipart MIME document and extract the object and
                # metadata from it. If we don't, then the proxy won't
                # actually send the footer metadata.
                have_metadata_footer = False
                use_multiphase_commit = False
                mime_documents_iter = iter([])
                obj_input = request.environ['wsgi.input']

                hundred_continue_headers = []
                if config_true_value(
                        request.headers.get(
                            'X-Backend-Obj-Multiphase-Commit')):
                    use_multiphase_commit = True
                    hundred_continue_headers.append(
                        ('X-Obj-Multiphase-Commit', 'yes'))

                if config_true_value(
                        request.headers.get('X-Backend-Obj-Metadata-Footer')):
                    have_metadata_footer = True
                    hundred_continue_headers.append(
                        ('X-Obj-Metadata-Footer', 'yes'))

                if have_metadata_footer or use_multiphase_commit:
                    obj_input.set_hundred_continue_response_headers(
                        hundred_continue_headers)
                    mime_boundary = request.headers.get(
                        'X-Backend-Obj-Multipart-Mime-Boundary')
                    if not mime_boundary:
                        return HTTPBadRequest("no MIME boundary")

                    try:
                        with ChunkReadTimeout(self.client_timeout):
                            mime_documents_iter = iter_mime_headers_and_bodies(
                                request.environ['wsgi.input'],
                                mime_boundary, self.network_chunk_size)
                            _junk_hdrs, obj_input = next(mime_documents_iter)
                    except ChunkReadTimeout:
                        return HTTPRequestTimeout(request=request)

                timeout_reader = self._make_timeout_reader(obj_input)
                try:
                    for chunk in iter(timeout_reader, ''):
                        start_time = time.time()
                        if start_time > upload_expiration:
                            self.logger.increment('PUT.timeouts')
                            return HTTPRequestTimeout(request=request)
                        etag.update(chunk)
                        upload_size = writer.write(chunk)
                        elapsed_time += time.time() - start_time
                except ChunkReadTimeout:
                    return HTTPRequestTimeout(request=request)
                if upload_size:
                    self.logger.transfer_rate(
                        'PUT.' + device + '.timing', elapsed_time,
                        upload_size)
                if fsize is not None and fsize != upload_size:
                    return HTTPClientDisconnect(request=request)

                footer_meta = {}
                if have_metadata_footer:
                    footer_meta = self._read_metadata_footer(
                        mime_documents_iter)

                request_etag = (footer_meta.get('etag') or
                                request.headers.get('etag', '')).lower()
                etag = etag.hexdigest()
                if request_etag and request_etag != etag:
                    return HTTPUnprocessableEntity(request=request)
                metadata = {
                    'X-Timestamp': request.timestamp.internal,
                    'Content-Type': request.headers['content-type'],
                    'ETag': etag,
                    'Content-Length': str(upload_size),
                }
                metadata.update(val for val in request.headers.iteritems()
                                if is_sys_or_user_meta('object', val[0]))
                metadata.update(val for val in footer_meta.iteritems()
                                if is_sys_or_user_meta('object', val[0]))
                headers_to_copy = (
                    request.headers.get(
                        'X-Backend-Replication-Headers', '').split() +
                    list(self.allowed_headers))
                for header_key in headers_to_copy:
                    if header_key in request.headers:
                        header_caps = header_key.title()
                        metadata[header_caps] = request.headers[header_key]
                writer.put(metadata)

                # if the PUT requires a two-phase commit (a data and a commit
                # phase) send the proxy server another 100-continue response
                # to indicate that we are finished writing object data
                if use_multiphase_commit:
                    request.environ['wsgi.input'].\
                        send_hundred_continue_response()
                    if not self._read_put_commit_message(mime_documents_iter):
                        return HTTPServerError(request=request)
                    # got 2nd phase confirmation, write a timestamp.durable
                    # state file to indicate a successful PUT

                writer.commit(request.timestamp)

                # Drain any remaining MIME docs from the socket. There
                # shouldn't be any, but we must read the whole request body.
                try:
                    while True:
                        with ChunkReadTimeout(self.client_timeout):
                            _junk_hdrs, _junk_body = next(mime_documents_iter)
                        drain(_junk_body, self.network_chunk_size,
                              self.client_timeout)
                except ChunkReadTimeout:
                    raise HTTPClientDisconnect()
                except StopIteration:
                    pass

        except (DiskFileXattrNotSupported, DiskFileNoSpace):
            return HTTPInsufficientStorage(drive=device, request=request)
        if orig_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update(
                    'PUT', new_delete_at, account, container, obj, request,
                    device, policy)
            if orig_delete_at:
                self.delete_at_update(
                    'DELETE', orig_delete_at, account, container, obj,
                    request, device, policy)
        update_headers = HeaderKeyDict({
            'x-size': metadata['Content-Length'],
            'x-content-type': metadata['Content-Type'],
            'x-timestamp': metadata['X-Timestamp'],
            'x-etag': metadata['ETag']})
        # apply any container update header overrides sent with request
        self._check_container_override(update_headers, request.headers)
        self._check_container_override(update_headers, footer_meta)
        self.container_update(
            'PUT', account, container, obj, request,
            update_headers,
            device, policy)
        return HTTPCreated(request=request, etag=etag)

    @public
    @timing_stats()
    def GET(self, request):
        """Handle HTTP GET requests for the Swift Object Server."""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        keep_cache = self.keep_cache_private or (
            'X-Auth-Token' not in request.headers and
            'X-Storage-Token' not in request.headers)
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy=policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            with disk_file.open():
                metadata = disk_file.get_metadata()
                obj_size = int(metadata['Content-Length'])
                file_x_ts = Timestamp(metadata['X-Timestamp'])
                keep_cache = (self.keep_cache_private or
                              ('X-Auth-Token' not in request.headers and
                               'X-Storage-Token' not in request.headers))
                conditional_etag = None
                if 'X-Backend-Etag-Is-At' in request.headers:
                    conditional_etag = metadata.get(
                        request.headers['X-Backend-Etag-Is-At'])
                response = Response(
                    app_iter=disk_file.reader(keep_cache=keep_cache),
                    request=request, conditional_response=True,
                    conditional_etag=conditional_etag)
                response.headers['Content-Type'] = metadata.get(
                    'Content-Type', 'application/octet-stream')
                for key, value in metadata.iteritems():
                    if is_sys_or_user_meta('object', key) or \
                            key.lower() in self.allowed_headers:
                        response.headers[key] = value
                response.etag = metadata['ETag']
                response.last_modified = math.ceil(float(file_x_ts))
                response.content_length = obj_size
                try:
                    response.content_encoding = metadata[
                        'Content-Encoding']
                except KeyError:
                    pass
                response.headers['X-Timestamp'] = file_x_ts.normal
                response.headers['X-Backend-Timestamp'] = file_x_ts.internal
                resp = request.get_response(response)
        except DiskFileXattrNotSupported:
            return HTTPInsufficientStorage(drive=device, request=request)
        except (DiskFileNotExist, DiskFileQuarantined) as e:
            headers = {}
            if hasattr(e, 'timestamp'):
                headers['X-Backend-Timestamp'] = e.timestamp.internal
            resp = HTTPNotFound(request=request, headers=headers,
                                conditional_response=True)
        return resp

    @public
    @timing_stats(sample_rate=0.8)
    def HEAD(self, request):
        """Handle HTTP HEAD requests for the Swift Object Server."""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy=policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            metadata = disk_file.read_metadata()
        except DiskFileXattrNotSupported:
            return HTTPInsufficientStorage(drive=device, request=request)
        except (DiskFileNotExist, DiskFileQuarantined) as e:
            headers = {}
            if hasattr(e, 'timestamp'):
                headers['X-Backend-Timestamp'] = e.timestamp.internal
            return HTTPNotFound(request=request, headers=headers,
                                conditional_response=True)
        conditional_etag = None
        if 'X-Backend-Etag-Is-At' in request.headers:
            conditional_etag = metadata.get(
                request.headers['X-Backend-Etag-Is-At'])
        response = Response(request=request, conditional_response=True,
                            conditional_etag=conditional_etag)
        response.headers['Content-Type'] = metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in metadata.iteritems():
            if is_sys_or_user_meta('object', key) or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = metadata['ETag']
        ts = Timestamp(metadata['X-Timestamp'])
        response.last_modified = math.ceil(float(ts))
        # Needed for container sync feature
        response.headers['X-Timestamp'] = ts.normal
        response.headers['X-Backend-Timestamp'] = ts.internal
        response.content_length = int(metadata['Content-Length'])
        try:
            response.content_encoding = metadata['Content-Encoding']
        except KeyError:
            pass
        return response

    @public
    @timing_stats()
    def DELETE(self, request):
        """Handle HTTP DELETE requests for the Swift Object Server."""
        device, partition, account, container, obj, policy = \
            get_name_and_placement(request, 5, 5, True)
        req_timestamp = valid_timestamp(request)
        try:
            disk_file = self.get_diskfile(
                device, partition, account, container, obj,
                policy=policy)
        except DiskFileDeviceUnavailable:
            return HTTPInsufficientStorage(drive=device, request=request)
        try:
            orig_metadata = disk_file.read_metadata()
        except DiskFileXattrNotSupported:
            return HTTPInsufficientStorage(drive=device, request=request)
        except DiskFileExpired as e:
            orig_timestamp = e.timestamp
            orig_metadata = e.metadata
            response_class = HTTPNotFound
        except DiskFileDeleted as e:
            orig_timestamp = e.timestamp
            orig_metadata = {}
            response_class = HTTPNotFound
        except (DiskFileNotExist, DiskFileQuarantined):
            orig_timestamp = 0
            orig_metadata = {}
            response_class = HTTPNotFound
        else:
            orig_timestamp = Timestamp(orig_metadata.get('X-Timestamp', 0))
            if orig_timestamp < req_timestamp:
                response_class = HTTPNoContent
            else:
                response_class = HTTPConflict
        response_timestamp = max(orig_timestamp, req_timestamp)
        orig_delete_at = int(orig_metadata.get('X-Delete-At') or 0)
        try:
            req_if_delete_at_val = request.headers['x-if-delete-at']
            req_if_delete_at = int(req_if_delete_at_val)
        except KeyError:
            pass
        except ValueError:
            return HTTPBadRequest(
                request=request,
                body='Bad X-If-Delete-At header value')
        else:
            # request includes x-if-delete-at; we must not place a tombstone
            # if we can not verify the x-if-delete-at time
            if not orig_timestamp:
                # no object found at all
                return HTTPNotFound()
            if orig_delete_at != req_if_delete_at:
                return HTTPPreconditionFailed(
                    request=request,
                    body='X-If-Delete-At and X-Delete-At do not match')
            else:
                # differentiate success from no object at all
                response_class = HTTPNoContent
        if orig_delete_at:
            self.delete_at_update('DELETE', orig_delete_at, account,
                                  container, obj, request, device,
                                  policy)
        if orig_timestamp < req_timestamp:
            disk_file.delete(req_timestamp)
            self.container_update(
                'DELETE', account, container, obj, request,
                HeaderKeyDict({'x-timestamp': req_timestamp.internal}),
                device, policy)
        return response_class(
            request=request,
            headers={'X-Backend-Timestamp': response_timestamp.internal})

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.

        Note that the name REPLICATE is preserved for historical reasons as
        this verb really just returns the hashes information for the specified
        parameters and is used, for example, by both replication and EC.
        """
        device, partition, suffix_parts, policy = \
            get_name_and_placement(request, 2, 3, True)
        suffixes = suffix_parts.split('-') if suffix_parts else []
        try:
            hashes = self._diskfile_router[policy].get_hashes(
                device, partition, suffixes, policy)
        except DiskFileDeviceUnavailable:
            resp = HTTPInsufficientStorage(drive=device, request=request)
        else:
            resp = Response(body=pickle.dumps(hashes))
        return resp

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def SSYNC(self, request):
        return Response(app_iter=ssync_receiver.Receiver(self, request)())

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
                    if req.method not in self.allowed_methods:
                        raise AttributeError('Not allowed method.')
                except AttributeError:
                    res = HTTPMethodNotAllowed()
                else:
                    method = getattr(self, req.method)
                    res = method(req)
            except DiskFileCollision:
                res = HTTPForbidden(request=req)
            except HTTPException as error_response:
                res = error_response
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s'
                    ' %(path)s '), {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = time.time() - start_time
        if self.log_requests:
            log_line = get_log_line(req, res, trans_time, '')
            if req.method in ('REPLICATE', 'SSYNC') or \
                    'X-Backend-Replication' in req.headers:
                self.logger.debug(log_line)
            else:
                self.logger.info(log_line)
        if req.method in ('PUT', 'DELETE'):
            slow = self.slow - trans_time
            if slow > 0:
                sleep(slow)

        # To be able to zero-copy send the object, we need a few things.
        # First, we have to be responding successfully to a GET, or else we're
        # not sending the object. Second, we have to be able to extract the
        # socket file descriptor from the WSGI input object. Third, the
        # diskfile has to support zero-copy send.
        #
        # There's a good chance that this could work for 206 responses too,
        # but the common case is sending the whole object, so we'll start
        # there.
        if req.method == 'GET' and res.status_int == 200 and \
           isinstance(env['wsgi.input'], wsgi.Input):
            app_iter = getattr(res, 'app_iter', None)
            checker = getattr(app_iter, 'can_zero_copy_send', None)
            if checker and checker():
                # For any kind of zero-copy thing like sendfile or splice, we
                # need the file descriptor. Eventlet doesn't provide a clean
                # way of getting that, so we resort to this.
                wsock = env['wsgi.input'].get_socket()
                wsockfd = wsock.fileno()

                # Don't call zero_copy_send() until after we force the HTTP
                # headers out of Eventlet and into the socket.
                def zero_copy_iter():
                    # If possible, set TCP_CORK so that headers don't
                    # immediately go on the wire, but instead, wait for some
                    # response body to make the TCP frames as large as
                    # possible (and hence as few packets as possible).
                    #
                    # On non-Linux systems, we might consider TCP_NODELAY, but
                    # since the only known zero-copy-capable diskfile uses
                    # Linux-specific syscalls, we'll defer that work until
                    # someone needs it.
                    if hasattr(socket, 'TCP_CORK'):
                        wsock.setsockopt(socket.IPPROTO_TCP,
                                         socket.TCP_CORK, 1)
                    yield EventletPlungerString()
                    try:
                        app_iter.zero_copy_send(wsockfd)
                    except Exception:
                        self.logger.exception("zero_copy_send() blew up")
                        raise
                    yield ''

                # Get headers ready to go out
                res(env, start_response)
                return zero_copy_iter()
            else:
                return res(env, start_response)
        else:
            return res(env, start_response)


def global_conf_callback(preloaded_app_conf, global_conf):
    """
    Callback for swift.common.wsgi.run_wsgi during the global_conf
    creation so that we can add our replication_semaphore, used to
    limit the number of concurrent REPLICATION_REQUESTS across all
    workers.

    :param preloaded_app_conf: The preloaded conf for the WSGI app.
                               This conf instance will go away, so
                               just read from it, don't write.
    :param global_conf: The global conf that will eventually be
                        passed to the app_factory function later.
                        This conf is created before the worker
                        subprocesses are forked, so can be useful to
                        set up semaphores, shared memory, etc.
    """
    replication_concurrency = int(
        preloaded_app_conf.get('replication_concurrency') or 4)
    if replication_concurrency:
        # Have to put the value in a list so it can get past paste
        global_conf['replication_semaphore'] = [
            multiprocessing.BoundedSemaphore(replication_concurrency)]


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
