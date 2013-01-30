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

# NOTE: swift_conn
# You'll see swift_conn passed around a few places in this file. This is the
# source httplib connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

import itertools
import mimetypes
import re
import time
from datetime import datetime
from urllib import unquote, quote
from hashlib import md5
from random import shuffle

from eventlet import sleep, GreenPile
from eventlet.queue import Queue
from eventlet.timeout import Timeout

from swift.common.utils import ContextPool, normalize_timestamp, \
    config_true_value, public, json, csv_append
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_metadata, check_object_creation, \
    CONTAINER_LISTING_LIMIT, MAX_FILE_SIZE
from swift.common.exceptions import ChunkReadTimeout, \
    ChunkWriteTimeout, ConnectionTimeout, ListingIterNotFound, \
    ListingIterNotAuthorized, ListingIterError
from swift.common.http import is_success, is_client_error, HTTP_CONTINUE, \
    HTTP_CREATED, HTTP_MULTIPLE_CHOICES, HTTP_NOT_FOUND, \
    HTTP_INTERNAL_SERVER_ERROR, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_INSUFFICIENT_STORAGE
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPRequestEntityTooLarge, HTTPRequestTimeout, \
    HTTPServerError, HTTPServiceUnavailable, Request, Response, \
    HTTPClientDisconnect


class SegmentedIterable(object):
    """
    Iterable that returns the object contents for a segmented object in Swift.

    If there's a failure that cuts the transfer short, the response's
    `status_int` will be updated (again, just for logging since the original
    status would have already been sent to the client).

    :param controller: The ObjectController instance to work with.
    :param container: The container the object segments are within.
    :param listing: The listing of object segments to iterate over; this may
                    be an iterator or list that returns dicts with 'name' and
                    'bytes' keys.
    :param response: The swob.Response this iterable is associated with, if
                     any (default: None)
    """

    def __init__(self, controller, container, listing, response=None):
        self.controller = controller
        self.container = container
        self.listing = iter(listing)
        self.segment = 0
        self.segment_dict = None
        self.segment_peek = None
        self.seek = 0
        self.segment_iter = None
        # See NOTE: swift_conn at top of file about this.
        self.segment_iter_swift_conn = None
        self.position = 0
        self.response = response
        if not self.response:
            self.response = Response()
        self.next_get_time = 0

    def _load_next_segment(self):
        """
        Loads the self.segment_iter with the next object segment's contents.

        :raises: StopIteration when there are no more object segments.
        """
        try:
            self.segment += 1
            self.segment_dict = self.segment_peek or self.listing.next()
            self.segment_peek = None
            partition, nodes = self.controller.app.object_ring.get_nodes(
                self.controller.account_name, self.container,
                self.segment_dict['name'])
            path = '/%s/%s/%s' % (self.controller.account_name, self.container,
                                  self.segment_dict['name'])
            req = Request.blank(path)
            if self.seek:
                req.range = 'bytes=%s-' % self.seek
                self.seek = 0
            if self.segment > self.controller.app.rate_limit_after_segment:
                sleep(max(self.next_get_time - time.time(), 0))
            self.next_get_time = time.time() + \
                1.0 / self.controller.app.rate_limit_segments_per_sec
            shuffle(nodes)
            resp = self.controller.GETorHEAD_base(
                req, _('Object'), partition,
                self.controller.iter_nodes(partition, nodes,
                                           self.controller.app.object_ring),
                path, len(nodes))
            if not is_success(resp.status_int):
                raise Exception(_(
                    'Could not load object segment %(path)s:'
                    ' %(status)s') % {'path': path, 'status': resp.status_int})
            self.segment_iter = resp.app_iter
            # See NOTE: swift_conn at top of file about this.
            self.segment_iter_swift_conn = getattr(resp, 'swift_conn', None)
        except StopIteration:
            raise
        except (Exception, Timeout), err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception(_(
                    'ERROR: While processing manifest '
                    '/%(acc)s/%(cont)s/%(obj)s'),
                    {'acc': self.controller.account_name,
                     'cont': self.controller.container_name,
                     'obj': self.controller.object_name})
                err.swift_logged = True
                self.response.status_int = HTTP_SERVICE_UNAVAILABLE
            raise

    def next(self):
        return iter(self).next()

    def __iter__(self):
        """ Standard iterator function that returns the object's contents. """
        try:
            while True:
                if not self.segment_iter:
                    self._load_next_segment()
                while True:
                    with ChunkReadTimeout(self.controller.app.node_timeout):
                        try:
                            chunk = self.segment_iter.next()
                            break
                        except StopIteration:
                            self._load_next_segment()
                self.position += len(chunk)
                yield chunk
        except StopIteration:
            raise
        except (Exception, Timeout), err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception(_(
                    'ERROR: While processing manifest '
                    '/%(acc)s/%(cont)s/%(obj)s'),
                    {'acc': self.controller.account_name,
                     'cont': self.controller.container_name,
                     'obj': self.controller.object_name})
                err.swift_logged = True
                self.response.status_int = HTTP_SERVICE_UNAVAILABLE
            raise

    def app_iter_range(self, start, stop):
        """
        Non-standard iterator function for use with Webob in serving Range
        requests more quickly. This will skip over segments and do a range
        request on the first segment to return data from, if needed.

        :param start: The first byte (zero-based) to return. None for 0.
        :param stop: The last byte (zero-based) to return. None for end.
        """
        try:
            if start:
                self.segment_peek = self.listing.next()
                while start >= self.position + self.segment_peek['bytes']:
                    self.segment += 1
                    self.position += self.segment_peek['bytes']
                    self.segment_peek = self.listing.next()
                self.seek = start - self.position
            else:
                start = 0
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
            # See NOTE: swift_conn at top of file about this.
            if self.segment_iter_swift_conn:
                try:
                    self.segment_iter_swift_conn.close()
                except Exception:
                    pass
                self.segment_iter_swift_conn = None
            if self.segment_iter:
                try:
                    while self.segment_iter.next():
                        pass
                except Exception:
                    pass
                self.segment_iter = None
        except StopIteration:
            raise
        except (Exception, Timeout), err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception(_(
                    'ERROR: While processing manifest '
                    '/%(acc)s/%(cont)s/%(obj)s'),
                    {'acc': self.controller.account_name,
                     'cont': self.controller.container_name,
                     'obj': self.controller.object_name})
                err.swift_logged = True
                self.response.status_int = HTTP_SERVICE_UNAVAILABLE
            raise


class ObjectController(Controller):
    """WSGI controller for object requests."""
    server_type = 'Object'

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)

    def _listing_iter(self, lcontainer, lprefix, env):
        for page in self._listing_pages_iter(lcontainer, lprefix, env):
            for item in page:
                yield item

    def _listing_pages_iter(self, lcontainer, lprefix, env):
        lpartition, lnodes = self.app.container_ring.get_nodes(
            self.account_name, lcontainer)
        marker = ''
        while True:
            lreq = Request.blank('i will be overridden by env', environ=env)
            # Don't quote PATH_INFO, by WSGI spec
            lreq.environ['PATH_INFO'] = \
                '/%s/%s' % (self.account_name, lcontainer)
            lreq.environ['REQUEST_METHOD'] = 'GET'
            lreq.environ['QUERY_STRING'] = \
                'format=json&prefix=%s&marker=%s' % (quote(lprefix),
                                                     quote(marker))
            shuffle(lnodes)
            lresp = self.GETorHEAD_base(
                lreq, _('Container'), lpartition, lnodes, lreq.path_info,
                len(lnodes))
            if 'swift.authorize' in env:
                lreq.acl = lresp.headers.get('x-container-read')
                aresp = env['swift.authorize'](lreq)
                if aresp:
                    raise ListingIterNotAuthorized(aresp)
            if lresp.status_int == HTTP_NOT_FOUND:
                raise ListingIterNotFound()
            elif not is_success(lresp.status_int):
                raise ListingIterError()
            if not lresp.body:
                break
            sublisting = json.loads(lresp.body)
            if not sublisting:
                break
            marker = sublisting[-1]['name']
            yield sublisting

    def _remaining_items(self, listing_iter):
        """
        Returns an item-by-item iterator for a page-by-page iterator
        of item listings.

        Swallows listing-related errors; this iterator is only used
        after we've already started streaming a response to the
        client, and so if we start getting errors from the container
        servers now, it's too late to send an error to the client, so
        we just quit looking for segments.
        """
        try:
            for page in listing_iter:
                for item in page:
                    yield item
        except ListingIterNotFound:
            pass
        except ListingIterError:
            pass
        except ListingIterNotAuthorized:
            pass

    def is_good_source(self, src):
        """
        Indicates whether or not the request made to the backend found
        what it was looking for.

        In the case of an object, a 416 indicates that we found a
        backend with the object.
        """
        return src.status == 416 or \
            super(ObjectController, self).is_good_source(src)

    def GETorHEAD(self, req):
        """Handle HTTP GET or HEAD requests."""
        container_info = self.container_info(self.account_name,
                                             self.container_name)
        req.acl = container_info['read_acl']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp

        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        shuffle(nodes)
        resp = self.GETorHEAD_base(
            req, _('Object'), partition,
            self.iter_nodes(partition, nodes, self.app.object_ring),
            req.path_info, len(nodes))

        if 'x-object-manifest' in resp.headers:
            lcontainer, lprefix = \
                resp.headers['x-object-manifest'].split('/', 1)
            lcontainer = unquote(lcontainer)
            lprefix = unquote(lprefix)
            try:
                pages_iter = iter(self._listing_pages_iter(lcontainer, lprefix,
                                                           req.environ))
                listing_page1 = pages_iter.next()
                listing = itertools.chain(listing_page1,
                                          self._remaining_items(pages_iter))
            except ListingIterNotFound:
                return HTTPNotFound(request=req)
            except ListingIterNotAuthorized, err:
                return err.aresp
            except ListingIterError:
                return HTTPServerError(request=req)
            except StopIteration:
                listing_page1 = listing = ()

            if len(listing_page1) >= CONTAINER_LISTING_LIMIT:
                resp = Response(headers=resp.headers, request=req,
                                conditional_response=True)
                if req.method == 'HEAD':
                    # These shenanigans are because swob translates the HEAD
                    # request into a swob EmptyResponse for the body, which
                    # has a len, which eventlet translates as needing a
                    # content-length header added. So we call the original
                    # swob resp for the headers but return an empty iterator
                    # for the body.

                    def head_response(environ, start_response):
                        resp(environ, start_response)
                        return iter([])

                    head_response.status_int = resp.status_int
                    return head_response
                else:
                    resp.app_iter = SegmentedIterable(
                        self, lcontainer, listing, resp)

            else:
                # For objects with a reasonable number of segments, we'll serve
                # them with a set content-length and computed etag.
                if listing:
                    listing = list(listing)
                    content_length = sum(o['bytes'] for o in listing)
                    last_modified = max(o['last_modified'] for o in listing)
                    last_modified = datetime(*map(int, re.split('[^\d]',
                                             last_modified)[:-1]))
                    etag = md5(
                        ''.join(o['hash'] for o in listing)).hexdigest()
                else:
                    content_length = 0
                    last_modified = resp.last_modified
                    etag = md5().hexdigest()
                resp = Response(headers=resp.headers, request=req,
                                conditional_response=True)
                resp.app_iter = SegmentedIterable(self, lcontainer, listing,
                                                  resp)
                resp.content_length = content_length
                resp.last_modified = last_modified
                resp.etag = etag
            resp.headers['accept-ranges'] = 'bytes'
            # In case of a manifest file of nonzero length, the
            # backend may have sent back a Content-Range header for
            # the manifest. It's wrong for the client, though.
            resp.content_range = None

        return resp

    @public
    @cors_validation
    @delay_denial
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    @cors_validation
    @delay_denial
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    @public
    @cors_validation
    @delay_denial
    def POST(self, req):
        """HTTP POST request handler."""
        if 'x-delete-after' in req.headers:
            try:
                x_delete_after = int(req.headers['x-delete-after'])
            except ValueError:
                return HTTPBadRequest(request=req,
                                      content_type='text/plain',
                                      body='Non-integer X-Delete-After')
            req.headers['x-delete-at'] = '%d' % (time.time() + x_delete_after)
        if self.app.object_post_as_copy:
            req.method = 'PUT'
            req.path_info = '/%s/%s/%s' % (
                self.account_name, self.container_name, self.object_name)
            req.headers['Content-Length'] = 0
            req.headers['X-Copy-From'] = quote('/%s/%s' % (self.container_name,
                                               self.object_name))
            req.headers['X-Fresh-Metadata'] = 'true'
            req.environ['swift_versioned_copy'] = True
            resp = self.PUT(req)
            # Older editions returned 202 Accepted on object POSTs, so we'll
            # convert any 201 Created responses to that for compatibility with
            # picky clients.
            if resp.status_int != HTTP_CREATED:
                return resp
            return HTTPAccepted(request=req)
        else:
            error_response = check_metadata(req, 'object')
            if error_response:
                return error_response
            container_info = self.container_info(
                self.account_name, self.container_name,
                account_autocreate=self.app.account_autocreate)
            container_partition = container_info['partition']
            containers = container_info['nodes']
            req.acl = container_info['write_acl']
            if 'swift.authorize' in req.environ:
                aresp = req.environ['swift.authorize'](req)
                if aresp:
                    return aresp
            if not containers:
                return HTTPNotFound(request=req)
            if 'x-delete-at' in req.headers:
                try:
                    x_delete_at = int(req.headers['x-delete-at'])
                    if x_delete_at < time.time():
                        return HTTPBadRequest(
                            body='X-Delete-At in past', request=req,
                            content_type='text/plain')
                except ValueError:
                    return HTTPBadRequest(request=req,
                                          content_type='text/plain',
                                          body='Non-integer X-Delete-At')
                delete_at_container = str(
                    x_delete_at /
                    self.app.expiring_objects_container_divisor *
                    self.app.expiring_objects_container_divisor)
                delete_at_part, delete_at_nodes = \
                    self.app.container_ring.get_nodes(
                        self.app.expiring_objects_account, delete_at_container)
            else:
                delete_at_part = delete_at_nodes = None
            partition, nodes = self.app.object_ring.get_nodes(
                self.account_name, self.container_name, self.object_name)
            req.headers['X-Timestamp'] = normalize_timestamp(time.time())

            headers = self._backend_requests(
                req, len(nodes), container_partition, containers,
                delete_at_part, delete_at_nodes)

            resp = self.make_requests(req, self.app.object_ring, partition,
                                      'POST', req.path_info, headers)
            return resp

    def _backend_requests(self, req, n_outgoing,
                          container_partition, containers,
                          delete_at_partition=None, delete_at_nodes=None):
        headers = [dict(req.headers.iteritems())
                   for _junk in range(n_outgoing)]

        for header in headers:
            header['Connection'] = 'close'

        for i, container in enumerate(containers):
            i = i % len(headers)

            headers[i]['X-Container-Partition'] = container_partition
            headers[i]['X-Container-Host'] = csv_append(
                headers[i].get('X-Container-Host'),
                '%(ip)s:%(port)s' % container)
            headers[i]['X-Container-Device'] = csv_append(
                headers[i].get('X-Container-Device'),
                container['device'])

        for i, node in enumerate(delete_at_nodes or []):
            i = i % len(headers)

            headers[i]['X-Delete-At-Partition'] = delete_at_partition
            headers[i]['X-Delete-At-Host'] = csv_append(
                headers[i].get('X-Delete-At-Host'),
                '%(ip)s:%(port)s' % node)
            headers[i]['X-Delete-At-Device'] = csv_append(
                headers[i].get('X-Delete-At-Device'),
                node['device'])

        return headers

    def _send_file(self, conn, path):
        """Method for a file PUT coro"""
        while True:
            chunk = conn.queue.get()
            if not conn.failed:
                try:
                    with ChunkWriteTimeout(self.app.node_timeout):
                        conn.send(chunk)
                except (Exception, ChunkWriteTimeout):
                    conn.failed = True
                    self.exception_occurred(conn.node, _('Object'),
                                            _('Trying to write to %s') % path)
            conn.queue.task_done()

    def _connect_put_node(self, nodes, part, path, headers,
                          logger_thread_locals):
        """Method for a file PUT connect"""
        self.app.logger.thread_locals = logger_thread_locals
        for node in nodes:
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(
                        node['ip'], node['port'], node['device'], part, 'PUT',
                        path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getexpect()
                if resp.status == HTTP_CONTINUE:
                    conn.resp = None
                    conn.node = node
                    return conn
                elif is_success(resp.status):
                    conn.resp = resp
                    conn.node = node
                    return conn
                elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                    self.error_limit(node)
            except:
                self.exception_occurred(node, _('Object'),
                                        _('Expect: 100-continue on %s') % path)

    @public
    @cors_validation
    @delay_denial
    def PUT(self, req):
        """HTTP PUT request handler."""
        container_info = self.container_info(
            self.account_name, self.container_name,
            account_autocreate=self.app.account_autocreate)
        container_partition = container_info['partition']
        containers = container_info['nodes']
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']
        object_versions = container_info['versions']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not containers:
            return HTTPNotFound(request=req)
        if 'x-delete-after' in req.headers:
            try:
                x_delete_after = int(req.headers['x-delete-after'])
            except ValueError:
                    return HTTPBadRequest(request=req,
                                          content_type='text/plain',
                                          body='Non-integer X-Delete-After')
            req.headers['x-delete-at'] = '%d' % (time.time() + x_delete_after)
        if 'x-delete-at' in req.headers:
            try:
                x_delete_at = int(req.headers['x-delete-at'])
                if x_delete_at < time.time():
                    return HTTPBadRequest(
                        body='X-Delete-At in past', request=req,
                        content_type='text/plain')
            except ValueError:
                return HTTPBadRequest(request=req, content_type='text/plain',
                                      body='Non-integer X-Delete-At')
            delete_at_container = str(
                x_delete_at /
                self.app.expiring_objects_container_divisor *
                self.app.expiring_objects_container_divisor)
            delete_at_part, delete_at_nodes = \
                self.app.container_ring.get_nodes(
                    self.app.expiring_objects_account, delete_at_container)
        else:
            delete_at_part = delete_at_nodes = None
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        # do a HEAD request for container sync and checking object versions
        if 'x-timestamp' in req.headers or \
                (object_versions and not
                 req.environ.get('swift_versioned_copy')):
            hreq = Request.blank(req.path_info, headers={'X-Newest': 'True'},
                                 environ={'REQUEST_METHOD': 'HEAD'})
            hresp = self.GETorHEAD_base(hreq, _('Object'), partition, nodes,
                                        hreq.path_info, len(nodes))
        # Used by container sync feature
        if 'x-timestamp' in req.headers:
            try:
                req.headers['X-Timestamp'] = \
                    normalize_timestamp(float(req.headers['x-timestamp']))
                if hresp.environ and 'swift_x_timestamp' in hresp.environ and \
                    float(hresp.environ['swift_x_timestamp']) >= \
                        float(req.headers['x-timestamp']):
                    return HTTPAccepted(request=req)
            except ValueError:
                return HTTPBadRequest(
                    request=req, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % req.headers['x-timestamp'])
        else:
            req.headers['X-Timestamp'] = normalize_timestamp(time.time())
        # Sometimes the 'content-type' header exists, but is set to None.
        content_type_manually_set = True
        if not req.headers.get('content-type'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            req.headers['Content-Type'] = guessed_type or \
                'application/octet-stream'
            content_type_manually_set = False
        error_response = check_object_creation(req, self.object_name)
        if error_response:
            return error_response
        if object_versions and not req.environ.get('swift_versioned_copy'):
            is_manifest = 'x-object-manifest' in req.headers or \
                          'x-object-manifest' in hresp.headers
            if hresp.status_int != HTTP_NOT_FOUND and not is_manifest:
                # This is a version manifest and needs to be handled
                # differently. First copy the existing data to a new object,
                # then write the data from this request to the version manifest
                # object.
                lcontainer = object_versions.split('/')[0]
                prefix_len = '%03x' % len(self.object_name)
                lprefix = prefix_len + self.object_name + '/'
                ts_source = hresp.environ.get('swift_x_timestamp')
                if ts_source is None:
                    ts_source = time.mktime(time.strptime(
                                            hresp.headers['last-modified'],
                                            '%a, %d %b %Y %H:%M:%S GMT'))
                new_ts = normalize_timestamp(ts_source)
                vers_obj_name = lprefix + new_ts
                copy_headers = {
                    'Destination': '%s/%s' % (lcontainer, vers_obj_name)}
                copy_environ = {'REQUEST_METHOD': 'COPY',
                                'swift_versioned_copy': True
                                }
                copy_req = Request.blank(req.path_info, headers=copy_headers,
                                         environ=copy_environ)
                copy_resp = self.COPY(copy_req)
                if is_client_error(copy_resp.status_int):
                    # missing container or bad permissions
                    return HTTPPreconditionFailed(request=req)
                elif not is_success(copy_resp.status_int):
                    # could not copy the data, bail
                    return HTTPServiceUnavailable(request=req)

        reader = req.environ['wsgi.input'].read
        data_source = iter(lambda: reader(self.app.client_chunk_size), '')
        source_header = req.headers.get('X-Copy-From')
        source_resp = None
        if source_header:
            source_header = unquote(source_header)
            acct = req.path_info.split('/', 2)[1]
            if isinstance(acct, unicode):
                acct = acct.encode('utf-8')
            if not source_header.startswith('/'):
                source_header = '/' + source_header
            source_header = '/' + acct + source_header
            try:
                src_container_name, src_obj_name = \
                    source_header.split('/', 3)[2:]
            except ValueError:
                return HTTPPreconditionFailed(
                    request=req,
                    body='X-Copy-From header must be of the form'
                         '<container name>/<object name>')
            source_req = req.copy_get()
            source_req.path_info = source_header
            source_req.headers['X-Newest'] = 'true'
            orig_obj_name = self.object_name
            orig_container_name = self.container_name
            self.object_name = src_obj_name
            self.container_name = src_container_name
            source_resp = self.GET(source_req)
            if source_resp.status_int >= HTTP_MULTIPLE_CHOICES:
                return source_resp
            self.object_name = orig_obj_name
            self.container_name = orig_container_name
            new_req = Request.blank(req.path_info,
                                    environ=req.environ, headers=req.headers)
            data_source = source_resp.app_iter
            new_req.content_length = source_resp.content_length
            if new_req.content_length is None:
                # This indicates a transfer-encoding: chunked source object,
                # which currently only happens because there are more than
                # CONTAINER_LISTING_LIMIT segments in a segmented object. In
                # this case, we're going to refuse to do the server-side copy.
                return HTTPRequestEntityTooLarge(request=req)
            new_req.etag = source_resp.etag
            # we no longer need the X-Copy-From header
            del new_req.headers['X-Copy-From']
            if not content_type_manually_set:
                new_req.headers['Content-Type'] = \
                    source_resp.headers['Content-Type']
            if not config_true_value(
                    new_req.headers.get('x-fresh-metadata', 'false')):
                for k, v in source_resp.headers.items():
                    if k.lower().startswith('x-object-meta-'):
                        new_req.headers[k] = v
                for k, v in req.headers.items():
                    if k.lower().startswith('x-object-meta-'):
                        new_req.headers[k] = v
            req = new_req
        node_iter = self.iter_nodes(partition, nodes, self.app.object_ring)
        pile = GreenPile(len(nodes))
        chunked = req.headers.get('transfer-encoding')

        outgoing_headers = self._backend_requests(
            req, len(nodes), container_partition, containers,
            delete_at_part, delete_at_nodes)

        for nheaders in outgoing_headers:
            # RFC2616:8.2.3 disallows 100-continue without a body
            if (req.content_length > 0) or chunked:
                nheaders['Expect'] = '100-continue'
            pile.spawn(self._connect_put_node, node_iter, partition,
                       req.path_info, nheaders, self.app.logger.thread_locals)

        conns = [conn for conn in pile if conn]
        if len(conns) <= len(nodes) / 2:
            self.app.logger.error(
                _('Object PUT returning 503, %(conns)s/%(nodes)s '
                  'required connections'),
                {'conns': len(conns), 'nodes': len(nodes) // 2 + 1})
            return HTTPServiceUnavailable(request=req)
        bytes_transferred = 0
        try:
            with ContextPool(len(nodes)) as pool:
                for conn in conns:
                    conn.failed = False
                    conn.queue = Queue(self.app.put_queue_depth)
                    pool.spawn(self._send_file, conn, req.path)
                while True:
                    with ChunkReadTimeout(self.app.client_timeout):
                        try:
                            chunk = next(data_source)
                        except StopIteration:
                            if chunked:
                                [conn.queue.put('0\r\n\r\n') for conn in conns]
                            break
                    bytes_transferred += len(chunk)
                    if bytes_transferred > MAX_FILE_SIZE:
                        return HTTPRequestEntityTooLarge(request=req)
                    for conn in list(conns):
                        if not conn.failed:
                            conn.queue.put(
                                '%x\r\n%s\r\n' % (len(chunk), chunk)
                                if chunked else chunk)
                        else:
                            conns.remove(conn)
                    if len(conns) <= len(nodes) / 2:
                        self.app.logger.error(_(
                            'Object PUT exceptions during'
                            ' send, %(conns)s/%(nodes)s required connections'),
                            {'conns': len(conns), 'nodes': len(nodes) / 2 + 1})
                        return HTTPServiceUnavailable(request=req)
                for conn in conns:
                    if conn.queue.unfinished_tasks:
                        conn.queue.join()
            conns = [conn for conn in conns if not conn.failed]
        except ChunkReadTimeout, err:
            self.app.logger.warn(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            self.app.logger.increment('client_timeouts')
            return HTTPRequestTimeout(request=req)
        except (Exception, Timeout):
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            return HTTPClientDisconnect(request=req)
        if req.content_length and bytes_transferred < req.content_length:
            req.client_disconnect = True
            self.app.logger.warn(
                _('Client disconnected without sending enough data'))
            self.app.logger.increment('client_disconnects')
            return HTTPClientDisconnect(request=req)
        statuses = []
        reasons = []
        bodies = []
        etags = set()
        for conn in conns:
            try:
                with Timeout(self.app.node_timeout):
                    if conn.resp:
                        response = conn.resp
                    else:
                        response = conn.getresponse()
                    statuses.append(response.status)
                    reasons.append(response.reason)
                    bodies.append(response.read())
                    if response.status >= HTTP_INTERNAL_SERVER_ERROR:
                        self.error_occurred(
                            conn.node,
                            _('ERROR %(status)d %(body)s From Object Server '
                              're: %(path)s') %
                            {'status': response.status,
                             'body': bodies[-1][:1024], 'path': req.path})
                    elif is_success(response.status):
                        etags.add(response.getheader('etag').strip('"'))
            except (Exception, Timeout):
                self.exception_occurred(
                    conn.node, _('Object'),
                    _('Trying to get final status of PUT to %s') % req.path)
        if len(etags) > 1:
            self.app.logger.error(
                _('Object servers returned %s mismatched etags'), len(etags))
            return HTTPServerError(request=req)
        etag = len(etags) and etags.pop() or None
        while len(statuses) < len(nodes):
            statuses.append(HTTP_SERVICE_UNAVAILABLE)
            reasons.append('')
            bodies.append('')
        resp = self.best_response(req, statuses, reasons, bodies,
                                  _('Object PUT'), etag=etag)
        if source_header:
            resp.headers['X-Copied-From'] = quote(
                source_header.split('/', 2)[2])
            if 'last-modified' in source_resp.headers:
                resp.headers['X-Copied-From-Last-Modified'] = \
                    source_resp.headers['last-modified']
            for k, v in req.headers.items():
                if k.lower().startswith('x-object-meta-'):
                    resp.headers[k] = v
        resp.last_modified = float(req.headers['X-Timestamp'])
        return resp

    @public
    @cors_validation
    @delay_denial
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        container_info = self.container_info(self.account_name,
                                             self.container_name)
        container_partition = container_info['partition']
        containers = container_info['nodes']
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']
        object_versions = container_info['versions']
        if object_versions:
            # this is a version manifest and needs to be handled differently
            lcontainer = object_versions.split('/')[0]
            prefix_len = '%03x' % len(self.object_name)
            lprefix = prefix_len + self.object_name + '/'
            last_item = None
            try:
                for last_item in self._listing_iter(lcontainer, lprefix,
                                                    req.environ):
                    pass
            except ListingIterNotFound:
                # no worries, last_item is None
                pass
            except ListingIterNotAuthorized, err:
                return err.aresp
            except ListingIterError:
                return HTTPServerError(request=req)
            if last_item:
                # there are older versions so copy the previous version to the
                # current object and delete the previous version
                orig_container = self.container_name
                orig_obj = self.object_name
                self.container_name = lcontainer
                self.object_name = last_item['name']
                copy_path = '/' + self.account_name + '/' + \
                            self.container_name + '/' + self.object_name
                copy_headers = {'X-Newest': 'True',
                                'Destination': orig_container + '/' + orig_obj
                                }
                copy_environ = {'REQUEST_METHOD': 'COPY',
                                'swift_versioned_copy': True
                                }
                creq = Request.blank(copy_path, headers=copy_headers,
                                     environ=copy_environ)
                copy_resp = self.COPY(creq)
                if is_client_error(copy_resp.status_int):
                    # some user error, maybe permissions
                    return HTTPPreconditionFailed(request=req)
                elif not is_success(copy_resp.status_int):
                    # could not copy the data, bail
                    return HTTPServiceUnavailable(request=req)
                # reset these because the COPY changed them
                self.container_name = lcontainer
                self.object_name = last_item['name']
                new_del_req = Request.blank(copy_path, environ=req.environ)
                container_info = self.container_info(self.account_name,
                                                     self.container_name)
                container_partition = container_info['partition']
                containers = container_info['nodes']
                new_del_req.acl = container_info['write_acl']
                new_del_req.path_info = copy_path
                req = new_del_req
                # remove 'X-If-Delete-At', since it is not for the older copy
                if 'X-If-Delete-At' in req.headers:
                    del req.headers['X-If-Delete-At']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not containers:
            return HTTPNotFound(request=req)
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        # Used by container sync feature
        if 'x-timestamp' in req.headers:
            try:
                req.headers['X-Timestamp'] = \
                    normalize_timestamp(float(req.headers['x-timestamp']))
            except ValueError:
                return HTTPBadRequest(
                    request=req, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % req.headers['x-timestamp'])
        else:
            req.headers['X-Timestamp'] = normalize_timestamp(time.time())

        headers = self._backend_requests(
            req, len(nodes), container_partition, containers)
        resp = self.make_requests(req, self.app.object_ring,
                                  partition, 'DELETE', req.path_info, headers)
        return resp

    @public
    @cors_validation
    @delay_denial
    def COPY(self, req):
        """HTTP COPY request handler."""
        dest = req.headers.get('Destination')
        if not dest:
            return HTTPPreconditionFailed(request=req,
                                          body='Destination header required')
        dest = unquote(dest)
        if not dest.startswith('/'):
            dest = '/' + dest
        try:
            _junk, dest_container, dest_object = dest.split('/', 2)
        except ValueError:
            return HTTPPreconditionFailed(
                request=req,
                body='Destination header must be of the form '
                     '<container name>/<object name>')
        source = '/' + self.container_name + '/' + self.object_name
        self.container_name = dest_container
        self.object_name = dest_object
        # re-write the existing request as a PUT instead of creating a new one
        # since this one is already attached to the posthooklogger
        req.method = 'PUT'
        req.path_info = '/' + self.account_name + dest
        req.headers['Content-Length'] = 0
        req.headers['X-Copy-From'] = quote(source)
        del req.headers['Destination']
        return self.PUT(req)
