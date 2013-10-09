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
from swift import gettext_ as _
from urllib import unquote, quote
from hashlib import md5
from sys import exc_info

from eventlet import sleep, GreenPile
from eventlet.queue import Queue
from eventlet.timeout import Timeout

from swift.common.utils import ContextPool, normalize_timestamp, \
    config_true_value, public, json, csv_append, GreenthreadSafeIterator, \
    quorum_size, split_path, override_bytes_from_content_type, \
    get_valid_utf8_str
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_metadata, check_object_creation, \
    CONTAINER_LISTING_LIMIT, MAX_FILE_SIZE
from swift.common.exceptions import ChunkReadTimeout, \
    ChunkWriteTimeout, ConnectionTimeout, ListingIterNotFound, \
    ListingIterNotAuthorized, ListingIterError, SegmentError
from swift.common.http import is_success, is_client_error, HTTP_CONTINUE, \
    HTTP_CREATED, HTTP_MULTIPLE_CHOICES, HTTP_NOT_FOUND, HTTP_CONFLICT, \
    HTTP_INTERNAL_SERVER_ERROR, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_INSUFFICIENT_STORAGE, HTTP_OK
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPRequestEntityTooLarge, HTTPRequestTimeout, \
    HTTPServerError, HTTPServiceUnavailable, Request, Response, \
    HTTPClientDisconnect, HTTPNotImplemented, HTTPException


def segment_listing_iter(listing):
    listing = iter(listing)
    while True:
        seg_dict = listing.next()
        if isinstance(seg_dict['name'], unicode):
            seg_dict['name'] = seg_dict['name'].encode('utf-8')
        yield seg_dict


def copy_headers_into(from_r, to_r):
    """
    Will copy desired headers from from_r to to_r
    :params from_r: a swob Request or Response
    :params to_r: a swob Request or Response
    """
    pass_headers = ['x-delete-at']
    for k, v in from_r.headers.items():
        if k.lower().startswith('x-object-meta-') or k.lower() in pass_headers:
            to_r.headers[k] = v


def check_content_type(req):
    if not req.environ.get('swift.content_type_overriden') and \
            ';' in req.headers.get('content-type', ''):
        for param in req.headers['content-type'].split(';')[1:]:
            if param.lstrip().startswith('swift_'):
                return HTTPBadRequest("Invalid Content-Type, "
                                      "swift_* is not a valid parameter name.")
    return None


class SegmentedIterable(object):
    """
    Iterable that returns the object contents for a segmented object in Swift.

    If there's a failure that cuts the transfer short, the response's
    `status_int` will be updated (again, just for logging since the original
    status would have already been sent to the client).

    :param controller: The ObjectController instance to work with.
    :param container: The container the object segments are within. If
                      container is None will derive container from elements
                      in listing using split('/', 1).
    :param listing: The listing of object segments to iterate over; this may
                    be an iterator or list that returns dicts with 'name' and
                    'bytes' keys.
    :param response: The swob.Response this iterable is associated with, if
                     any (default: None)
    :param is_slo: A boolean, defaults to False, as to whether this references
                   a SLO object.
    :param max_lo_time: Defaults to 86400. The connection for the
                        SegmentedIterable will drop after that many seconds.
    """

    def __init__(self, controller, container, listing, response=None,
                 is_slo=False, max_lo_time=86400):
        self.controller = controller
        self.container = container
        self.listing = segment_listing_iter(listing)
        self.is_slo = is_slo
        self.max_lo_time = max_lo_time
        self.ratelimit_index = 0
        self.segment_dict = None
        self.segment_peek = None
        self.seek = 0
        self.length = None
        self.segment_iter = None
        # See NOTE: swift_conn at top of file about this.
        self.segment_iter_swift_conn = None
        self.position = 0
        self.have_yielded_data = False
        self.response = response
        if not self.response:
            self.response = Response()
        self.next_get_time = 0
        self.start_time = time.time()

    def _load_next_segment(self):
        """
        Loads the self.segment_iter with the next object segment's contents.

        :raises: StopIteration when there are no more object segments or
                 segment no longer matches SLO manifest specifications.
        """
        try:
            self.ratelimit_index += 1
            if time.time() - self.start_time > self.max_lo_time:
                raise SegmentError(
                    _('Max LO GET time of %s exceeded.') % self.max_lo_time)
            self.segment_dict = self.segment_peek or self.listing.next()
            self.segment_peek = None
            if self.container is None:
                container, obj = \
                    self.segment_dict['name'].lstrip('/').split('/', 1)
            else:
                container, obj = self.container, self.segment_dict['name']
            partition = self.controller.app.object_ring.get_part(
                self.controller.account_name, container, obj)
            path = '/%s/%s/%s' % (self.controller.account_name, container, obj)
            req = Request.blank(path)
            if self.seek or (self.length and self.length > 0):
                bytes_available = \
                    self.segment_dict['bytes'] - self.seek
                range_tail = ''
                if self.length:
                    if bytes_available >= self.length:
                        range_tail = self.seek + self.length - 1
                        self.length = 0
                    else:
                        self.length -= bytes_available
                if self.seek or range_tail:
                    req.range = 'bytes=%s-%s' % (self.seek, range_tail)
                self.seek = 0
            if not self.is_slo and self.ratelimit_index > \
                    self.controller.app.rate_limit_after_segment:
                sleep(max(self.next_get_time - time.time(), 0))
            self.next_get_time = time.time() + \
                1.0 / self.controller.app.rate_limit_segments_per_sec
            resp = self.controller.GETorHEAD_base(
                req, _('Object'), self.controller.app.object_ring, partition,
                path)
            if self.is_slo and resp.status_int == HTTP_NOT_FOUND:
                raise SegmentError(_(
                    'Could not load object segment %(path)s:'
                    ' %(status)s') % {'path': path, 'status': resp.status_int})
            if not is_success(resp.status_int):
                raise Exception(_(
                    'Could not load object segment %(path)s:'
                    ' %(status)s') % {'path': path, 'status': resp.status_int})
            if self.is_slo:
                if (resp.etag != self.segment_dict['hash'] or
                        (resp.content_length != self.segment_dict['bytes'] and
                         not req.range)):
                    # The content-length check is for security reasons. Seems
                    # possible that an attacker could upload a >1mb object and
                    # then replace it with a much smaller object with same
                    # etag.  Then create a big nested SLO that calls that
                    # object many times which would hammer our obj servers. If
                    # this is a range request, don't check content-length
                    # because it won't match.
                    raise SegmentError(_(
                        'Object segment no longer valid: '
                        '%(path)s etag: %(r_etag)s != %(s_etag)s or '
                        '%(r_size)s != %(s_size)s.') %
                        {'path': path, 'r_etag': resp.etag,
                         'r_size': resp.content_length,
                         's_etag': self.segment_dict['hash'],
                         's_size': self.segment_dict['bytes']})
            self.segment_iter = resp.app_iter
            # See NOTE: swift_conn at top of file about this.
            self.segment_iter_swift_conn = getattr(resp, 'swift_conn', None)
        except StopIteration:
            raise
        except SegmentError as err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.error(_(
                    'ERROR: While processing manifest '
                    '/%(acc)s/%(cont)s/%(obj)s, %(err)s'),
                    {'acc': self.controller.account_name,
                     'cont': self.controller.container_name,
                     'obj': self.controller.object_name, 'err': err})
                err.swift_logged = True
                self.response.status_int = HTTP_CONFLICT
            raise
        except (Exception, Timeout) as err:
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
        """Standard iterator function that returns the object's contents."""
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
                            if self.length is None or self.length > 0:
                                self._load_next_segment()
                            else:
                                return
                self.position += len(chunk)
                self.have_yielded_data = True
                yield chunk
        except StopIteration:
            raise
        except SegmentError:
            # I have to save this error because yielding the ' ' below clears
            # the exception from the current stack frame.
            err = exc_info()
            if not self.have_yielded_data:
                # Normally, exceptions before any data has been yielded will
                # cause Eventlet to send a 5xx response. In this particular
                # case of SegmentError we don't want that and we'd rather
                # just send the normal 2xx response and then hang up early
                # since 5xx codes are often used to judge Service Level
                # Agreements and this SegmentError indicates the user has
                # created an invalid condition.
                yield ' '
            raise err
        except (Exception, Timeout) as err:
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
        Non-standard iterator function for use with Swob in serving Range
        requests more quickly. This will skip over segments and do a range
        request on the first segment to return data from, if needed.

        :param start: The first byte (zero-based) to return. None for 0.
        :param stop: The last byte (zero-based) to return. None for end.
        """
        try:
            if start:
                self.segment_peek = self.listing.next()
                while start >= self.position + self.segment_peek['bytes']:
                    self.position += self.segment_peek['bytes']
                    self.segment_peek = self.listing.next()
                self.seek = start - self.position
            else:
                start = 0
            if stop is not None:
                length = stop - start
                self.length = length
            else:
                length = None
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length <= 0:
                        if length < 0:
                            # Chop off the extra:
                            yield chunk[:length]
                        else:
                            yield chunk
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
        except (Exception, Timeout) as err:
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
    max_slo_recusion_depth = 10

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)
        self.slo_recursion_depth = 0

    def _listing_iter(self, lcontainer, lprefix, env):
        for page in self._listing_pages_iter(lcontainer, lprefix, env):
            for item in page:
                yield item

    def _listing_pages_iter(self, lcontainer, lprefix, env):
        lpartition = self.app.container_ring.get_part(
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
            lresp = self.GETorHEAD_base(
                lreq, _('Container'), self.app.container_ring, lpartition,
                lreq.path_info)
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
            marker = sublisting[-1]['name'].encode('utf-8')
            yield sublisting

    def _slo_listing_obj_iter(self, incoming_req, account, container, obj,
                              partition=None, initial_resp=None):
        """
        The initial_resp indicated that this is a SLO manifest file. This will
        create an iterable that will expand nested SLOs as it walks though the
        listing.
        :params incoming_req: The original GET request from client
        :params initial_resp: the first resp from the above request
        """

        if initial_resp and initial_resp.status_int == HTTP_OK and \
                incoming_req.method == 'GET' and not incoming_req.range:
            valid_resp = initial_resp
        else:
            new_req = incoming_req.copy_get()
            new_req.method = 'GET'
            new_req.range = None
            new_req.path_info = '/'.join(['', account, container, obj])
            if partition is None:
                try:
                    partition = self.app.object_ring.get_part(
                        account, container, obj)
                except ValueError:
                    raise HTTPException(
                        "Invalid path to whole SLO manifest: %s" %
                        new_req.path)
            valid_resp = self.GETorHEAD_base(
                new_req, _('Object'), self.app.object_ring, partition,
                new_req.path_info)

        if 'swift.authorize' in incoming_req.environ:
            incoming_req.acl = valid_resp.headers.get('x-container-read')
            auth_resp = incoming_req.environ['swift.authorize'](incoming_req)
            if auth_resp:
                raise ListingIterNotAuthorized(auth_resp)
        if valid_resp.status_int == HTTP_NOT_FOUND:
            raise ListingIterNotFound()
        elif not is_success(valid_resp.status_int):
            raise ListingIterError()
        try:
            listing = json.loads(valid_resp.body)
        except ValueError:
            listing = []
        for seg_dict in listing:
            if config_true_value(seg_dict.get('sub_slo')):
                if incoming_req.method == 'HEAD':
                    override_bytes_from_content_type(seg_dict,
                                                     logger=self.app.logger)
                    yield seg_dict
                    continue
                sub_path = get_valid_utf8_str(seg_dict['name'])
                sub_cont, sub_obj = split_path(sub_path, 2, 2, True)
                self.slo_recursion_depth += 1
                if self.slo_recursion_depth >= self.max_slo_recusion_depth:
                    raise ListingIterError("Max recursion depth exceeded")
                for sub_seg_dict in self._slo_listing_obj_iter(
                        incoming_req, account, sub_cont, sub_obj):
                    yield sub_seg_dict
                self.slo_recursion_depth -= 1
            else:
                yield seg_dict

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

    def iter_nodes_local_first(self, ring, partition):
        """
        Yields nodes for a ring partition.

        If the 'write_affinity' setting is non-empty, then this will yield N
        local nodes (as defined by the write_affinity setting) first, then the
        rest of the nodes as normal. It is a re-ordering of the nodes such
        that the local ones come first; no node is omitted. The effect is
        that the request will be serviced by local object servers first, but
        nonlocal ones will be employed if not enough local ones are available.

        :param ring: ring to get nodes from
        :param partition: ring partition to yield nodes for
        """

        primary_nodes = ring.get_part_nodes(partition)
        num_locals = self.app.write_affinity_node_count(ring)
        is_local = self.app.write_affinity_is_local_fn

        if is_local is None:
            return self.iter_nodes(ring, partition)

        all_nodes = itertools.chain(primary_nodes,
                                    ring.get_more_nodes(partition))
        first_n_local_nodes = list(itertools.islice(
            itertools.ifilter(is_local, all_nodes), num_locals))

        # refresh it; it moved when we computed first_n_local_nodes
        all_nodes = itertools.chain(primary_nodes,
                                    ring.get_more_nodes(partition))
        local_first_node_iter = itertools.chain(
            first_n_local_nodes,
            itertools.ifilter(lambda node: node not in first_n_local_nodes,
                              all_nodes))

        return self.iter_nodes(
            ring, partition, node_iter=local_first_node_iter)

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
        container_info = self.container_info(
            self.account_name, self.container_name, req)
        req.acl = container_info['read_acl']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp

        partition = self.app.object_ring.get_part(
            self.account_name, self.container_name, self.object_name)
        resp = self.GETorHEAD_base(
            req, _('Object'), self.app.object_ring, partition, req.path_info)

        if ';' in resp.headers.get('content-type', ''):
            # strip off swift_bytes from content-type
            content_type, check_extra_meta = \
                resp.headers['content-type'].rsplit(';', 1)
            if check_extra_meta.lstrip().startswith('swift_bytes='):
                resp.content_type = content_type

        large_object = None
        if config_true_value(resp.headers.get('x-static-large-object')) and \
                req.params.get('multipart-manifest') == 'get' and \
                'X-Copy-From' not in req.headers and \
                self.app.allow_static_large_object:
            resp.content_type = 'application/json'
            resp.charset = 'utf-8'

        if config_true_value(resp.headers.get('x-static-large-object')) and \
                req.params.get('multipart-manifest') != 'get' and \
                self.app.allow_static_large_object:
            large_object = 'SLO'
            lcontainer = None  # container name is included in listing
            try:
                seg_iter = iter(self._slo_listing_obj_iter(
                    req, self.account_name, self.container_name,
                    self.object_name, partition=partition, initial_resp=resp))
                listing_page1 = []
                for seg in seg_iter:
                    listing_page1.append(seg)
                    if len(listing_page1) >= CONTAINER_LISTING_LIMIT:
                        break
                listing = itertools.chain(listing_page1,
                                          self._remaining_items(seg_iter))
            except ListingIterNotFound:
                return HTTPNotFound(request=req)
            except ListingIterNotAuthorized, err:
                return err.aresp
            except ListingIterError:
                return HTTPServerError(request=req)
            except StopIteration:
                listing_page1 = listing = ()
            except HTTPException:
                return HTTPServiceUnavailable(
                    "Unable to load SLO manifest", request=req)

        if 'x-object-manifest' in resp.headers and \
                req.params.get('multipart-manifest') != 'get':
            large_object = 'DLO'
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
            except ListingIterNotAuthorized as err:
                return err.aresp
            except ListingIterError:
                return HTTPServerError(request=req)
            except StopIteration:
                listing_page1 = listing = ()

        if large_object:
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
                        self, lcontainer, listing, resp,
                        is_slo=(large_object == 'SLO'),
                        max_lo_time=self.app.max_large_object_get_time)

            else:
                # For objects with a reasonable number of segments, we'll serve
                # them with a set content-length and computed etag.
                listing = list(listing)
                if listing:
                    try:
                        content_length = sum(o['bytes'] for o in listing)
                        last_modified = \
                            max(o['last_modified'] for o in listing)
                        last_modified = datetime(*map(int, re.split('[^\d]',
                                                 last_modified)[:-1]))
                        etag = md5(
                            ''.join(o['hash'] for o in listing)).hexdigest()
                    except KeyError:
                        return HTTPServerError('Invalid Manifest File',
                                               request=req)

                else:
                    content_length = 0
                    last_modified = resp.last_modified
                    etag = md5().hexdigest()
                resp = Response(headers=resp.headers, request=req,
                                conditional_response=True)
                resp.app_iter = SegmentedIterable(
                    self, lcontainer, listing, resp,
                    is_slo=(large_object == 'SLO'),
                    max_lo_time=self.app.max_large_object_get_time)
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
            if req.environ.get('QUERY_STRING'):
                req.environ['QUERY_STRING'] += '&multipart-manifest=get'
            else:
                req.environ['QUERY_STRING'] = 'multipart-manifest=get'
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
                self.account_name, self.container_name, req)
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
                req.environ.setdefault('swift.log_info', []).append(
                    'x-delete-at:%d' % x_delete_at)
                delete_at_container = str(
                    x_delete_at /
                    self.app.expiring_objects_container_divisor *
                    self.app.expiring_objects_container_divisor)
                delete_at_part, delete_at_nodes = \
                    self.app.container_ring.get_nodes(
                        self.app.expiring_objects_account, delete_at_container)
            else:
                delete_at_container = delete_at_part = delete_at_nodes = None
            partition, nodes = self.app.object_ring.get_nodes(
                self.account_name, self.container_name, self.object_name)
            req.headers['X-Timestamp'] = normalize_timestamp(time.time())

            headers = self._backend_requests(
                req, len(nodes), container_partition, containers,
                delete_at_container, delete_at_part, delete_at_nodes)

            resp = self.make_requests(req, self.app.object_ring, partition,
                                      'POST', req.path_info, headers)
            return resp

    def _backend_requests(self, req, n_outgoing,
                          container_partition, containers,
                          delete_at_container=None, delete_at_partition=None,
                          delete_at_nodes=None):
        headers = [self.generate_request_headers(req, additional=req.headers)
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

            headers[i]['X-Delete-At-Container'] = delete_at_container
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
                start_time = time.time()
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(
                        node['ip'], node['port'], node['device'], part, 'PUT',
                        path, headers)
                self.app.set_node_timing(node, time.time() - start_time)
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
                    self.error_limit(node, _('ERROR Insufficient Storage'))
            except (Exception, Timeout):
                self.exception_occurred(node, _('Object'),
                                        _('Expect: 100-continue on %s') % path)

    @public
    @cors_validation
    @delay_denial
    def PUT(self, req):
        """HTTP PUT request handler."""
        container_info = self.container_info(
            self.account_name, self.container_name, req)
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
        try:
            ml = req.message_length()
        except ValueError as e:
            return HTTPBadRequest(request=req, content_type='text/plain',
                                  body=str(e))
        except AttributeError as e:
            return HTTPNotImplemented(request=req, content_type='text/plain',
                                      body=str(e))
        if ml is not None and ml > MAX_FILE_SIZE:
            return HTTPRequestEntityTooLarge(request=req)
        if 'x-delete-after' in req.headers:
            try:
                x_delete_after = int(req.headers['x-delete-after'])
            except ValueError:
                return HTTPBadRequest(request=req,
                                      content_type='text/plain',
                                      body='Non-integer X-Delete-After')
            req.headers['x-delete-at'] = '%d' % (time.time() + x_delete_after)
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        # do a HEAD request for container sync and checking object versions
        if 'x-timestamp' in req.headers or \
                (object_versions and not
                 req.environ.get('swift_versioned_copy')):
            hreq = Request.blank(req.path_info, headers={'X-Newest': 'True'},
                                 environ={'REQUEST_METHOD': 'HEAD'})
            hresp = self.GETorHEAD_base(
                hreq, _('Object'), self.app.object_ring, partition,
                hreq.path_info)
        # Used by container sync feature
        if 'x-timestamp' in req.headers:
            try:
                req.headers['X-Timestamp'] = \
                    normalize_timestamp(req.headers['x-timestamp'])
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
        detect_content_type = \
            config_true_value(req.headers.get('x-detect-content-type'))
        if detect_content_type or not req.headers.get('content-type'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            req.headers['Content-Type'] = guessed_type or \
                'application/octet-stream'
            if detect_content_type:
                req.headers.pop('x-detect-content-type')
            else:
                content_type_manually_set = False

        error_response = check_object_creation(req, self.object_name) or \
            check_content_type(req)
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
            if req.environ.get('swift.orig_req_method', req.method) != 'POST':
                req.environ.setdefault('swift.log_info', []).append(
                    'x-copy-from:%s' % source_header)
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
            if new_req.content_length > MAX_FILE_SIZE:
                return HTTPRequestEntityTooLarge(request=req)
            new_req.etag = source_resp.etag
            # we no longer need the X-Copy-From header
            del new_req.headers['X-Copy-From']
            if not content_type_manually_set:
                new_req.headers['Content-Type'] = \
                    source_resp.headers['Content-Type']
            if not config_true_value(
                    new_req.headers.get('x-fresh-metadata', 'false')):
                copy_headers_into(source_resp, new_req)
                copy_headers_into(req, new_req)
            # copy over x-static-large-object for POSTs and manifest copies
            if 'X-Static-Large-Object' in source_resp.headers and \
                    req.params.get('multipart-manifest') == 'get':
                new_req.headers['X-Static-Large-Object'] = \
                    source_resp.headers['X-Static-Large-Object']

            req = new_req

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
            req.environ.setdefault('swift.log_info', []).append(
                'x-delete-at:%d' % x_delete_at)
            delete_at_container = str(
                x_delete_at /
                self.app.expiring_objects_container_divisor *
                self.app.expiring_objects_container_divisor)
            delete_at_part, delete_at_nodes = \
                self.app.container_ring.get_nodes(
                    self.app.expiring_objects_account, delete_at_container)
        else:
            delete_at_container = delete_at_part = delete_at_nodes = None

        node_iter = GreenthreadSafeIterator(
            self.iter_nodes_local_first(self.app.object_ring, partition))
        pile = GreenPile(len(nodes))
        te = req.headers.get('transfer-encoding', '')
        chunked = ('chunked' in te)

        outgoing_headers = self._backend_requests(
            req, len(nodes), container_partition, containers,
            delete_at_container, delete_at_part, delete_at_nodes)

        for nheaders in outgoing_headers:
            # RFC2616:8.2.3 disallows 100-continue without a body
            if (req.content_length > 0) or chunked:
                nheaders['Expect'] = '100-continue'
            pile.spawn(self._connect_put_node, node_iter, partition,
                       req.path_info, nheaders, self.app.logger.thread_locals)

        conns = [conn for conn in pile if conn]
        min_conns = quorum_size(len(nodes))
        if len(conns) < min_conns:
            self.app.logger.error(
                _('Object PUT returning 503, %(conns)s/%(nodes)s '
                  'required connections'),
                {'conns': len(conns), 'nodes': min_conns})
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
                                for conn in conns:
                                    conn.queue.put('0\r\n\r\n')
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
                    if len(conns) < min_conns:
                        self.app.logger.error(_(
                            'Object PUT exceptions during'
                            ' send, %(conns)s/%(nodes)s required connections'),
                            {'conns': len(conns), 'nodes': min_conns})
                        return HTTPServiceUnavailable(request=req)
                for conn in conns:
                    if conn.queue.unfinished_tasks:
                        conn.queue.join()
            conns = [conn for conn in conns if not conn.failed]
        except ChunkReadTimeout as err:
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
        etag = etags.pop() if len(etags) else None
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
            copy_headers_into(req, resp)
        resp.last_modified = float(req.headers['X-Timestamp'])
        return resp

    @public
    @cors_validation
    @delay_denial
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        container_info = self.container_info(
            self.account_name, self.container_name, req)
        container_partition = container_info['partition']
        containers = container_info['nodes']
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']
        object_versions = container_info['versions']
        if object_versions:
            # this is a version manifest and needs to be handled differently
            object_versions = unquote(object_versions)
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
            except ListingIterNotAuthorized as err:
                return err.aresp
            except ListingIterError:
                return HTTPServerError(request=req)
            if last_item:
                # there are older versions so copy the previous version to the
                # current object and delete the previous version
                orig_container = self.container_name
                orig_obj = self.object_name
                self.container_name = lcontainer
                self.object_name = last_item['name'].encode('utf-8')
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
                self.object_name = last_item['name'].encode('utf-8')
                new_del_req = Request.blank(copy_path, environ=req.environ)
                container_info = self.container_info(
                    self.account_name, self.container_name, req)
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
                    normalize_timestamp(req.headers['x-timestamp'])
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
