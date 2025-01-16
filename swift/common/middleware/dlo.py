# Copyright (c) 2013 OpenStack Foundation
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
Middleware that will provide Dynamic Large Object (DLO) support.

---------------
Using ``swift``
---------------

The quickest way to try out this feature is use the ``swift`` Swift Tool
included with the `python-swiftclient`_ library.  You can use the ``-S``
option to specify the segment size to use when splitting a large file. For
example::

    swift upload test_container -S 1073741824 large_file

This would split the large_file into 1G segments and begin uploading those
segments in parallel. Once all the segments have been uploaded, ``swift`` will
then create the manifest file so the segments can be downloaded as one.

So now, the following ``swift`` command would download the entire large
object::

    swift download test_container large_file

``swift`` command uses a strict convention for its segmented object
support. In the above example it will upload all the segments into a
second container named test_container_segments. These segments will
have names like large_file/1290206778.25/21474836480/00000000,
large_file/1290206778.25/21474836480/00000001, etc.

The main benefit for using a separate container is that the main container
listings will not be polluted with all the segment names. The reason for using
the segment name format of <name>/<timestamp>/<size>/<segment> is so that an
upload of a new file with the same name won't overwrite the contents of the
first until the last moment when the manifest file is updated.

``swift`` will manage these segment files for you, deleting old segments on
deletes and overwrites, etc. You can override this behavior with the
``--leave-segments`` option if desired; this is useful if you want to have
multiple versions of the same large object available.

.. _`python-swiftclient`: http://github.com/openstack/python-swiftclient

----------
Direct API
----------

You can also work with the segments and manifests directly with HTTP
requests instead of having ``swift`` do that for you. You can just
upload the segments like you would any other object and the manifest
is just a zero-byte (not enforced) file with an extra
``X-Object-Manifest`` header.

All the object segments need to be in the same container, have a common object
name prefix, and sort in the order in which they should be concatenated.
Object names are sorted lexicographically as UTF-8 byte strings.
They don't have to be in the same container as the manifest file will be, which
is useful to keep container listings clean as explained above with ``swift``.

The manifest file is simply a zero-byte (not enforced) file with the extra
``X-Object-Manifest: <container>/<prefix>`` header, where ``<container>`` is
the container the object segments are in and ``<prefix>`` is the common prefix
for all the segments.

It is best to upload all the segments first and then create or update the
manifest. In this way, the full object won't be available for downloading
until the upload is complete. Also, you can upload a new set of segments to
a second location and then update the manifest to point to this new location.
During the upload of the new segments, the original manifest will still be
available to download the first set of segments.

.. note::

    When updating a manifest object using a POST request, a
    ``X-Object-Manifest`` header must be included for the object to
    continue to behave as a manifest object.

    The manifest file should have no content. However, this is not enforced.
    If the manifest path itself conforms to container/prefix specified in
    ``X-Object-Manifest``, and if manifest has some content/data in it, it
    would also be considered as segment and manifest's content will be part of
    the concatenated GET response. The order of concatenation follows the usual
    DLO logic which is - the order of concatenation adheres to order returned
    when segment names are sorted.


Here's an example using ``curl`` with tiny 1-byte segments::

    # First, upload the segments
    curl -X PUT -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject/00000001 --data-binary '1'
    curl -X PUT -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject/00000002 --data-binary '2'
    curl -X PUT -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject/00000003 --data-binary '3'

    # Next, create the manifest file
    curl -X PUT -H 'X-Auth-Token: <token>' \
        -H 'X-Object-Manifest: container/myobject/' \
        http://<storage_url>/container/myobject --data-binary ''

    # And now we can download the segments as a single object
    curl -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject
"""

import json

from swift.common import constraints
from swift.common.exceptions import ListingIterError, SegmentError
from swift.common.http import is_success
from swift.common.swob import Request, Response, HTTPException, \
    HTTPRequestedRangeNotSatisfiable, HTTPBadRequest, HTTPConflict, \
    str_to_wsgi, wsgi_to_str, wsgi_quote, wsgi_unquote, normalize_etag
from swift.common.utils import get_logger, \
    RateLimitedIterator, quote, close_if_possible, closing_if_possible, \
    drain_and_close, md5
from swift.common.request_helpers import SegmentedIterable, \
    update_ignore_range_header
from swift.common.wsgi import WSGIContext, make_subrequest, load_app_config


class GetContext(WSGIContext):
    def __init__(self, dlo, logger):
        super(GetContext, self).__init__(dlo.app)
        self.dlo = dlo
        self.logger = logger

    def _get_container_listing(self, req, version, account, container,
                               prefix, marker=''):
        '''
        :param version: whatever
        :param account: native
        :param container: native
        :param prefix: native
        :param marker: native
        '''
        con_req = make_subrequest(
            req.environ,
            path=wsgi_quote('/'.join([
                '', str_to_wsgi(version),
                str_to_wsgi(account), str_to_wsgi(container)])),
            method='GET',
            headers={'x-auth-token': req.headers.get('x-auth-token')},
            agent=('%(orig)s ' + 'DLO MultipartGET'), swift_source='DLO')
        con_req.query_string = 'prefix=%s' % quote(prefix)
        if marker:
            con_req.query_string += '&marker=%s' % quote(marker)

        con_resp = con_req.get_response(self.dlo.app)
        if not is_success(con_resp.status_int):
            if req.method == 'HEAD':
                con_resp.body = b''
            return con_resp, None
        with closing_if_possible(con_resp.app_iter):
            return None, json.loads(b''.join(con_resp.app_iter))

    def _segment_listing_iterator(self, req, version, account, container,
                                  prefix, segments, first_byte=None,
                                  last_byte=None):
        '''
        :param req: upstream request
        :param version: native
        :param account: native
        :param container: native
        :param prefix: native
        :param segments: array of dicts, with native strings
        :param first_byte: number
        :param last_byte: number
        '''
        # It's sort of hokey that this thing takes in the first page of
        # segments as an argument, but we need to compute the etag and content
        # length from the first page, and it's better to have a hokey
        # interface than to make redundant requests.
        if first_byte is None:
            first_byte = 0
        if last_byte is None:
            last_byte = float("inf")

        while True:
            for segment in segments:
                seg_length = int(segment['bytes'])

                if first_byte >= seg_length:
                    # don't need any bytes from this segment
                    first_byte = max(first_byte - seg_length, -1)
                    last_byte = max(last_byte - seg_length, -1)
                    continue
                elif last_byte < 0:
                    # no bytes are needed from this or any future segment
                    break

                seg_name = segment['name']

                # We deliberately omit the etag and size here;
                # SegmentedIterable will check size and etag if
                # specified, but we don't want it to. DLOs only care
                # that the objects' names match the specified prefix.
                # SegmentedIterable will instead check that the data read
                # from each segment matches the response headers.
                _path = "/".join(["", version, account, container, seg_name])
                _first = None if first_byte <= 0 else first_byte
                _last = None if last_byte >= seg_length - 1 else last_byte
                yield {
                    'path': _path,
                    'first_byte': _first,
                    'last_byte': _last
                }

                first_byte = max(first_byte - seg_length, -1)
                last_byte = max(last_byte - seg_length, -1)

            if len(segments) < constraints.CONTAINER_LISTING_LIMIT:
                # a short page means that we're done with the listing
                break
            elif last_byte < 0:
                break

            marker = segments[-1]['name']
            error_response, segments = self._get_container_listing(
                req, version, account, container, prefix, marker)
            if error_response:
                # we've already started sending the response body to the
                # client, so all we can do is raise an exception to make the
                # WSGI server close the connection early
                close_if_possible(error_response.app_iter)
                raise ListingIterError(
                    "Got status %d listing container /%s/%s" %
                    (error_response.status_int, account, container))

    def get_or_head_response(self, req, x_object_manifest):
        '''
        :param req: user's request
        :param x_object_manifest: as unquoted, native string
        '''
        response_headers = self._response_headers

        container, obj_prefix = x_object_manifest.split('/', 1)

        version, account, _junk = req.split_path(2, 3, True)
        version = wsgi_to_str(version)
        account = wsgi_to_str(account)
        error_response, segments = self._get_container_listing(
            req, version, account, container, obj_prefix)
        if error_response:
            return error_response
        have_complete_listing = len(segments) < \
            constraints.CONTAINER_LISTING_LIMIT

        first_byte = last_byte = None
        actual_content_length = None
        content_length_for_swob_range = None
        if req.range and len(req.range.ranges) == 1:
            content_length_for_swob_range = sum(o['bytes'] for o in segments)

            # This is a hack to handle suffix byte ranges (e.g. "bytes=-5"),
            # which we can't honor unless we have a complete listing.
            _junk, range_end = req.range.ranges_for_length(float("inf"))[0]

            # If this is all the segments, we know whether or not this
            # range request is satisfiable.
            #
            # Alternately, we may not have all the segments, but this range
            # falls entirely within the first page's segments, so we know
            # that it is satisfiable.
            if (have_complete_listing
               or range_end < content_length_for_swob_range):
                byteranges = req.range.ranges_for_length(
                    content_length_for_swob_range)
                if not byteranges:
                    headers = {'Accept-Ranges': 'bytes'}
                    if have_complete_listing:
                        headers['Content-Range'] = 'bytes */%d' % (
                            content_length_for_swob_range, )
                    return HTTPRequestedRangeNotSatisfiable(
                        request=req, headers=headers)
                first_byte, last_byte = byteranges[0]
                # For some reason, swob.Range.ranges_for_length adds 1 to the
                # last byte's position.
                last_byte -= 1
                actual_content_length = last_byte - first_byte + 1
            else:
                # The range may or may not be satisfiable, but we can't tell
                # based on just one page of listing, and we're not going to go
                # get more pages because that would use up too many resources,
                # so we ignore the Range header and return the whole object.
                actual_content_length = None
                content_length_for_swob_range = None
                req.range = None
        else:
            req.range = None

        response_headers = [
            (h, v) for h, v in response_headers
            if h.lower() not in ("content-length", "content-range")]

        if content_length_for_swob_range is not None:
            # Here, we have to give swob a big-enough content length so that
            # it can compute the actual content length based on the Range
            # header. This value will not be visible to the client; swob will
            # substitute its own Content-Length.
            #
            # Note: if the manifest points to at least CONTAINER_LISTING_LIMIT
            # segments, this may be less than the sum of all the segments'
            # sizes. However, it'll still be greater than the last byte in the
            # Range header, so it's good enough for swob.
            response_headers.append(('Content-Length',
                                     str(content_length_for_swob_range)))
        elif have_complete_listing:
            actual_content_length = sum(o['bytes'] for o in segments)
            response_headers.append(('Content-Length',
                                     str(actual_content_length)))

        if have_complete_listing:
            response_headers = [(h, v) for h, v in response_headers
                                if h.lower() != "etag"]
            etag = md5(usedforsecurity=False)
            for seg_dict in segments:
                etag.update(normalize_etag(seg_dict['hash']).encode('utf8'))
            response_headers.append(('Etag', '"%s"' % etag.hexdigest()))

        app_iter = None
        if req.method == 'GET':
            listing_iter = RateLimitedIterator(
                self._segment_listing_iterator(
                    req, version, account, container, obj_prefix, segments,
                    first_byte=first_byte, last_byte=last_byte),
                self.dlo.rate_limit_segments_per_sec,
                limit_after=self.dlo.rate_limit_after_segment)

            app_iter = SegmentedIterable(
                req, self.dlo.app, listing_iter, ua_suffix="DLO MultipartGET",
                swift_source="DLO", name=req.path, logger=self.logger,
                max_get_time=self.dlo.max_get_time,
                response_body_length=actual_content_length)

            try:
                app_iter.validate_first_segment()
            except HTTPException as err_resp:
                return err_resp
            except (SegmentError, ListingIterError):
                return HTTPConflict(request=req)

        resp = Response(request=req, headers=response_headers,
                        conditional_response=True,
                        app_iter=app_iter)

        return resp

    def handle_request(self, req, start_response):
        """
        Take a GET or HEAD request, and if it is for a dynamic large object
        manifest, return an appropriate response.

        Otherwise, simply pass it through.
        """
        update_ignore_range_header(req, 'X-Object-Manifest')
        resp_iter = self._app_call(req.environ)

        # make sure this response is for a dynamic large object manifest
        for header, value in self._response_headers:
            if (header.lower() == 'x-object-manifest'):
                content_length = self._response_header_value('content-length')
                if content_length is not None and int(content_length) < 1024:
                    # Go ahead and consume small bodies
                    drain_and_close(resp_iter)
                close_if_possible(resp_iter)
                response = self.get_or_head_response(
                    req, wsgi_to_str(wsgi_unquote(value)))
                return response(req.environ, start_response)
        # Not a dynamic large object manifest; just pass it through.
        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        return resp_iter


class DynamicLargeObject(object):
    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route='dlo')

        # DLO functionality used to live in the proxy server, not middleware,
        # so let's try to go find config values in the proxy's config section
        # to ease cluster upgrades.
        self._populate_config_from_old_location(conf)

        self.max_get_time = int(conf.get('max_get_time', '86400'))
        self.rate_limit_after_segment = int(conf.get(
            'rate_limit_after_segment', '10'))
        self.rate_limit_segments_per_sec = int(conf.get(
            'rate_limit_segments_per_sec', '1'))

    def _populate_config_from_old_location(self, conf):
        if ('rate_limit_after_segment' in conf or
                'rate_limit_segments_per_sec' in conf or
                'max_get_time' in conf or
                '__file__' not in conf):
            return

        proxy_conf = load_app_config(conf['__file__'])
        for setting in ('rate_limit_after_segment',
                        'rate_limit_segments_per_sec',
                        'max_get_time'):
            if setting in proxy_conf:
                conf[setting] = proxy_conf[setting]

    def __call__(self, env, start_response):
        """
        WSGI entry point
        """
        req = Request(env)
        try:
            vrs, account, container, obj = req.split_path(4, 4, True)
            is_obj_req = True
        except ValueError:
            is_obj_req = False
        if not is_obj_req:
            return self.app(env, start_response)

        if ((req.method == 'GET' or req.method == 'HEAD') and
                req.params.get('multipart-manifest') != 'get'):
            return GetContext(self, self.logger).\
                handle_request(req, start_response)
        elif req.method == 'PUT':
            error_response = self._validate_x_object_manifest_header(req)
            if error_response:
                return error_response(env, start_response)
        return self.app(env, start_response)

    def _validate_x_object_manifest_header(self, req):
        """
        Make sure that X-Object-Manifest is valid if present.
        """
        if 'X-Object-Manifest' in req.headers:
            value = req.headers['X-Object-Manifest']
            container = prefix = None
            try:
                container, prefix = value.split('/', 1)
            except ValueError:
                pass
            if not container or not prefix or '?' in value or '&' in value or \
                    prefix.startswith('/'):
                return HTTPBadRequest(
                    request=req,
                    body=('X-Object-Manifest must be in the '
                          'format container/prefix'))


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def dlo_filter(app):
        return DynamicLargeObject(app, conf)
    return dlo_filter
