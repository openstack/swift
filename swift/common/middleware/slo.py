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
Middleware that will provide Static Large Object (SLO) support.

This feature is very similar to Dynamic Large Object (DLO) support in that
it allows the user to upload many objects concurrently and afterwards
download them as a single object. It is different in that it does not rely
on eventually consistent container listings to do so. Instead, a user
defined manifest of the object segments is used.

----------------------
Uploading the Manifest
----------------------

After the user has uploaded the objects to be concatenated a manifest is
uploaded. The request must be a PUT with the query parameter::

    ?multipart-manifest=put

The body of this request will be an ordered list of files in
json data format. The data to be supplied for each segment is::

    path: the path to the segment (not including account)
          /container/object_name
    etag: the etag given back when the segment was PUT
    size_bytes: the size of the segment in bytes

The format of the list will be::

    json:
    [{"path": "/cont/object",
      "etag": "etagoftheobjectsegment",
      "size_bytes": 1048576}, ...]

The number of object segments is limited to a configurable amount, default
1000. Each segment, except for the final one, must be at least 1 megabyte
(configurable). On upload, the middleware will head every segment passed in and
verify the size and etag of each. If any of the objects do not match (not
found, size/etag mismatch, below minimum size) then the user will receive a 4xx
error response. If everything does match, the user will receive a 2xx response
and the SLO object is ready for downloading.

Behind the scenes, on success, a json manifest generated from the user input is
sent to object servers with an extra "X-Static-Large-Object: True" header
and a modified Content-Type. The parameter: swift_bytes=$total_size will be
appended to the existing Content-Type, where total_size is the sum of all
the included segments' size_bytes. This extra parameter will be hidden from
the user.

Manifest files can reference objects in separate containers, which will improve
concurrent upload speed. Objects can be referenced by multiple manifests. The
segments of a SLO manifest can even be other SLO manifests. Treat them as any
other object i.e., use the Etag and Content-Length given on the PUT of the
sub-SLO in the manifest to the parent SLO.

-------------------------
Retrieving a Large Object
-------------------------

A GET request to the manifest object will return the concatenation of the
objects from the manifest much like DLO. If any of the segments from the
manifest are not found or their Etag/Content Length no longer match the
connection will drop. In this case a 409 Conflict will be logged in the proxy
logs and the user will receive incomplete results.

The headers from this GET or HEAD request will return the metadata attached
to the manifest object itself with some exceptions::

    Content-Length: the total size of the SLO (the sum of the sizes of
                    the segments in the manifest)
    X-Static-Large-Object: True
    Etag: the etag of the SLO (generated the same way as DLO)

A GET request with the query parameter::

    ?multipart-manifest=get

Will return the actual manifest file itself. This is generated json and does
not match the data sent from the original multipart-manifest=put. This call's
main purpose is for debugging.

When the manifest object is uploaded you are more or less guaranteed that
every segment in the manifest exists and matched the specifications.
However, there is nothing that prevents the user from breaking the
SLO download by deleting/replacing a segment referenced in the manifest. It is
left to the user to use caution in handling the segments.

-----------------------
Deleting a Large Object
-----------------------

A DELETE request will just delete the manifest object itself.

A DELETE with a query parameter::

    ?multipart-manifest=delete

will delete all the segments referenced in the manifest and then the manifest
itself. The failure response will be similar to the bulk delete middleware.

------------------------
Modifying a Large Object
------------------------

PUTs / POSTs will work as expected, PUTs will just overwrite the manifest
object for example.

------------------
Container Listings
------------------

In a container listing the size listed for SLO manifest objects will be the
total_size of the concatenated segments in the manifest. The overall
X-Container-Bytes-Used for the container (and subsequently for the account)
will not reflect total_size of the manifest but the actual size of the json
data stored. The reason for this somewhat confusing discrepancy is we want the
container listing to reflect the size of the manifest object when it is
downloaded. We do not, however, want to count the bytes-used twice (for both
the manifest and the segments it's referring to) in the container and account
metadata which can be used for stats purposes.
"""

from cStringIO import StringIO
from datetime import datetime
import mimetypes
import re
from hashlib import md5
from swift.common.exceptions import ListingIterError, SegmentError
from swift.common.swob import Request, HTTPBadRequest, HTTPServerError, \
    HTTPMethodNotAllowed, HTTPRequestEntityTooLarge, HTTPLengthRequired, \
    HTTPOk, HTTPPreconditionFailed, HTTPException, HTTPNotFound, \
    HTTPUnauthorized, HTTPConflict, HTTPRequestedRangeNotSatisfiable,\
    Response
from swift.common.utils import json, get_logger, config_true_value, \
    get_valid_utf8_str, override_bytes_from_content_type, split_path, \
    register_swift_info, RateLimitedIterator, quote
from swift.common.request_helpers import SegmentedIterable, \
    closing_if_possible, close_if_possible
from swift.common.constraints import check_utf8, MAX_BUFFERED_SLO_SEGMENTS
from swift.common.http import HTTP_NOT_FOUND, HTTP_UNAUTHORIZED, is_success
from swift.common.wsgi import WSGIContext, make_subrequest
from swift.common.middleware.bulk import get_response_body, \
    ACCEPTABLE_FORMATS, Bulk


DEFAULT_MIN_SEGMENT_SIZE = 1024 * 1024  # 1 MiB
DEFAULT_MAX_MANIFEST_SEGMENTS = 1000
DEFAULT_MAX_MANIFEST_SIZE = 1024 * 1024 * 2  # 2 MiB


def parse_input(raw_data):
    """
    Given a request will parse the body and return a list of dictionaries
    :raises: HTTPException on parse errors
    :returns: a list of dictionaries on success
    """
    try:
        parsed_data = json.loads(raw_data)
    except ValueError:
        raise HTTPBadRequest("Manifest must be valid json.")

    req_keys = set(['path', 'etag', 'size_bytes'])
    try:
        for seg_dict in parsed_data:
            if (set(seg_dict) != req_keys or
                    '/' not in seg_dict['path'].lstrip('/')):
                raise HTTPBadRequest('Invalid SLO Manifest File')
    except (AttributeError, TypeError):
        raise HTTPBadRequest('Invalid SLO Manifest File')

    return parsed_data


class SloPutContext(WSGIContext):
    def __init__(self, slo, slo_etag):
        super(SloPutContext, self).__init__(slo.app)
        self.slo_etag = '"' + slo_etag.hexdigest() + '"'

    def handle_slo_put(self, req, start_response):
        app_resp = self._app_call(req.environ)

        for i in xrange(len(self._response_headers)):
            if self._response_headers[i][0].lower() == 'etag':
                self._response_headers[i] = ('Etag', self.slo_etag)
                break

        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        return app_resp


class SloGetContext(WSGIContext):

    max_slo_recursion_depth = 10

    def __init__(self, slo):
        self.slo = slo
        self.first_byte = None
        self.last_byte = None
        super(SloGetContext, self).__init__(slo.app)

    def _fetch_sub_slo_segments(self, req, version, acc, con, obj):
        """
        Fetch the submanifest, parse it, and return it.
        Raise exception on failures.
        """
        sub_req = make_subrequest(
            req.environ, path='/'.join(['', version, acc, con, obj]),
            method='GET',
            headers={'x-auth-token': req.headers.get('x-auth-token')},
            agent=('%(orig)s ' + 'SLO MultipartGET'), swift_source='SLO')
        sub_resp = sub_req.get_response(self.slo.app)

        if not is_success(sub_resp.status_int):
            raise ListingIterError(
                'ERROR: while fetching %s, GET of submanifest %s '
                'failed with status %d' % (req.path, sub_req.path,
                                           sub_resp.status_int))

        try:
            with closing_if_possible(sub_resp.app_iter):
                return json.loads(''.join(sub_resp.app_iter))
        except ValueError as err:
            raise ListingIterError(
                'ERROR: while fetching %s, JSON-decoding of submanifest %s '
                'failed with %s' % (req.path, sub_req.path, err))

    def _segment_listing_iterator(self, req, version, account, segments,
                                  recursion_depth=1):
        for seg_dict in segments:
            if config_true_value(seg_dict.get('sub_slo')):
                override_bytes_from_content_type(seg_dict,
                                                 logger=self.slo.logger)

        # We handle the range stuff here so that we can be smart about
        # skipping unused submanifests. For example, if our first segment is a
        # submanifest referencing 50 MiB total, but self.first_byte falls in
        # the 51st MiB, then we can avoid fetching the first submanifest.
        #
        # If we were to make SegmentedIterable handle all the range
        # calculations, we would be unable to make this optimization.
        total_length = sum(int(seg['bytes']) for seg in segments)
        if self.first_byte is None:
            self.first_byte = 0
        if self.last_byte is None:
            self.last_byte = total_length - 1

        for seg_dict in segments:
            seg_length = int(seg_dict['bytes'])

            if self.first_byte >= seg_length:
                # don't need any bytes from this segment
                self.first_byte = max(self.first_byte - seg_length, -1)
                self.last_byte = max(self.last_byte - seg_length, -1)
                continue

            if self.last_byte < 0:
                # no bytes are needed from this or any future segment
                break

            if config_true_value(seg_dict.get('sub_slo')):
                # do this check here so that we can avoid fetching this last
                # manifest before raising the exception
                if recursion_depth >= self.max_slo_recursion_depth:
                    raise ListingIterError("Max recursion depth exceeded")

                sub_path = get_valid_utf8_str(seg_dict['name'])
                sub_cont, sub_obj = split_path(sub_path, 2, 2, True)
                sub_segments = self._fetch_sub_slo_segments(
                    req, version, account, sub_cont, sub_obj)
                for sub_seg_dict, sb, eb in self._segment_listing_iterator(
                        req, version, account, sub_segments,
                        recursion_depth=recursion_depth + 1):
                    yield sub_seg_dict, sb, eb
            else:
                if isinstance(seg_dict['name'], unicode):
                    seg_dict['name'] = seg_dict['name'].encode("utf-8")
                seg_length = int(seg_dict['bytes'])
                yield (seg_dict,
                       (None if self.first_byte <= 0 else self.first_byte),
                       (None if self.last_byte >=
                        seg_length - 1 else self.last_byte))
                self.first_byte = max(self.first_byte - seg_length, -1)
                self.last_byte = max(self.last_byte - seg_length, -1)

    def _need_to_refetch_manifest(self, req):
        """
        Just because a response shows that an object is a SLO manifest does not
        mean that response's body contains the entire SLO manifest. If it
        doesn't, we need to make a second request to actually get the whole
        thing.

        Note: this assumes that X-Static-Large-Object has already been found.
        """
        if req.method == 'HEAD':
            return True

        response_status = int(self._response_status[:3])

        # These are based on etag, and the SLO's etag is almost certainly not
        # the manifest object's etag. Still, it's highly likely that the
        # submitted If-None-Match won't match the manifest object's etag, so
        # we can avoid re-fetching the manifest if we got a successful
        # response.
        if ((req.if_match or req.if_none_match) and
                not is_success(response_status)):
            return True

        if req.range and response_status in (206, 416):
            content_range = ''
            for header, value in self._response_headers:
                if header.lower() == 'content-range':
                    content_range = value
                    break
            # e.g. Content-Range: bytes 0-14289/14290
            match = re.match('bytes (\d+)-(\d+)/(\d+)$', content_range)
            if not match:
                # Malformed or missing, so we don't know what we got.
                return True
            first_byte, last_byte, length = [int(x) for x in match.groups()]
            # If and only if we actually got back the full manifest body, then
            # we can avoid re-fetching the object.
            got_everything = (first_byte == 0 and last_byte == length - 1)
            return not got_everything

        return False

    def handle_slo_get_or_head(self, req, start_response):
        """
        Takes a request and a start_response callable and does the normal WSGI
        thing with them. Returns an iterator suitable for sending up the WSGI
        chain.

        :param req: swob.Request object; is a GET or HEAD request aimed at
                    what may be a static large object manifest (or may not).
        :param start_response: WSGI start_response callable
        """
        resp_iter = self._app_call(req.environ)

        # make sure this response is for a static large object manifest
        for header, value in self._response_headers:
            if (header.lower() == 'x-static-large-object' and
                    config_true_value(value)):
                break
        else:
            # Not a static large object manifest. Just pass it through.
            start_response(self._response_status,
                           self._response_headers,
                           self._response_exc_info)
            return resp_iter

        # Handle pass-through request for the manifest itself
        if req.params.get('multipart-manifest') == 'get':
            new_headers = []
            for header, value in self._response_headers:
                if header.lower() == 'content-type':
                    new_headers.append(('Content-Type',
                                        'application/json; charset=utf-8'))
                else:
                    new_headers.append((header, value))
            self._response_headers = new_headers
            start_response(self._response_status,
                           self._response_headers,
                           self._response_exc_info)
            return resp_iter

        if self._need_to_refetch_manifest(req):
            req.environ['swift.non_client_disconnect'] = True
            close_if_possible(resp_iter)
            del req.environ['swift.non_client_disconnect']

            get_req = make_subrequest(
                req.environ, method='GET',
                headers={'x-auth-token': req.headers.get('x-auth-token')},
                agent=('%(orig)s ' + 'SLO MultipartGET'), swift_source='SLO')
            resp_iter = self._app_call(get_req.environ)

        # Any Content-Range from a manifest is almost certainly wrong for the
        # full large object.
        resp_headers = [(h, v) for h, v in self._response_headers
                        if not h.lower() == 'content-range']

        response = self.get_or_head_response(
            req, resp_headers, resp_iter)
        return response(req.environ, start_response)

    def get_or_head_response(self, req, resp_headers, resp_iter):
        resp_body = ''.join(resp_iter)
        try:
            segments = json.loads(resp_body)
        except ValueError:
            segments = []

        etag = md5()
        content_length = 0
        for seg_dict in segments:
            etag.update(seg_dict['hash'])

            if config_true_value(seg_dict.get('sub_slo')):
                override_bytes_from_content_type(
                    seg_dict, logger=self.slo.logger)
            content_length += int(seg_dict['bytes'])

        response_headers = [(h, v) for h, v in resp_headers
                            if h.lower() not in ('etag', 'content-length')]
        response_headers.append(('Content-Length', str(content_length)))
        response_headers.append(('Etag', '"%s"' % etag.hexdigest()))

        if req.method == 'HEAD':
            return self._manifest_head_response(req, response_headers)
        else:
            return self._manifest_get_response(
                req, content_length, response_headers, segments)

    def _manifest_head_response(self, req, response_headers):
        return HTTPOk(request=req, headers=response_headers, body='',
                      conditional_response=True)

    def _manifest_get_response(self, req, content_length, response_headers,
                               segments):
        self.first_byte, self.last_byte = None, None
        if req.range:
            byteranges = req.range.ranges_for_length(content_length)
            if len(byteranges) == 0:
                return HTTPRequestedRangeNotSatisfiable(request=req)
            elif len(byteranges) == 1:
                self.first_byte, self.last_byte = byteranges[0]
                # For some reason, swob.Range.ranges_for_length adds 1 to the
                # last byte's position.
                self.last_byte -= 1
            else:
                req.range = None

        ver, account, _junk = req.split_path(3, 3, rest_with_last=True)
        plain_listing_iter = self._segment_listing_iterator(
            req, ver, account, segments)

        ratelimited_listing_iter = RateLimitedIterator(
            plain_listing_iter,
            self.slo.rate_limit_segments_per_sec,
            limit_after=self.slo.rate_limit_after_segment)

        # self._segment_listing_iterator gives us 3-tuples of (segment dict,
        # start byte, end byte), but SegmentedIterable wants (obj path, etag,
        # size, start byte, end byte), so we clean that up here
        segment_listing_iter = (
            ("/{ver}/{acc}/{conobj}".format(
                ver=ver, acc=account, conobj=seg_dict['name'].lstrip('/')),
                seg_dict['hash'], int(seg_dict['bytes']),
                start_byte, end_byte)
            for seg_dict, start_byte, end_byte in ratelimited_listing_iter)

        segmented_iter = SegmentedIterable(
            req, self.slo.app, segment_listing_iter,
            name=req.path, logger=self.slo.logger,
            ua_suffix="SLO MultipartGET",
            swift_source="SLO",
            max_get_time=self.slo.max_get_time)

        try:
            segmented_iter.validate_first_segment()
        except (ListingIterError, SegmentError):
            # Copy from the SLO explanation in top of this file.
            # If any of the segments from the manifest are not found or
            # their Etag/Content Length no longer match the connection
            # will drop. In this case a 409 Conflict will be logged in
            # the proxy logs and the user will receive incomplete results.
            return HTTPConflict(request=req)

        response = Response(request=req, content_length=content_length,
                            headers=response_headers,
                            conditional_response=True,
                            app_iter=segmented_iter)
        if req.range:
            response.headers.pop('Etag')
        return response


class StaticLargeObject(object):
    """
    StaticLargeObject Middleware

    See above for a full description.

    The proxy logs created for any subrequests made will have swift.source set
    to "SLO".

    :param app: The next WSGI filter or app in the paste.deploy chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf, min_segment_size=DEFAULT_MIN_SEGMENT_SIZE,
                 max_manifest_segments=DEFAULT_MAX_MANIFEST_SEGMENTS,
                 max_manifest_size=DEFAULT_MAX_MANIFEST_SIZE):
        self.conf = conf
        self.app = app
        self.logger = get_logger(conf, log_route='slo')
        self.max_manifest_segments = max_manifest_segments
        self.max_manifest_size = max_manifest_size
        self.min_segment_size = min_segment_size
        self.max_get_time = int(self.conf.get('max_get_time', 86400))
        self.rate_limit_after_segment = int(self.conf.get(
            'rate_limit_after_segment', '10'))
        self.rate_limit_segments_per_sec = int(self.conf.get(
            'rate_limit_segments_per_sec', '0'))
        self.bulk_deleter = Bulk(app, {}, logger=self.logger)

    def handle_multipart_get_or_head(self, req, start_response):
        """
        Handles the GET or HEAD of a SLO manifest.

        The response body (only on GET, of course) will consist of the
        concatenation of the segments.

        :params req: a swob.Request with a path referencing an object
        :raises: HttpException on errors
        """
        return SloGetContext(self).handle_slo_get_or_head(req, start_response)

    def copy_hook(self, inner_hook):

        def slo_hook(source_req, source_resp, sink_req):
            x_slo = source_resp.headers.get('X-Static-Large-Object')
            if (config_true_value(x_slo)
                    and source_req.params.get('multipart-manifest') != 'get'):
                source_resp = SloGetContext(self).get_or_head_response(
                    source_req, source_resp.headers.items(),
                    source_resp.app_iter)
            return inner_hook(source_req, source_resp, sink_req)

        return slo_hook

    def handle_multipart_put(self, req, start_response):
        """
        Will handle the PUT of a SLO manifest.
        Heads every object in manifest to check if is valid and if so will
        save a manifest generated from the user input. Uses WSGIContext to
        call self and start_response and returns a WSGI iterator.

        :params req: a swob.Request with an obj in path
        :raises: HttpException on errors
        """
        try:
            vrs, account, container, obj = req.split_path(1, 4, True)
        except ValueError:
            return self.app(req.environ, start_response)
        if req.content_length > self.max_manifest_size:
            raise HTTPRequestEntityTooLarge(
                "Manifest File > %d bytes" % self.max_manifest_size)
        if req.headers.get('X-Copy-From'):
            raise HTTPMethodNotAllowed(
                'Multipart Manifest PUTs cannot be COPY requests')
        if req.content_length is None and \
                req.headers.get('transfer-encoding', '').lower() != 'chunked':
            raise HTTPLengthRequired(request=req)
        parsed_data = parse_input(req.body_file.read(self.max_manifest_size))
        problem_segments = []

        if len(parsed_data) > self.max_manifest_segments:
            raise HTTPRequestEntityTooLarge(
                'Number of segments must be <= %d' %
                self.max_manifest_segments)
        total_size = 0
        out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
        if not out_content_type:
            out_content_type = 'text/plain'
        data_for_storage = []
        slo_etag = md5()
        for index, seg_dict in enumerate(parsed_data):
            obj_name = seg_dict['path']
            if isinstance(obj_name, unicode):
                obj_name = obj_name.encode('utf-8')
            obj_path = '/'.join(['', vrs, account, obj_name.lstrip('/')])
            try:
                seg_size = int(seg_dict['size_bytes'])
            except (ValueError, TypeError):
                raise HTTPBadRequest('Invalid Manifest File')
            if seg_size < self.min_segment_size and \
                    index < len(parsed_data) - 1:
                raise HTTPBadRequest(
                    'Each segment, except the last, must be at least '
                    '%d bytes.' % self.min_segment_size)

            new_env = req.environ.copy()
            new_env['PATH_INFO'] = obj_path
            new_env['REQUEST_METHOD'] = 'HEAD'
            new_env['swift.source'] = 'SLO'
            del(new_env['wsgi.input'])
            del(new_env['QUERY_STRING'])
            new_env['CONTENT_LENGTH'] = 0
            new_env['HTTP_USER_AGENT'] = \
                '%s MultipartPUT' % req.environ.get('HTTP_USER_AGENT')
            head_seg_resp = \
                Request.blank(obj_path, new_env).get_response(self)
            if head_seg_resp.is_success:
                total_size += seg_size
                if seg_size != head_seg_resp.content_length:
                    problem_segments.append([quote(obj_name), 'Size Mismatch'])
                if seg_dict['etag'] == head_seg_resp.etag:
                    slo_etag.update(seg_dict['etag'])
                else:
                    problem_segments.append([quote(obj_name), 'Etag Mismatch'])
                if head_seg_resp.last_modified:
                    last_modified = head_seg_resp.last_modified
                else:
                    # shouldn't happen
                    last_modified = datetime.now()

                last_modified_formatted = \
                    last_modified.strftime('%Y-%m-%dT%H:%M:%S.%f')
                seg_data = {'name': '/' + seg_dict['path'].lstrip('/'),
                            'bytes': seg_size,
                            'hash': seg_dict['etag'],
                            'content_type': head_seg_resp.content_type,
                            'last_modified': last_modified_formatted}
                if config_true_value(
                        head_seg_resp.headers.get('X-Static-Large-Object')):
                    seg_data['sub_slo'] = True
                data_for_storage.append(seg_data)

            else:
                problem_segments.append([quote(obj_name),
                                         head_seg_resp.status])
        if problem_segments:
            resp_body = get_response_body(
                out_content_type, {}, problem_segments)
            raise HTTPBadRequest(resp_body, content_type=out_content_type)
        env = req.environ

        if not env.get('CONTENT_TYPE'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            env['CONTENT_TYPE'] = guessed_type or 'application/octet-stream'
        env['swift.content_type_overridden'] = True
        env['CONTENT_TYPE'] += ";swift_bytes=%d" % total_size
        env['HTTP_X_STATIC_LARGE_OBJECT'] = 'True'
        json_data = json.dumps(data_for_storage)
        env['CONTENT_LENGTH'] = str(len(json_data))
        env['wsgi.input'] = StringIO(json_data)

        slo_put_context = SloPutContext(self, slo_etag)
        return slo_put_context.handle_slo_put(req, start_response)

    def get_segments_to_delete_iter(self, req):
        """
        A generator function to be used to delete all the segments and
        sub-segments referenced in a manifest.

        :params req: a swob.Request with an SLO manifest in path
        :raises HTTPPreconditionFailed: on invalid UTF8 in request path
        :raises HTTPBadRequest: on too many buffered sub segments and
                                on invalid SLO manifest path
        """
        if not check_utf8(req.path_info):
            raise HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
        vrs, account, container, obj = req.split_path(4, 4, True)

        segments = [{
            'sub_slo': True,
            'name': ('/%s/%s' % (container, obj)).decode('utf-8')}]
        while segments:
            if len(segments) > MAX_BUFFERED_SLO_SEGMENTS:
                raise HTTPBadRequest(
                    'Too many buffered slo segments to delete.')
            seg_data = segments.pop(0)
            if seg_data.get('sub_slo'):
                try:
                    segments.extend(
                        self.get_slo_segments(seg_data['name'], req))
                except HTTPException as err:
                    # allow bulk delete response to report errors
                    seg_data['error'] = {'code': err.status_int,
                                         'message': err.body}

                # add manifest back to be deleted after segments
                seg_data['sub_slo'] = False
                segments.append(seg_data)
            else:
                seg_data['name'] = seg_data['name'].encode('utf-8')
                yield seg_data

    def get_slo_segments(self, obj_name, req):
        """
        Performs a swob.Request and returns the SLO manifest's segments.

        :raises HTTPServerError: on unable to load obj_name or
                                 on unable to load the SLO manifest data.
        :raises HTTPBadRequest: on not an SLO manifest
        :raises HTTPNotFound: on SLO manifest not found
        :returns: SLO manifest's segments
        """
        vrs, account, _junk = req.split_path(2, 3, True)
        new_env = req.environ.copy()
        new_env['REQUEST_METHOD'] = 'GET'
        del(new_env['wsgi.input'])
        new_env['QUERY_STRING'] = 'multipart-manifest=get'
        new_env['CONTENT_LENGTH'] = 0
        new_env['HTTP_USER_AGENT'] = \
            '%s MultipartDELETE' % new_env.get('HTTP_USER_AGENT')
        new_env['swift.source'] = 'SLO'
        new_env['PATH_INFO'] = (
            '/%s/%s/%s' % (vrs, account, obj_name.lstrip('/'))
        ).encode('utf-8')
        resp = Request.blank('', new_env).get_response(self.app)

        if resp.is_success:
            if config_true_value(resp.headers.get('X-Static-Large-Object')):
                try:
                    return json.loads(resp.body)
                except ValueError:
                    raise HTTPServerError('Unable to load SLO manifest')
            else:
                raise HTTPBadRequest('Not an SLO manifest')
        elif resp.status_int == HTTP_NOT_FOUND:
            raise HTTPNotFound('SLO manifest not found')
        elif resp.status_int == HTTP_UNAUTHORIZED:
            raise HTTPUnauthorized('401 Unauthorized')
        else:
            raise HTTPServerError('Unable to load SLO manifest or segment.')

    def handle_multipart_delete(self, req):
        """
        Will delete all the segments in the SLO manifest and then, if
        successful, will delete the manifest file.

        :params req: a swob.Request with an obj in path
        :returns: swob.Response whose app_iter set to Bulk.handle_delete_iter
        """
        resp = HTTPOk(request=req)
        out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
        if out_content_type:
            resp.content_type = out_content_type
        resp.app_iter = self.bulk_deleter.handle_delete_iter(
            req, objs_to_delete=self.get_segments_to_delete_iter(req),
            user_agent='MultipartDELETE', swift_source='SLO',
            out_content_type=out_content_type)
        return resp

    def __call__(self, env, start_response):
        """
        WSGI entry point
        """
        req = Request(env)
        try:
            vrs, account, container, obj = req.split_path(4, 4, True)
        except ValueError:
            return self.app(env, start_response)

        # install our COPY-callback hook
        env['swift.copy_hook'] = self.copy_hook(
            env.get('swift.copy_hook',
                    lambda src_req, src_resp, sink_req: src_resp))

        try:
            if req.method == 'PUT' and \
                    req.params.get('multipart-manifest') == 'put':
                return self.handle_multipart_put(req, start_response)
            if req.method == 'DELETE' and \
                    req.params.get('multipart-manifest') == 'delete':
                return self.handle_multipart_delete(req)(env, start_response)
            if req.method == 'GET' or req.method == 'HEAD':
                return self.handle_multipart_get_or_head(req, start_response)
            if 'X-Static-Large-Object' in req.headers:
                raise HTTPBadRequest(
                    request=req,
                    body='X-Static-Large-Object is a reserved header. '
                    'To create a static large object add query param '
                    'multipart-manifest=put.')
        except HTTPException as err_resp:
            return err_resp(env, start_response)

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    max_manifest_segments = int(conf.get('max_manifest_segments',
                                         DEFAULT_MAX_MANIFEST_SEGMENTS))
    max_manifest_size = int(conf.get('max_manifest_size',
                                     DEFAULT_MAX_MANIFEST_SIZE))
    min_segment_size = int(conf.get('min_segment_size',
                                    DEFAULT_MIN_SEGMENT_SIZE))

    register_swift_info('slo',
                        max_manifest_segments=max_manifest_segments,
                        max_manifest_size=max_manifest_size,
                        min_segment_size=min_segment_size)

    def slo_filter(app):
        return StaticLargeObject(
            app, conf,
            max_manifest_segments=max_manifest_segments,
            max_manifest_size=max_manifest_size,
            min_segment_size=min_segment_size)
    return slo_filter
