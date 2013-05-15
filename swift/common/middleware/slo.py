# Copyright (c) 2013 OpenStack, LLC.
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

Manifest files can reference objects in separate containers, which
will improve concurrent upload speed. Objects can be referenced by
multiple manifests.

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
left to the user use caution in handling the segments.

-----------------------
Deleting a Large Object
-----------------------

A DELETE request will just delete the manifest object itself.

A DELETE with a query parameter::

    ?multipart-manifest=delete

will delete all the segments referenced in the manifest and then, if
successful, the manifest itself. The failure response will be similar to
the bulk delete middleware.

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

from urllib import quote
from cStringIO import StringIO
from datetime import datetime
import mimetypes
from swift.common.swob import Request, HTTPBadRequest, HTTPServerError, \
    HTTPMethodNotAllowed, HTTPRequestEntityTooLarge, HTTPLengthRequired, \
    HTTPOk, HTTPPreconditionFailed, wsgify
from swift.common.utils import json, get_logger, config_true_value
from swift.common.constraints import check_utf8
from swift.common.middleware.bulk import get_response_body, \
    ACCEPTABLE_FORMATS, Bulk


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
            if (set(seg_dict.keys()) != req_keys or
                    '/' not in seg_dict['path'].lstrip('/')):
                raise HTTPBadRequest('Invalid SLO Manifest File')
    except (AttributeError, TypeError):
        raise HTTPBadRequest('Invalid SLO Manifest File')

    return parsed_data


class StaticLargeObject(object):
    """
    StaticLargeObject Middleware

    See above for a full description.

    The proxy logs created for any subrequests made will have swift.source set
    to "SLO".

    :param app: The next WSGI filter or app in the paste.deploy chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf):
        self.conf = conf
        self.app = app
        self.logger = get_logger(conf, log_route='slo')
        self.max_manifest_segments = int(self.conf.get('max_manifest_segments',
                                         1000))
        self.max_manifest_size = int(self.conf.get('max_manifest_size',
                                     1024 * 1024 * 2))
        self.min_segment_size = int(self.conf.get('min_segment_size',
                                    1024 * 1024))
        self.bulk_deleter = Bulk(
            app, {'max_deletes_per_request': self.max_manifest_segments})

    def handle_multipart_put(self, req):
        """
        Will handle the PUT of a SLO manifest.
        Heads every object in manifest to check if is valid and if so will
        save a manifest generated from the user input.

        :params req: a swob.Request with an obj in path
        :raises: HttpException on errors
        """
        try:
            vrs, account, container, obj = req.split_path(1, 4, True)
        except ValueError:
            return self.app
        if req.content_length > self.max_manifest_size:
            raise HTTPRequestEntityTooLarge(
                "Manifest File > %d bytes" % self.max_manifest_size)
        if req.headers.get('X-Copy-From'):
            raise HTTPMethodNotAllowed(
                'Multipart Manifest PUTs cannot be Copy requests')
        if req.content_length is None and \
                req.headers.get('transfer-encoding', '').lower() != 'chunked':
            raise HTTPLengthRequired(request=req)
        parsed_data = parse_input(req.body_file.read(self.max_manifest_size))
        problem_segments = []

        if len(parsed_data) > self.max_manifest_segments:
            raise HTTPRequestEntityTooLarge(
                'Number segments must be <= %d' % self.max_manifest_segments)
        total_size = 0
        out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
        if not out_content_type:
            out_content_type = 'text/plain'
        data_for_storage = []
        for index, seg_dict in enumerate(parsed_data):
            obj_path = '/'.join(
                ['', vrs, account, seg_dict['path'].lstrip('/')])
            try:
                seg_size = int(seg_dict['size_bytes'])
            except (ValueError, TypeError):
                raise HTTPBadRequest('Invalid Manifest File')
            if seg_size < self.min_segment_size and \
                    (index == 0 or index < len(parsed_data) - 1):
                raise HTTPBadRequest(
                    'Each segment, except the last, must be larger than '
                    '%d bytes.' % self.min_segment_size)

            new_env = req.environ.copy()
            if isinstance(obj_path, unicode):
                obj_path = obj_path.encode('utf-8')
            new_env['PATH_INFO'] = obj_path
            new_env['REQUEST_METHOD'] = 'HEAD'
            new_env['swift.source'] = 'SLO'
            del(new_env['wsgi.input'])
            del(new_env['QUERY_STRING'])
            new_env['CONTENT_LENGTH'] = 0
            new_env['HTTP_USER_AGENT'] = \
                '%s MultipartPUT' % req.environ.get('HTTP_USER_AGENT')
            head_seg_resp = \
                Request.blank(obj_path, new_env).get_response(self.app)
            if head_seg_resp.status_int // 100 == 2:
                total_size += seg_size
                if seg_size != head_seg_resp.content_length:
                    problem_segments.append([quote(obj_path), 'Size Mismatch'])
                if seg_dict['etag'] != head_seg_resp.etag:
                    problem_segments.append([quote(obj_path), 'Etag Mismatch'])
                if 'X-Static-Large-Object' in head_seg_resp.headers or \
                        'X-Object-Manifest' in head_seg_resp.headers:
                    problem_segments.append(
                        [quote(obj_path), 'Segments cannot be Large Objects'])
                if head_seg_resp.last_modified:
                    last_modified = head_seg_resp.last_modified
                else:
                    # shouldn't happen
                    last_modified = datetime.now()

                last_modified_formatted = \
                    last_modified.strftime('%Y-%m-%dT%H:%M:%S.%f')
                data_for_storage.append(
                    {'name': '/' + seg_dict['path'].lstrip('/'),
                     'bytes': seg_size,
                     'hash': seg_dict['etag'],
                     'content_type': head_seg_resp.content_type,
                     'last_modified': last_modified_formatted})

            else:
                problem_segments.append([quote(obj_path),
                                         head_seg_resp.status])
        if problem_segments:
            resp_body = get_response_body(
                out_content_type, {}, problem_segments)
            raise HTTPBadRequest(resp_body, content_type=out_content_type)
        env = req.environ

        if not env.get('CONTENT_TYPE'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            env['CONTENT_TYPE'] = guessed_type or 'application/octet-stream'
        env['swift.content_type_overriden'] = True
        env['CONTENT_TYPE'] += ";swift_bytes=%d" % total_size
        env['HTTP_X_STATIC_LARGE_OBJECT'] = 'True'
        json_data = json.dumps(data_for_storage)
        env['CONTENT_LENGTH'] = str(len(json_data))
        env['wsgi.input'] = StringIO(json_data)
        return self.app

    def handle_multipart_delete(self, req):
        """
        Will delete all the segments in the SLO manifest and then, if
        successful, will delete the manifest file.
        :params req: a swob.Request with an obj in path
        :raises HTTPServerError: on invalid manifest
        :returns: swob.Response whose app_iter set to Bulk.handle_delete_iter
        """
        if not check_utf8(req.path_info):
            raise HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
        try:
            vrs, account, container, obj = req.split_path(4, 4, True)
        except ValueError:
            raise HTTPBadRequest('Not an SLO manifest')
        new_env = req.environ.copy()
        new_env['REQUEST_METHOD'] = 'GET'
        del(new_env['wsgi.input'])
        new_env['QUERY_STRING'] = 'multipart-manifest=get'
        new_env['CONTENT_LENGTH'] = 0
        new_env['HTTP_USER_AGENT'] = \
            '%s MultipartDELETE' % req.environ.get('HTTP_USER_AGENT')
        new_env['swift.source'] = 'SLO'
        get_man_resp = \
            Request.blank('', new_env).get_response(self.app)
        if get_man_resp.status_int // 100 == 2:
            if not config_true_value(
                    get_man_resp.headers.get('X-Static-Large-Object')):
                raise HTTPBadRequest('Not an SLO manifest')
            try:
                manifest = json.loads(get_man_resp.body)
                # append the manifest file for deletion at the end
                manifest.append(
                    {'name': '/'.join(['', container, obj]).decode('utf-8')})
            except ValueError:
                raise HTTPServerError('Invalid manifest file')
            resp = HTTPOk(request=req)
            resp.app_iter = self.bulk_deleter.handle_delete_iter(
                req,
                objs_to_delete=[o['name'].encode('utf-8') for o in manifest],
                user_agent='MultipartDELETE', swift_source='SLO')
            return resp
        return get_man_resp

    @wsgify
    def __call__(self, req):
        """
        WSGI entry point
        """
        try:
            vrs, account, container, obj = req.split_path(1, 4, True)
        except ValueError:
            return self.app
        if obj:
            if req.method == 'PUT' and \
                    req.params.get('multipart-manifest') == 'put':
                return self.handle_multipart_put(req)
            if req.method == 'DELETE' and \
                    req.params.get('multipart-manifest') == 'delete':
                return self.handle_multipart_delete(req)
            if 'X-Static-Large-Object' in req.headers:
                raise HTTPBadRequest(
                    request=req,
                    body='X-Static-Large-Object is a reserved header. '
                    'To create a static large object add query param '
                    'multipart-manifest=put.')

        return self.app


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def slo_filter(app):
        return StaticLargeObject(app, conf)
    return slo_filter
