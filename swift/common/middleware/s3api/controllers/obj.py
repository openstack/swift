# Copyright (c) 2010-2014 OpenStack Foundation.
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

from io import BytesIO
import json

from swift.common import constraints
from swift.common.http import HTTP_OK, HTTP_PARTIAL_CONTENT, HTTP_NO_CONTENT
from swift.common.request_helpers import update_etag_is_at_header
from swift.common.swob import Range, content_range_header_value, \
    normalize_etag
from swift.common.utils import public, list_from_csv
from swift.common.registry import get_swift_info

from swift.common.middleware.versioned_writes.object_versioning import \
    DELETE_MARKER_CONTENT_TYPE
from swift.common.middleware.s3api.utils import S3Timestamp, sysmeta_header
from swift.common.middleware.s3api.controllers.base import Controller
from swift.common.middleware.s3api.s3response import S3NotImplemented, \
    InvalidRange, NoSuchKey, NoSuchVersion, InvalidArgument, HTTPNoContent, \
    PreconditionFailed, KeyTooLongError


class ObjectController(Controller):
    """
    Handles requests on objects
    """
    def _gen_head_range_resp(self, req_range, resp):
        """
        Swift doesn't handle Range header for HEAD requests.
        So, this method generates HEAD range response from HEAD response.
        S3 return HEAD range response, if the value of range satisfies the
        conditions which are described in the following document.
        - http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        """
        length = int(resp.headers.get('Content-Length'))

        try:
            content_range = Range(req_range)
        except ValueError:
            return resp

        ranges = content_range.ranges_for_length(length)
        if ranges == []:
            raise InvalidRange()
        elif ranges:
            if len(ranges) == 1:
                start, end = ranges[0]
                resp.headers['Content-Range'] = \
                    content_range_header_value(start, end, length)
                resp.headers['Content-Length'] = (end - start)
                resp.status = HTTP_PARTIAL_CONTENT
                return resp
            else:
                # TODO: It is necessary to confirm whether need to respond to
                #       multi-part response.(e.g. bytes=0-10,20-30)
                pass

        return resp

    def GETorHEAD(self, req):
        had_match = False
        for match_header in ('if-match', 'if-none-match'):
            if match_header not in req.headers:
                continue
            had_match = True
            for value in list_from_csv(req.headers[match_header]):
                value = normalize_etag(value)
                if value.endswith('-N'):
                    # Deal with fake S3-like etags for SLOs uploaded via Swift
                    req.headers[match_header] += ', ' + value[:-2]

        if had_match:
            # Update where to look
            update_etag_is_at_header(req, sysmeta_header('object', 'etag'))

        object_name = req.object_name
        version_id = req.params.get('versionId')
        if version_id not in ('null', None) and \
                'object_versioning' not in get_swift_info():
            raise S3NotImplemented()
        part_number = req.validate_part_number(check_max=False)

        query = {}
        if version_id is not None:
            query['version-id'] = version_id
        if part_number is not None:
            query['part-number'] = part_number

        if version_id not in ('null', None):
            container_info = req.get_container_info(self.app)
            if not container_info.get(
                    'sysmeta', {}).get('versions-container', ''):
                # Versioning has never been enabled
                raise NoSuchVersion(object_name, version_id)

        resp = req.get_response(self.app, query=query)

        if not resp.is_slo:
            # SLO ignores part_number for non-slo objects, but s3api only
            # allows the query param for non-MPU if it's exactly 1.
            part_number = req.validate_part_number(parts_count=1)
            if part_number == 1:
                # When the query param *is* exactly 1 the response status code
                # and headers are updated.
                resp.status = HTTP_PARTIAL_CONTENT
                resp.headers['Content-Range'] = \
                    'bytes 0-%d/%s' % (int(resp.headers['Content-Length']) - 1,
                                       resp.headers['Content-Length'])
            # else: part_number is None

        if req.method == 'HEAD':
            resp.app_iter = None

        if 'x-amz-meta-deleted' in resp.headers:
            raise NoSuchKey(object_name)

        for key in ('content-type', 'content-language', 'expires',
                    'cache-control', 'content-disposition',
                    'content-encoding'):
            if 'response-' + key in req.params:
                resp.headers[key] = req.params['response-' + key]

        return resp

    @public
    def HEAD(self, req):
        """
        Handle HEAD Object request
        """
        resp = self.GETorHEAD(req)

        if 'range' in req.headers:
            req_range = req.headers['range']
            resp = self._gen_head_range_resp(req_range, resp)

        return resp

    @public
    def GET(self, req):
        """
        Handle GET Object request
        """
        return self.GETorHEAD(req)

    @public
    def PUT(self, req):
        """
        Handle PUT Object and PUT Object (Copy) request
        """
        if len(req.object_name) > constraints.MAX_OBJECT_NAME_LENGTH:
            raise KeyTooLongError()
        # set X-Timestamp by s3api to use at copy resp body
        req_timestamp = S3Timestamp.now()
        req.headers['X-Timestamp'] = req_timestamp.internal
        if all(h in req.headers
               for h in ('X-Amz-Copy-Source', 'X-Amz-Copy-Source-Range')):
            raise InvalidArgument('x-amz-copy-source-range',
                                  req.headers['X-Amz-Copy-Source-Range'],
                                  'Illegal copy header')
        req.check_copy_source(self.app)
        if not req.headers.get('Content-Type'):
            # can't setdefault because it can be None for some reason
            req.headers['Content-Type'] = 'binary/octet-stream'
        resp = req.get_response(self.app)

        if 'X-Amz-Copy-Source' in req.headers:
            resp.append_copy_resp_body(req.controller_name,
                                       req_timestamp.s3xmlformat)
            # delete object metadata from response
            for key in list(resp.headers.keys()):
                if key.lower().startswith('x-amz-meta-'):
                    del resp.headers[key]

        resp.status = HTTP_OK
        return resp

    @public
    def POST(self, req):
        raise S3NotImplemented()

    def _restore_on_delete(self, req):
        resp = req.get_response(self.app, 'GET', req.container_name, '',
                                query={'prefix': req.object_name,
                                       'versions': True})
        if resp.status_int != HTTP_OK:
            return resp
        old_versions = json.loads(resp.body)
        resp = None
        for item in old_versions:
            if item['content_type'] == DELETE_MARKER_CONTENT_TYPE:
                resp = None
                break
            try:
                resp = req.get_response(self.app, 'PUT', query={
                    'version-id': item['version_id']})
            except PreconditionFailed:
                self.logger.debug('skipping failed PUT?version-id=%s' %
                                  item['version_id'])
                continue
            # if that worked, we'll go ahead and fix up the status code
            resp.status_int = HTTP_NO_CONTENT
            break
        return resp

    @public
    def DELETE(self, req):
        """
        Handle DELETE Object request
        """
        if 'versionId' in req.params and \
                req.params['versionId'] != 'null' and \
                'object_versioning' not in get_swift_info():
            raise S3NotImplemented()

        version_id = req.params.get('versionId')
        if version_id not in ('null', None):
            container_info = req.get_container_info(self.app)
            if not container_info.get(
                    'sysmeta', {}).get('versions-container', ''):
                # Versioning has never been enabled
                return HTTPNoContent(headers={'x-amz-version-id': version_id})

        try:
            try:
                query = req.gen_multipart_manifest_delete_query(
                    self.app, version=version_id)
            except NoSuchKey:
                query = {}

            req.headers['Content-Type'] = None  # Ignore client content-type

            if version_id is not None:
                query['version-id'] = version_id
                query['symlink'] = 'get'

            resp = req.get_response(self.app, query=query)
            # If we're going to continue using this request, we need to
            # replace the now-spent body
            req.environ['wsgi.input'] = BytesIO(b'')
            req.headers['content-length'] = '0'
            req.headers.pop('transfer-encoding', None)
            if query.get('multipart-manifest') and resp.status_int == HTTP_OK:
                for chunk in resp.app_iter:
                    pass  # drain the bulk-deleter response
                resp.status = HTTP_NO_CONTENT
                resp.body = b''
            if resp.sw_headers.get('X-Object-Current-Version-Id') == 'null':
                new_resp = self._restore_on_delete(req)
                if new_resp:
                    resp = new_resp
        except NoSuchKey:
            # expect to raise NoSuchBucket when the bucket doesn't exist
            req.get_container_info(self.app)
            # else -- it's gone! Success.
            return HTTPNoContent()
        return resp
