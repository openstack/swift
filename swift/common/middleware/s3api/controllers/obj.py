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

from swift.common.http import HTTP_OK, HTTP_PARTIAL_CONTENT, HTTP_NO_CONTENT
from swift.common.request_helpers import update_etag_is_at_header
from swift.common.swob import Range, content_range_header_value
from swift.common.utils import public, list_from_csv

from swift.common.middleware.s3api.utils import S3Timestamp, sysmeta_header
from swift.common.middleware.s3api.controllers.base import Controller
from swift.common.middleware.s3api.s3response import S3NotImplemented, \
    InvalidRange, NoSuchKey, InvalidArgument, HTTPNoContent


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
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                if value.endswith('-N'):
                    # Deal with fake S3-like etags for SLOs uploaded via Swift
                    req.headers[match_header] += ', ' + value[:-2]

        if had_match:
            # Update where to look
            update_etag_is_at_header(req, sysmeta_header('object', 'etag'))

        resp = req.get_response(self.app)

        if req.method == 'HEAD':
            resp.app_iter = None

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
        # set X-Timestamp by s3api to use at copy resp body
        req_timestamp = S3Timestamp.now()
        req.headers['X-Timestamp'] = req_timestamp.internal
        if all(h in req.headers
               for h in ('X-Amz-Copy-Source', 'X-Amz-Copy-Source-Range')):
            raise InvalidArgument('x-amz-copy-source-range',
                                  req.headers['X-Amz-Copy-Source-Range'],
                                  'Illegal copy header')
        req.check_copy_source(self.app)
        resp = req.get_response(self.app)

        if 'X-Amz-Copy-Source' in req.headers:
            resp.append_copy_resp_body(req.controller_name,
                                       req_timestamp.s3xmlformat)

            # delete object metadata from response
            for key in list(resp.headers.keys()):
                if key.startswith('x-amz-meta-'):
                    del resp.headers[key]

        resp.status = HTTP_OK
        return resp

    @public
    def POST(self, req):
        raise S3NotImplemented()

    @public
    def DELETE(self, req):
        """
        Handle DELETE Object request
        """
        try:
            query = req.gen_multipart_manifest_delete_query(self.app)
            req.headers['Content-Type'] = None  # Ignore client content-type
            resp = req.get_response(self.app, query=query)
            if query and resp.status_int == HTTP_OK:
                for chunk in resp.app_iter:
                    pass  # drain the bulk-deleter response
                resp.status = HTTP_NO_CONTENT
                resp.body = ''
        except NoSuchKey:
            # expect to raise NoSuchBucket when the bucket doesn't exist
            req.get_container_info(self.app)
            # else -- it's gone! Success.
            return HTTPNoContent()
        return resp
