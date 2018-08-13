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

from base64 import standard_b64encode as b64encode
from base64 import standard_b64decode as b64decode

from swift.common.http import HTTP_OK
from swift.common.utils import json, public, config_true_value

from swift.common.middleware.s3api.controllers.base import Controller
from swift.common.middleware.s3api.etree import Element, SubElement, tostring, \
    fromstring, XMLSyntaxError, DocumentInvalid
from swift.common.middleware.s3api.s3response import HTTPOk, S3NotImplemented, \
    InvalidArgument, \
    MalformedXML, InvalidLocationConstraint, NoSuchBucket, \
    BucketNotEmpty, InternalError, ServiceUnavailable, NoSuchKey
from swift.common.middleware.s3api.utils import MULTIUPLOAD_SUFFIX

MAX_PUT_BUCKET_BODY_SIZE = 10240


class BucketController(Controller):
    """
    Handles bucket request.
    """
    def _delete_segments_bucket(self, req):
        """
        Before delete bucket, delete segments bucket if existing.
        """
        container = req.container_name + MULTIUPLOAD_SUFFIX
        marker = ''
        seg = ''

        try:
            resp = req.get_response(self.app, 'HEAD')
            if int(resp.sw_headers['X-Container-Object-Count']) > 0:
                raise BucketNotEmpty()
            # FIXME: This extra HEAD saves unexpected segment deletion
            # but if a complete multipart upload happen while cleanup
            # segment container below, completed object may be missing its
            # segments unfortunately. To be safer, it might be good
            # to handle if the segments can be deleted for each object.
        except NoSuchBucket:
            pass

        try:
            while True:
                # delete all segments
                resp = req.get_response(self.app, 'GET', container,
                                        query={'format': 'json',
                                               'marker': marker})
                segments = json.loads(resp.body)
                for seg in segments:
                    try:
                        req.get_response(self.app, 'DELETE', container,
                                         seg['name'])
                    except NoSuchKey:
                        pass
                    except InternalError:
                        raise ServiceUnavailable()
                if segments:
                    marker = seg['name']
                else:
                    break
            req.get_response(self.app, 'DELETE', container)
        except NoSuchBucket:
            return
        except (BucketNotEmpty, InternalError):
            raise ServiceUnavailable()

    @public
    def HEAD(self, req):
        """
        Handle HEAD Bucket (Get Metadata) request
        """
        resp = req.get_response(self.app)

        return HTTPOk(headers=resp.headers)

    @public
    def GET(self, req):
        """
        Handle GET Bucket (List Objects) request
        """

        max_keys = req.get_validated_param(
            'max-keys', self.conf.max_bucket_listing)
        # TODO: Separate max_bucket_listing and default_bucket_listing
        tag_max_keys = max_keys
        max_keys = min(max_keys, self.conf.max_bucket_listing)

        encoding_type = req.params.get('encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        query = {
            'format': 'json',
            'limit': max_keys + 1,
        }
        if 'marker' in req.params:
            query.update({'marker': req.params['marker']})
        if 'prefix' in req.params:
            query.update({'prefix': req.params['prefix']})
        if 'delimiter' in req.params:
            query.update({'delimiter': req.params['delimiter']})

        # GET Bucket (List Objects) Version 2 parameters
        is_v2 = int(req.params.get('list-type', '1')) == 2
        fetch_owner = False
        if is_v2:
            if 'start-after' in req.params:
                query.update({'marker': req.params['start-after']})
            # continuation-token overrides start-after
            if 'continuation-token' in req.params:
                decoded = b64decode(req.params['continuation-token'])
                query.update({'marker': decoded})
            if 'fetch-owner' in req.params:
                fetch_owner = config_true_value(req.params['fetch-owner'])

        resp = req.get_response(self.app, query=query)

        objects = json.loads(resp.body)

        elem = Element('ListBucketResult')
        SubElement(elem, 'Name').text = req.container_name
        SubElement(elem, 'Prefix').text = req.params.get('prefix')

        # in order to judge that truncated is valid, check whether
        # max_keys + 1 th element exists in swift.
        is_truncated = max_keys > 0 and len(objects) > max_keys
        objects = objects[:max_keys]

        if not is_v2:
            SubElement(elem, 'Marker').text = req.params.get('marker')
            if is_truncated and 'delimiter' in req.params:
                if 'name' in objects[-1]:
                    SubElement(elem, 'NextMarker').text = \
                        objects[-1]['name']
                if 'subdir' in objects[-1]:
                    SubElement(elem, 'NextMarker').text = \
                        objects[-1]['subdir']
        else:
            if is_truncated:
                if 'name' in objects[-1]:
                    SubElement(elem, 'NextContinuationToken').text = \
                        b64encode(objects[-1]['name'])
                if 'subdir' in objects[-1]:
                    SubElement(elem, 'NextContinuationToken').text = \
                        b64encode(objects[-1]['subdir'])
            if 'continuation-token' in req.params:
                SubElement(elem, 'ContinuationToken').text = \
                    req.params['continuation-token']
            if 'start-after' in req.params:
                SubElement(elem, 'StartAfter').text = \
                    req.params['start-after']
            SubElement(elem, 'KeyCount').text = str(len(objects))

        SubElement(elem, 'MaxKeys').text = str(tag_max_keys)

        if 'delimiter' in req.params:
            SubElement(elem, 'Delimiter').text = req.params['delimiter']

        if encoding_type is not None:
            SubElement(elem, 'EncodingType').text = encoding_type

        SubElement(elem, 'IsTruncated').text = \
            'true' if is_truncated else 'false'

        for o in objects:
            if 'subdir' not in o:
                contents = SubElement(elem, 'Contents')
                SubElement(contents, 'Key').text = o['name']
                SubElement(contents, 'LastModified').text = \
                    o['last_modified'][:-3] + 'Z'
                SubElement(contents, 'ETag').text = '"%s"' % o['hash']
                SubElement(contents, 'Size').text = str(o['bytes'])
                if fetch_owner or not is_v2:
                    owner = SubElement(contents, 'Owner')
                    SubElement(owner, 'ID').text = req.user_id
                    SubElement(owner, 'DisplayName').text = req.user_id
                SubElement(contents, 'StorageClass').text = 'STANDARD'

        for o in objects:
            if 'subdir' in o:
                common_prefixes = SubElement(elem, 'CommonPrefixes')
                SubElement(common_prefixes, 'Prefix').text = o['subdir']

        body = tostring(elem, encoding_type=encoding_type)

        return HTTPOk(body=body, content_type='application/xml')

    @public
    def PUT(self, req):
        """
        Handle PUT Bucket request
        """
        xml = req.xml(MAX_PUT_BUCKET_BODY_SIZE)
        if xml:
            # check location
            try:
                elem = fromstring(
                    xml, 'CreateBucketConfiguration', self.logger)
                location = elem.find('./LocationConstraint').text
            except (XMLSyntaxError, DocumentInvalid):
                raise MalformedXML()
            except Exception as e:
                self.logger.error(e)
                raise

            if location != self.conf.location:
                # s3api cannot support multiple regions currently.
                raise InvalidLocationConstraint()

        resp = req.get_response(self.app)

        resp.status = HTTP_OK
        resp.location = '/' + req.container_name

        return resp

    @public
    def DELETE(self, req):
        """
        Handle DELETE Bucket request
        """
        if self.conf.allow_multipart_uploads:
            self._delete_segments_bucket(req)
        resp = req.get_response(self.app)
        return resp

    @public
    def POST(self, req):
        """
        Handle POST Bucket request
        """
        raise S3NotImplemented()
