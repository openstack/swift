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

from urllib.parse import quote

from swift.common import swob
from swift.common.http import HTTP_OK
from swift.common.middleware.versioned_writes.object_versioning import \
    DELETE_MARKER_CONTENT_TYPE
from swift.common.utils import json, public, config_true_value, Timestamp, \
    cap_length
from swift.common.registry import get_swift_info

from swift.common.middleware.s3api.controllers.base import Controller
from swift.common.middleware.s3api.etree import Element, SubElement, \
    tostring, fromstring, XMLSyntaxError, DocumentInvalid
from swift.common.middleware.s3api.s3response import \
    HTTPOk, S3NotImplemented, InvalidArgument, \
    MalformedXML, InvalidLocationConstraint, NoSuchBucket, \
    BucketNotEmpty, VersionedBucketNotEmpty, InternalError, \
    ServiceUnavailable, NoSuchKey
from swift.common.middleware.s3api.utils import MULTIUPLOAD_SUFFIX, S3Timestamp

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
                if resp.sw_headers.get('X-Container-Sysmeta-Versions-Enabled'):
                    raise VersionedBucketNotEmpty()
                else:
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
                        req.get_response(
                            self.app, 'DELETE', container,
                            swob.bytes_to_wsgi(seg['name'].encode('utf8')))
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

    def _parse_request_options(self, req, max_keys):
        encoding_type = req.params.get('encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        # in order to judge that truncated is valid, check whether
        # max_keys + 1 th element exists in swift.
        query = {
            'limit': max_keys + 1,
        }
        if 'prefix' in req.params:
            query['prefix'] = swob.wsgi_to_str(req.params['prefix'])
        if 'delimiter' in req.params:
            query['delimiter'] = swob.wsgi_to_str(req.params['delimiter'])
        fetch_owner = False
        if 'versions' in req.params:
            query['versions'] = swob.wsgi_to_str(req.params['versions'])
            listing_type = 'object-versions'
            version_marker = swob.wsgi_to_str(req.params.get(
                'version-id-marker'))
            if 'key-marker' in req.params:
                query['marker'] = swob.wsgi_to_str(req.params['key-marker'])
                if version_marker is not None:
                    if version_marker != 'null':
                        try:
                            Timestamp(version_marker)
                        except ValueError:
                            raise InvalidArgument(
                                'version-id-marker', version_marker,
                                'Invalid version id specified')
                    query['version_marker'] = version_marker
            elif version_marker is not None:
                err_msg = ('A version-id marker cannot be specified without '
                           'a key marker.')
                raise InvalidArgument('version-id-marker',
                                      version_marker, err_msg)
        elif int(req.params.get('list-type', '1')) == 2:
            listing_type = 'version-2'
            if 'start-after' in req.params:
                query['marker'] = swob.wsgi_to_str(req.params['start-after'])
            # continuation-token overrides start-after
            if 'continuation-token' in req.params:
                decoded = b64decode(
                    req.params['continuation-token']).decode('utf8')
                query['marker'] = decoded
            if 'fetch-owner' in req.params:
                fetch_owner = config_true_value(req.params['fetch-owner'])
        else:
            listing_type = 'version-1'
            if 'marker' in req.params:
                query['marker'] = swob.wsgi_to_str(req.params['marker'])

        return encoding_type, query, listing_type, fetch_owner

    def _build_versions_result(self, req, objects, encoding_type,
                               tag_max_keys, is_truncated):
        elem = Element('ListVersionsResult')
        SubElement(elem, 'Name').text = req.container_name
        prefix = swob.wsgi_to_str(req.params.get('prefix'))
        if prefix and encoding_type == 'url':
            prefix = quote(prefix)
        SubElement(elem, 'Prefix').text = prefix
        key_marker = swob.wsgi_to_str(req.params.get('key-marker'))
        if key_marker and encoding_type == 'url':
            key_marker = quote(key_marker)
        SubElement(elem, 'KeyMarker').text = key_marker
        SubElement(elem, 'VersionIdMarker').text = swob.wsgi_to_str(
            req.params.get('version-id-marker'))
        if is_truncated:
            if 'name' in objects[-1]:
                SubElement(elem, 'NextKeyMarker').text = \
                    objects[-1]['name']
                SubElement(elem, 'NextVersionIdMarker').text = \
                    objects[-1].get('version') or 'null'
            if 'subdir' in objects[-1]:
                SubElement(elem, 'NextKeyMarker').text = \
                    objects[-1]['subdir']
                SubElement(elem, 'NextVersionIdMarker').text = 'null'
        SubElement(elem, 'MaxKeys').text = str(tag_max_keys)
        delimiter = swob.wsgi_to_str(req.params.get('delimiter'))
        if delimiter is not None:
            if encoding_type == 'url':
                delimiter = quote(delimiter)
            SubElement(elem, 'Delimiter').text = delimiter
        if encoding_type == 'url':
            SubElement(elem, 'EncodingType').text = encoding_type
        SubElement(elem, 'IsTruncated').text = \
            'true' if is_truncated else 'false'
        return elem

    def _build_base_listing_element(self, req, encoding_type):
        elem = Element('ListBucketResult')
        SubElement(elem, 'Name').text = req.container_name
        prefix = swob.wsgi_to_str(req.params.get('prefix'))
        if prefix and encoding_type == 'url':
            prefix = quote(prefix)
        SubElement(elem, 'Prefix').text = prefix
        return elem

    def _build_list_bucket_result_type_one(self, req, objects, encoding_type,
                                           tag_max_keys, is_truncated):
        elem = self._build_base_listing_element(req, encoding_type)
        marker = swob.wsgi_to_str(req.params.get('marker'))
        if marker and encoding_type == 'url':
            marker = quote(marker)
        SubElement(elem, 'Marker').text = marker
        if is_truncated and 'delimiter' in req.params:
            if 'name' in objects[-1]:
                name = objects[-1]['name']
            else:
                name = objects[-1]['subdir']
            if encoding_type == 'url':
                name = quote(name.encode('utf-8'))
            SubElement(elem, 'NextMarker').text = name
        # XXX: really? no NextMarker when no delimiter??
        SubElement(elem, 'MaxKeys').text = str(tag_max_keys)
        delimiter = swob.wsgi_to_str(req.params.get('delimiter'))
        if delimiter:
            if encoding_type == 'url':
                delimiter = quote(delimiter)
            SubElement(elem, 'Delimiter').text = delimiter
        if encoding_type == 'url':
            SubElement(elem, 'EncodingType').text = encoding_type
        SubElement(elem, 'IsTruncated').text = \
            'true' if is_truncated else 'false'
        return elem

    def _build_list_bucket_result_type_two(self, req, objects, encoding_type,
                                           tag_max_keys, is_truncated):
        elem = self._build_base_listing_element(req, encoding_type)
        if is_truncated:
            if 'name' in objects[-1]:
                SubElement(elem, 'NextContinuationToken').text = \
                    b64encode(objects[-1]['name'].encode('utf8'))
            if 'subdir' in objects[-1]:
                SubElement(elem, 'NextContinuationToken').text = \
                    b64encode(objects[-1]['subdir'].encode('utf8'))
        if 'continuation-token' in req.params:
            SubElement(elem, 'ContinuationToken').text = \
                swob.wsgi_to_str(req.params['continuation-token'])
        start_after = swob.wsgi_to_str(req.params.get('start-after'))
        if start_after is not None:
            if encoding_type == 'url':
                start_after = quote(start_after)
            SubElement(elem, 'StartAfter').text = start_after
        SubElement(elem, 'KeyCount').text = str(len(objects))
        SubElement(elem, 'MaxKeys').text = str(tag_max_keys)
        delimiter = swob.wsgi_to_str(req.params.get('delimiter'))
        if delimiter:
            if encoding_type == 'url':
                delimiter = quote(delimiter)
            SubElement(elem, 'Delimiter').text = delimiter
        if encoding_type == 'url':
            SubElement(elem, 'EncodingType').text = encoding_type
        SubElement(elem, 'IsTruncated').text = \
            'true' if is_truncated else 'false'
        return elem

    def _add_subdir(self, elem, o, encoding_type):
        common_prefixes = SubElement(elem, 'CommonPrefixes')
        name = o['subdir']
        if encoding_type == 'url':
            name = quote(name.encode('utf-8'))
        SubElement(common_prefixes, 'Prefix').text = name

    def _add_object(self, req, elem, o, encoding_type, listing_type,
                    fetch_owner):
        name = o['name']
        if encoding_type == 'url':
            name = quote(name.encode('utf-8'))

        if listing_type == 'object-versions':
            if o['content_type'] == DELETE_MARKER_CONTENT_TYPE:
                contents = SubElement(elem, 'DeleteMarker')
            else:
                contents = SubElement(elem, 'Version')
            SubElement(contents, 'Key').text = name
            SubElement(contents, 'VersionId').text = o.get(
                'version_id') or 'null'
            if 'object_versioning' in get_swift_info():
                SubElement(contents, 'IsLatest').text = (
                    'true' if o['is_latest'] else 'false')
            else:
                SubElement(contents, 'IsLatest').text = 'true'
        else:
            contents = SubElement(elem, 'Contents')
            SubElement(contents, 'Key').text = name
        SubElement(contents, 'LastModified').text = \
            S3Timestamp.from_isoformat(o['last_modified']).s3xmlformat
        if contents.tag != 'DeleteMarker':
            if 's3_etag' in o:
                # New-enough MUs are already in the right format
                etag = o['s3_etag']
            elif 'slo_etag' in o:
                # SLOs may be in something *close* to the MU format
                etag = '"%s-N"' % o['slo_etag'].strip('"')
            else:
                # Normal objects just use the MD5
                etag = o['hash']
                if len(etag) < 2 or etag[::len(etag) - 1] != '""':
                    # Normal objects just use the MD5
                    etag = '"%s"' % o['hash']
                    # This also catches sufficiently-old SLOs, but we have
                    # no way to identify those from container listings
                # Otherwise, somebody somewhere (proxyfs, maybe?) made this
                # look like an RFC-compliant ETag; we don't need to
                # quote-wrap.
            SubElement(contents, 'ETag').text = etag
            SubElement(contents, 'Size').text = str(o['bytes'])
        if fetch_owner or listing_type != 'version-2':
            owner = SubElement(contents, 'Owner')
            SubElement(owner, 'ID').text = req.user_id
            SubElement(owner, 'DisplayName').text = req.user_id
        if contents.tag != 'DeleteMarker':
            SubElement(contents, 'StorageClass').text = 'STANDARD'

    def _add_objects_to_result(self, req, elem, objects, encoding_type,
                               listing_type, fetch_owner):
        for o in objects:
            if 'subdir' in o:
                self._add_subdir(elem, o, encoding_type)
            else:
                self._add_object(req, elem, o, encoding_type, listing_type,
                                 fetch_owner)

    @public
    def GET(self, req):
        """
        Handle GET Bucket (List Objects) request
        """
        tag_max_keys = req.get_validated_param(
            'max-keys', self.conf.max_bucket_listing)
        # TODO: Separate max_bucket_listing and default_bucket_listing
        max_keys = min(tag_max_keys, self.conf.max_bucket_listing)

        encoding_type, query, listing_type, fetch_owner = \
            self._parse_request_options(req, max_keys)

        resp = req.get_response(self.app, query=query)

        try:
            objects = json.loads(resp.body)
        except (TypeError, ValueError):
            self.logger.error('Got non-JSON response trying to list %s: %r',
                              req.path, cap_length(resp.body, 60))
            raise

        is_truncated = max_keys > 0 and len(objects) > max_keys
        objects = objects[:max_keys]

        if listing_type == 'object-versions':
            func = self._build_versions_result
        elif listing_type == 'version-2':
            func = self._build_list_bucket_result_type_two
        else:
            func = self._build_list_bucket_result_type_one
        elem = func(req, objects, encoding_type, tag_max_keys, is_truncated)
        self._add_objects_to_result(
            req, elem, objects, encoding_type, listing_type, fetch_owner)

        body = tostring(elem)

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

            if location not in (self.conf.location,
                                self.conf.location.lower()):
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
        # NB: object_versioning is responsible for cleaning up its container
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
