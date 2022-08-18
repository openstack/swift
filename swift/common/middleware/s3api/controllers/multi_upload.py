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
"""
Implementation of S3 Multipart Upload.

This module implements S3 Multipart Upload APIs with the Swift SLO feature.
The following explains how S3api uses swift container and objects to store S3
upload information:

-----------------
[bucket]+segments
-----------------

A container to store upload information. [bucket] is the original bucket
where multipart upload is initiated.

-----------------------------
[bucket]+segments/[upload_id]
-----------------------------

An object of the ongoing upload id. The object is empty and used for
checking the target upload status. If the object exists, it means that the
upload is initiated but not either completed or aborted.

-------------------------------------------
[bucket]+segments/[upload_id]/[part_number]
-------------------------------------------

The last suffix is the part number under the upload id. When the client uploads
the parts, they will be stored in the namespace with
[bucket]+segments/[upload_id]/[part_number].

Example listing result in the [bucket]+segments container::

  [bucket]+segments/[upload_id1]  # upload id object for upload_id1
  [bucket]+segments/[upload_id1]/1  # part object for upload_id1
  [bucket]+segments/[upload_id1]/2  # part object for upload_id1
  [bucket]+segments/[upload_id1]/3  # part object for upload_id1
  [bucket]+segments/[upload_id2]  # upload id object for upload_id2
  [bucket]+segments/[upload_id2]/1  # part object for upload_id2
  [bucket]+segments/[upload_id2]/2  # part object for upload_id2
     .
     .

Those part objects are directly used as segments of a Swift
Static Large Object when the multipart upload is completed.

"""

import binascii
import copy
import os
import re
import time

from swift.common import constraints
from swift.common.swob import Range, bytes_to_wsgi, normalize_etag, \
    wsgi_to_str
from swift.common.utils import json, public, reiterate, md5, Timestamp
from swift.common.request_helpers import get_container_update_override_key, \
    get_param

from urllib.parse import quote, urlparse

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation, object_operation, check_container_existence
from swift.common.middleware.s3api.s3response import InvalidArgument, \
    ErrorResponse, MalformedXML, BadDigest, KeyTooLongError, \
    InvalidPart, BucketAlreadyExists, EntityTooSmall, InvalidPartOrder, \
    InvalidRequest, HTTPOk, HTTPNoContent, NoSuchKey, NoSuchUpload, \
    NoSuchBucket, BucketAlreadyOwnedByYou, ServiceUnavailable
from swift.common.middleware.s3api.utils import unique_id, \
    MULTIUPLOAD_SUFFIX, S3Timestamp, sysmeta_header
from swift.common.middleware.s3api.etree import Element, SubElement, \
    fromstring, tostring, XMLSyntaxError, DocumentInvalid
from swift.common.storage_policy import POLICIES

DEFAULT_MAX_PARTS_LISTING = 1000
DEFAULT_MAX_UPLOADS = 1000

MAX_COMPLETE_UPLOAD_BODY_SIZE = 2048 * 1024


def _get_upload_info(req, app, upload_id):
    """
    Make a HEAD request for existing upload object metadata. Tries the upload
    marker first, and then falls back to the manifest object.

    :param req: an S3Request object.
    :param app: the wsgi app.
    :param upload_id: the upload id.
    :returns: a tuple of (S3Response, boolean) where the boolean is True if the
        response is from the upload marker and False otherwise.
    :raises: NoSuchUpload if neither the marker nor the manifest were found.
    """

    container = req.container_name + MULTIUPLOAD_SUFFIX
    obj = '%s/%s' % (req.object_name, upload_id)

    # XXX: if we leave the copy-source header, somewhere later we might
    # drop in a ?version-id=... query string that's utterly inappropriate
    # for the upload marker. Until we get around to fixing that, just pop
    # it off for now...
    copy_source = req.headers.pop('X-Amz-Copy-Source', None)
    try:
        resp = req.get_response(app, 'HEAD', container=container, obj=obj)
        return resp, True
    except NoSuchKey:
        # ensure consistent path and policy are logged despite manifest HEAD
        upload_marker_path = req.environ.get('s3api.backend_path')
        policy_index = req.policy_index
        try:
            resp = req.get_response(app, 'HEAD')
            if resp.sysmeta_headers.get(sysmeta_header(
                    'object', 'upload-id')) == upload_id:
                return resp, False
        except NoSuchKey:
            pass
        finally:
            # Ops often find it more useful for us to log the upload marker
            # path, so put it back
            if upload_marker_path is not None:
                req.environ['s3api.backend_path'] = upload_marker_path
            if policy_index is not None:
                req.policy_index = policy_index
        raise NoSuchUpload(upload_id=upload_id)
    finally:
        # ...making sure to restore any copy-source before returning
        if copy_source is not None:
            req.headers['X-Amz-Copy-Source'] = copy_source


def _make_complete_body(req, s3_etag, yielded_anything):
    result_elem = Element('CompleteMultipartUploadResult')

    # NOTE: boto with sig v4 appends port to HTTP_HOST value at
    # the request header when the port is non default value and it
    # makes req.host_url like as http://localhost:8080:8080/path
    # that obviously invalid. Probably it should be resolved at
    # swift.common.swob though, tentatively we are parsing and
    # reconstructing the correct host_url info here.
    # in detail, https://github.com/boto/boto/pull/3513
    parsed_url = urlparse(req.host_url)
    host_url = '%s://%s' % (parsed_url.scheme, parsed_url.hostname)
    # Why are we doing our own port parsing? Because py3 decided
    # to start raising ValueErrors on access after parsing such
    # an invalid port
    netloc = parsed_url.netloc.split('@')[-1].split(']')[-1]
    if ':' in netloc:
        port = netloc.split(':', 2)[1]
        host_url += ':%s' % port

    SubElement(result_elem, 'Location').text = host_url + req.path
    SubElement(result_elem, 'Bucket').text = req.container_name
    SubElement(result_elem, 'Key').text = wsgi_to_str(req.object_name)
    SubElement(result_elem, 'ETag').text = '"%s"' % s3_etag
    body = tostring(result_elem, xml_declaration=not yielded_anything)
    if yielded_anything:
        return b'\n' + body
    return body


class PartController(Controller):
    """
    Handles the following APIs:

    * Upload Part
    * Upload Part - Copy

    Those APIs are logged as PART operations in the S3 server log.
    """
    @public
    @object_operation
    @check_container_existence
    def PUT(self, req):
        """
        Handles Upload Part and Upload Part Copy.
        """

        if 'uploadId' not in req.params:
            raise InvalidArgument('ResourceType', 'partNumber',
                                  'Unexpected query string parameter')

        part_number = req.validate_part_number()

        upload_id = get_param(req, 'uploadId')
        _get_upload_info(req, self.app, upload_id)

        req.container_name += MULTIUPLOAD_SUFFIX
        req.object_name = '%s/%s/%d' % (req.object_name, upload_id,
                                        part_number)

        req_timestamp = S3Timestamp.now()
        req.headers['X-Timestamp'] = req_timestamp.internal
        source_resp = req.check_copy_source(self.app)
        if 'X-Amz-Copy-Source' in req.headers and \
                'X-Amz-Copy-Source-Range' in req.headers:
            rng = req.headers['X-Amz-Copy-Source-Range']

            header_valid = True
            try:
                rng_obj = Range(rng)
                if len(rng_obj.ranges) != 1:
                    header_valid = False
            except ValueError:
                header_valid = False
            if not header_valid:
                err_msg = ('The x-amz-copy-source-range value must be of the '
                           'form bytes=first-last where first and last are '
                           'the zero-based offsets of the first and last '
                           'bytes to copy')
                raise InvalidArgument('x-amz-source-range', rng, err_msg)

            source_size = int(source_resp.headers['Content-Length'])
            if not rng_obj.ranges_for_length(source_size):
                err_msg = ('Range specified is not valid for source object '
                           'of size: %s' % source_size)
                raise InvalidArgument('x-amz-source-range', rng, err_msg)

            req.headers['Range'] = rng
            del req.headers['X-Amz-Copy-Source-Range']
        if 'X-Amz-Copy-Source' in req.headers:
            # Clear some problematic headers that might be on the source
            req.headers.update({
                sysmeta_header('object', 'etag'): '',
                'X-Object-Sysmeta-Swift3-Etag': '',  # for legacy data
                'X-Object-Sysmeta-Slo-Etag': '',
                'X-Object-Sysmeta-Slo-Size': '',
                get_container_update_override_key('etag'): '',
            })
        resp = req.get_response(self.app)

        if 'X-Amz-Copy-Source' in req.headers:
            resp.append_copy_resp_body(req.controller_name,
                                       req_timestamp.s3xmlformat)

        resp.status = 200
        return resp


class UploadsController(Controller):
    """
    Handles the following APIs:

    * List Multipart Uploads
    * Initiate Multipart Upload

    Those APIs are logged as UPLOADS operations in the S3 server log.
    """
    @public
    @bucket_operation(err_resp=InvalidRequest,
                      err_msg="Key is not expected for the GET method "
                              "?uploads subresource")
    @check_container_existence
    def GET(self, req):
        """
        Handles List Multipart Uploads
        """

        def separate_uploads(uploads, prefix, delimiter):
            """
            separate_uploads will separate uploads into non_delimited_uploads
            (a subset of uploads) and common_prefixes according to the
            specified delimiter. non_delimited_uploads is a list of uploads
            which exclude the delimiter. common_prefixes is a set of prefixes
            prior to the specified delimiter. Note that the prefix in the
            common_prefixes includes the delimiter itself.

            i.e. if '/' delimiter specified and then the uploads is consists of
            ['foo', 'foo/bar'], this function will return (['foo'], ['foo/']).

            :param uploads: A list of uploads dictionary
            :param prefix: A string of prefix reserved on the upload path.
                           (i.e. the delimiter must be searched behind the
                            prefix)
            :param delimiter: A string of delimiter to split the path in each
                              upload

            :return (non_delimited_uploads, common_prefixes)
            """
            non_delimited_uploads = []
            common_prefixes = set()
            for upload in uploads:
                key = upload['key']
                end = key.find(delimiter, len(prefix))
                if end >= 0:
                    common_prefix = key[:end + len(delimiter)]
                    common_prefixes.add(common_prefix)
                else:
                    non_delimited_uploads.append(upload)
            return non_delimited_uploads, sorted(common_prefixes)

        encoding_type = get_param(req, 'encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        keymarker = get_param(req, 'key-marker', '')
        uploadid = get_param(req, 'upload-id-marker', '')
        maxuploads = req.get_validated_param(
            'max-uploads', DEFAULT_MAX_UPLOADS, DEFAULT_MAX_UPLOADS)

        query = {
            'format': 'json',
            'marker': '',
        }

        if uploadid and keymarker:
            query.update({'marker': '%s/%s' % (keymarker, uploadid)})
        elif keymarker:
            query.update({'marker': '%s/~' % (keymarker)})
        if 'prefix' in req.params:
            query.update({'prefix': get_param(req, 'prefix')})

        container = req.container_name + MULTIUPLOAD_SUFFIX
        uploads = []
        prefixes = []

        def object_to_upload(object_info):
            obj, upid = object_info['name'].rsplit('/', 1)
            obj_dict = {'key': obj,
                        'upload_id': upid,
                        'last_modified': object_info['last_modified']}
            return obj_dict

        is_segment = re.compile('.*/[0-9]+$')

        while len(uploads) < maxuploads:
            try:
                resp = req.get_response(self.app, container=container,
                                        query=query)
                objects = json.loads(resp.body)
            except NoSuchBucket:
                # Assume NoSuchBucket as no uploads
                objects = []
            if not objects:
                break

            new_uploads = [object_to_upload(obj) for obj in objects
                           if not is_segment.match(obj.get('name', ''))]
            new_prefixes = []
            if 'delimiter' in req.params:
                prefix = get_param(req, 'prefix', '')
                delimiter = get_param(req, 'delimiter')
                new_uploads, new_prefixes = separate_uploads(
                    new_uploads, prefix, delimiter)
            uploads.extend(new_uploads)
            prefixes.extend(new_prefixes)
            query['marker'] = objects[-1]['name']

        truncated = len(uploads) >= maxuploads
        if len(uploads) > maxuploads:
            uploads = uploads[:maxuploads]

        nextkeymarker = ''
        nextuploadmarker = ''
        if len(uploads) > 1:
            nextuploadmarker = uploads[-1]['upload_id']
            nextkeymarker = uploads[-1]['key']

        result_elem = Element('ListMultipartUploadsResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        SubElement(result_elem, 'KeyMarker').text = keymarker
        SubElement(result_elem, 'UploadIdMarker').text = uploadid
        SubElement(result_elem, 'NextKeyMarker').text = nextkeymarker
        SubElement(result_elem, 'NextUploadIdMarker').text = nextuploadmarker
        if 'delimiter' in req.params:
            SubElement(result_elem, 'Delimiter').text = \
                get_param(req, 'delimiter')
        if 'prefix' in req.params:
            SubElement(result_elem, 'Prefix').text = get_param(req, 'prefix')
        SubElement(result_elem, 'MaxUploads').text = str(maxuploads)
        if encoding_type is not None:
            SubElement(result_elem, 'EncodingType').text = encoding_type
        SubElement(result_elem, 'IsTruncated').text = \
            'true' if truncated else 'false'

        # TODO: don't show uploads which are initiated before this bucket is
        # created.
        for u in uploads:
            upload_elem = SubElement(result_elem, 'Upload')
            name = u['key']
            if encoding_type == 'url':
                name = quote(name)
            SubElement(upload_elem, 'Key').text = name
            SubElement(upload_elem, 'UploadId').text = u['upload_id']
            initiator_elem = SubElement(upload_elem, 'Initiator')
            SubElement(initiator_elem, 'ID').text = req.user_id
            SubElement(initiator_elem, 'DisplayName').text = req.user_id
            owner_elem = SubElement(upload_elem, 'Owner')
            SubElement(owner_elem, 'ID').text = req.user_id
            SubElement(owner_elem, 'DisplayName').text = req.user_id
            SubElement(upload_elem, 'StorageClass').text = 'STANDARD'
            SubElement(upload_elem, 'Initiated').text = \
                S3Timestamp.from_isoformat(u['last_modified']).s3xmlformat

        for p in prefixes:
            elem = SubElement(result_elem, 'CommonPrefixes')
            SubElement(elem, 'Prefix').text = p

        body = tostring(result_elem)

        return HTTPOk(body=body, content_type='application/xml')

    @public
    @object_operation
    @check_container_existence
    def POST(self, req):
        """
        Handles Initiate Multipart Upload.
        """
        if len(req.object_name) > constraints.MAX_OBJECT_NAME_LENGTH:
            # Note that we can still run into trouble where the MPU is just
            # within the limit, which means the segment names will go over
            raise KeyTooLongError()

        # Create a unique S3 upload id from UUID to avoid duplicates.
        upload_id = unique_id()

        seg_container = req.container_name + MULTIUPLOAD_SUFFIX
        content_type = req.headers.get('Content-Type')
        if content_type:
            req.headers[sysmeta_header('object', 'has-content-type')] = 'yes'
            req.headers[
                sysmeta_header('object', 'content-type')] = content_type
        else:
            req.headers[sysmeta_header('object', 'has-content-type')] = 'no'
        req.headers['Content-Type'] = 'application/directory'

        try:
            seg_req = copy.copy(req)
            seg_req.environ = copy.copy(req.environ)
            seg_req.container_name = seg_container
            seg_req.get_container_info(self.app)
        except NoSuchBucket:
            try:
                # multi-upload bucket doesn't exist, create one with
                # same storage policy and acls as the primary bucket
                info = req.get_container_info(self.app)
                policy_name = POLICIES[info['storage_policy']].name
                hdrs = {'X-Storage-Policy': policy_name}
                if info.get('read_acl'):
                    hdrs['X-Container-Read'] = info['read_acl']
                if info.get('write_acl'):
                    hdrs['X-Container-Write'] = info['write_acl']
                seg_req.get_response(self.app, 'PUT', seg_container, '',
                                     headers=hdrs)
            except (BucketAlreadyExists, BucketAlreadyOwnedByYou):
                pass

        obj = '%s/%s' % (req.object_name, upload_id)

        req.headers.pop('Etag', None)
        req.headers.pop('Content-Md5', None)

        req.get_response(self.app, 'PUT', seg_container, obj, body='')

        result_elem = Element('InitiateMultipartUploadResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        SubElement(result_elem, 'Key').text = wsgi_to_str(req.object_name)
        SubElement(result_elem, 'UploadId').text = upload_id

        body = tostring(result_elem)

        return HTTPOk(body=body, content_type='application/xml')


class UploadController(Controller):
    """
    Handles the following APIs:

    * List Parts
    * Abort Multipart Upload
    * Complete Multipart Upload

    Those APIs are logged as UPLOAD operations in the S3 server log.
    """
    @public
    @object_operation
    @check_container_existence
    def GET(self, req):
        """
        Handles List Parts.
        """
        def filter_part_num_marker(o):
            try:
                num = int(os.path.basename(o['name']))
                return num > part_num_marker
            except ValueError:
                return False

        encoding_type = get_param(req, 'encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        upload_id = get_param(req, 'uploadId')
        _get_upload_info(req, self.app, upload_id)

        maxparts = req.get_validated_param(
            'max-parts', DEFAULT_MAX_PARTS_LISTING,
            self.conf.max_parts_listing)
        part_num_marker = req.get_validated_param(
            'part-number-marker', 0)

        object_name = wsgi_to_str(req.object_name)
        query = {
            'format': 'json',
            'prefix': '%s/%s/' % (object_name, upload_id),
            'delimiter': '/',
            'marker': '',
        }

        container = req.container_name + MULTIUPLOAD_SUFFIX
        # Because the parts are out of order in Swift, we list up to the
        # maximum number of parts and then apply the marker and limit options.
        objects = []
        while True:
            resp = req.get_response(self.app, container=container, obj='',
                                    query=query)
            new_objects = json.loads(resp.body)
            if not new_objects:
                break
            objects.extend(new_objects)
            query['marker'] = new_objects[-1]['name']

        last_part = 0

        # If the caller requested a list starting at a specific part number,
        # construct a sub-set of the object list.
        objList = [obj for obj in objects if filter_part_num_marker(obj)]

        # pylint: disable-msg=E1103
        objList.sort(key=lambda o: int(o['name'].split('/')[-1]))

        if len(objList) > maxparts:
            objList = objList[:maxparts]
            truncated = True
        else:
            truncated = False
        # TODO: We have to retrieve object list again when truncated is True
        # and some objects filtered by invalid name because there could be no
        # enough objects for limit defined by maxparts.

        if objList:
            o = objList[-1]
            last_part = os.path.basename(o['name'])

        result_elem = Element('ListPartsResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        if encoding_type == 'url':
            object_name = quote(object_name)
        SubElement(result_elem, 'Key').text = object_name
        SubElement(result_elem, 'UploadId').text = upload_id

        initiator_elem = SubElement(result_elem, 'Initiator')
        SubElement(initiator_elem, 'ID').text = req.user_id
        SubElement(initiator_elem, 'DisplayName').text = req.user_id
        owner_elem = SubElement(result_elem, 'Owner')
        SubElement(owner_elem, 'ID').text = req.user_id
        SubElement(owner_elem, 'DisplayName').text = req.user_id

        SubElement(result_elem, 'StorageClass').text = 'STANDARD'
        SubElement(result_elem, 'PartNumberMarker').text = str(part_num_marker)
        SubElement(result_elem, 'NextPartNumberMarker').text = str(last_part)
        SubElement(result_elem, 'MaxParts').text = str(maxparts)
        if 'encoding-type' in req.params:
            SubElement(result_elem, 'EncodingType').text = \
                get_param(req, 'encoding-type')
        SubElement(result_elem, 'IsTruncated').text = \
            'true' if truncated else 'false'

        for i in objList:
            part_elem = SubElement(result_elem, 'Part')
            SubElement(part_elem, 'PartNumber').text = i['name'].split('/')[-1]
            SubElement(part_elem, 'LastModified').text = \
                S3Timestamp.from_isoformat(i['last_modified']).s3xmlformat
            SubElement(part_elem, 'ETag').text = '"%s"' % i['hash']
            SubElement(part_elem, 'Size').text = str(i['bytes'])

        body = tostring(result_elem)

        return HTTPOk(body=body, content_type='application/xml')

    @public
    @object_operation
    @check_container_existence
    def DELETE(self, req):
        """
        Handles Abort Multipart Upload.
        """
        upload_id = get_param(req, 'uploadId')
        _get_upload_info(req, self.app, upload_id)

        # First check to see if this multi-part upload was already
        # completed.  Look in the primary container, if the object exists,
        # then it was completed and we return an error here.
        container = req.container_name + MULTIUPLOAD_SUFFIX
        obj = '%s/%s' % (req.object_name, upload_id)
        req.get_response(self.app, container=container, obj=obj)

        # The completed object was not found so this
        # must be a multipart upload abort.
        # We must delete any uploaded segments for this UploadID and then
        # delete the object in the main container as well
        object_name = wsgi_to_str(req.object_name)
        query = {
            'format': 'json',
            'prefix': '%s/%s/' % (object_name, upload_id),
            'delimiter': '/',
        }

        resp = req.get_response(self.app, 'GET', container, '', query=query)

        #  Iterate over the segment objects and delete them individually
        objects = json.loads(resp.body)
        while objects:
            for o in objects:
                container = req.container_name + MULTIUPLOAD_SUFFIX
                obj = bytes_to_wsgi(o['name'].encode('utf-8'))
                req.get_response(self.app, container=container, obj=obj)
            query['marker'] = objects[-1]['name']
            resp = req.get_response(self.app, 'GET', container, '',
                                    query=query)
            objects = json.loads(resp.body)

        return HTTPNoContent()

    @public
    @object_operation
    @check_container_existence
    def POST(self, req):
        """
        Handles Complete Multipart Upload.
        """
        upload_id = get_param(req, 'uploadId')
        resp, is_marker = _get_upload_info(req, self.app, upload_id)
        if (is_marker and
                resp.sw_headers.get('X-Backend-Timestamp') >= Timestamp.now()):
            # Somehow the marker was created in the future w.r.t. this thread's
            # clock. The manifest PUT may succeed but the subsequent marker
            # DELETE will fail, so don't attempt either.
            raise ServiceUnavailable

        headers = {'Accept': 'application/json',
                   sysmeta_header('object', 'upload-id'): upload_id}
        for key, val in resp.headers.items():
            _key = key.lower()
            if _key.startswith('x-amz-meta-'):
                headers['x-object-meta-' + _key[11:]] = val
            elif _key in ('content-encoding', 'content-language',
                          'content-disposition', 'expires', 'cache-control'):
                headers[key] = val

        hct_header = sysmeta_header('object', 'has-content-type')
        if resp.sysmeta_headers.get(hct_header) == 'yes':
            content_type = resp.sysmeta_headers.get(
                sysmeta_header('object', 'content-type'))
        elif hct_header in resp.sysmeta_headers:
            # has-content-type is present but false, so no content type was
            # set on initial upload. In that case, we won't set one on our
            # PUT request. Swift will end up guessing one based on the
            # object name.
            content_type = None
        else:
            content_type = resp.headers.get('Content-Type')

        if content_type:
            headers['Content-Type'] = content_type

        container = req.container_name + MULTIUPLOAD_SUFFIX
        s3_etag_hasher = md5(usedforsecurity=False)
        manifest = []
        previous_number = 0
        try:
            xml = req.xml(MAX_COMPLETE_UPLOAD_BODY_SIZE)
            if not xml:
                raise InvalidRequest(msg='You must specify at least one part')
            if 'content-md5' in req.headers:
                # If an MD5 was provided, we need to verify it.
                # Note that S3Request already took care of translating to ETag
                if req.headers['etag'] != md5(
                        xml, usedforsecurity=False).hexdigest():
                    raise BadDigest(content_md5=req.headers['content-md5'])
                # We're only interested in the body here, in the
                # multipart-upload controller -- *don't* let it get
                # plumbed down to the object-server
                del req.headers['etag']

            complete_elem = fromstring(
                xml, 'CompleteMultipartUpload', self.logger)
            for part_elem in complete_elem.iterchildren('Part'):
                part_number = int(part_elem.find('./PartNumber').text)

                if part_number <= previous_number:
                    raise InvalidPartOrder(upload_id=upload_id)
                previous_number = part_number

                etag = normalize_etag(part_elem.find('./ETag').text)
                if etag is None:
                    raise InvalidPart(upload_id=upload_id,
                                      part_number=part_number,
                                      e_tag=etag)
                if len(etag) != 32 or any(c not in '0123456789abcdef'
                                          for c in etag):
                    raise InvalidPart(upload_id=upload_id,
                                      part_number=part_number,
                                      e_tag=etag)
                manifest.append({
                    'path': '/%s/%s/%s/%d' % (
                        wsgi_to_str(container), wsgi_to_str(req.object_name),
                        upload_id, part_number),
                    'etag': etag})
                s3_etag_hasher.update(binascii.a2b_hex(etag))
        except (XMLSyntaxError, DocumentInvalid):
            # NB: our schema definitions catch uploads with no parts here
            raise MalformedXML()
        except ErrorResponse:
            raise
        except Exception as e:
            self.logger.error(e)
            raise

        s3_etag = '%s-%d' % (s3_etag_hasher.hexdigest(), len(manifest))
        s3_etag_header = sysmeta_header('object', 'etag')
        if resp.sysmeta_headers.get(s3_etag_header) == s3_etag:
            # This header should only already be present if the upload marker
            # has been cleaned up and the current target uses the same
            # upload-id; assuming the segments to use haven't changed, the work
            # is already done
            return HTTPOk(body=_make_complete_body(req, s3_etag, False),
                          content_type='application/xml')
        headers[s3_etag_header] = s3_etag
        # Leave base header value blank; SLO will populate
        c_etag = '; s3_etag=%s' % s3_etag
        headers[get_container_update_override_key('etag')] = c_etag

        too_small_message = ('s3api requires that each segment be at least '
                             '%d bytes' % self.conf.min_segment_size)

        def size_checker(manifest):
            # Check the size of each segment except the last and make sure
            # they are all more than the minimum upload chunk size.
            # Note that we need to use the *internal* keys, since we're
            # looking at the manifest that's about to be written.
            return [
                (item['name'], too_small_message)
                for item in manifest[:-1]
                if item and item['bytes'] < self.conf.min_segment_size]

        req.environ['swift.callback.slo_manifest_hook'] = size_checker
        start_time = time.time()

        def response_iter():
            # NB: XML requires that the XML declaration, if present, be at the
            # very start of the document. Clients *will* call us out on not
            # being valid XML if we pass through whitespace before it.
            # Track whether we've sent anything yet so we can yield out that
            # declaration *first*
            yielded_anything = False

            try:
                try:
                    # TODO: add support for versioning
                    put_resp = req.get_response(
                        self.app, 'PUT', body=json.dumps(manifest),
                        query={'multipart-manifest': 'put',
                               'heartbeat': 'on'},
                        headers=headers)
                    if put_resp.status_int == 202:
                        body = []
                        put_resp.fix_conditional_response()
                        for chunk in put_resp.response_iter:
                            if not chunk.strip():
                                if time.time() - start_time < 10:
                                    # Include some grace period to keep
                                    # ceph-s3tests happy
                                    continue
                                if not yielded_anything:
                                    yield (b'<?xml version="1.0" '
                                           b'encoding="UTF-8"?>\n')
                                yielded_anything = True
                                yield chunk
                                continue
                            body.append(chunk)
                        body = json.loads(b''.join(body))
                        if body['Response Status'] != '201 Created':
                            for seg, err in body['Errors']:
                                if err == too_small_message:
                                    raise EntityTooSmall()
                                elif err in ('Etag Mismatch', '404 Not Found'):
                                    raise InvalidPart(upload_id=upload_id)
                            raise InvalidRequest(
                                status=body['Response Status'],
                                msg='\n'.join(': '.join(err)
                                              for err in body['Errors']))
                except InvalidRequest as err_resp:
                    msg = err_resp._msg
                    if too_small_message in msg:
                        raise EntityTooSmall(msg)
                    elif ', Etag Mismatch' in msg:
                        raise InvalidPart(upload_id=upload_id)
                    elif ', 404 Not Found' in msg:
                        raise InvalidPart(upload_id=upload_id)
                    else:
                        raise

                # clean up the multipart-upload record
                obj = '%s/%s' % (req.object_name, upload_id)
                try:
                    req.get_response(self.app, 'DELETE', container, obj)
                except NoSuchKey:
                    # The important thing is that we wrote out a tombstone to
                    # make sure the marker got cleaned up. If it's already
                    # gone (e.g., because of concurrent completes or a retried
                    # complete), so much the better.
                    pass

                yield _make_complete_body(req, s3_etag, yielded_anything)
            except ErrorResponse as err_resp:
                if yielded_anything:
                    err_resp.xml_declaration = False
                    yield b'\n'
                else:
                    # Oh good, we can still change HTTP status code, too!
                    resp.status = err_resp.status
                for chunk in err_resp({}, lambda *a: None):
                    yield chunk

        resp = HTTPOk()  # assume we're good for now... but see above!
        resp.app_iter = reiterate(response_iter())
        resp.content_type = "application/xml"

        return resp
