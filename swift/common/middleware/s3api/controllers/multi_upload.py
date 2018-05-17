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

A object of the ongoing upload id. The object is empty and used for
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

import os
import re
import sys

from swift.common.swob import Range
from swift.common.utils import json, public
from swift.common.db import utf8encode

from six.moves.urllib.parse import urlparse  # pylint: disable=F0401

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation, object_operation, check_container_existence
from swift.common.middleware.s3api.s3response import InvalidArgument, \
    ErrorResponse, MalformedXML, \
    InvalidPart, BucketAlreadyExists, EntityTooSmall, InvalidPartOrder, \
    InvalidRequest, HTTPOk, HTTPNoContent, NoSuchKey, NoSuchUpload, \
    NoSuchBucket
from swift.common.middleware.s3api.exception import BadSwiftRequest
from swift.common.middleware.s3api.utils import unique_id, \
    MULTIUPLOAD_SUFFIX, S3Timestamp, sysmeta_header
from swift.common.middleware.s3api.etree import Element, SubElement, \
    fromstring, tostring, XMLSyntaxError, DocumentInvalid

DEFAULT_MAX_PARTS_LISTING = 1000
DEFAULT_MAX_UPLOADS = 1000

MAX_COMPLETE_UPLOAD_BODY_SIZE = 2048 * 1024


def _get_upload_info(req, app, upload_id):

    container = req.container_name + MULTIUPLOAD_SUFFIX
    obj = '%s/%s' % (req.object_name, upload_id)

    try:
        return req.get_response(app, 'HEAD', container=container, obj=obj)
    except NoSuchKey:
        raise NoSuchUpload(upload_id=upload_id)


def _check_upload_info(req, app, upload_id):

    _get_upload_info(req, app, upload_id)


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

        try:
            part_number = int(req.params['partNumber'])
            if part_number < 1 or self.conf.max_upload_part_num < part_number:
                raise Exception()
        except Exception:
            err_msg = 'Part number must be an integer between 1 and %d,' \
                      ' inclusive' % self.conf.max_upload_part_num
            raise InvalidArgument('partNumber', req.params['partNumber'],
                                  err_msg)

        upload_id = req.params['uploadId']
        _check_upload_info(req, self.app, upload_id)

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
            (prefix, delimiter) = \
                utf8encode(prefix, delimiter)
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

        encoding_type = req.params.get('encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        keymarker = req.params.get('key-marker', '')
        uploadid = req.params.get('upload-id-marker', '')
        maxuploads = req.get_validated_param(
            'max-uploads', DEFAULT_MAX_UPLOADS, DEFAULT_MAX_UPLOADS)

        query = {
            'format': 'json',
            'limit': maxuploads + 1,
        }

        if uploadid and keymarker:
            query.update({'marker': '%s/%s' % (keymarker, uploadid)})
        elif keymarker:
            query.update({'marker': '%s/~' % (keymarker)})
        if 'prefix' in req.params:
            query.update({'prefix': req.params['prefix']})

        container = req.container_name + MULTIUPLOAD_SUFFIX
        try:
            resp = req.get_response(self.app, container=container, query=query)
            objects = json.loads(resp.body)
        except NoSuchBucket:
            # Assume NoSuchBucket as no uploads
            objects = []

        def object_to_upload(object_info):
            obj, upid = object_info['name'].rsplit('/', 1)
            obj_dict = {'key': obj,
                        'upload_id': upid,
                        'last_modified': object_info['last_modified']}
            return obj_dict

        # uploads is a list consists of dict, {key, upload_id, last_modified}
        # Note that pattern matcher will drop whole segments objects like as
        # object_name/upload_id/1.
        pattern = re.compile('/[0-9]+$')
        uploads = [object_to_upload(obj) for obj in objects if
                   pattern.search(obj.get('name', '')) is None]

        prefixes = []
        if 'delimiter' in req.params:
            prefix = req.params.get('prefix', '')
            delimiter = req.params['delimiter']
            uploads, prefixes = \
                separate_uploads(uploads, prefix, delimiter)

        if len(uploads) > maxuploads:
            uploads = uploads[:maxuploads]
            truncated = True
        else:
            truncated = False

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
                req.params['delimiter']
        if 'prefix' in req.params:
            SubElement(result_elem, 'Prefix').text = req.params['prefix']
        SubElement(result_elem, 'MaxUploads').text = str(maxuploads)
        if encoding_type is not None:
            SubElement(result_elem, 'EncodingType').text = encoding_type
        SubElement(result_elem, 'IsTruncated').text = \
            'true' if truncated else 'false'

        # TODO: don't show uploads which are initiated before this bucket is
        # created.
        for u in uploads:
            upload_elem = SubElement(result_elem, 'Upload')
            SubElement(upload_elem, 'Key').text = u['key']
            SubElement(upload_elem, 'UploadId').text = u['upload_id']
            initiator_elem = SubElement(upload_elem, 'Initiator')
            SubElement(initiator_elem, 'ID').text = req.user_id
            SubElement(initiator_elem, 'DisplayName').text = req.user_id
            owner_elem = SubElement(upload_elem, 'Owner')
            SubElement(owner_elem, 'ID').text = req.user_id
            SubElement(owner_elem, 'DisplayName').text = req.user_id
            SubElement(upload_elem, 'StorageClass').text = 'STANDARD'
            SubElement(upload_elem, 'Initiated').text = \
                u['last_modified'][:-3] + 'Z'

        for p in prefixes:
            elem = SubElement(result_elem, 'CommonPrefixes')
            SubElement(elem, 'Prefix').text = p

        body = tostring(result_elem, encoding_type=encoding_type)

        return HTTPOk(body=body, content_type='application/xml')

    @public
    @object_operation
    @check_container_existence
    def POST(self, req):
        """
        Handles Initiate Multipart Upload.
        """

        # Create a unique S3 upload id from UUID to avoid duplicates.
        upload_id = unique_id()

        container = req.container_name + MULTIUPLOAD_SUFFIX
        content_type = req.headers.get('Content-Type')
        if content_type:
            req.headers[sysmeta_header('object', 'has-content-type')] = 'yes'
            req.headers[
                sysmeta_header('object', 'content-type')] = content_type
        else:
            req.headers[sysmeta_header('object', 'has-content-type')] = 'no'
        req.headers['Content-Type'] = 'application/directory'

        try:
            req.get_response(self.app, 'PUT', container, '')
        except BucketAlreadyExists:
            pass

        obj = '%s/%s' % (req.object_name, upload_id)

        req.headers.pop('Etag', None)
        req.headers.pop('Content-Md5', None)

        req.get_response(self.app, 'PUT', container, obj, body='')

        result_elem = Element('InitiateMultipartUploadResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        SubElement(result_elem, 'Key').text = req.object_name
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

        encoding_type = req.params.get('encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        upload_id = req.params['uploadId']
        _check_upload_info(req, self.app, upload_id)

        maxparts = req.get_validated_param(
            'max-parts', DEFAULT_MAX_PARTS_LISTING,
            self.conf.max_parts_listing)
        part_num_marker = req.get_validated_param(
            'part-number-marker', 0)

        query = {
            'format': 'json',
            'limit': maxparts + 1,
            'prefix': '%s/%s/' % (req.object_name, upload_id),
            'delimiter': '/'
        }

        container = req.container_name + MULTIUPLOAD_SUFFIX
        resp = req.get_response(self.app, container=container, obj='',
                                query=query)
        objects = json.loads(resp.body)

        last_part = 0

        # If the caller requested a list starting at a specific part number,
        # construct a sub-set of the object list.
        objList = filter(filter_part_num_marker, objects)

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
        SubElement(result_elem, 'Key').text = req.object_name
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
                req.params['encoding-type']
        SubElement(result_elem, 'IsTruncated').text = \
            'true' if truncated else 'false'

        for i in objList:
            part_elem = SubElement(result_elem, 'Part')
            SubElement(part_elem, 'PartNumber').text = i['name'].split('/')[-1]
            SubElement(part_elem, 'LastModified').text = \
                i['last_modified'][:-3] + 'Z'
            SubElement(part_elem, 'ETag').text = '"%s"' % i['hash']
            SubElement(part_elem, 'Size').text = str(i['bytes'])

        body = tostring(result_elem, encoding_type=encoding_type)

        return HTTPOk(body=body, content_type='application/xml')

    @public
    @object_operation
    @check_container_existence
    def DELETE(self, req):
        """
        Handles Abort Multipart Upload.
        """
        upload_id = req.params['uploadId']
        _check_upload_info(req, self.app, upload_id)

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
        query = {
            'format': 'json',
            'prefix': '%s/%s/' % (req.object_name, upload_id),
            'delimiter': '/',
        }

        resp = req.get_response(self.app, 'GET', container, '', query=query)

        #  Iterate over the segment objects and delete them individually
        objects = json.loads(resp.body)
        for o in objects:
            container = req.container_name + MULTIUPLOAD_SUFFIX
            req.get_response(self.app, container=container, obj=o['name'])

        return HTTPNoContent()

    @public
    @object_operation
    @check_container_existence
    def POST(self, req):
        """
        Handles Complete Multipart Upload.
        """
        upload_id = req.params['uploadId']
        resp = _get_upload_info(req, self.app, upload_id)
        headers = {}
        for key, val in resp.headers.iteritems():
            _key = key.lower()
            if _key.startswith('x-amz-meta-'):
                headers['x-object-meta-' + _key[11:]] = val

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

        # Query for the objects in the segments area to make sure it completed
        query = {
            'format': 'json',
            'prefix': '%s/%s/' % (req.object_name, upload_id),
            'delimiter': '/'
        }

        container = req.container_name + MULTIUPLOAD_SUFFIX
        resp = req.get_response(self.app, 'GET', container, '', query=query)
        objinfo = json.loads(resp.body)
        objtable = dict((o['name'],
                         {'path': '/'.join(['', container, o['name']]),
                          'etag': o['hash'],
                          'size_bytes': o['bytes']}) for o in objinfo)

        manifest = []
        previous_number = 0
        try:
            xml = req.xml(MAX_COMPLETE_UPLOAD_BODY_SIZE)
            if not xml:
                raise InvalidRequest(msg='You must specify at least one part')

            complete_elem = fromstring(
                xml, 'CompleteMultipartUpload', self.logger)
            for part_elem in complete_elem.iterchildren('Part'):
                part_number = int(part_elem.find('./PartNumber').text)

                if part_number <= previous_number:
                    raise InvalidPartOrder(upload_id=upload_id)
                previous_number = part_number

                etag = part_elem.find('./ETag').text
                if len(etag) >= 2 and etag[0] == '"' and etag[-1] == '"':
                    # strip double quotes
                    etag = etag[1:-1]

                info = objtable.get("%s/%s/%s" % (req.object_name, upload_id,
                                                  part_number))
                if info is None or info['etag'] != etag:
                    raise InvalidPart(upload_id=upload_id,
                                      part_number=part_number)

                info['size_bytes'] = int(info['size_bytes'])
                manifest.append(info)
        except (XMLSyntaxError, DocumentInvalid):
            raise MalformedXML()
        except ErrorResponse:
            raise
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.error(e)
            raise exc_type, exc_value, exc_traceback

        # Check the size of each segment except the last and make sure they are
        # all more than the minimum upload chunk size
        for info in manifest[:-1]:
            if info['size_bytes'] < self.conf.min_segment_size:
                raise EntityTooSmall()

        try:
            # TODO: add support for versioning
            if manifest:
                resp = req.get_response(self.app, 'PUT',
                                        body=json.dumps(manifest),
                                        query={'multipart-manifest': 'put'},
                                        headers=headers)
            else:
                # the upload must have consisted of a single zero-length part
                # just write it directly
                resp = req.get_response(self.app, 'PUT', body='',
                                        headers=headers)
        except BadSwiftRequest as e:
            msg = str(e)
            expected_msg = 'too small; each segment must be at least 1 byte'
            if expected_msg in msg:
                # FIXME: AWS S3 allows a smaller object than 5 MB if there is
                # only one part.  Use a COPY request to copy the part object
                # from the segments container instead.
                raise EntityTooSmall(msg)
            else:
                raise

        # clean up the multipart-upload record
        obj = '%s/%s' % (req.object_name, upload_id)
        try:
            req.get_response(self.app, 'DELETE', container, obj)
        except NoSuchKey:
            pass  # We know that this existed long enough for us to HEAD

        result_elem = Element('CompleteMultipartUploadResult')

        # NOTE: boto with sig v4 appends port to HTTP_HOST value at the
        # request header when the port is non default value and it makes
        # req.host_url like as http://localhost:8080:8080/path
        # that obviously invalid. Probably it should be resolved at
        # swift.common.swob though, tentatively we are parsing and
        # reconstructing the correct host_url info here.
        # in detail, https://github.com/boto/boto/pull/3513
        parsed_url = urlparse(req.host_url)
        host_url = '%s://%s' % (parsed_url.scheme, parsed_url.hostname)
        if parsed_url.port:
            host_url += ':%s' % parsed_url.port

        SubElement(result_elem, 'Location').text = host_url + req.path
        SubElement(result_elem, 'Bucket').text = req.container_name
        SubElement(result_elem, 'Key').text = req.object_name
        SubElement(result_elem, 'ETag').text = resp.etag

        resp.body = tostring(result_elem)
        resp.status = 200
        resp.content_type = "application/xml"

        return resp
