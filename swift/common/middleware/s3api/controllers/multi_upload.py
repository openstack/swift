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
import time

from swift.common import constraints
from swift.common.middleware.mpu import MPUSloCallbackHandler, \
    parse_external_upload_id
from swift.common.swob import Range, normalize_etag, wsgi_to_str
from swift.common.utils import json, public, reiterate, md5
from swift.common.request_helpers import get_container_update_override_key, \
    get_param

from urllib.parse import quote, urlparse

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation, object_operation, check_container_existence
from swift.common.middleware.s3api.s3response import InvalidArgument, \
    ErrorResponse, MalformedXML, BadDigest, KeyTooLongError, \
    InvalidPart, EntityTooSmall, InvalidPartOrder, InvalidRequest, HTTPOk, \
    NoSuchUpload, NoSuchBucket, PreconditionFailed, S3NotImplemented
from swift.common.middleware.s3api.utils import S3Timestamp, sysmeta_header
from swift.common.middleware.s3api.etree import Element, SubElement, \
    fromstring, tostring, XMLSyntaxError, DocumentInvalid

DEFAULT_MAX_PARTS_LISTING = 1000
DEFAULT_MAX_UPLOADS = 1000

MAX_COMPLETE_UPLOAD_BODY_SIZE = 2048 * 1024


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


def get_valid_upload_id(req):
    upload_id_str = get_param(req, 'uploadId')
    try:
        # Check validity of the given upload-id
        # NB: we cannot check that the upload id is valid *for the path*
        # because we don't yet have the complete backend path with the account;
        # mpu middleware will check the upload-id is valid for the path.
        parse_external_upload_id(upload_id_str)
        return upload_id_str
    except ValueError:
        raise NoSuchUpload(upload_id=upload_id_str)


class PartController(Controller):
    """
    Handles the following APIs:

    * Upload Part
    * Upload Part - Copy

    Those APIs are logged as PART operations in the S3 server log.
    """

    def _check_copy_source_range(self, req):
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
        upload_id = get_valid_upload_id(req)
        self._check_copy_source_range(req)
        query = {'upload-id': upload_id,
                 'part-number': part_number}
        resp = req.get_response(self.app, query=query)

        if 'X-Amz-Copy-Source' in req.headers:
            ts = S3Timestamp.from_http_date(resp.headers['Last-Modified'])
            resp.append_copy_resp_body(req.controller_name, ts.s3xmlformat)

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

        # TODO: shift this into mpu middleware?
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

        query = {
            'uploads': 'true',
            'format': 'json',
        }
        key_marker = get_param(req, 'key-marker', '')
        upload_id_marker = get_param(req, 'upload-id-marker', '')
        if key_marker:
            query['marker'] = key_marker
            if upload_id_marker:
                query['upload-id-marker'] = upload_id_marker
        if 'prefix' in req.params:
            query['prefix'] = get_param(req, 'prefix')

        uploads = []
        prefixes = []
        max_uploads = req.get_validated_param(
            'max-uploads', DEFAULT_MAX_UPLOADS, DEFAULT_MAX_UPLOADS)
        while len(uploads) < max_uploads:
            try:
                resp = req.get_response(self.app, query=query)
                objects = json.loads(resp.body)
            except NoSuchBucket:
                # Assume NoSuchBucket as no uploads
                objects = []
            if not objects:
                break

            new_uploads = [{'key': obj['name'],
                            'upload_id': obj['upload_id'],
                            'last_modified': obj['last_modified']}
                           for obj in objects]
            new_prefixes = []
            if 'delimiter' in req.params:
                prefix = get_param(req, 'prefix', '')
                delimiter = get_param(req, 'delimiter')
                new_uploads, new_prefixes = separate_uploads(
                    new_uploads, prefix, delimiter)
            uploads.extend(new_uploads)
            prefixes.extend(new_prefixes)
            query['marker'] = objects[-1]['name']
            query['upload-id-marker'] = objects[-1]['upload_id']

        truncated = len(uploads) >= max_uploads
        if len(uploads) > max_uploads:
            uploads = uploads[:max_uploads]

        next_key_marker = ''
        next_upload_id_marker = ''
        if len(uploads) > 1:
            next_upload_id_marker = uploads[-1]['upload_id']
            next_key_marker = uploads[-1]['key']

        result_elem = Element('ListMultipartUploadsResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        SubElement(result_elem, 'KeyMarker').text = key_marker
        SubElement(result_elem, 'UploadIdMarker').text = upload_id_marker
        SubElement(result_elem, 'NextKeyMarker').text = next_key_marker
        SubElement(result_elem,
                   'NextUploadIdMarker').text = next_upload_id_marker
        if 'delimiter' in req.params:
            SubElement(result_elem, 'Delimiter').text = \
                get_param(req, 'delimiter')
        if 'prefix' in req.params:
            SubElement(result_elem, 'Prefix').text = get_param(req, 'prefix')
        SubElement(result_elem, 'MaxUploads').text = str(max_uploads)
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

        query = {'uploads': 'true'}
        resp = req.get_response(self.app, query=query)
        upload_id = resp.sw_headers.get('X-Upload-Id')

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
        encoding_type = get_param(req, 'encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        upload_id = get_valid_upload_id(req)

        maxparts = req.get_validated_param(
            'max-parts', DEFAULT_MAX_PARTS_LISTING,
            self.conf.max_parts_listing)
        part_num_marker = req.get_validated_param(
            'part-number-marker', 0)

        object_name = wsgi_to_str(req.object_name)
        query = {
            'format': 'json',
            'upload-id': upload_id,
        }
        if part_num_marker:
            query['part-number-marker'] = str(part_num_marker)

        resp = req.get_response(self.app, query=query)
        objects = json.loads(resp.body)
        last_part = 0

        if len(objects) > maxparts:
            objects = objects[:maxparts]
            truncated = True
        else:
            truncated = False
        # TODO: We have to retrieve object list again when truncated is True
        # and some objects filtered by invalid name because there could be no
        # enough objects for limit defined by maxparts.

        if objects:
            o = objects[-1]
            last_part = int(o['name'].split('/')[-1])

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

        for i in objects:
            part_elem = SubElement(result_elem, 'Part')
            part_num = str(int(i['name'].split('/')[-1]))
            SubElement(part_elem, 'PartNumber').text = part_num
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
        upload_id = get_valid_upload_id(req)
        query = {'upload-id': upload_id}
        return req.get_response(self.app, query=query)

    def _parse_user_manifest(self, req, upload_id):
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
                md5_body = md5(xml, usedforsecurity=False).hexdigest()
                if req.headers['etag'] != md5_body:
                    raise BadDigest(
                        expected_digest=req.headers['content-md5'])
                # We're only interested in the body here, in the
                # multipart-upload controller -- *don't* let it get
                # plumbed down to the object-server
                del req.headers['etag']

            complete_elem = fromstring(
                xml, 'CompleteMultipartUpload', self.logger)
            for part_elem in complete_elem.iterchildren('Part'):
                part_number = int(part_elem.find('./PartNumber').text)

                if part_number <= previous_number:
                    # duplicates check in mpu middleware
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
                    'part_number': part_number,
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
        return manifest, s3_etag

    @public
    @object_operation
    @check_container_existence
    def POST(self, req):
        """
        Handles Complete Multipart Upload.
        """
        upload_id = get_valid_upload_id(req)
        # Check for conditional requests before getting upload info so the
        # headers can't bleed into the HEAD
        if req.headers.get('If-None-Match', '*') != '*' or any(
                h in req.headers for h in (
                    'If-Match', 'If-Modified-Since', 'If-Unmodified-Since')):
            raise S3NotImplemented(
                'Conditional uploads are not supported.')
        manifest, s3_etag = self._parse_user_manifest(req, upload_id)

        # TODO: do we want to have independent size checking for s3api vs
        #   native mpu? if so, we'll need mpu to pass on the callback from slo
        # too_small_message = ('s3api requires that each segment be at least '
        #                      '%d bytes' % self.conf.min_segment_size)
        #
        # def size_checker(manifest):
        #     # Check the size of each segment except the last and make sure
        #     # they are all more than the minimum upload chunk size.
        #     # Note that we need to use the *internal* keys, since we're
        #     # looking at the manifest that's about to be written.
        #     return [
        #         (item['name'], too_small_message)
        #         for item in manifest[:-1]
        #         if item and item['bytes'] < self.conf.min_segment_size]
        #
        # req.environ['swift.callback.slo_manifest_hook'] = size_checker
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
                    post_resp = req.get_response(
                        self.app, 'POST', body=json.dumps(manifest),
                        query={'upload-id': upload_id})
                    if post_resp.status_int == 202:
                        body = []
                        post_resp.fix_conditional_response()
                        for chunk in post_resp.response_iter:
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
                        if body['Response Status'] == \
                                '412 Precondition Failed':
                            raise PreconditionFailed
                        elif body['Response Status'] not in ('201 Created',
                                                             '202 Accepted'):
                            # swift will return 202 if the backends respond 409
                            # to the mpu PUT e.g. if the mpu PUT timestamp is
                            # older than on-disk state
                            for seg, err in body['Errors']:
                                if MPUSloCallbackHandler.ERROR_MSG in err:
                                    raise EntityTooSmall()
                                elif err in ('Etag Mismatch', '404 Not Found'):
                                    raise InvalidPart(upload_id=upload_id)
                            raise InvalidRequest(
                                status=body['Response Status'],
                                msg='\n'.join(': '.join(err)
                                              for err in body['Errors']))
                except InvalidRequest as err_resp:
                    msg = err_resp._msg
                    if MPUSloCallbackHandler.ERROR_MSG in msg:
                        raise EntityTooSmall(msg)
                    elif ', Etag Mismatch' in msg:
                        raise InvalidPart(upload_id=upload_id)
                    elif ', 404 Not Found' in msg:
                        raise InvalidPart(upload_id=upload_id)
                    else:
                        raise

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
