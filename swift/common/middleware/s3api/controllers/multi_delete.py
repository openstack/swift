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

import copy
import json

from swift.common.constraints import MAX_OBJECT_NAME_LENGTH
from swift.common.http import HTTP_NO_CONTENT
from swift.common.swob import str_to_wsgi
from swift.common.utils import public, StreamingPile
from swift.common.registry import get_swift_info

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation
from swift.common.middleware.s3api.etree import Element, SubElement, \
    fromstring, tostring, XMLSyntaxError, DocumentInvalid
from swift.common.middleware.s3api.s3response import HTTPOk, \
    S3NotImplemented, NoSuchKey, ErrorResponse, MalformedXML, \
    UserKeyMustBeSpecified, AccessDenied, MissingRequestBodyError


class MultiObjectDeleteController(Controller):
    """
    Handles Delete Multiple Objects, which is logged as a MULTI_OBJECT_DELETE
    operation in the S3 server log.
    """
    def _gen_error_body(self, error, elem, delete_list):
        for key, version in delete_list:
            error_elem = SubElement(elem, 'Error')
            SubElement(error_elem, 'Key').text = key
            if version is not None:
                SubElement(error_elem, 'VersionId').text = version
            SubElement(error_elem, 'Code').text = error.__class__.__name__
            SubElement(error_elem, 'Message').text = error._msg

        return tostring(elem)

    @public
    @bucket_operation
    def POST(self, req):
        """
        Handles Delete Multiple Objects.
        """
        def object_key_iter(elem):
            for obj in elem.iterchildren('Object'):
                key = obj.find('./Key').text
                if not key:
                    raise UserKeyMustBeSpecified()
                version = obj.find('./VersionId')
                if version is not None:
                    version = version.text

                yield key, version

        max_body_size = min(
            # FWIW, AWS limits multideletes to 1000 keys, and swift limits
            # object names to 1024 bytes (by default). Add a factor of two to
            # allow some slop.
            2 * self.conf.max_multi_delete_objects * MAX_OBJECT_NAME_LENGTH,
            # But, don't let operators shoot themselves in the foot
            10 * 1024 * 1024)

        try:
            xml = req.xml(max_body_size)
            if not xml:
                raise MissingRequestBodyError()

            if 'x-amz-content-sha256' not in req.headers:
                # SHA256 got checked when we read the body, so there's at
                # least *some* verification. Recent versions of boto3 stopped
                # sending Content-MD5, so it can't *always* be required.
                # See https://bugs.launchpad.net/swift/+bug/2098529
                req.check_md5(xml)
            elem = fromstring(xml, 'Delete', self.logger)

            quiet = elem.find('./Quiet')
            self.quiet = quiet is not None and quiet.text.lower() == 'true'

            delete_list = list(object_key_iter(elem))
            if len(delete_list) > self.conf.max_multi_delete_objects:
                raise MalformedXML()
        except (XMLSyntaxError, DocumentInvalid):
            raise MalformedXML()
        except ErrorResponse:
            raise
        except Exception as e:
            self.logger.error(e)
            raise

        elem = Element('DeleteResult')

        # check bucket existence
        try:
            req.get_response(self.app, 'HEAD')
        except AccessDenied as error:
            body = self._gen_error_body(error, elem, delete_list)
            return HTTPOk(body=body)

        if 'object_versioning' not in get_swift_info() and any(
                version not in ('null', None)
                for _key, version in delete_list):
            raise S3NotImplemented()

        def do_delete(base_req, key, version):
            req = copy.copy(base_req)
            req.environ = copy.copy(base_req.environ)
            req.object_name = str_to_wsgi(key)
            if version:
                req.params = {'version-id': version, 'symlink': 'get'}

            try:
                try:
                    query = req.gen_multipart_manifest_delete_query(
                        self.app, version=version)
                except NoSuchKey:
                    query = {}
                if version:
                    query['version-id'] = version
                    query['symlink'] = 'get'

                resp = req.get_response(self.app, method='DELETE', query=query,
                                        headers={'Accept': 'application/json'})
                # If async segment cleanup is available, we expect to get
                # back a 204; otherwise, the delete is synchronous and we
                # have to read the response to actually do the SLO delete
                if query.get('multipart-manifest') and \
                        resp.status_int != HTTP_NO_CONTENT:
                    try:
                        delete_result = json.loads(resp.body)
                        if delete_result['Errors']:
                            # NB: bulk includes 404s in "Number Not Found",
                            # not "Errors"
                            msg_parts = [delete_result['Response Status']]
                            msg_parts.extend(
                                '%s: %s' % (obj, status)
                                for obj, status in delete_result['Errors'])
                            return key, {'code': 'SLODeleteError',
                                         'message': '\n'.join(msg_parts)}
                        # else, all good
                    except (ValueError, TypeError, KeyError):
                        # Logs get all the gory details
                        self.logger.exception(
                            'Could not parse SLO delete response (%s): %s',
                            resp.status, resp.body)
                        # Client gets something more generic
                        return key, {'code': 'SLODeleteError',
                                     'message': 'Unexpected swift response'}
            except NoSuchKey:
                pass
            except ErrorResponse as e:
                return key, {'code': e.__class__.__name__, 'message': e._msg}
            except Exception:
                self.logger.exception(
                    'Unexpected Error handling DELETE of %r %r' % (
                        req.container_name, key))
                return key, {'code': 'Server Error', 'message': 'Server Error'}

            return key, None

        with StreamingPile(self.conf.multi_delete_concurrency) as pile:
            for key, err in pile.asyncstarmap(do_delete, (
                    (req, key, version) for key, version in delete_list)):
                if err:
                    error = SubElement(elem, 'Error')
                    SubElement(error, 'Key').text = key
                    SubElement(error, 'Code').text = err['code']
                    SubElement(error, 'Message').text = err['message']
                elif not self.quiet:
                    deleted = SubElement(elem, 'Deleted')
                    SubElement(deleted, 'Key').text = key

        body = tostring(elem)

        return HTTPOk(body=body)
