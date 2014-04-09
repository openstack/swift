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

import tarfile
from urllib import quote, unquote
from xml.sax import saxutils
from time import time
from eventlet import sleep
import zlib
from swift.common.swob import Request, HTTPBadGateway, \
    HTTPCreated, HTTPBadRequest, HTTPNotFound, HTTPUnauthorized, HTTPOk, \
    HTTPPreconditionFailed, HTTPRequestEntityTooLarge, HTTPNotAcceptable, \
    HTTPLengthRequired, HTTPException, HTTPServerError, wsgify
from swift.common.utils import json, get_logger, register_swift_info
from swift.common import constraints
from swift.common.http import HTTP_UNAUTHORIZED, HTTP_NOT_FOUND, HTTP_CONFLICT


class CreateContainerError(Exception):
    def __init__(self, msg, status_int, status):
        self.status_int = status_int
        self.status = status
        Exception.__init__(self, msg)


ACCEPTABLE_FORMATS = ['text/plain', 'application/json', 'application/xml',
                      'text/xml']


def get_response_body(data_format, data_dict, error_list):
    """
    Returns a properly formatted response body according to format. Handles
    json and xml, otherwise will return text/plain. Note: xml response does not
    include xml declaration.
    :params data_format: resulting format
    :params data_dict: generated data about results.
    :params error_list: list of quoted filenames that failed
    """
    if data_format == 'application/json':
        data_dict['Errors'] = error_list
        return json.dumps(data_dict)
    if data_format and data_format.endswith('/xml'):
        output = '<delete>\n'
        for key in sorted(data_dict):
            xml_key = key.replace(' ', '_').lower()
            output += '<%s>%s</%s>\n' % (xml_key, data_dict[key], xml_key)
        output += '<errors>\n'
        output += '\n'.join(
            ['<object>'
             '<name>%s</name><status>%s</status>'
             '</object>' % (saxutils.escape(name), status) for
             name, status in error_list])
        output += '</errors>\n</delete>\n'
        return output

    output = ''
    for key in sorted(data_dict):
        output += '%s: %s\n' % (key, data_dict[key])
    output += 'Errors:\n'
    output += '\n'.join(
        ['%s, %s' % (name, status)
         for name, status in error_list])
    return output


class Bulk(object):
    """
    Middleware that will do many operations on a single request.

    Extract Archive:

    Expand tar files into a swift account. Request must be a PUT with the
    query parameter ?extract-archive=format specifying the format of archive
    file. Accepted formats are tar, tar.gz, and tar.bz2.

    For a PUT to the following url:

    /v1/AUTH_Account/$UPLOAD_PATH?extract-archive=tar.gz

    UPLOAD_PATH is where the files will be expanded to. UPLOAD_PATH can be a
    container, a pseudo-directory within a container, or an empty string. The
    destination of a file in the archive will be built as follows:

    /v1/AUTH_Account/$UPLOAD_PATH/$FILE_PATH

    Where FILE_PATH is the file name from the listing in the tar file.

    If the UPLOAD_PATH is an empty string, containers will be auto created
    accordingly and files in the tar that would not map to any container (files
    in the base directory) will be ignored.

    Only regular files will be uploaded. Empty directories, symlinks, etc will
    not be uploaded.

    The response from bulk operations functions differently from other swift
    responses. This is because a short request body sent from the client could
    result in many operations on the proxy server and precautions need to be
    made to prevent the request from timing out due to lack of activity. To
    this end, the client will always receive a 200 OK response, regardless of
    the actual success of the call.  The body of the response must be parsed to
    determine the actual success of the operation. In addition to this the
    client may receive zero or more whitespace characters prepended to the
    actual response body while the proxy server is completing the request.

    The format of the response body defaults to text/plain but can be either
    json or xml depending on the Accept header. Acceptable formats are
    text/plain, application/json, application/xml, and text/xml. An example
    body is as follows:

    {"Response Status": "201 Created",
     "Response Body": "",
     "Errors": [],
     "Number Files Created": 10}

    If all valid files were uploaded successfully the Response Status will be
    201 Created.  If any files failed to be created the response code
    corresponds to the subrequest's error. Possible codes are 400, 401, 502 (on
    server errors), etc. In both cases the response body will specify the
    number of files successfully uploaded and a list of the files that failed.

    There are proxy logs created for each file (which becomes a subrequest) in
    the tar. The subrequest's proxy log will have a swift.source set to "EA"
    the log's content length will reflect the unzipped size of the file. If
    double proxy-logging is used the leftmost logger will not have a
    swift.source set and the content length will reflect the size of the
    payload sent to the proxy (the unexpanded size of the tar.gz).

    Bulk Delete:

    Will delete multiple objects or containers from their account with a
    single request. Responds to POST requests with query parameter
    ?bulk-delete set. The request url is your storage url. The Content-Type
    should be set to text/plain. The body of the POST request will be a
    newline separated list of url encoded objects to delete. You can delete
    10,000 (configurable) objects per request. The objects specified in the
    POST request body must be URL encoded and in the form:

    /container_name/obj_name

    or for a container (which must be empty at time of delete)

    /container_name

    The response is similar to extract archive as in every response will be a
    200 OK and you must parse the response body for actual results. An example
    response is:

    {"Number Not Found": 0,
     "Response Status": "200 OK",
     "Response Body": "",
     "Errors": [],
     "Number Deleted": 6}

    If all items were successfully deleted (or did not exist), the Response
    Status will be 200 OK. If any failed to delete, the response code
    corresponds to the subrequest's error. Possible codes are 400, 401, 502 (on
    server errors), etc. In all cases the response body will specify the number
    of items successfully deleted, not found, and a list of those that failed.
    The return body will be formatted in the way specified in the request's
    Accept header. Acceptable formats are text/plain, application/json,
    application/xml, and text/xml.

    There are proxy logs created for each object or container (which becomes a
    subrequest) that is deleted. The subrequest's proxy log will have a
    swift.source set to "BD" the log's content length of 0. If double
    proxy-logging is used the leftmost logger will not have a
    swift.source set and the content length will reflect the size of the
    payload sent to the proxy (the list of objects/containers to be deleted).
    """

    def __init__(self, app, conf, max_containers_per_extraction=10000,
                 max_failed_extractions=1000, max_deletes_per_request=10000,
                 max_failed_deletes=1000, yield_frequency=10, retry_count=0,
                 retry_interval=1.5, logger=None):
        self.app = app
        self.logger = logger or get_logger(conf, log_route='bulk')
        self.max_containers = max_containers_per_extraction
        self.max_failed_extractions = max_failed_extractions
        self.max_failed_deletes = max_failed_deletes
        self.max_deletes_per_request = max_deletes_per_request
        self.yield_frequency = yield_frequency
        self.retry_count = retry_count
        self.retry_interval = retry_interval
        self.max_path_length = constraints.MAX_OBJECT_NAME_LENGTH \
            + constraints.MAX_CONTAINER_NAME_LENGTH + 2

    def create_container(self, req, container_path):
        """
        Checks if the container exists and if not try to create it.
        :params container_path: an unquoted path to a container to be created
        :returns: True if created container, False if container exists
        :raises: CreateContainerError when unable to create container
        """
        new_env = req.environ.copy()
        new_env['PATH_INFO'] = container_path
        new_env['swift.source'] = 'EA'
        new_env['REQUEST_METHOD'] = 'HEAD'
        head_cont_req = Request.blank(container_path, environ=new_env)
        resp = head_cont_req.get_response(self.app)
        if resp.is_success:
            return False
        if resp.status_int == 404:
            new_env = req.environ.copy()
            new_env['PATH_INFO'] = container_path
            new_env['swift.source'] = 'EA'
            new_env['REQUEST_METHOD'] = 'PUT'
            create_cont_req = Request.blank(container_path, environ=new_env)
            resp = create_cont_req.get_response(self.app)
            if resp.is_success:
                return True
        raise CreateContainerError(
            "Create Container Failed: " + container_path,
            resp.status_int, resp.status)

    def get_objs_to_delete(self, req):
        """
        Will populate objs_to_delete with data from request input.
        :params req: a Swob request
        :returns: a list of the contents of req.body when separated by newline.
        :raises: HTTPException on failures
        """
        line = ''
        data_remaining = True
        objs_to_delete = []
        if req.content_length is None and \
                req.headers.get('transfer-encoding', '').lower() != 'chunked':
            raise HTTPLengthRequired(request=req)

        while data_remaining:
            if '\n' in line:
                obj_to_delete, line = line.split('\n', 1)
                obj_to_delete = obj_to_delete.strip()
                objs_to_delete.append(
                    {'name': unquote(obj_to_delete)})
            else:
                data = req.body_file.read(self.max_path_length)
                if data:
                    line += data
                else:
                    data_remaining = False
                    obj_to_delete = line.strip()
                    if obj_to_delete:
                        objs_to_delete.append(
                            {'name': unquote(obj_to_delete)})
            if len(objs_to_delete) > self.max_deletes_per_request:
                raise HTTPRequestEntityTooLarge(
                    'Maximum Bulk Deletes: %d per request' %
                    self.max_deletes_per_request)
            if len(line) > self.max_path_length * 2:
                raise HTTPBadRequest('Invalid File Name')
        return objs_to_delete

    def handle_delete_iter(self, req, objs_to_delete=None,
                           user_agent='BulkDelete', swift_source='BD',
                           out_content_type='text/plain'):
        """
        A generator that can be assigned to a swob Response's app_iter which,
        when iterated over, will delete the objects specified in request body.
        Will occasionally yield whitespace while request is being processed.
        When the request is completed will yield a response body that can be
        parsed to determine success. See above documentation for details.

        :params req: a swob Request
        :params objs_to_delete: a list of dictionaries that specifies the
            objects to be deleted. If None, uses self.get_objs_to_delete to
            query request.
        """
        last_yield = time()
        separator = ''
        failed_files = []
        resp_dict = {'Response Status': HTTPOk().status,
                     'Response Body': '',
                     'Number Deleted': 0,
                     'Number Not Found': 0}
        try:
            if not out_content_type:
                raise HTTPNotAcceptable(request=req)
            if out_content_type.endswith('/xml'):
                yield '<?xml version="1.0" encoding="UTF-8"?>\n'

            try:
                vrs, account, _junk = req.split_path(2, 3, True)
            except ValueError:
                raise HTTPNotFound(request=req)

            incoming_format = req.headers.get('Content-Type')
            if incoming_format and \
                    not incoming_format.startswith('text/plain'):
                # For now only accept newline separated object names
                raise HTTPNotAcceptable(request=req)

            if objs_to_delete is None:
                objs_to_delete = self.get_objs_to_delete(req)
            failed_file_response = {'type': HTTPBadRequest}
            req.environ['eventlet.minimum_write_chunk_size'] = 0
            for obj_to_delete in objs_to_delete:
                if last_yield + self.yield_frequency < time():
                    separator = '\r\n\r\n'
                    last_yield = time()
                    yield ' '
                obj_name = obj_to_delete['name']
                if not obj_name:
                    continue
                if len(failed_files) >= self.max_failed_deletes:
                    raise HTTPBadRequest('Max delete failures exceeded')
                if obj_to_delete.get('error'):
                    if obj_to_delete['error']['code'] == HTTP_NOT_FOUND:
                        resp_dict['Number Not Found'] += 1
                    else:
                        failed_files.append([quote(obj_name),
                                            obj_to_delete['error']['message']])
                    continue
                delete_path = '/'.join(['', vrs, account,
                                        obj_name.lstrip('/')])
                if not constraints.check_utf8(delete_path):
                    failed_files.append([quote(obj_name),
                                         HTTPPreconditionFailed().status])
                    continue
                new_env = req.environ.copy()
                new_env['PATH_INFO'] = delete_path
                del(new_env['wsgi.input'])
                new_env['CONTENT_LENGTH'] = 0
                new_env['REQUEST_METHOD'] = 'DELETE'
                new_env['HTTP_USER_AGENT'] = \
                    '%s %s' % (req.environ.get('HTTP_USER_AGENT'), user_agent)
                new_env['swift.source'] = swift_source
                self._process_delete(delete_path, obj_name, new_env, resp_dict,
                                     failed_files, failed_file_response)

            if failed_files:
                resp_dict['Response Status'] = \
                    failed_file_response['type']().status
            elif not (resp_dict['Number Deleted'] or
                      resp_dict['Number Not Found']):
                resp_dict['Response Status'] = HTTPBadRequest().status
                resp_dict['Response Body'] = 'Invalid bulk delete.'

        except HTTPException as err:
            resp_dict['Response Status'] = err.status
            resp_dict['Response Body'] = err.body
        except Exception:
            self.logger.exception('Error in bulk delete.')
            resp_dict['Response Status'] = HTTPServerError().status

        yield separator + get_response_body(out_content_type,
                                            resp_dict, failed_files)

    def handle_extract_iter(self, req, compress_type,
                            out_content_type='text/plain'):
        """
        A generator that can be assigned to a swob Response's app_iter which,
        when iterated over, will extract and PUT the objects pulled from the
        request body. Will occasionally yield whitespace while request is being
        processed. When the request is completed will yield a response body
        that can be parsed to determine success. See above documentation for
        details.

        :params req: a swob Request
        :params compress_type: specifying the compression type of the tar.
            Accepts '', 'gz', or 'bz2'
        """
        resp_dict = {'Response Status': HTTPCreated().status,
                     'Response Body': '', 'Number Files Created': 0}
        failed_files = []
        last_yield = time()
        separator = ''
        containers_accessed = set()
        try:
            if not out_content_type:
                raise HTTPNotAcceptable(request=req)
            if out_content_type.endswith('/xml'):
                yield '<?xml version="1.0" encoding="UTF-8"?>\n'

            if req.content_length is None and \
                    req.headers.get('transfer-encoding',
                                    '').lower() != 'chunked':
                raise HTTPLengthRequired(request=req)
            try:
                vrs, account, extract_base = req.split_path(2, 3, True)
            except ValueError:
                raise HTTPNotFound(request=req)
            extract_base = extract_base or ''
            extract_base = extract_base.rstrip('/')
            tar = tarfile.open(mode='r|' + compress_type,
                               fileobj=req.body_file)
            failed_response_type = HTTPBadRequest
            req.environ['eventlet.minimum_write_chunk_size'] = 0
            containers_created = 0
            while True:
                if last_yield + self.yield_frequency < time():
                    separator = '\r\n\r\n'
                    last_yield = time()
                    yield ' '
                tar_info = tar.next()
                if tar_info is None or \
                        len(failed_files) >= self.max_failed_extractions:
                    break
                if tar_info.isfile():
                    obj_path = tar_info.name
                    if obj_path.startswith('./'):
                        obj_path = obj_path[2:]
                    obj_path = obj_path.lstrip('/')
                    if extract_base:
                        obj_path = extract_base + '/' + obj_path
                    if '/' not in obj_path:
                        continue  # ignore base level file

                    destination = '/'.join(
                        ['', vrs, account, obj_path])
                    container = obj_path.split('/', 1)[0]
                    if not constraints.check_utf8(destination):
                        failed_files.append(
                            [quote(obj_path[:self.max_path_length]),
                             HTTPPreconditionFailed().status])
                        continue
                    if tar_info.size > constraints.MAX_FILE_SIZE:
                        failed_files.append([
                            quote(obj_path[:self.max_path_length]),
                            HTTPRequestEntityTooLarge().status])
                        continue
                    container_failure = None
                    if container not in containers_accessed:
                        cont_path = '/'.join(['', vrs, account, container])
                        try:
                            if self.create_container(req, cont_path):
                                containers_created += 1
                                if containers_created > self.max_containers:
                                    raise HTTPBadRequest(
                                        'More than %d containers to create '
                                        'from tar.' % self.max_containers)
                        except CreateContainerError as err:
                            # the object PUT to this container still may
                            # succeed if acls are set
                            container_failure = [
                                quote(cont_path[:self.max_path_length]),
                                err.status]
                            if err.status_int == HTTP_UNAUTHORIZED:
                                raise HTTPUnauthorized(request=req)
                        except ValueError:
                            failed_files.append([
                                quote(obj_path[:self.max_path_length]),
                                HTTPBadRequest().status])
                            continue

                    tar_file = tar.extractfile(tar_info)
                    new_env = req.environ.copy()
                    new_env['REQUEST_METHOD'] = 'PUT'
                    new_env['wsgi.input'] = tar_file
                    new_env['PATH_INFO'] = destination
                    new_env['CONTENT_LENGTH'] = tar_info.size
                    new_env['swift.source'] = 'EA'
                    new_env['HTTP_USER_AGENT'] = \
                        '%s BulkExpand' % req.environ.get('HTTP_USER_AGENT')
                    create_obj_req = Request.blank(destination, new_env)
                    resp = create_obj_req.get_response(self.app)
                    containers_accessed.add(container)
                    if resp.is_success:
                        resp_dict['Number Files Created'] += 1
                    else:
                        if container_failure:
                            failed_files.append(container_failure)
                        if resp.status_int == HTTP_UNAUTHORIZED:
                            failed_files.append([
                                quote(obj_path[:self.max_path_length]),
                                HTTPUnauthorized().status])
                            raise HTTPUnauthorized(request=req)
                        if resp.status_int // 100 == 5:
                            failed_response_type = HTTPBadGateway
                        failed_files.append([
                            quote(obj_path[:self.max_path_length]),
                            resp.status])

            if failed_files:
                resp_dict['Response Status'] = failed_response_type().status
            elif not resp_dict['Number Files Created']:
                resp_dict['Response Status'] = HTTPBadRequest().status
                resp_dict['Response Body'] = 'Invalid Tar File: No Valid Files'

        except HTTPException as err:
            resp_dict['Response Status'] = err.status
            resp_dict['Response Body'] = err.body
        except (tarfile.TarError, zlib.error) as tar_error:
            resp_dict['Response Status'] = HTTPBadRequest().status
            resp_dict['Response Body'] = 'Invalid Tar File: %s' % tar_error
        except Exception:
            self.logger.exception('Error in extract archive.')
            resp_dict['Response Status'] = HTTPServerError().status

        yield separator + get_response_body(
            out_content_type, resp_dict, failed_files)

    def _process_delete(self, delete_path, obj_name, env, resp_dict,
                        failed_files, failed_file_response, retry=0):
        delete_obj_req = Request.blank(delete_path, env)
        resp = delete_obj_req.get_response(self.app)
        if resp.status_int // 100 == 2:
            resp_dict['Number Deleted'] += 1
        elif resp.status_int == HTTP_NOT_FOUND:
            resp_dict['Number Not Found'] += 1
        elif resp.status_int == HTTP_UNAUTHORIZED:
            failed_files.append([quote(obj_name),
                                 HTTPUnauthorized().status])
        elif resp.status_int == HTTP_CONFLICT and \
                self.retry_count > 0 and self.retry_count > retry:
            retry += 1
            sleep(self.retry_interval ** retry)
            self._process_delete(delete_path, obj_name, env, resp_dict,
                                 failed_files, failed_file_response,
                                 retry)
        else:
            if resp.status_int // 100 == 5:
                failed_file_response['type'] = HTTPBadGateway
            failed_files.append([quote(obj_name), resp.status])

    @wsgify
    def __call__(self, req):
        extract_type = req.params.get('extract-archive')
        resp = None
        if extract_type is not None and req.method == 'PUT':
            archive_type = {
                'tar': '', 'tar.gz': 'gz',
                'tar.bz2': 'bz2'}.get(extract_type.lower().strip('.'))
            if archive_type is not None:
                resp = HTTPOk(request=req)
                out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
                if out_content_type:
                    resp.content_type = out_content_type
                resp.app_iter = self.handle_extract_iter(
                    req, archive_type, out_content_type=out_content_type)
            else:
                resp = HTTPBadRequest("Unsupported archive format")
        if 'bulk-delete' in req.params and req.method in ['POST', 'DELETE']:
            resp = HTTPOk(request=req)
            out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
            if out_content_type:
                resp.content_type = out_content_type
            resp.app_iter = self.handle_delete_iter(
                req, out_content_type=out_content_type)

        return resp or self.app


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    max_containers_per_extraction = \
        int(conf.get('max_containers_per_extraction', 10000))
    max_failed_extractions = int(conf.get('max_failed_extractions', 1000))
    max_deletes_per_request = int(conf.get('max_deletes_per_request', 10000))
    max_failed_deletes = int(conf.get('max_failed_deletes', 1000))
    yield_frequency = int(conf.get('yield_frequency', 10))
    retry_count = int(conf.get('delete_container_retry_count', 0))
    retry_interval = 1.5

    register_swift_info(
        'bulk_upload',
        max_containers_per_extraction=max_containers_per_extraction,
        max_failed_extractions=max_failed_extractions)
    register_swift_info(
        'bulk_delete',
        max_deletes_per_request=max_deletes_per_request,
        max_failed_deletes=max_failed_deletes)

    def bulk_filter(app):
        return Bulk(
            app, conf,
            max_containers_per_extraction=max_containers_per_extraction,
            max_failed_extractions=max_failed_extractions,
            max_deletes_per_request=max_deletes_per_request,
            max_failed_deletes=max_failed_deletes,
            yield_frequency=yield_frequency,
            retry_count=retry_count,
            retry_interval=retry_interval)
    return bulk_filter
