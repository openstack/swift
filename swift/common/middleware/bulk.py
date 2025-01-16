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

"""
Middleware that will perform many operations on a single request.

---------------
Extract Archive
---------------

Expand tar files into a Swift account. Request must be a PUT with the
query parameter ``?extract-archive=format`` specifying the format of archive
file. Accepted formats are tar, tar.gz, and tar.bz2.

For a PUT to the following url::

    /v1/AUTH_Account/$UPLOAD_PATH?extract-archive=tar.gz

UPLOAD_PATH is where the files will be expanded to. UPLOAD_PATH can be a
container, a pseudo-directory within a container, or an empty string. The
destination of a file in the archive will be built as follows::

    /v1/AUTH_Account/$UPLOAD_PATH/$FILE_PATH

Where FILE_PATH is the file name from the listing in the tar file.

If the UPLOAD_PATH is an empty string, containers will be auto created
accordingly and files in the tar that would not map to any container (files
in the base directory) will be ignored.

Only regular files will be uploaded. Empty directories, symlinks, etc will
not be uploaded.

------------
Content Type
------------

If the content-type header is set in the extract-archive call, Swift will
assign that content-type to all the underlying files. The bulk middleware
will extract the archive file and send the internal files using PUT
operations using the same headers from the original request
(e.g. auth-tokens, content-Type, etc.). Notice that any middleware call
that follows the bulk middleware does not know if this was a bulk request
or if these were individual requests sent by the user.

In order to make Swift detect the content-type for the files based on the
file extension, the content-type in the extract-archive call should not be
set. Alternatively, it is possible to explicitly tell Swift to detect the
content type using this header::

    X-Detect-Content-Type: true

For example::

    curl -X PUT http://127.0.0.1/v1/AUTH_acc/cont/$?extract-archive=tar
     -T backup.tar
     -H "Content-Type: application/x-tar"
     -H "X-Auth-Token: xxx"
     -H "X-Detect-Content-Type: true"

------------------
Assigning Metadata
------------------

The tar file format (1) allows for UTF-8 key/value pairs to be associated
with each file in an archive. If a file has extended attributes, then tar
will store those as key/value pairs. The bulk middleware can read those
extended attributes and convert them to Swift object metadata. Attributes
starting with "user.meta" are converted to object metadata, and
"user.mime_type" is converted to Content-Type.

For example::

    setfattr -n user.mime_type -v "application/python-setup" setup.py
    setfattr -n user.meta.lunch -v "burger and fries" setup.py
    setfattr -n user.meta.dinner -v "baked ziti" setup.py
    setfattr -n user.stuff -v "whee" setup.py

Will get translated to headers::

    Content-Type: application/python-setup
    X-Object-Meta-Lunch: burger and fries
    X-Object-Meta-Dinner: baked ziti

The bulk middleware  will handle xattrs stored by both GNU and BSD tar (2).
Only xattrs ``user.mime_type`` and ``user.meta.*`` are processed. Other
attributes are ignored.

In addition to the extended attributes, the object metadata and the
x-delete-at/x-delete-after headers set in the request are also assigned to the
extracted objects.

Notes:

(1) The POSIX 1003.1-2001 (pax) format. The default format on GNU tar
1.27.1 or later.

(2) Even with pax-format tarballs, different encoders store xattrs slightly
differently; for example, GNU tar stores the xattr "user.userattribute" as
pax header "SCHILY.xattr.user.userattribute", while BSD tar (which uses
libarchive) stores it as "LIBARCHIVE.xattr.user.userattribute".

--------
Response
--------

The response from bulk operations functions differently from other Swift
responses. This is because a short request body sent from the client could
result in many operations on the proxy server and precautions need to be
made to prevent the request from timing out due to lack of activity. To
this end, the client will always receive a 200 OK response, regardless of
the actual success of the call.  The body of the response must be parsed to
determine the actual success of the operation. In addition to this the
client may receive zero or more whitespace characters prepended to the
actual response body while the proxy server is completing the request.

The format of the response body defaults to text/plain but can be either
json or xml depending on the ``Accept`` header. Acceptable formats are
``text/plain``, ``application/json``, ``application/xml``, and ``text/xml``.
An example body is as follows::

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

-----------
Bulk Delete
-----------

Will delete multiple objects or containers from their account with a
single request. Responds to POST requests with query parameter
``?bulk-delete`` set. The request url is your storage url. The Content-Type
should be set to ``text/plain``. The body of the POST request will be a
newline separated list of url encoded objects to delete. You can delete
10,000 (configurable) objects per request. The objects specified in the
POST request body must be URL encoded and in the form::

    /container_name/obj_name

or for a container (which must be empty at time of delete)::

    /container_name

The response is similar to extract archive as in every response will be a
200 OK and you must parse the response body for actual results. An example
response is::

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
``Accept`` header. Acceptable formats are ``text/plain``, ``application/json``,
``application/xml``, and ``text/xml``.

There are proxy logs created for each object or container (which becomes a
subrequest) that is deleted. The subrequest's proxy log will have a
swift.source set to "BD" the log's content length of 0. If double
proxy-logging is used the leftmost logger will not have a
swift.source set and the content length will reflect the size of the
payload sent to the proxy (the list of objects/containers to be deleted).
"""

import json
import tarfile
from xml.sax.saxutils import escape  # nosec B406
from time import time
from eventlet import sleep
import zlib
from swift.common.swob import Request, HTTPBadGateway, \
    HTTPCreated, HTTPBadRequest, HTTPNotFound, HTTPUnauthorized, HTTPOk, \
    HTTPPreconditionFailed, HTTPRequestEntityTooLarge, HTTPNotAcceptable, \
    HTTPLengthRequired, HTTPException, HTTPServerError, wsgify, \
    bytes_to_wsgi, str_to_wsgi, wsgi_unquote, wsgi_quote, wsgi_to_str
from swift.common.utils import get_logger, StreamingPile
from swift.common.registry import register_swift_info
from swift.common import constraints
from swift.common.http import HTTP_UNAUTHORIZED, HTTP_NOT_FOUND, HTTP_CONFLICT
from swift.common.request_helpers import is_user_meta
from swift.common.wsgi import make_subrequest


class CreateContainerError(Exception):
    def __init__(self, msg, status_int, status):
        self.status_int = status_int
        self.status = status
        super(CreateContainerError, self).__init__(msg)


ACCEPTABLE_FORMATS = ['text/plain', 'application/json', 'application/xml',
                      'text/xml']


def get_response_body(data_format, data_dict, error_list, root_tag):
    """
    Returns a properly formatted response body according to format.

    Handles json and xml, otherwise will return text/plain.
    Note: xml response does not include xml declaration.

    :params data_format: resulting format
    :params data_dict: generated data about results.
    :params error_list: list of quoted filenames that failed
    :params root_tag: the tag name to use for root elements when returning XML;
                      e.g. 'extract' or 'delete'
    """
    if data_format == 'application/json':
        data_dict['Errors'] = error_list
        return json.dumps(data_dict).encode('ascii')
    if data_format and data_format.endswith('/xml'):
        output = ['<', root_tag, '>\n']
        for key in sorted(data_dict):
            xml_key = key.replace(' ', '_').lower()
            output.extend([
                '<', xml_key, '>',
                escape(str(data_dict[key])),
                '</', xml_key, '>\n',
            ])
        output.append('<errors>\n')
        for name, status in error_list:
            output.extend([
                '<object><name>', escape(name), '</name><status>',
                escape(status), '</status></object>\n',
            ])
        output.extend(['</errors>\n</', root_tag, '>\n'])
        return ''.join(output).encode('utf-8')

    output = []
    for key in sorted(data_dict):
        output.append('%s: %s\n' % (key, data_dict[key]))
    output.append('Errors:\n')
    output.extend(
        '%s, %s\n' % (name, status)
        for name, status in error_list)
    return ''.join(output).encode('utf-8')


def pax_key_to_swift_header(pax_key):
    if (pax_key == u"SCHILY.xattr.user.mime_type" or
            pax_key == u"LIBARCHIVE.xattr.user.mime_type"):
        return "Content-Type"
    elif pax_key.startswith(u"SCHILY.xattr.user.meta."):
        useful_part = pax_key[len(u"SCHILY.xattr.user.meta."):]
        return str_to_wsgi("X-Object-Meta-" + useful_part)
    elif pax_key.startswith(u"LIBARCHIVE.xattr.user.meta."):
        useful_part = pax_key[len(u"LIBARCHIVE.xattr.user.meta."):]
        return str_to_wsgi("X-Object-Meta-" + useful_part)
    else:
        # You can get things like atime/mtime/ctime or filesystem ACLs in
        # pax headers; those aren't really user metadata. The same goes for
        # other, non-user metadata.
        return None


class Bulk(object):

    def __init__(self, app, conf, max_containers_per_extraction=10000,
                 max_failed_extractions=1000, max_deletes_per_request=10000,
                 max_failed_deletes=1000, yield_frequency=10,
                 delete_concurrency=2, retry_count=0, retry_interval=1.5,
                 logger=None):
        self.app = app
        self.logger = logger or get_logger(conf, log_route='bulk')
        self.max_containers = max_containers_per_extraction
        self.max_failed_extractions = max_failed_extractions
        self.max_failed_deletes = max_failed_deletes
        self.max_deletes_per_request = max_deletes_per_request
        self.yield_frequency = yield_frequency
        self.delete_concurrency = min(1000, max(1, delete_concurrency))
        self.retry_count = retry_count
        self.retry_interval = retry_interval
        self.max_path_length = constraints.MAX_OBJECT_NAME_LENGTH \
            + constraints.MAX_CONTAINER_NAME_LENGTH + 2

    def create_container(self, req, container_path):
        """
        Checks if the container exists and if not try to create it.
        :params container_path: an unquoted path to a container to be created
        :returns: True if created container, False if container exists
        :raises CreateContainerError: when unable to create container
        """
        head_cont_req = make_subrequest(
            req.environ, method='HEAD', path=wsgi_quote(container_path),
            headers={'X-Auth-Token': req.headers.get('X-Auth-Token')},
            swift_source='EA')
        resp = head_cont_req.get_response(self.app)
        if resp.is_success:
            return False
        if resp.status_int == HTTP_NOT_FOUND:
            create_cont_req = make_subrequest(
                req.environ, method='PUT', path=wsgi_quote(container_path),
                headers={'X-Auth-Token': req.headers.get('X-Auth-Token')},
                swift_source='EA')
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
        :raises HTTPException: on failures
        """
        line = b''
        data_remaining = True
        objs_to_delete = []
        if req.content_length is None and \
                req.headers.get('transfer-encoding', '').lower() != 'chunked':
            raise HTTPLengthRequired(request=req)

        while data_remaining:
            if b'\n' in line:
                obj_to_delete, line = line.split(b'\n', 1)
                # yeah, all this chaining is pretty terrible...
                # but it gets even worse trying to use UTF-8 and
                # errors='surrogateescape' when dealing with terrible
                # input like b'\xe2%98\x83'
                obj_to_delete = wsgi_to_str(wsgi_unquote(
                    bytes_to_wsgi(obj_to_delete.strip())))
                objs_to_delete.append({'name': obj_to_delete})
            else:
                data = req.body_file.read(self.max_path_length)
                if data:
                    line += data
                else:
                    data_remaining = False
                    obj_to_delete = wsgi_to_str(wsgi_unquote(
                        bytes_to_wsgi(line.strip())))
                    if obj_to_delete:
                        objs_to_delete.append({'name': obj_to_delete})
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
            (native string) objects to be deleted. If None, uses
            self.get_objs_to_delete to query request.
        """
        last_yield = time()
        if out_content_type and out_content_type.endswith('/xml'):
            to_yield = b'<?xml version="1.0" encoding="UTF-8"?>\n'
        else:
            to_yield = b' '
        separator = b''
        failed_files = []
        resp_dict = {'Response Status': HTTPOk().status,
                     'Response Body': '',
                     'Number Deleted': 0,
                     'Number Not Found': 0}
        req.environ['eventlet.minimum_write_chunk_size'] = 0
        try:
            if not out_content_type:
                raise HTTPNotAcceptable(request=req)

            try:
                vrs, account, _junk = req.split_path(2, 3, True)
            except ValueError:
                raise HTTPNotFound(request=req)
            vrs = wsgi_to_str(vrs)
            account = wsgi_to_str(account)

            incoming_format = req.headers.get('Content-Type')
            if incoming_format and \
                    not incoming_format.startswith('text/plain'):
                # For now only accept newline separated object names
                raise HTTPNotAcceptable(request=req)

            if objs_to_delete is None:
                objs_to_delete = self.get_objs_to_delete(req)
            failed_file_response = {'type': HTTPBadRequest}

            def delete_filter(predicate, objs_to_delete):
                for obj_to_delete in objs_to_delete:
                    obj_name = obj_to_delete['name']
                    if not obj_name:
                        continue
                    if not predicate(obj_name):
                        continue
                    if obj_to_delete.get('error'):
                        if obj_to_delete['error']['code'] == HTTP_NOT_FOUND:
                            resp_dict['Number Not Found'] += 1
                        else:
                            failed_files.append([
                                wsgi_quote(str_to_wsgi(obj_name)),
                                obj_to_delete['error']['message']])
                        continue
                    delete_path = '/'.join(['', vrs, account,
                                            obj_name.lstrip('/')])
                    if not constraints.check_utf8(delete_path):
                        failed_files.append([wsgi_quote(str_to_wsgi(obj_name)),
                                             HTTPPreconditionFailed().status])
                        continue
                    yield (obj_name, delete_path,
                           obj_to_delete.get('version_id'))

            def objs_then_containers(objs_to_delete):
                # process all objects first
                yield delete_filter(lambda name: '/' in name.strip('/'),
                                    objs_to_delete)
                # followed by containers
                yield delete_filter(lambda name: '/' not in name.strip('/'),
                                    objs_to_delete)

            def do_delete(obj_name, delete_path, version_id):
                delete_obj_req = make_subrequest(
                    req.environ, method='DELETE',
                    path=wsgi_quote(str_to_wsgi(delete_path)),
                    headers={'X-Auth-Token': req.headers.get('X-Auth-Token')},
                    body='', agent='%(orig)s ' + user_agent,
                    swift_source=swift_source)
                if version_id is None:
                    delete_obj_req.params = {}
                else:
                    delete_obj_req.params = {'version-id': version_id}
                return (delete_obj_req.get_response(self.app), obj_name, 0)

            with StreamingPile(self.delete_concurrency) as pile:
                for names_to_delete in objs_then_containers(objs_to_delete):
                    for resp, obj_name, retry in pile.asyncstarmap(
                            do_delete, names_to_delete):
                        if last_yield + self.yield_frequency < time():
                            last_yield = time()
                            yield to_yield
                            to_yield, separator = b' ', b'\r\n\r\n'
                        self._process_delete(resp, pile, obj_name,
                                             resp_dict, failed_files,
                                             failed_file_response, retry)
                        if len(failed_files) >= self.max_failed_deletes:
                            # Abort, but drain off the in-progress deletes
                            for resp, obj_name, retry in pile:
                                if last_yield + self.yield_frequency < time():
                                    last_yield = time()
                                    yield to_yield
                                    to_yield, separator = b' ', b'\r\n\r\n'
                                # Don't pass in the pile, as we shouldn't retry
                                self._process_delete(
                                    resp, None, obj_name, resp_dict,
                                    failed_files, failed_file_response, retry)
                            msg = 'Max delete failures exceeded'
                            raise HTTPBadRequest(msg)

            if failed_files:
                resp_dict['Response Status'] = \
                    failed_file_response['type']().status
            elif not (resp_dict['Number Deleted'] or
                      resp_dict['Number Not Found']):
                resp_dict['Response Status'] = HTTPBadRequest().status
                resp_dict['Response Body'] = 'Invalid bulk delete.'

        except HTTPException as err:
            resp_dict['Response Status'] = err.status
            resp_dict['Response Body'] = err.body.decode('utf-8')
        except Exception:
            self.logger.exception('Error in bulk delete.')
            resp_dict['Response Status'] = HTTPServerError().status

        yield separator + get_response_body(out_content_type,
                                            resp_dict, failed_files, 'delete')

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
        if out_content_type and out_content_type.endswith('/xml'):
            to_yield = b'<?xml version="1.0" encoding="UTF-8"?>\n'
        else:
            to_yield = b' '
        separator = b''
        containers_accessed = set()
        req.environ['eventlet.minimum_write_chunk_size'] = 0
        try:
            if not out_content_type:
                raise HTTPNotAcceptable(request=req)

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
            containers_created = 0
            while True:
                if last_yield + self.yield_frequency < time():
                    last_yield = time()
                    yield to_yield
                    to_yield, separator = b' ', b'\r\n\r\n'
                tar_info = tar.next()
                if tar_info is None or \
                        len(failed_files) >= self.max_failed_extractions:
                    break
                if tar_info.isfile():
                    obj_path = tar_info.name.encode('utf-8', 'surrogateescape')
                    obj_path = bytes_to_wsgi(obj_path)
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
                    if not constraints.check_utf8(wsgi_to_str(destination)):
                        failed_files.append(
                            [wsgi_quote(obj_path[:self.max_path_length]),
                             HTTPPreconditionFailed().status])
                        continue
                    if tar_info.size > constraints.MAX_FILE_SIZE:
                        failed_files.append([
                            wsgi_quote(obj_path[:self.max_path_length]),
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
                                wsgi_quote(cont_path[:self.max_path_length]),
                                err.status]
                            if err.status_int == HTTP_UNAUTHORIZED:
                                raise HTTPUnauthorized(request=req)
                        except ValueError:
                            failed_files.append([
                                wsgi_quote(obj_path[:self.max_path_length]),
                                HTTPBadRequest().status])
                            continue

                    tar_file = tar.extractfile(tar_info)
                    create_headers = {
                        'Content-Length': tar_info.size,
                        'X-Auth-Token': req.headers.get('X-Auth-Token'),
                    }

                    # Copy some whitelisted headers to the subrequest
                    for k, v in req.headers.items():
                        if ((k.lower() in ('x-delete-at', 'x-delete-after'))
                                or is_user_meta('object', k)):
                            create_headers[k] = v

                    create_obj_req = make_subrequest(
                        req.environ, method='PUT',
                        path=wsgi_quote(destination),
                        headers=create_headers,
                        agent='%(orig)s BulkExpand', swift_source='EA')
                    create_obj_req.environ['wsgi.input'] = tar_file

                    for pax_key, pax_value in tar_info.pax_headers.items():
                        header_name = pax_key_to_swift_header(pax_key)
                        if header_name:
                            # Both pax_key and pax_value are unicode
                            # strings; the key is already UTF-8 encoded, but
                            # we still have to encode the value.
                            create_obj_req.headers[header_name] = \
                                pax_value.encode("utf-8")

                    resp = create_obj_req.get_response(self.app)
                    containers_accessed.add(container)
                    if resp.is_success:
                        resp_dict['Number Files Created'] += 1
                    else:
                        if container_failure:
                            failed_files.append(container_failure)
                        if resp.status_int == HTTP_UNAUTHORIZED:
                            failed_files.append([
                                wsgi_quote(obj_path[:self.max_path_length]),
                                HTTPUnauthorized().status])
                            raise HTTPUnauthorized(request=req)
                        if resp.status_int // 100 == 5:
                            failed_response_type = HTTPBadGateway
                        failed_files.append([
                            wsgi_quote(obj_path[:self.max_path_length]),
                            resp.status])

            if failed_files:
                resp_dict['Response Status'] = failed_response_type().status
            elif not resp_dict['Number Files Created']:
                resp_dict['Response Status'] = HTTPBadRequest().status
                resp_dict['Response Body'] = 'Invalid Tar File: No Valid Files'

        except HTTPException as err:
            resp_dict['Response Status'] = err.status
            resp_dict['Response Body'] = err.body.decode('utf-8')
        except (tarfile.TarError, zlib.error) as tar_error:
            resp_dict['Response Status'] = HTTPBadRequest().status
            resp_dict['Response Body'] = 'Invalid Tar File: %s' % tar_error
        except Exception:
            self.logger.exception('Error in extract archive.')
            resp_dict['Response Status'] = HTTPServerError().status

        yield separator + get_response_body(
            out_content_type, resp_dict, failed_files, 'extract')

    def _process_delete(self, resp, pile, obj_name, resp_dict,
                        failed_files, failed_file_response, retry=0):
        if resp.status_int // 100 == 2:
            resp_dict['Number Deleted'] += 1
        elif resp.status_int == HTTP_NOT_FOUND:
            resp_dict['Number Not Found'] += 1
        elif resp.status_int == HTTP_UNAUTHORIZED:
            failed_files.append([wsgi_quote(str_to_wsgi(obj_name)),
                                 HTTPUnauthorized().status])
        elif resp.status_int == HTTP_CONFLICT and pile and \
                self.retry_count > 0 and self.retry_count > retry:
            retry += 1
            sleep(self.retry_interval ** retry)
            delete_obj_req = Request.blank(resp.environ['PATH_INFO'],
                                           resp.environ)

            def _retry(req, app, obj_name, retry):
                return req.get_response(app), obj_name, retry
            pile.spawn(_retry, delete_obj_req, self.app, obj_name, retry)
        else:
            if resp.status_int // 100 == 5:
                failed_file_response['type'] = HTTPBadGateway
            failed_files.append([wsgi_quote(str_to_wsgi(obj_name)),
                                 resp.status])

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
                try:
                    out_content_type = req.accept.best_match(
                        ACCEPTABLE_FORMATS)
                except ValueError:
                    out_content_type = None  # Ignore invalid header
                if out_content_type:
                    resp.content_type = out_content_type
                resp.app_iter = self.handle_extract_iter(
                    req, archive_type, out_content_type=out_content_type)
            else:
                resp = HTTPBadRequest("Unsupported archive format")
        if 'bulk-delete' in req.params and req.method in ['POST', 'DELETE']:
            resp = HTTPOk(request=req)
            try:
                out_content_type = req.accept.best_match(ACCEPTABLE_FORMATS)
            except ValueError:
                out_content_type = None  # Ignore invalid header
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
    delete_concurrency = min(1000, max(1, int(
        conf.get('delete_concurrency', 2))))
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
            delete_concurrency=delete_concurrency,
            retry_count=retry_count,
            retry_interval=retry_interval)
    return bulk_filter
