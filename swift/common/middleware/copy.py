# Copyright (c) 2015 OpenStack Foundation
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
Server side copy is a feature that enables users/clients to COPY objects
between accounts and containers without the need to download and then
re-upload objects, thus eliminating additional bandwidth consumption and
also saving time. This may be used when renaming/moving an object which
in Swift is a (COPY + DELETE) operation.

The server side copy middleware should be inserted in the pipeline after auth
and before the quotas and large object middlewares. If it is not present in the
pipeline in the proxy-server configuration file, it will be inserted
automatically. There is no configurable option provided to turn off server
side copy.

--------
Metadata
--------
* All metadata of source object is preserved during object copy.
* One can also provide additional metadata during PUT/COPY request. This will
  over-write any existing conflicting keys.
* Server side copy can also be used to change content-type of an existing
  object.

-----------
Object Copy
-----------
* The destination container must exist before requesting copy of the object.
* When several replicas exist, the system copies from the most recent replica.
  That is, the copy operation behaves as though the X-Newest header is in the
  request.
* The request to copy an object should have no body (i.e. content-length of the
  request must be zero).

There are two ways in which an object can be copied:

1. Send a PUT request to the new object (destination/target) with an additional
   header named ``X-Copy-From`` specifying the source object
   (in '/container/object' format). Example::

    curl -i -X PUT http://<storage_url>/container1/destination_obj
     -H 'X-Auth-Token: <token>'
     -H 'X-Copy-From: /container2/source_obj'
     -H 'Content-Length: 0'

2. Send a COPY request with an existing object in URL with an additional header
   named ``Destination`` specifying the destination/target object
   (in '/container/object' format). Example::

    curl -i -X COPY http://<storage_url>/container2/source_obj
     -H 'X-Auth-Token: <token>'
     -H 'Destination: /container1/destination_obj'
     -H 'Content-Length: 0'

Note that if the incoming request has some conditional headers (e.g. ``Range``,
``If-Match``), the *source* object will be evaluated for these headers (i.e. if
PUT with both ``X-Copy-From`` and ``Range``, Swift will make a partial copy to
the destination object).

-------------------------
Cross Account Object Copy
-------------------------
Objects can also be copied from one account to another account if the user
has the necessary permissions (i.e. permission to read from container
in source account and permission to write to container in destination account).

Similar to examples mentioned above, there are two ways to copy objects across
accounts:

1. Like the example above, send PUT request to copy object but with an
   additional header named ``X-Copy-From-Account`` specifying the source
   account. Example::

    curl -i -X PUT http://<host>:<port>/v1/AUTH_test1/container/destination_obj
     -H 'X-Auth-Token: <token>'
     -H 'X-Copy-From: /container/source_obj'
     -H 'X-Copy-From-Account: AUTH_test2'
     -H 'Content-Length: 0'

2. Like the previous example, send a COPY request but with an additional header
   named ``Destination-Account`` specifying the name of destination account.
   Example::

    curl -i -X COPY http://<host>:<port>/v1/AUTH_test2/container/source_obj
     -H 'X-Auth-Token: <token>'
     -H 'Destination: /container/destination_obj'
     -H 'Destination-Account: AUTH_test1'
     -H 'Content-Length: 0'

-------------------
Large Object Copy
-------------------
The best option to copy a large object is to copy segments individually.
To copy the manifest object of a large object, add the query parameter to
the copy request::

    ?multipart-manifest=get

If a request is sent without the query parameter, an attempt will be made to
copy the whole object but will fail if the object size is
greater than 5GB.

"""

from swift.common.utils import get_logger, config_true_value, FileLikeIter, \
    close_if_possible
from swift.common.swob import Request, HTTPPreconditionFailed, \
    HTTPRequestEntityTooLarge, HTTPBadRequest, HTTPException, \
    wsgi_quote, wsgi_unquote
from swift.common.http import HTTP_MULTIPLE_CHOICES, is_success, HTTP_OK
from swift.common.constraints import check_account_format, MAX_FILE_SIZE
from swift.common.request_helpers import copy_header_subset, remove_items, \
    is_sys_meta, is_sys_or_user_meta, is_object_transient_sysmeta, \
    check_path_header, OBJECT_SYSMETA_CONTAINER_UPDATE_OVERRIDE_PREFIX
from swift.common.wsgi import WSGIContext, make_subrequest


def _check_copy_from_header(req):
    """
    Validate that the value from x-copy-from header is
    well formatted. We assume the caller ensures that
    x-copy-from header is present in req.headers.

    :param req: HTTP request object
    :returns: A tuple with container name and object name
    :raise HTTPPreconditionFailed: if x-copy-from value
            is not well formatted.
    """
    return check_path_header(req, 'X-Copy-From', 2,
                             'X-Copy-From header must be of the form '
                             '<container name>/<object name>')


def _check_destination_header(req):
    """
    Validate that the value from destination header is
    well formatted. We assume the caller ensures that
    destination header is present in req.headers.

    :param req: HTTP request object
    :returns: A tuple with container name and object name
    :raise HTTPPreconditionFailed: if destination value
            is not well formatted.
    """
    return check_path_header(req, 'Destination', 2,
                             'Destination header must be of the form '
                             '<container name>/<object name>')


def _copy_headers(src, dest):
    """
    Will copy desired headers from src to dest.

    :params src: an instance of collections.Mapping
    :params dest: an instance of collections.Mapping
    """
    for k, v in src.items():
        if (is_sys_or_user_meta('object', k) or
                is_object_transient_sysmeta(k) or
                k.lower() == 'x-delete-at'):
            dest[k] = v


class ServerSideCopyWebContext(WSGIContext):

    def __init__(self, app, logger):
        super(ServerSideCopyWebContext, self).__init__(app)
        self.app = app
        self.logger = logger

    def get_source_resp(self, req):
        sub_req = make_subrequest(
            req.environ, path=wsgi_quote(req.path_info), headers=req.headers,
            swift_source='SSC')
        return sub_req.get_response(self.app)

    def send_put_req(self, req, additional_resp_headers, start_response):
        app_resp = self._app_call(req.environ)
        self._adjust_put_response(req, additional_resp_headers)
        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        return app_resp

    def _adjust_put_response(self, req, additional_resp_headers):
        if is_success(self._get_status_int()):
            for header, value in additional_resp_headers.items():
                self._response_headers.append((header, value))

    def handle_OPTIONS_request(self, req, start_response):
        app_resp = self._app_call(req.environ)
        if is_success(self._get_status_int()):
            for i, (header, value) in enumerate(self._response_headers):
                if header.lower() == 'allow' and 'COPY' not in value:
                    self._response_headers[i] = ('Allow', value + ', COPY')
                if header.lower() == 'access-control-allow-methods' and \
                        'COPY' not in value:
                    self._response_headers[i] = \
                        ('Access-Control-Allow-Methods', value + ', COPY')
        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        return app_resp


class ServerSideCopyMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="copy")

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            (version, account, container, obj) = req.split_path(4, 4, True)
            is_obj_req = True
        except ValueError:
            is_obj_req = False
        if not is_obj_req:
            # If obj component is not present in req, do not proceed further.
            return self.app(env, start_response)

        try:
            # In some cases, save off original request method since it gets
            # mutated into PUT during handling. This way logging can display
            # the method the client actually sent.
            if req.method == 'PUT' and req.headers.get('X-Copy-From'):
                return self.handle_PUT(req, start_response)
            elif req.method == 'COPY':
                req.environ['swift.orig_req_method'] = req.method
                return self.handle_COPY(req, start_response,
                                        account, container, obj)
            elif req.method == 'OPTIONS':
                # Does not interfere with OPTIONS response from
                # (account,container) servers and /info response.
                return self.handle_OPTIONS(req, start_response)

        except HTTPException as e:
            return e(req.environ, start_response)

        return self.app(env, start_response)

    def handle_COPY(self, req, start_response, account, container, obj):
        if not req.headers.get('Destination'):
            return HTTPPreconditionFailed(request=req,
                                          body='Destination header required'
                                          )(req.environ, start_response)
        dest_account = account
        if 'Destination-Account' in req.headers:
            dest_account = wsgi_unquote(req.headers.get('Destination-Account'))
            dest_account = check_account_format(req, dest_account)
            req.headers['X-Copy-From-Account'] = wsgi_quote(account)
            account = dest_account
            del req.headers['Destination-Account']
        dest_container, dest_object = _check_destination_header(req)
        source = '/%s/%s' % (container, obj)
        container = dest_container
        obj = dest_object
        # re-write the existing request as a PUT instead of creating a new one
        req.method = 'PUT'
        # As this the path info is updated with destination container,
        # the proxy server app will use the right object controller
        # implementation corresponding to the container's policy type.
        ver, _junk = req.split_path(1, 2, rest_with_last=True)
        req.path_info = '/%s/%s/%s/%s' % (
            ver, dest_account, dest_container, dest_object)
        req.headers['Content-Length'] = 0
        req.headers['X-Copy-From'] = wsgi_quote(source)
        del req.headers['Destination']
        return self.handle_PUT(req, start_response)

    def _get_source_object(self, ssc_ctx, source_path, req):
        source_req = req.copy_get()

        # make sure the source request uses it's container_info
        source_req.headers.pop('X-Backend-Storage-Policy-Index', None)
        source_req.path_info = source_path
        source_req.headers['X-Newest'] = 'true'

        # in case we are copying an SLO manifest, set format=raw parameter
        params = source_req.params
        if params.get('multipart-manifest') == 'get':
            params['format'] = 'raw'
            source_req.params = params

        source_resp = ssc_ctx.get_source_resp(source_req)

        if source_resp.content_length is None:
            # This indicates a transfer-encoding: chunked source object,
            # which currently only happens because there are more than
            # CONTAINER_LISTING_LIMIT segments in a segmented object. In
            # this case, we're going to refuse to do the server-side copy.
            close_if_possible(source_resp.app_iter)
            return HTTPRequestEntityTooLarge(request=req)

        if source_resp.content_length > MAX_FILE_SIZE:
            close_if_possible(source_resp.app_iter)
            return HTTPRequestEntityTooLarge(request=req)

        return source_resp

    def _create_response_headers(self, source_path, source_resp, sink_req):
        resp_headers = dict()
        acct, path = source_path.split('/', 3)[2:4]
        resp_headers['X-Copied-From-Account'] = wsgi_quote(acct)
        resp_headers['X-Copied-From'] = wsgi_quote(path)
        if 'last-modified' in source_resp.headers:
            resp_headers['X-Copied-From-Last-Modified'] = \
                source_resp.headers['last-modified']
        if 'X-Object-Version-Id' in source_resp.headers:
            resp_headers['X-Copied-From-Version-Id'] = \
                source_resp.headers['X-Object-Version-Id']
        # Existing sys and user meta of source object is added to response
        # headers in addition to the new ones.
        _copy_headers(sink_req.headers, resp_headers)
        return resp_headers

    def handle_PUT(self, req, start_response):
        if req.content_length:
            return HTTPBadRequest(body='Copy requests require a zero byte '
                                  'body', request=req,
                                  content_type='text/plain')(req.environ,
                                                             start_response)

        # Form the path of source object to be fetched
        ver, acct, _rest = req.split_path(2, 3, True)
        src_account_name = req.headers.get('X-Copy-From-Account')
        if src_account_name:
            src_account_name = check_account_format(
                req, wsgi_unquote(src_account_name))
        else:
            src_account_name = acct
        src_container_name, src_obj_name = _check_copy_from_header(req)
        source_path = '/%s/%s/%s/%s' % (ver, src_account_name,
                                        src_container_name, src_obj_name)

        # GET the source object, bail out on error
        ssc_ctx = ServerSideCopyWebContext(self.app, self.logger)
        source_resp = self._get_source_object(ssc_ctx, source_path, req)
        if source_resp.status_int >= HTTP_MULTIPLE_CHOICES:
            return source_resp(source_resp.environ, start_response)

        # Create a new Request object based on the original request instance.
        # This will preserve original request environ including headers.
        sink_req = Request.blank(req.path_info, environ=req.environ)

        def is_object_sysmeta(k):
            return is_sys_meta('object', k)

        if config_true_value(req.headers.get('x-fresh-metadata', 'false')):
            # x-fresh-metadata only applies to copy, not post-as-copy: ignore
            # existing user metadata, update existing sysmeta with new
            copy_header_subset(source_resp, sink_req, is_object_sysmeta)
            copy_header_subset(req, sink_req, is_object_sysmeta)
        else:
            # First copy existing sysmeta, user meta and other headers from the
            # source to the sink, apart from headers that are conditionally
            # copied below and timestamps.
            exclude_headers = ('x-static-large-object', 'x-object-manifest',
                               'etag', 'content-type', 'x-timestamp',
                               'x-backend-timestamp')
            copy_header_subset(source_resp, sink_req,
                               lambda k: k.lower() not in exclude_headers)
            # now update with original req headers
            sink_req.headers.update(req.headers)

        params = sink_req.params
        params_updated = False

        if params.get('multipart-manifest') == 'get':
            if 'X-Static-Large-Object' in source_resp.headers:
                params['multipart-manifest'] = 'put'
            if 'X-Object-Manifest' in source_resp.headers:
                del params['multipart-manifest']
                sink_req.headers['X-Object-Manifest'] = \
                    source_resp.headers['X-Object-Manifest']
            params_updated = True

        if 'version-id' in params:
            del params['version-id']
            params_updated = True

        if params_updated:
            sink_req.params = params

        # Set swift.source, data source, content length and etag
        # for the PUT request
        sink_req.environ['swift.source'] = 'SSC'
        sink_req.environ['wsgi.input'] = FileLikeIter(source_resp.app_iter)
        sink_req.content_length = source_resp.content_length
        if (source_resp.status_int == HTTP_OK and
                'X-Static-Large-Object' not in source_resp.headers and
                ('X-Object-Manifest' not in source_resp.headers or
                 req.params.get('multipart-manifest') == 'get')):
            # copy source etag so that copied content is verified, unless:
            #  - not a 200 OK response: source etag may not match the actual
            #    content, for example with a 206 Partial Content response to a
            #    ranged request
            #  - SLO manifest: etag cannot be specified in manifest PUT; SLO
            #    generates its own etag value which may differ from source
            #  - SLO: etag in SLO response is not hash of actual content
            #  - DLO: etag in DLO response is not hash of actual content
            sink_req.headers['Etag'] = source_resp.etag
        else:
            # since we're not copying the source etag, make sure that any
            # container update override values are not copied.
            remove_items(sink_req.headers, lambda k: k.startswith(
                OBJECT_SYSMETA_CONTAINER_UPDATE_OVERRIDE_PREFIX.title()))

        # We no longer need these headers
        sink_req.headers.pop('X-Copy-From', None)
        sink_req.headers.pop('X-Copy-From-Account', None)

        # If the copy request does not explicitly override content-type,
        # use the one present in the source object.
        if not req.headers.get('content-type'):
            sink_req.headers['Content-Type'] = \
                source_resp.headers['Content-Type']

        # Create response headers for PUT response
        resp_headers = self._create_response_headers(source_path,
                                                     source_resp, sink_req)

        put_resp = ssc_ctx.send_put_req(sink_req, resp_headers, start_response)
        close_if_possible(source_resp.app_iter)
        return put_resp

    def handle_OPTIONS(self, req, start_response):
        return ServerSideCopyWebContext(self.app, self.logger).\
            handle_OPTIONS_request(req, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def copy_filter(app):
        return ServerSideCopyMiddleware(app, conf)

    return copy_filter
