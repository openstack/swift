# Copyright (c) 2010-2017 OpenStack Foundation
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
Symlink Middleware

Symlinks are objects stored in Swift that contain a reference to another
object (hereinafter, this is called "target object"). They are analogous to
symbolic links in Unix-like operating systems. The existence of a symlink
object does not affect the target object in any way. An important use case is
to use a path in one container to access an object in a different container,
with a different policy. This allows policy cost/performance trade-offs to be
made on individual objects.

Clients create a Swift symlink by performing a zero-length PUT request
with the header ``X-Symlink-Target: <container>/<object>``. For a cross-account
symlink, the header ``X-Symlink-Target-Account: <account>`` must be included.
If omitted, it is inserted automatically with the account of the symlink
object in the PUT request process.

Symlinks must be zero-byte objects. Attempting to PUT a symlink with a
non-empty request body will result in a 400-series error. Also, POST with
``X-Symlink-Target`` header always results in a 400-series error. The target
object need not exist at symlink creation time.

Clients may optionally include a ``X-Symlink-Target-Etag: <etag>`` header
during the PUT. If present, this will create a "static symlink" instead of a
"dynamic symlink".  Static symlinks point to a specific object rather than a
specific name.  They do this by using the value set in their
``X-Symlink-Target-Etag`` header when created to verify it still matches the
ETag of the object they're pointing at on a GET.  In contrast to a dynamic
symlink the target object referenced in the ``X-Symlink-Target`` header must
exist and its ETag must match the ``X-Symlink-Target-Etag`` or the symlink
creation will return a client error.

A GET/HEAD request to a symlink will result in a request to the target
object referenced by the symlink's ``X-Symlink-Target-Account`` and
``X-Symlink-Target`` headers. The response of the GET/HEAD request will contain
a ``Content-Location`` header with the path location of the target object. A
GET/HEAD request to a symlink with the query parameter ``?symlink=get`` will
result in the request targeting the symlink itself.

A symlink can point to another symlink. Chained symlinks will be traversed
until the target is not a symlink. If the number of chained symlinks exceeds
the limit ``symloop_max`` an error response will be produced. The value of
``symloop_max`` can be defined in the symlink config section of
`proxy-server.conf`. If not specified, the default ``symloop_max`` value is 2.
If a value less than 1 is specified, the default value will be used.

If a static symlink (i.e. a symlink created with a ``X-Symlink-Target-Etag``
header) targets another static symlink, both of the ``X-Symlink-Target-Etag``
headers must match the target object for the GET to succeed.  If a static
symlink targets a dynamic symlink (i.e. a symlink created without a
``X-Symlink-Target-Etag`` header) then the ``X-Symlink-Target-Etag`` header of
the static symlink must be the Etag of the zero-byte object.  If a symlink with
a ``X-Symlink-Target-Etag`` targets a large object manifest it must match the
ETag of the manifest (e.g. the ETag as returned by ``multipart-manifest=get``
or value in the ``X-Manifest-Etag`` header).

A HEAD/GET request to a symlink object behaves as a normal HEAD/GET request
to the target object. Therefore issuing a HEAD request to the symlink will
return the target metadata, and issuing a GET request to the symlink will
return the data and metadata of the target object. To return the symlink
metadata (with its empty body) a GET/HEAD request with the ``?symlink=get``
query parameter must be sent to a symlink object.

A POST request to a symlink will result in a 307 Temporary Redirect response.
The response will contain a ``Location`` header with the path of the target
object as the value. The request is never redirected to the target object by
Swift. Nevertheless, the metadata in the POST request will be applied to the
symlink because object servers cannot know for sure if the current object is a
symlink or not in eventual consistency.

A symlink's ``Content-Type`` is completely independent from its target.  As a
convenience Swift will automatically set the ``Content-Type`` on a symlink PUT
if not explicitly set by the client.  If the client sends a
``X-Symlink-Target-Etag`` Swift will set the symlink's ``Content-Type`` to that
of the target, otherwise it will be set to ``application/symlink``.  You can
review a symlink's ``Content-Type`` using the ``?symlink=get`` interface.  You
can change a symlink's ``Content-Type`` using a POST request.  The symlink's
``Content-Type`` will appear in the container listing.

A DELETE request to a symlink will delete the symlink itself. The target
object will not be deleted.

A COPY request, or a PUT request with a ``X-Copy-From`` header, to a symlink
will copy the target object. The same request to a symlink with the query
parameter ``?symlink=get`` will copy the symlink itself.

An OPTIONS request to a symlink will respond with the options for the symlink
only; the request will not be redirected to the target object. Please note that
if the symlink's target object is in another container with CORS settings, the
response will not reflect the settings.

Tempurls can be used to GET/HEAD symlink objects, but PUT is not allowed and
will result in a 400-series error. The GET/HEAD tempurls honor the scope of
the tempurl key. Container tempurl will only work on symlinks where the target
container is the same as the symlink. In case a symlink targets an object
in a different container, a GET/HEAD request will result in a 401 Unauthorized
error. The account level tempurl will allow cross-container symlinks, but not
cross-account symlinks.

If a symlink object is overwritten while it is in a versioned container, the
symlink object itself is versioned, not the referenced object.

A GET request with query parameter ``?format=json`` to a container which
contains symlinks will respond with additional information ``symlink_path``
for each symlink object in the container listing. The ``symlink_path`` value
is the target path of the symlink. Clients can differentiate symlinks and
other objects by this function. Note that responses in any other format
(e.g. ``?format=xml``) won't include ``symlink_path`` info.  If a
``X-Symlink-Target-Etag`` header was included on the symlink, JSON container
listings will include that value in a ``symlink_etag`` key and the target
object's ``Content-Length`` will be included in the key ``symlink_bytes``.

If a static symlink targets a static large object manifest it will carry
forward the SLO's size and slo_etag in the container listing using the
``symlink_bytes`` and ``slo_etag`` keys.  However, manifests created before
swift v2.12.0 (released Dec 2016) do not contain enough metadata to propagate
the extra SLO information to the listing.  Clients may recreate the manifest
(COPY w/ ``?multipart-manfiest=get``) before creating a static symlink to add
the requisite metadata.

Errors

* PUT with the header ``X-Symlink-Target`` with non-zero Content-Length
  will produce a 400 BadRequest error.

* POST with the header ``X-Symlink-Target`` will produce a
  400 BadRequest error.

* GET/HEAD traversing more than ``symloop_max`` chained symlinks will
  produce a 409 Conflict error.

* PUT/GET/HEAD on a symlink that inclues a ``X-Symlink-Target-Etag`` header
  that does not match the target will poduce a 409 Conflict error.

* POSTs will produce a 307 Temporary Redirect error.

----------
Deployment
----------

Symlinks are enabled by adding the `symlink` middleware to the proxy server
WSGI pipeline and including a corresponding filter configuration section in the
`proxy-server.conf` file. The `symlink` middleware should be placed after
`slo`, `dlo` and `versioned_writes` middleware, but before `encryption`
middleware in the pipeline. See the `proxy-server.conf-sample` file for further
details. :ref:`Additional steps <symlink_container_sync_client_config>` are
required if the container sync feature is being used.

.. note::

    Once you have deployed `symlink` middleware in your pipeline, you should
    neither remove the `symlink` middleware nor downgrade swift to a version
    earlier than symlinks being supported. Doing so may result in unexpected
    container listing results in addition to symlink objects behaving like a
    normal object.

.. _symlink_container_sync_client_config:

Container sync configuration
----------------------------

If container sync is being used then the `symlink` middleware
must be added to the container sync internal client pipeline. The following
configuration steps are required:

#. Create a custom internal client configuration file for container sync (if
   one is not already in use) based on the sample file
   `internal-client.conf-sample`. For example, copy
   `internal-client.conf-sample` to `/etc/swift/container-sync-client.conf`.
#. Modify this file to include the `symlink` middleware in the pipeline in
   the same way as described above for the proxy server.
#. Modify the container-sync section of all container server config files to
   point to this internal client config file using the
   ``internal_client_conf_path`` option. For example::

     internal_client_conf_path = /etc/swift/container-sync-client.conf

.. note::

    These container sync configuration steps will be necessary for container
    sync probe tests to pass if the `symlink` middleware is included in the
    proxy pipeline of a test cluster.
"""

import json
import os

from swift.common.utils import get_logger, split_path, \
    MD5_OF_EMPTY_STRING, close_if_possible, closing_if_possible, \
    config_true_value, drain_and_close, parse_header
from swift.common.registry import register_swift_info
from swift.common.constraints import check_account_format
from swift.common.wsgi import WSGIContext, make_subrequest, \
    make_pre_authed_request
from swift.common.request_helpers import get_sys_meta_prefix, \
    check_path_header, get_container_update_override_key, \
    update_ignore_range_header
from swift.common.swob import Request, HTTPBadRequest, HTTPTemporaryRedirect, \
    HTTPException, HTTPConflict, HTTPPreconditionFailed, wsgi_quote, \
    wsgi_unquote, status_map, normalize_etag
from swift.common.http import is_success, HTTP_NOT_FOUND
from swift.common.exceptions import LinkIterError
from swift.common.header_key_dict import HeaderKeyDict

DEFAULT_SYMLOOP_MAX = 2
# Header values for symlink target path strings will be quoted values.
TGT_OBJ_SYMLINK_HDR = 'x-symlink-target'
TGT_ACCT_SYMLINK_HDR = 'x-symlink-target-account'
TGT_ETAG_SYMLINK_HDR = 'x-symlink-target-etag'
TGT_BYTES_SYMLINK_HDR = 'x-symlink-target-bytes'
TGT_OBJ_SYSMETA_SYMLINK_HDR = get_sys_meta_prefix('object') + 'symlink-target'
TGT_ACCT_SYSMETA_SYMLINK_HDR = \
    get_sys_meta_prefix('object') + 'symlink-target-account'
TGT_ETAG_SYSMETA_SYMLINK_HDR = \
    get_sys_meta_prefix('object') + 'symlink-target-etag'
TGT_BYTES_SYSMETA_SYMLINK_HDR = \
    get_sys_meta_prefix('object') + 'symlink-target-bytes'
SYMLOOP_EXTEND = get_sys_meta_prefix('object') + 'symloop-extend'
ALLOW_RESERVED_NAMES = get_sys_meta_prefix('object') + 'allow-reserved-names'


def _validate_and_prep_request_headers(req):
    """
    Validate that the value from x-symlink-target header is well formatted
    and that the x-symlink-target-etag header (if present) does not contain
    problematic characters. We assume the caller ensures that
    x-symlink-target header is present in req.headers.

    :param req: HTTP request object
    :returns: a tuple, the full versioned path to the object (as a WSGI string)
              and the X-Symlink-Target-Etag header value which may be None
    :raise: HTTPPreconditionFailed if x-symlink-target value
            is not well formatted.
    :raise: HTTPBadRequest if the x-symlink-target value points to the request
            path.
    :raise: HTTPBadRequest if the x-symlink-target-etag value contains
            a semicolon, double-quote, or backslash.
    """
    # N.B. check_path_header doesn't assert the leading slash and
    # copy middleware may accept the format. In the symlink, API
    # says apparently to use "container/object" format so add the
    # validation first, here.
    error_body = 'X-Symlink-Target header must be of the form ' \
                 '<container name>/<object name>'
    if wsgi_unquote(req.headers[TGT_OBJ_SYMLINK_HDR]).startswith('/'):
        raise HTTPPreconditionFailed(
            body=error_body,
            request=req, content_type='text/plain')

    # check container and object format
    container, obj = check_path_header(
        req, TGT_OBJ_SYMLINK_HDR, 2,
        error_body)
    req.headers[TGT_OBJ_SYMLINK_HDR] = wsgi_quote('%s/%s' % (container, obj))

    # Check account format if it exists
    account = check_account_format(
        req, wsgi_unquote(req.headers[TGT_ACCT_SYMLINK_HDR])) \
        if TGT_ACCT_SYMLINK_HDR in req.headers else None

    # Extract request path
    _junk, req_acc, req_cont, req_obj = req.split_path(4, 4, True)

    if account:
        req.headers[TGT_ACCT_SYMLINK_HDR] = wsgi_quote(account)
    else:
        account = req_acc

    # Check if symlink targets the symlink itself or not
    if (account, container, obj) == (req_acc, req_cont, req_obj):
        raise HTTPBadRequest(
            body='Symlink cannot target itself',
            request=req, content_type='text/plain')
    etag = normalize_etag(req.headers.get(TGT_ETAG_SYMLINK_HDR, None))
    if etag and any(c in etag for c in ';"\\'):
        # See utils.parse_header for why the above chars are problematic
        raise HTTPBadRequest(
            body='Bad %s format' % TGT_ETAG_SYMLINK_HDR.title(),
            request=req, content_type='text/plain')
    if not (etag or req.headers.get('Content-Type')):
        req.headers['Content-Type'] = 'application/symlink'
    return '/v1/%s/%s/%s' % (account, container, obj), etag


def symlink_usermeta_to_sysmeta(headers):
    """
    Helper function to translate from client-facing X-Symlink-* headers
    to cluster-facing X-Object-Sysmeta-Symlink-* headers.

    :param headers: request headers dict. Note that the headers dict
        will be updated directly.
    """
    # To preseve url-encoded value in the symlink header, use raw value
    for user_hdr, sysmeta_hdr in (
            (TGT_OBJ_SYMLINK_HDR, TGT_OBJ_SYSMETA_SYMLINK_HDR),
            (TGT_ACCT_SYMLINK_HDR, TGT_ACCT_SYSMETA_SYMLINK_HDR)):
        if user_hdr in headers:
            headers[sysmeta_hdr] = headers.pop(user_hdr)


def symlink_sysmeta_to_usermeta(headers):
    """
    Helper function to translate from cluster-facing
    X-Object-Sysmeta-Symlink-* headers to client-facing X-Symlink-* headers.

    :param headers: request headers dict. Note that the headers dict
        will be updated directly.
    """
    for user_hdr, sysmeta_hdr in (
            (TGT_OBJ_SYMLINK_HDR, TGT_OBJ_SYSMETA_SYMLINK_HDR),
            (TGT_ACCT_SYMLINK_HDR, TGT_ACCT_SYSMETA_SYMLINK_HDR),
            (TGT_ETAG_SYMLINK_HDR, TGT_ETAG_SYSMETA_SYMLINK_HDR),
            (TGT_BYTES_SYMLINK_HDR, TGT_BYTES_SYSMETA_SYMLINK_HDR)):
        if sysmeta_hdr in headers:
            headers[user_hdr] = headers.pop(sysmeta_hdr)


class SymlinkContainerContext(WSGIContext):
    def __init__(self, wsgi_app, logger):
        super(SymlinkContainerContext, self).__init__(wsgi_app)
        self.logger = logger

    def handle_container(self, req, start_response):
        """
        Handle container requests.

        :param req: a :class:`~swift.common.swob.Request`
        :param start_response: start_response function

        :return: Response Iterator after start_response called.
        """
        app_resp = self._app_call(req.environ)

        if req.method == 'GET' and is_success(self._get_status_int()):
            app_resp = self._process_json_resp(app_resp, req)

        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)

        return app_resp

    def _process_json_resp(self, resp_iter, req):
        """
        Iterate through json body looking for symlinks and modify its content
        :return: modified json body
        """
        with closing_if_possible(resp_iter):
            resp_body = b''.join(resp_iter)
        body_json = json.loads(resp_body)
        swift_version, account, _junk = split_path(req.path, 2, 3, True)
        new_body = json.dumps(
            [self._extract_symlink_path_json(obj_dict, swift_version, account)
             for obj_dict in body_json]).encode('ascii')
        self.update_content_length(len(new_body))
        return [new_body]

    def _extract_symlink_path_json(self, obj_dict, swift_version, account):
        """
        Extract the symlink info from the hash value
        :return: object dictionary with additional key:value pairs when object
                 is a symlink. i.e. new symlink_path, symlink_etag and
                 symlink_bytes keys
        """
        if 'hash' in obj_dict:
            hash_value, meta = parse_header(obj_dict['hash'])
            obj_dict['hash'] = hash_value
            target = None
            for key in meta:
                if key == 'symlink_target':
                    target = meta[key]
                elif key == 'symlink_target_account':
                    account = meta[key]
                elif key == 'symlink_target_etag':
                    obj_dict['symlink_etag'] = meta[key]
                elif key == 'symlink_target_bytes':
                    obj_dict['symlink_bytes'] = int(meta[key])
                else:
                    # make sure to add all other (key, values) back in place
                    obj_dict['hash'] += '; %s=%s' % (key, meta[key])
            else:
                if target:
                    obj_dict['symlink_path'] = os.path.join(
                        '/', swift_version, account, target)

        return obj_dict


class SymlinkObjectContext(WSGIContext):

    def __init__(self, wsgi_app, logger, symloop_max):
        super(SymlinkObjectContext, self).__init__(wsgi_app)
        self.symloop_max = symloop_max
        self.logger = logger
        # N.B. _loop_count and _last_target_path are used to keep
        # the statement in the _recursive_get. Hence they should not be touched
        # from other resources.
        self._loop_count = 0
        self._last_target_path = None

    def handle_get_head_symlink(self, req):
        """
        Handle get/head request when client sent parameter ?symlink=get

        :param req: HTTP GET or HEAD object request with param ?symlink=get
        :returns: Response Iterator
        """
        resp = self._app_call(req.environ)
        response_header_dict = HeaderKeyDict(self._response_headers)
        symlink_sysmeta_to_usermeta(response_header_dict)
        self._response_headers = list(response_header_dict.items())
        return resp

    def handle_get_head(self, req):
        """
        Handle get/head request and in case the response is a symlink,
        redirect request to target object.

        :param req: HTTP GET or HEAD object request
        :returns: Response Iterator
        """
        update_ignore_range_header(req, TGT_OBJ_SYSMETA_SYMLINK_HDR)
        try:
            return self._recursive_get_head(req)
        except LinkIterError:
            errmsg = 'Too many levels of symbolic links, ' \
                     'maximum allowed is %d' % self.symloop_max
            raise HTTPConflict(body=errmsg, request=req,
                               content_type='text/plain')

    def _recursive_get_head(self, req, target_etag=None,
                            follow_softlinks=True, orig_req=None):
        if not orig_req:
            orig_req = req
        resp = self._app_call(req.environ)

        def build_traversal_req(symlink_target):
            """
            :returns: new request for target path if it's symlink otherwise
                      None
            """
            version, account, _junk = req.split_path(2, 3, True)
            account = self._response_header_value(
                TGT_ACCT_SYSMETA_SYMLINK_HDR) or wsgi_quote(account)
            target_path = os.path.join(
                '/', version, account,
                symlink_target.lstrip('/'))
            self._last_target_path = target_path

            subreq_headers = dict(req.headers)
            if self._response_header_value(ALLOW_RESERVED_NAMES):
                # this symlink's sysmeta says it can point to reserved names,
                # we're infering that some piece of middleware had previously
                # authorized this request because users can't access reserved
                # names directly
                subreq_meth = make_pre_authed_request
                subreq_headers['X-Backend-Allow-Reserved-Names'] = 'true'
            else:
                subreq_meth = make_subrequest
            new_req = subreq_meth(orig_req.environ, path=target_path,
                                  method=req.method, headers=subreq_headers,
                                  swift_source='SYM')
            new_req.headers.pop('X-Backend-Storage-Policy-Index', None)
            return new_req

        symlink_target = self._response_header_value(
            TGT_OBJ_SYSMETA_SYMLINK_HDR)
        resp_etag = self._response_header_value(
            TGT_ETAG_SYSMETA_SYMLINK_HDR)
        if symlink_target and (resp_etag or follow_softlinks):
            # Should be a zero-byte object
            drain_and_close(resp)
            found_etag = resp_etag or self._response_header_value('etag')
            if target_etag and target_etag != found_etag:
                raise HTTPConflict(
                    body='X-Symlink-Target-Etag headers do not match',
                    headers={
                        'Content-Type': 'text/plain',
                        'Content-Location': self._last_target_path})
            if self._loop_count >= self.symloop_max:
                raise LinkIterError()
            # format: /<account name>/<container name>/<object name>
            new_req = build_traversal_req(symlink_target)
            if not config_true_value(
                    self._response_header_value(SYMLOOP_EXTEND)):
                self._loop_count += 1
            return self._recursive_get_head(new_req, target_etag=resp_etag,
                                            orig_req=req)
        else:
            final_etag = self._response_header_value('etag')
            if final_etag and target_etag and target_etag != final_etag:
                # do *not* drain; we don't know how big this is
                close_if_possible(resp)
                body = ('Object Etag %r does not match '
                        'X-Symlink-Target-Etag header %r')
                raise HTTPConflict(
                    body=body % (final_etag, target_etag),
                    headers={
                        'Content-Type': 'text/plain',
                        'Content-Location': self._last_target_path})

            if self._last_target_path:
                # Content-Location will be applied only when one or more
                # symlink recursion occurred.
                # In this case, Content-Location is applied to show which
                # object path caused the error response.
                # To preserve '%2F'(= quote('/')) in X-Symlink-Target
                # header value as it is, Content-Location value comes from
                # TGT_OBJ_SYMLINK_HDR, not req.path
                self._response_headers.extend(
                    [('Content-Location', self._last_target_path)])

            return resp

    def _validate_etag_and_update_sysmeta(self, req, symlink_target_path,
                                          etag):
        if req.environ.get('swift.symlink_override'):
            req.headers[TGT_ETAG_SYSMETA_SYMLINK_HDR] = etag
            req.headers[TGT_BYTES_SYSMETA_SYMLINK_HDR] = \
                req.headers[TGT_BYTES_SYMLINK_HDR]
            return

        # next we'll make sure the E-Tag matches a real object
        new_req = make_subrequest(
            req.environ, path=wsgi_quote(symlink_target_path), method='HEAD',
            swift_source='SYM')
        if req.allow_reserved_names:
            new_req.headers['X-Backend-Allow-Reserved-Names'] = 'true'
        self._last_target_path = symlink_target_path
        resp = self._recursive_get_head(new_req, target_etag=etag,
                                        follow_softlinks=False)
        if self._get_status_int() == HTTP_NOT_FOUND:
            raise HTTPConflict(
                body='X-Symlink-Target does not exist',
                request=req,
                headers={
                    'Content-Type': 'text/plain',
                    'Content-Location': self._last_target_path})
        if not is_success(self._get_status_int()):
            drain_and_close(resp)
            raise status_map[self._get_status_int()](request=req)
        response_headers = HeaderKeyDict(self._response_headers)
        # carry forward any etag update params (e.g. "slo_etag"), we'll append
        # symlink_target_* params to this header after this method returns
        override_header = get_container_update_override_key('etag')
        if override_header in response_headers and \
                override_header not in req.headers:
            sep, params = response_headers[override_header].partition(';')[1:]
            req.headers[override_header] = MD5_OF_EMPTY_STRING + sep + params

        # It's troublesome that there's so much leakage with SLO
        if 'X-Object-Sysmeta-Slo-Etag' in response_headers and \
                override_header not in req.headers:
            req.headers[override_header] = '%s; slo_etag=%s' % (
                MD5_OF_EMPTY_STRING,
                response_headers['X-Object-Sysmeta-Slo-Etag'])
        req.headers[TGT_BYTES_SYSMETA_SYMLINK_HDR] = (
            response_headers.get('x-object-sysmeta-slo-size') or
            response_headers['Content-Length'])

        req.headers[TGT_ETAG_SYSMETA_SYMLINK_HDR] = etag

        if not req.headers.get('Content-Type'):
            req.headers['Content-Type'] = response_headers['Content-Type']

    def handle_put(self, req):
        """
        Handle put request when it contains X-Symlink-Target header.

        Symlink headers are validated and moved to sysmeta namespace.
        :param req: HTTP PUT object request
        :returns: Response Iterator
        """
        if req.content_length is None:
            has_body = (req.body_file.read(1) != b'')
        else:
            has_body = (req.content_length != 0)
        if has_body:
            raise HTTPBadRequest(
                body='Symlink requests require a zero byte body',
                request=req,
                content_type='text/plain')

        symlink_target_path, etag = _validate_and_prep_request_headers(req)
        if etag:
            self._validate_etag_and_update_sysmeta(
                req, symlink_target_path, etag)
        # N.B. TGT_ETAG_SYMLINK_HDR was converted as part of verifying it
        symlink_usermeta_to_sysmeta(req.headers)
        # Store info in container update that this object is a symlink.
        # We have a design decision to use etag space to store symlink info for
        # object listing because it's immutable unless the object is
        # overwritten. This may impact the downgrade scenario that the symlink
        # info can appear as the suffix in the hash value of object
        # listing result for clients.
        # To create override etag easily, we have a constraint that the symlink
        # must be 0 byte so we can add etag of the empty string + symlink info
        # here, simply (if no other override etag was provided). Note that this
        # override etag may be encrypted in the container db by encryption
        # middleware.

        etag_override = [
            req.headers.get(get_container_update_override_key('etag'),
                            MD5_OF_EMPTY_STRING),
            'symlink_target=%s' % req.headers[TGT_OBJ_SYSMETA_SYMLINK_HDR]
        ]
        if TGT_ACCT_SYSMETA_SYMLINK_HDR in req.headers:
            etag_override.append(
                'symlink_target_account=%s' %
                req.headers[TGT_ACCT_SYSMETA_SYMLINK_HDR])
        if TGT_ETAG_SYSMETA_SYMLINK_HDR in req.headers:
            # if _validate_etag_and_update_sysmeta or a middleware sets
            # TGT_ETAG_SYSMETA_SYMLINK_HDR then they need to also set
            # TGT_BYTES_SYSMETA_SYMLINK_HDR.  If they forget, they get a
            # KeyError traceback and client gets a ServerError
            etag_override.extend([
                'symlink_target_etag=%s' %
                req.headers[TGT_ETAG_SYSMETA_SYMLINK_HDR],
                'symlink_target_bytes=%s' %
                req.headers[TGT_BYTES_SYSMETA_SYMLINK_HDR],
            ])
        req.headers[get_container_update_override_key('etag')] = \
            '; '.join(etag_override)

        return self._app_call(req.environ)

    def handle_post(self, req):
        """
        Handle post request. If POSTing to a symlink, a HTTPTemporaryRedirect
        error message is returned to client.

        Clients that POST to symlinks should understand that the POST is not
        redirected to the target object like in a HEAD/GET request. POSTs to a
        symlink will be handled just like a normal object by the object server.
        It cannot reject it because it may not have symlink state when the POST
        lands.  The object server has no knowledge of what is a symlink object
        is. On the other hand, on POST requests, the object server returns all
        sysmeta of the object. This method uses that sysmeta to determine if
        the stored object is a symlink or not.

        :param req: HTTP POST object request
        :raises: HTTPTemporaryRedirect if POSTing to a symlink.
        :returns: Response Iterator
        """
        if TGT_OBJ_SYMLINK_HDR in req.headers:
            raise HTTPBadRequest(
                body='A PUT request is required to set a symlink target',
                request=req,
                content_type='text/plain')

        resp = self._app_call(req.environ)
        if not is_success(self._get_status_int()):
            return resp

        tgt_co = self._response_header_value(TGT_OBJ_SYSMETA_SYMLINK_HDR)
        if tgt_co:
            version, account, _junk = req.split_path(2, 3, True)
            target_acc = self._response_header_value(
                TGT_ACCT_SYSMETA_SYMLINK_HDR) or wsgi_quote(account)
            location_hdr = os.path.join(
                '/', version, target_acc, tgt_co)
            headers = {'location': location_hdr}
            tgt_etag = self._response_header_value(
                TGT_ETAG_SYSMETA_SYMLINK_HDR)
            if tgt_etag:
                headers[TGT_ETAG_SYMLINK_HDR] = tgt_etag
            req.environ['swift.leave_relative_location'] = True
            errmsg = 'The requested POST was applied to a symlink. POST ' +\
                     'directly to the target to apply requested metadata.'
            for key, value in self._response_headers:
                if key.lower().startswith('x-object-sysmeta-'):
                    headers[key] = value
            raise HTTPTemporaryRedirect(
                body=errmsg, headers=headers)
        else:
            return resp

    def handle_object(self, req, start_response):
        """
        Handle object requests.

        :param req: a :class:`~swift.common.swob.Request`
        :param start_response: start_response function
        :returns: Response Iterator after start_response has been called
        """
        if req.method in ('GET', 'HEAD'):
            if req.params.get('symlink') == 'get':
                resp = self.handle_get_head_symlink(req)
            else:
                resp = self.handle_get_head(req)
        elif req.method == 'PUT' and (TGT_OBJ_SYMLINK_HDR in req.headers):
            resp = self.handle_put(req)
        elif req.method == 'POST':
            resp = self.handle_post(req)
        else:
            # DELETE and OPTIONS reqs for a symlink and
            # PUT reqs without X-Symlink-Target behave like any other object
            resp = self._app_call(req.environ)

        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)

        return resp


class SymlinkMiddleware(object):
    """
    Middleware that implements symlinks.

    Symlinks are objects stored in Swift that contain a reference to another
    object (i.e., the target object). An important use case is to use a path in
    one container to access an object in a different container, with a
    different policy. This allows policy cost/performance trade-offs to be made
    on individual objects.
    """

    def __init__(self, app, conf, symloop_max):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='symlink')
        self.symloop_max = symloop_max

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            version, acc, cont, obj = req.split_path(3, 4, True)
            is_cont_or_obj_req = True
        except ValueError:
            is_cont_or_obj_req = False
        if not is_cont_or_obj_req:
            return self.app(env, start_response)

        try:
            if obj:
                # object context
                context = SymlinkObjectContext(self.app, self.logger,
                                               self.symloop_max)
                return context.handle_object(req, start_response)
            else:
                # container context
                context = SymlinkContainerContext(self.app, self.logger)
                return context.handle_container(req, start_response)
        except HTTPException as err_resp:
            return err_resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    symloop_max = int(conf.get('symloop_max', DEFAULT_SYMLOOP_MAX))
    if symloop_max < 1:
        symloop_max = int(DEFAULT_SYMLOOP_MAX)
    register_swift_info('symlink', symloop_max=symloop_max, static_links=True)

    def symlink_mw(app):
        return SymlinkMiddleware(app, conf, symloop_max)
    return symlink_mw
