# Copyright (c) 2014 OpenStack Foundation
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
Object versioning in swift is implemented by setting a flag on the container
to tell swift to version all objects in the container. The flag is the
``X-Versions-Location`` header on the container, and its value is the
container where the versions are stored. It is recommended to use a different
``X-Versions-Location`` container for each container that is being versioned.

When data is ``PUT`` into a versioned container (a container with the
versioning flag turned on), the existing data in the file is redirected to a
new object and the data in the ``PUT`` request is saved as the data for the
versioned object. The new object name (for the previous version) is
``<versions_container>/<length><object_name>/<timestamp>``, where ``length``
is the 3-character zero-padded hexadecimal length of the ``<object_name>`` and
``<timestamp>`` is the timestamp of when the previous version was created.

A ``GET`` to a versioned object will return the current version of the object
without having to do any request redirects or metadata lookups.

A ``POST`` to a versioned object will update the object metadata as normal,
but will not create a new version of the object. In other words, new versions
are only created when the content of the object changes.

A ``DELETE`` to a versioned object will only remove the current version of the
object. If you have 5 total versions of the object, you must delete the
object 5 times to completely remove the object.

--------------------------------------------------
How to Enable Object Versioning in a Swift Cluster
--------------------------------------------------

This middleware was written as an effort to refactor parts of the proxy server,
so this functionality was already available in previous releases and every
attempt was made to maintain backwards compatibility. To allow operators to
perform a seamless upgrade, it is not required to add the middleware to the
proxy pipeline and the flag ``allow_versions`` in the container server
configuration files are still valid. In future releases, ``allow_versions``
will be deprecated in favor of adding this middleware to the pipeline to enable
or disable the feature.

In case the middleware is added to the proxy pipeline, you must also
set ``allow_versioned_writes`` to ``True`` in the middleware options
to enable the information about this middleware to be returned in a /info
request.

Upgrade considerations: If ``allow_versioned_writes`` is set in the filter
configuration, you can leave the ``allow_versions`` flag in the container
server configuration files untouched. If you decide to disable or remove the
``allow_versions`` flag, you must re-set any existing containers that had
the 'X-Versions-Location' flag configured so that it can now be tracked by the
versioned_writes middleware.

-----------------------
Examples Using ``curl``
-----------------------

First, create a container with the ``X-Versions-Location`` header or add the
header to an existing container. Also make sure the container referenced by
the ``X-Versions-Location`` exists. In this example, the name of that
container is "versions"::

    curl -i -XPUT -H "X-Auth-Token: <token>" \
-H "X-Versions-Location: versions" http://<storage_url>/container
    curl -i -XPUT -H "X-Auth-Token: <token>" http://<storage_url>/versions

Create an object (the first version)::

    curl -i -XPUT --data-binary 1 -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject

Now create a new version of that object::

    curl -i -XPUT --data-binary 2 -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject

See a listing of the older versions of the object::

    curl -i -H "X-Auth-Token: <token>" \
http://<storage_url>/versions?prefix=008myobject/

Now delete the current version of the object and see that the older version is
gone from 'versions' container and back in 'container' container::

    curl -i -XDELETE -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject
    curl -i -H "X-Auth-Token: <token>" \
http://<storage_url>/versions?prefix=008myobject/
    curl -i -XGET -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject

---------------------------------------------------
How to Disable Object Versioning in a Swift Cluster
---------------------------------------------------

If you want to disable all functionality, set ``allow_versioned_writes`` to
``False`` in the middleware options.

Disable versioning from a container (x is any value except empty)::

    curl -i -XPOST -H "X-Auth-Token: <token>" \
-H "X-Remove-Versions-Location: x" http://<storage_url>/container
"""

import json
import six
from six.moves.urllib.parse import quote, unquote
import time
from swift.common.utils import get_logger, Timestamp, \
    register_swift_info, config_true_value
from swift.common.request_helpers import get_sys_meta_prefix
from swift.common.wsgi import WSGIContext, make_pre_authed_request
from swift.common.swob import Request, HTTPException
from swift.common.constraints import (
    check_account_format, check_container_format, check_destination_header)
from swift.proxy.controllers.base import get_container_info
from swift.common.http import (
    is_success, is_client_error, HTTP_NOT_FOUND)
from swift.common.swob import HTTPPreconditionFailed, HTTPServiceUnavailable, \
    HTTPServerError
from swift.common.exceptions import (
    ListingIterNotFound, ListingIterError)


class VersionedWritesContext(WSGIContext):

    def __init__(self, wsgi_app, logger):
        WSGIContext.__init__(self, wsgi_app)
        self.logger = logger

    def _listing_iter(self, account_name, lcontainer, lprefix, req):
        try:
            for page in self._listing_pages_iter(account_name, lcontainer,
                                                 lprefix, req.environ):
                for item in page:
                    yield item
        except ListingIterNotFound:
            pass
        except HTTPPreconditionFailed:
            raise HTTPPreconditionFailed(request=req)
        except ListingIterError:
            raise HTTPServerError(request=req)

    def _listing_pages_iter(self, account_name, lcontainer, lprefix, env):
        marker = ''
        while True:
            lreq = make_pre_authed_request(
                env, method='GET', swift_source='VW',
                path='/v1/%s/%s' % (account_name, lcontainer))
            lreq.environ['QUERY_STRING'] = \
                'format=json&prefix=%s&reverse=on&marker=%s' % (
                    quote(lprefix), quote(marker))
            lresp = lreq.get_response(self.app)
            if not is_success(lresp.status_int):
                if lresp.status_int == HTTP_NOT_FOUND:
                    raise ListingIterNotFound()
                elif is_client_error(lresp.status_int):
                    raise HTTPPreconditionFailed()
                else:
                    raise ListingIterError()

            if not lresp.body:
                break

            sublisting = json.loads(lresp.body)
            if not sublisting:
                break
            marker = sublisting[-1]['name'].encode('utf-8')
            yield sublisting

    def handle_obj_versions_put(self, req, object_versions,
                                object_name, policy_index):
        ret = None

        # do a HEAD request to check object versions
        _headers = {'X-Newest': 'True',
                    'X-Backend-Storage-Policy-Index': policy_index,
                    'x-auth-token': req.headers.get('x-auth-token')}

        # make a pre_auth request in case the user has write access
        # to container, but not READ. This was allowed in previous version
        # (i.e., before middleware) so keeping the same behavior here
        head_req = make_pre_authed_request(
            req.environ, path=req.path_info,
            headers=_headers, method='HEAD', swift_source='VW')
        hresp = head_req.get_response(self.app)

        is_dlo_manifest = 'X-Object-Manifest' in req.headers or \
                          'X-Object-Manifest' in hresp.headers

        # if there's an existing object, then copy it to
        # X-Versions-Location
        if is_success(hresp.status_int) and not is_dlo_manifest:
            lcontainer = object_versions.split('/')[0]
            prefix_len = '%03x' % len(object_name)
            lprefix = prefix_len + object_name + '/'
            ts_source = hresp.environ.get('swift_x_timestamp')
            if ts_source is None:
                ts_source = time.mktime(time.strptime(
                                        hresp.headers['last-modified'],
                                        '%a, %d %b %Y %H:%M:%S GMT'))
            new_ts = Timestamp(ts_source).internal
            vers_obj_name = lprefix + new_ts
            copy_headers = {
                'Destination': '%s/%s' % (lcontainer, vers_obj_name),
                'x-auth-token': req.headers.get('x-auth-token')}

            # COPY implementation sets X-Newest to True when it internally
            # does a GET on source object. So, we don't have to explicity
            # set it in request headers here.
            copy_req = make_pre_authed_request(
                req.environ, path=req.path_info,
                headers=copy_headers, method='COPY', swift_source='VW')
            copy_resp = copy_req.get_response(self.app)

            if is_success(copy_resp.status_int):
                # success versioning previous existing object
                # return None and handle original request
                ret = None
            else:
                if is_client_error(copy_resp.status_int):
                    # missing container or bad permissions
                    ret = HTTPPreconditionFailed(request=req)
                else:
                    # could not copy the data, bail
                    ret = HTTPServiceUnavailable(request=req)

        else:
            if hresp.status_int == HTTP_NOT_FOUND or is_dlo_manifest:
                # nothing to version
                # return None and handle original request
                ret = None
            else:
                # if not HTTP_NOT_FOUND, return error immediately
                ret = hresp

        return ret

    def handle_obj_versions_delete(self, req, object_versions,
                                   account_name, container_name, object_name):
        lcontainer = object_versions.split('/')[0]
        prefix_len = '%03x' % len(object_name)
        lprefix = prefix_len + object_name + '/'

        item_iter = self._listing_iter(account_name, lcontainer, lprefix, req)

        authed = False
        for previous_version in item_iter:
            if not authed:
                # we're about to start making COPY requests - need to
                # validate the write access to the versioned container
                if 'swift.authorize' in req.environ:
                    container_info = get_container_info(
                        req.environ, self.app)
                    req.acl = container_info.get('write_acl')
                    aresp = req.environ['swift.authorize'](req)
                    if aresp:
                        return aresp
                    authed = True

            # there are older versions so copy the previous version to the
            # current object and delete the previous version
            prev_obj_name = previous_version['name'].encode('utf-8')

            copy_path = '/v1/' + account_name + '/' + \
                        lcontainer + '/' + prev_obj_name

            copy_headers = {'X-Newest': 'True',
                            'Destination': container_name + '/' + object_name,
                            'x-auth-token': req.headers.get('x-auth-token')}

            copy_req = make_pre_authed_request(
                req.environ, path=copy_path,
                headers=copy_headers, method='COPY', swift_source='VW')
            copy_resp = copy_req.get_response(self.app)

            # if the version isn't there, keep trying with previous version
            if copy_resp.status_int == HTTP_NOT_FOUND:
                continue

            if not is_success(copy_resp.status_int):
                if is_client_error(copy_resp.status_int):
                    # some user error, maybe permissions
                    return HTTPPreconditionFailed(request=req)
                else:
                    # could not copy the data, bail
                    return HTTPServiceUnavailable(request=req)

            # reset these because the COPY changed them
            new_del_req = make_pre_authed_request(
                req.environ, path=copy_path, method='DELETE',
                swift_source='VW')
            req = new_del_req

            # remove 'X-If-Delete-At', since it is not for the older copy
            if 'X-If-Delete-At' in req.headers:
                del req.headers['X-If-Delete-At']
            break

        # handle DELETE request here in case it was modified
        return req.get_response(self.app)

    def handle_container_request(self, env, start_response):
        app_resp = self._app_call(env)
        if self._response_headers is None:
            self._response_headers = []
        sysmeta_version_hdr = get_sys_meta_prefix('container') + \
            'versions-location'
        location = ''
        for key, val in self._response_headers:
            if key.lower() == sysmeta_version_hdr:
                location = val

        if location:
            self._response_headers.extend([('X-Versions-Location', location)])

        start_response(self._response_status,
                       self._response_headers,
                       self._response_exc_info)
        return app_resp


class VersionedWritesMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, log_route='versioned_writes')

    def container_request(self, req, start_response, enabled):
        sysmeta_version_hdr = get_sys_meta_prefix('container') + \
            'versions-location'

        # set version location header as sysmeta
        if 'X-Versions-Location' in req.headers:
            val = req.headers.get('X-Versions-Location')
            if val:
                # diferently from previous version, we are actually
                # returning an error if user tries to set versions location
                # while feature is explicitly disabled.
                if not config_true_value(enabled) and \
                        req.method in ('PUT', 'POST'):
                    raise HTTPPreconditionFailed(
                        request=req, content_type='text/plain',
                        body='Versioned Writes is disabled')

                location = check_container_format(req, val)
                req.headers[sysmeta_version_hdr] = location

                # reset original header to maintain sanity
                # now only sysmeta is source of Versions Location
                req.headers['X-Versions-Location'] = ''

                # if both headers are in the same request
                # adding location takes precendence over removing
                if 'X-Remove-Versions-Location' in req.headers:
                    del req.headers['X-Remove-Versions-Location']
            else:
                # empty value is the same as X-Remove-Versions-Location
                req.headers['X-Remove-Versions-Location'] = 'x'

        # handle removing versions container
        val = req.headers.get('X-Remove-Versions-Location')
        if val:
            req.headers.update({sysmeta_version_hdr: ''})
            req.headers.update({'X-Versions-Location': ''})
            del req.headers['X-Remove-Versions-Location']

        # send request and translate sysmeta headers from response
        vw_ctx = VersionedWritesContext(self.app, self.logger)
        return vw_ctx.handle_container_request(req.environ, start_response)

    def object_request(self, req, version, account, container, obj,
                       allow_versioned_writes):
        account_name = unquote(account)
        container_name = unquote(container)
        object_name = unquote(obj)
        container_info = None
        resp = None
        is_enabled = config_true_value(allow_versioned_writes)
        if req.method in ('PUT', 'DELETE'):
            container_info = get_container_info(
                req.environ, self.app)
        elif req.method == 'COPY' and 'Destination' in req.headers:
            if 'Destination-Account' in req.headers:
                account_name = req.headers.get('Destination-Account')
                account_name = check_account_format(req, account_name)
            container_name, object_name = check_destination_header(req)
            req.environ['PATH_INFO'] = "/%s/%s/%s/%s" % (
                version, account_name, container_name, object_name)
            container_info = get_container_info(
                req.environ, self.app)

        if not container_info:
            return self.app

        # To maintain backwards compatibility, container version
        # location could be stored as sysmeta or not, need to check both.
        # If stored as sysmeta, check if middleware is enabled. If sysmeta
        # is not set, but versions property is set in container_info, then
        # for backwards compatibility feature is enabled.
        object_versions = container_info.get(
            'sysmeta', {}).get('versions-location')
        if object_versions and isinstance(object_versions, six.text_type):
            object_versions = object_versions.encode('utf-8')
        elif not object_versions:
            object_versions = container_info.get('versions')
            # if allow_versioned_writes is not set in the configuration files
            # but 'versions' is configured, enable feature to maintain
            # backwards compatibility
            if not allow_versioned_writes and object_versions:
                is_enabled = True

        if is_enabled and object_versions:
            object_versions = unquote(object_versions)
            vw_ctx = VersionedWritesContext(self.app, self.logger)
            if req.method in ('PUT', 'COPY'):
                policy_idx = req.headers.get(
                    'X-Backend-Storage-Policy-Index',
                    container_info['storage_policy'])
                resp = vw_ctx.handle_obj_versions_put(
                    req, object_versions, object_name, policy_idx)
            else:  # handle DELETE
                resp = vw_ctx.handle_obj_versions_delete(
                    req, object_versions, account_name,
                    container_name, object_name)

        if resp:
            return resp
        else:
            return self.app

    def __call__(self, env, start_response):
        # making a duplicate, because if this is a COPY request, we will
        # modify the PATH_INFO to find out if the 'Destination' is in a
        # versioned container
        req = Request(env.copy())
        try:
            (version, account, container, obj) = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)

        # In case allow_versioned_writes is set in the filter configuration,
        # the middleware becomes the authority on whether object
        # versioning is enabled or not. In case it is not set, then
        # the option in the container configuration is still checked
        # for backwards compatibility

        # For a container request, first just check if option is set,
        # can be either true or false.
        # If set, check if enabled when actually trying to set container
        # header. If not set, let request be handled by container server
        # for backwards compatibility.
        # For an object request, also check if option is set (either T or F).
        # If set, check if enabled when checking versions container in
        # sysmeta property. If it is not set check 'versions' property in
        # container_info
        allow_versioned_writes = self.conf.get('allow_versioned_writes')
        if allow_versioned_writes and container and not obj:
            try:
                return self.container_request(req, start_response,
                                              allow_versioned_writes)
            except HTTPException as error_response:
                return error_response(env, start_response)
        elif obj and req.method in ('PUT', 'COPY', 'DELETE'):
            try:
                return self.object_request(
                    req, version, account, container, obj,
                    allow_versioned_writes)(env, start_response)
            except HTTPException as error_response:
                return error_response(env, start_response)
        else:
            return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    if config_true_value(conf.get('allow_versioned_writes')):
        register_swift_info('versioned_writes')

    def obj_versions_filter(app):
        return VersionedWritesMiddleware(app, conf)

    return obj_versions_filter
