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

import calendar
import json
from six.moves.urllib.parse import quote, unquote
import time

from swift.common.utils import get_logger, Timestamp, \
    register_swift_info, config_true_value, close_if_possible, FileLikeIter
from swift.common.request_helpers import get_sys_meta_prefix, \
    copy_header_subset
from swift.common.wsgi import WSGIContext, make_pre_authed_request
from swift.common.swob import (
    Request, HTTPException, HTTPRequestEntityTooLarge)
from swift.common.constraints import check_container_format, MAX_FILE_SIZE
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

    def _in_proxy_reverse_listing(self, account_name, lcontainer, lprefix,
                                  env, failed_marker, failed_listing):
        '''Get the complete prefix listing and reverse it on the proxy.

        This is only necessary if we encounter a response from a
        container-server that does not respect the ``reverse`` param
        included by default in ``_listing_pages_iter``. This may happen
        during rolling upgrades from pre-2.6.0 swift.

        :param failed_marker: the marker that was used when we encountered
                              the non-reversed listing
        :param failed_listing: the non-reversed listing that was encountered.
                               If ``failed_marker`` is blank, we can use this
                               to save ourselves a request
        :returns: an iterator over all objects starting with ``lprefix`` (up
                  to but not including the failed marker) in reverse order
        '''
        complete_listing = []
        if not failed_marker:
            # We've never gotten a reversed listing. So save a request and
            # use the failed listing.
            complete_listing.extend(failed_listing)
            marker = complete_listing[-1]['name'].encode('utf8')
        else:
            # We've gotten at least one reversed listing. Have to start at
            # the beginning.
            marker = ''

        # First, take the *entire* prefix listing into memory
        try:
            for page in self._listing_pages_iter(
                    account_name, lcontainer, lprefix,
                    env, marker, end_marker=failed_marker, reverse=False):
                complete_listing.extend(page)
        except ListingIterNotFound:
            pass

        # Now that we've got everything, return the whole listing as one giant
        # reversed page
        return reversed(complete_listing)

    def _listing_pages_iter(self, account_name, lcontainer, lprefix,
                            env, marker='', end_marker='', reverse=True):
        '''Get "pages" worth of objects that start with a prefix.

        The optional keyword arguments ``marker``, ``end_marker``, and
        ``reverse`` are used similar to how they are for containers. We're
        either coming:

           - directly from ``_listing_iter``, in which case none of the
             optional args are specified, or

           - from ``_in_proxy_reverse_listing``, in which case ``reverse``
             is ``False`` and both ``marker`` and ``end_marker`` are specified
             (although they may still be blank).
        '''
        while True:
            lreq = make_pre_authed_request(
                env, method='GET', swift_source='VW',
                path='/v1/%s/%s' % (account_name, lcontainer))
            lreq.environ['QUERY_STRING'] = \
                'format=json&prefix=%s&marker=%s' % (
                    quote(lprefix), quote(marker))
            if end_marker:
                lreq.environ['QUERY_STRING'] += '&end_marker=%s' % (
                    quote(end_marker))
            if reverse:
                lreq.environ['QUERY_STRING'] += '&reverse=on'
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

            # When using the ``reverse`` param, check that the listing is
            # actually reversed
            first_item = sublisting[0]['name'].encode('utf-8')
            last_item = sublisting[-1]['name'].encode('utf-8')
            page_is_after_marker = marker and first_item > marker
            if reverse and (first_item < last_item or page_is_after_marker):
                # Apparently there's at least one pre-2.6.0 container server
                yield self._in_proxy_reverse_listing(
                    account_name, lcontainer, lprefix,
                    env, marker, sublisting)
                return

            marker = last_item
            yield sublisting

    def _get_source_object(self, req, path_info):
        # make a GET request to check object versions
        _headers = {'X-Newest': 'True',
                    'x-auth-token': req.headers.get('x-auth-token')}

        # make a pre_auth request in case the user has write access
        # to container, but not READ. This was allowed in previous version
        # (i.e., before middleware) so keeping the same behavior here
        get_req = make_pre_authed_request(
            req.environ, path=path_info,
            headers=_headers, method='GET', swift_source='VW')
        source_resp = get_req.get_response(self.app)

        if source_resp.content_length is None or \
                source_resp.content_length > MAX_FILE_SIZE:
            return HTTPRequestEntityTooLarge(request=req)

        return source_resp

    def _put_versioned_obj(self, req, put_path_info, source_resp):
        # Create a new Request object to PUT to the versions container, copying
        # all headers from the source object apart from x-timestamp.
        put_req = make_pre_authed_request(
            req.environ, path=put_path_info, method='PUT',
            swift_source='VW')
        copy_header_subset(source_resp, put_req,
                           lambda k: k.lower() != 'x-timestamp')
        put_req.headers['x-auth-token'] = req.headers.get('x-auth-token')
        put_req.environ['wsgi.input'] = FileLikeIter(source_resp.app_iter)
        return put_req.get_response(self.app)

    def _check_response_error(self, req, resp):
        """
        Raise Error Response in case of error
        """
        if is_success(resp.status_int):
            return
        if is_client_error(resp.status_int):
            # missing container or bad permissions
            raise HTTPPreconditionFailed(request=req)
        # could not version the data, bail
        raise HTTPServiceUnavailable(request=req)

    def handle_obj_versions_put(self, req, versions_cont, api_version,
                                account_name, object_name):
        """
        Copy current version of object to versions_container before proceding
        with original request.

        :param req: original request.
        :param versions_cont: container where previous versions of the object
                              are stored.
        :param api_version: api version.
        :param account_name: account name.
        :param object_name: name of object of original request
        """
        if 'X-Object-Manifest' in req.headers:
            # do not version DLO manifest, proceed with original request
            return self.app

        get_resp = self._get_source_object(req, req.path_info)

        if 'X-Object-Manifest' in get_resp.headers:
            # do not version DLO manifest, proceed with original request
            close_if_possible(get_resp.app_iter)
            return self.app
        if get_resp.status_int == HTTP_NOT_FOUND:
            # nothing to version, proceed with original request
            close_if_possible(get_resp.app_iter)
            return self.app

        # check for any other errors
        self._check_response_error(req, get_resp)

        # if there's an existing object, then copy it to
        # X-Versions-Location
        prefix_len = '%03x' % len(object_name)
        lprefix = prefix_len + object_name + '/'
        ts_source = get_resp.headers.get(
            'x-timestamp',
            calendar.timegm(time.strptime(
                get_resp.headers['last-modified'],
                '%a, %d %b %Y %H:%M:%S GMT')))
        vers_obj_name = lprefix + Timestamp(ts_source).internal

        put_path_info = "/%s/%s/%s/%s" % (
            api_version, account_name, versions_cont, vers_obj_name)
        put_resp = self._put_versioned_obj(req, put_path_info, get_resp)

        self._check_response_error(req, put_resp)
        return self.app

    def handle_obj_versions_delete(self, req, versions_cont, api_version,
                                   account_name, container_name, object_name):
        """
        Delete current version of object and pop previous version in its place.

        :param req: original request.
        :param versions_cont: container where previous versions of the object
                              are stored.
        :param api_version: api version.
        :param account_name: account name.
        :param container_name: container name.
        :param object_name: object name.
        """
        prefix_len = '%03x' % len(object_name)
        lprefix = prefix_len + object_name + '/'

        item_iter = self._listing_iter(account_name, versions_cont, lprefix,
                                       req)

        authed = False
        for previous_version in item_iter:
            if not authed:
                # validate the write access to the versioned container before
                # making any backend requests
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

            get_path = "/%s/%s/%s/%s" % (
                api_version, account_name, versions_cont, prev_obj_name)

            get_resp = self._get_source_object(req, get_path)

            # if the version isn't there, keep trying with previous version
            if get_resp.status_int == HTTP_NOT_FOUND:
                continue

            self._check_response_error(req, get_resp)

            put_path_info = "/%s/%s/%s/%s" % (
                api_version, account_name, container_name, object_name)
            put_resp = self._put_versioned_obj(req, put_path_info, get_resp)

            self._check_response_error(req, put_resp)

            # redirect the original DELETE to the source of the reinstated
            # version object - we already auth'd original req so make a
            # pre-authed request
            req = make_pre_authed_request(
                req.environ, path=get_path, method='DELETE',
                swift_source='VW')

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
                # differently from previous version, we are actually
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
                # adding location takes precedence over removing
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

    def object_request(self, req, api_version, account, container, obj,
                       allow_versioned_writes):
        account_name = unquote(account)
        container_name = unquote(container)
        object_name = unquote(obj)
        resp = None
        is_enabled = config_true_value(allow_versioned_writes)
        container_info = get_container_info(
            req.environ, self.app)

        # To maintain backwards compatibility, container version
        # location could be stored as sysmeta or not, need to check both.
        # If stored as sysmeta, check if middleware is enabled. If sysmeta
        # is not set, but versions property is set in container_info, then
        # for backwards compatibility feature is enabled.
        versions_cont = container_info.get(
            'sysmeta', {}).get('versions-location')
        if not versions_cont:
            versions_cont = container_info.get('versions')
            # if allow_versioned_writes is not set in the configuration files
            # but 'versions' is configured, enable feature to maintain
            # backwards compatibility
            if not allow_versioned_writes and versions_cont:
                is_enabled = True

        if is_enabled and versions_cont:
            versions_cont = unquote(versions_cont).split('/')[0]
            vw_ctx = VersionedWritesContext(self.app, self.logger)
            if req.method == 'PUT':
                resp = vw_ctx.handle_obj_versions_put(
                    req, versions_cont, api_version, account_name,
                    object_name)
            else:  # handle DELETE
                resp = vw_ctx.handle_obj_versions_delete(
                    req, versions_cont, api_version, account_name,
                    container_name, object_name)

        if resp:
            return resp
        else:
            return self.app

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            (api_version, account, container, obj) = req.split_path(3, 4, True)
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
        elif (obj and req.method in ('PUT', 'DELETE') and
                not req.environ.get('swift.post_as_copy')):
            try:
                return self.object_request(
                    req, api_version, account, container, obj,
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
