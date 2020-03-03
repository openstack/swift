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
.. note::
    This middleware supports two legacy modes of object versioning that is
    now replaced by a new mode. It is recommended to use the new
    :ref:`Object Versioning <object_versioning>` mode for new containers.

Object versioning in swift is implemented by setting a flag on the container
to tell swift to version all objects in the container. The value of the flag is
the URL-encoded container name where the versions are stored (commonly referred
to as the "archive container"). The flag itself is one of two headers, which
determines how object ``DELETE`` requests are handled:

* ``X-History-Location``

  On ``DELETE``, copy the current version of the object to the archive
  container, write a zero-byte "delete marker" object that notes when the
  delete took place, and delete the object from the versioned container. The
  object will no longer appear in container listings for the versioned
  container and future requests there will return ``404 Not Found``. However,
  the content will still be recoverable from the archive container.

* ``X-Versions-Location``

  On ``DELETE``, only remove the current version of the object. If any
  previous versions exist in the archive container, the most recent one is
  copied over the current version, and the copy in the archive container is
  deleted. As a result, if you have 5 total versions of the object, you must
  delete the object 5 times for that object name to start responding with
  ``404 Not Found``.

Either header may be used for the various containers within an account, but
only one may be set for any given container. Attempting to set both
simulataneously will result in a ``400 Bad Request`` response.

.. note::
    It is recommended to use a different archive container for
    each container that is being versioned.

.. note::
    Enabling versioning on an archive container is not recommended.

When data is ``PUT`` into a versioned container (a container with the
versioning flag turned on), the existing data in the file is redirected to a
new object in the archive container and the data in the ``PUT`` request is
saved as the data for the versioned object. The new object name (for the
previous version) is ``<archive_container>/<length><object_name>/<timestamp>``,
where ``length`` is the 3-character zero-padded hexadecimal length of the
``<object_name>`` and ``<timestamp>`` is the timestamp of when the previous
version was created.

A ``GET`` to a versioned object will return the current version of the object
without having to do any request redirects or metadata lookups.

A ``POST`` to a versioned object will update the object metadata as normal,
but will not create a new version of the object. In other words, new versions
are only created when the content of the object changes.

A ``DELETE`` to a versioned object will be handled in one of two ways,
as described above.

To restore a previous version of an object, find the desired version in the
archive container then issue a ``COPY`` with a ``Destination`` header
indicating the original location. This will archive the current version similar
to a ``PUT`` over the versioned object. If the client additionally wishes to
permanently delete what was the current version, it must find the newly-created
archive in the archive container and issue a separate ``DELETE`` to it.

--------------------------------------------------
How to Enable Object Versioning in a Swift Cluster
--------------------------------------------------

This middleware was written as an effort to refactor parts of the proxy server,
so this functionality was already available in previous releases and every
attempt was made to maintain backwards compatibility. To allow operators to
perform a seamless upgrade, it is not required to add the middleware to the
proxy pipeline and the flag ``allow_versions`` in the container server
configuration files are still valid, but only when using
``X-Versions-Location``. In future releases, ``allow_versions`` will be
deprecated in favor of adding this middleware to the pipeline to enable or
disable the feature.

In case the middleware is added to the proxy pipeline, you must also
set ``allow_versioned_writes`` to ``True`` in the middleware options
to enable the information about this middleware to be returned in a /info
request.

.. note::
    You need to add the middleware to the proxy pipeline and set
    ``allow_versioned_writes = True`` to use ``X-History-Location``. Setting
    ``allow_versions = True`` in the container server is not sufficient to
    enable the use of ``X-History-Location``.


Upgrade considerations
++++++++++++++++++++++

If ``allow_versioned_writes`` is set in the filter configuration, you can leave
the ``allow_versions`` flag in the container server configuration files
untouched. If you decide to disable or remove the ``allow_versions`` flag, you
must re-set any existing containers that had the ``X-Versions-Location`` flag
configured so that it can now be tracked by the versioned_writes middleware.

Clients should not use the ``X-History-Location`` header until all proxies in
the cluster have been upgraded to a version of Swift that supports it.
Attempting to use ``X-History-Location`` during a rolling upgrade may result
in some requests being served by proxies running old code, leading to data
loss.

----------------------------------------------------
Examples Using ``curl`` with ``X-Versions-Location``
----------------------------------------------------

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
Examples Using ``curl`` with ``X-History-Location``
---------------------------------------------------

As above, create a container with the ``X-History-Location`` header and ensure
that the container referenced by the ``X-History-Location`` exists. In this
example, the name of that container is "versions"::

    curl -i -XPUT -H "X-Auth-Token: <token>" \
-H "X-History-Location: versions" http://<storage_url>/container
    curl -i -XPUT -H "X-Auth-Token: <token>" http://<storage_url>/versions

Create an object (the first version)::

    curl -i -XPUT --data-binary 1 -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject

Now create a new version of that object::

    curl -i -XPUT --data-binary 2 -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject

Now delete the current version of the object. Subsequent requests will 404::

    curl -i -XDELETE -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject
    curl -i -H "X-Auth-Token: <token>" \
http://<storage_url>/container/myobject

A listing of the older versions of the object will include both the first and
second versions of the object, as well as a "delete marker" object::

    curl -i -H "X-Auth-Token: <token>" \
http://<storage_url>/versions?prefix=008myobject/

To restore a previous version, simply ``COPY`` it from the archive container::

    curl -i -XCOPY -H "X-Auth-Token: <token>" \
http://<storage_url>/versions/008myobject/<timestamp> \
-H "Destination: container/myobject"

Note that the archive container still has all previous versions of the object,
including the source for the restore::

    curl -i -H "X-Auth-Token: <token>" \
http://<storage_url>/versions?prefix=008myobject/

To permanently delete a previous version, ``DELETE`` it from the archive
container::

    curl -i -XDELETE -H "X-Auth-Token: <token>" \
http://<storage_url>/versions/008myobject/<timestamp>

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
import time

from swift.common.utils import get_logger, Timestamp, \
    config_true_value, close_if_possible, FileLikeIter, drain_and_close
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
    HTTPServerError, HTTPBadRequest, str_to_wsgi, bytes_to_wsgi, wsgi_quote, \
    wsgi_unquote
from swift.common.exceptions import (
    ListingIterNotFound, ListingIterError)


DELETE_MARKER_CONTENT_TYPE = 'application/x-deleted;swift_versions_deleted=1'
CLIENT_VERSIONS_LOC = 'x-versions-location'
CLIENT_HISTORY_LOC = 'x-history-location'
SYSMETA_VERSIONS_LOC = get_sys_meta_prefix('container') + 'versions-location'
SYSMETA_VERSIONS_MODE = get_sys_meta_prefix('container') + 'versions-mode'


class VersionedWritesContext(WSGIContext):

    def __init__(self, wsgi_app, logger):
        WSGIContext.__init__(self, wsgi_app)
        self.logger = logger

    def _listing_iter(self, account_name, lcontainer, lprefix, req):
        try:
            for page in self._listing_pages_iter(account_name, lcontainer,
                                                 lprefix, req):
                for item in page:
                    yield item
        except ListingIterNotFound:
            pass
        except ListingIterError:
            raise HTTPServerError(request=req)

    def _in_proxy_reverse_listing(self, account_name, lcontainer, lprefix,
                                  req, failed_marker, failed_listing):
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
            marker = bytes_to_wsgi(complete_listing[-1]['name'].encode('utf8'))
        else:
            # We've gotten at least one reversed listing. Have to start at
            # the beginning.
            marker = ''

        # First, take the *entire* prefix listing into memory
        try:
            for page in self._listing_pages_iter(
                    account_name, lcontainer, lprefix,
                    req, marker, end_marker=failed_marker, reverse=False):
                complete_listing.extend(page)
        except ListingIterNotFound:
            pass

        # Now that we've got everything, return the whole listing as one giant
        # reversed page
        return reversed(complete_listing)

    def _listing_pages_iter(self, account_name, lcontainer, lprefix,
                            req, marker='', end_marker='', reverse=True):
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
                req.environ, method='GET', swift_source='VW',
                path=wsgi_quote('/v1/%s/%s' % (account_name, lcontainer)))
            lreq.environ['QUERY_STRING'] = \
                'prefix=%s&marker=%s' % (wsgi_quote(lprefix),
                                         wsgi_quote(marker))
            if end_marker:
                lreq.environ['QUERY_STRING'] += '&end_marker=%s' % (
                    wsgi_quote(end_marker))
            if reverse:
                lreq.environ['QUERY_STRING'] += '&reverse=on'
            lresp = lreq.get_response(self.app)
            if not is_success(lresp.status_int):
                # errors should be short
                drain_and_close(lresp)
                if lresp.status_int == HTTP_NOT_FOUND:
                    raise ListingIterNotFound()
                elif is_client_error(lresp.status_int):
                    raise HTTPPreconditionFailed(request=req)
                else:
                    raise ListingIterError()

            if not lresp.body:
                break

            sublisting = json.loads(lresp.body)
            if not sublisting:
                break

            # When using the ``reverse`` param, check that the listing is
            # actually reversed
            first_item = bytes_to_wsgi(sublisting[0]['name'].encode('utf-8'))
            last_item = bytes_to_wsgi(sublisting[-1]['name'].encode('utf-8'))
            page_is_after_marker = marker and first_item > marker
            if reverse and (first_item < last_item or page_is_after_marker):
                # Apparently there's at least one pre-2.6.0 container server
                yield self._in_proxy_reverse_listing(
                    account_name, lcontainer, lprefix,
                    req, marker, sublisting)
                return

            marker = last_item
            yield sublisting

    def _get_source_object(self, req, path_info):
        # make a pre_auth request in case the user has write access
        # to container, but not READ. This was allowed in previous version
        # (i.e., before middleware) so keeping the same behavior here
        get_req = make_pre_authed_request(
            req.environ, path=wsgi_quote(path_info) + '?symlink=get',
            headers={'X-Newest': 'True'}, method='GET', swift_source='VW')
        source_resp = get_req.get_response(self.app)

        if source_resp.content_length is None or \
                source_resp.content_length > MAX_FILE_SIZE:
            # Consciously *don't* drain the response before closing;
            # any logged 499 is actually rather appropriate here
            close_if_possible(source_resp.app_iter)
            return HTTPRequestEntityTooLarge(request=req)

        return source_resp

    def _put_versioned_obj(self, req, put_path_info, source_resp):
        # Create a new Request object to PUT to the container, copying
        # all headers from the source object apart from x-timestamp.
        put_req = make_pre_authed_request(
            req.environ, path=wsgi_quote(put_path_info), method='PUT',
            swift_source='VW')
        copy_header_subset(source_resp, put_req,
                           lambda k: k.lower() != 'x-timestamp')
        slo_size = put_req.headers.get('X-Object-Sysmeta-Slo-Size')
        if slo_size:
            put_req.headers['Content-Type'] += '; swift_bytes=' + slo_size
            put_req.environ['swift.content_type_overridden'] = True

        put_req.environ['wsgi.input'] = FileLikeIter(source_resp.app_iter)
        put_resp = put_req.get_response(self.app)
        # the PUT was responsible for draining
        close_if_possible(source_resp.app_iter)
        return put_resp

    def _check_response_error(self, req, resp):
        """
        Raise Error Response in case of error
        """
        if is_success(resp.status_int):
            return
        # any error should be short
        drain_and_close(resp)
        if is_client_error(resp.status_int):
            # missing container or bad permissions
            raise HTTPPreconditionFailed(request=req)
        # could not version the data, bail
        raise HTTPServiceUnavailable(request=req)

    def _build_versions_object_prefix(self, object_name):
        return '%03x%s/' % (
            len(object_name),
            object_name)

    def _build_versions_object_name(self, object_name, ts):
        return ''.join((
            self._build_versions_object_prefix(object_name),
            Timestamp(ts).internal))

    def _copy_current(self, req, versions_cont, api_version, account_name,
                      object_name):
        # validate the write access to the versioned container before
        # making any backend requests
        if 'swift.authorize' in req.environ:
            container_info = get_container_info(
                req.environ, self.app, swift_source='VW')
            req.acl = container_info.get('write_acl')
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                raise aresp

        get_resp = self._get_source_object(req, req.path_info)

        if get_resp.status_int == HTTP_NOT_FOUND:
            # nothing to version, proceed with original request
            drain_and_close(get_resp)
            return

        # check for any other errors
        self._check_response_error(req, get_resp)

        # if there's an existing object, then copy it to
        # X-Versions-Location
        ts_source = get_resp.headers.get(
            'x-timestamp',
            calendar.timegm(time.strptime(
                get_resp.headers['last-modified'],
                '%a, %d %b %Y %H:%M:%S GMT')))
        vers_obj_name = self._build_versions_object_name(
            object_name, ts_source)

        put_path_info = "/%s/%s/%s/%s" % (
            api_version, account_name, versions_cont, vers_obj_name)
        req.environ['QUERY_STRING'] = ''
        put_resp = self._put_versioned_obj(req, put_path_info, get_resp)

        self._check_response_error(req, put_resp)
        # successful PUT response should be short
        drain_and_close(put_resp)

    def handle_obj_versions_put(self, req, versions_cont, api_version,
                                account_name, object_name):
        """
        Copy current version of object to versions_container before proceeding
        with original request.

        :param req: original request.
        :param versions_cont: container where previous versions of the object
                              are stored.
        :param api_version: api version.
        :param account_name: account name.
        :param object_name: name of object of original request
        """
        self._copy_current(req, versions_cont, api_version, account_name,
                           object_name)
        return self.app

    def handle_obj_versions_delete_push(self, req, versions_cont, api_version,
                                        account_name, container_name,
                                        object_name):
        """
        Handle DELETE requests when in history mode.

        Copy current version of object to versions_container and write a
        delete marker before proceeding with original request.

        :param req: original request.
        :param versions_cont: container where previous versions of the object
                              are stored.
        :param api_version: api version.
        :param account_name: account name.
        :param object_name: name of object of original request
        """
        self._copy_current(req, versions_cont, api_version, account_name,
                           object_name)

        marker_path = "/%s/%s/%s/%s" % (
            api_version, account_name, versions_cont,
            self._build_versions_object_name(object_name, time.time()))
        marker_headers = {
            # Definitive source of truth is Content-Type, and since we add
            # a swift_* param, we know users haven't set it themselves.
            # This is still open to users POSTing to update the content-type
            # but they're just shooting themselves in the foot then.
            'content-type': DELETE_MARKER_CONTENT_TYPE,
            'content-length': '0',
            'x-auth-token': req.headers.get('x-auth-token')}
        marker_req = make_pre_authed_request(
            req.environ, path=wsgi_quote(marker_path),
            headers=marker_headers, method='PUT', swift_source='VW')
        marker_req.environ['swift.content_type_overridden'] = True
        marker_resp = marker_req.get_response(self.app)
        self._check_response_error(req, marker_resp)
        drain_and_close(marker_resp)

        # successfully copied and created delete marker; safe to delete
        return self.app

    def _restore_data(self, req, versions_cont, api_version, account_name,
                      container_name, object_name, prev_obj_name):
        get_path = "/%s/%s/%s/%s" % (
            api_version, account_name, versions_cont, prev_obj_name)

        get_resp = self._get_source_object(req, get_path)

        # if the version isn't there, keep trying with previous version
        if get_resp.status_int == HTTP_NOT_FOUND:
            drain_and_close(get_resp)
            return False

        self._check_response_error(req, get_resp)

        put_path_info = "/%s/%s/%s/%s" % (
            api_version, account_name, container_name, object_name)
        put_resp = self._put_versioned_obj(req, put_path_info, get_resp)

        self._check_response_error(req, put_resp)
        drain_and_close(put_resp)
        return get_path

    def handle_obj_versions_delete_pop(self, req, versions_cont, api_version,
                                       account_name, container_name,
                                       object_name):
        """
        Handle DELETE requests when in stack mode.

        Delete current version of object and pop previous version in its place.

        :param req: original request.
        :param versions_cont: container where previous versions of the object
                              are stored.
        :param api_version: api version.
        :param account_name: account name.
        :param container_name: container name.
        :param object_name: object name.
        """
        listing_prefix = self._build_versions_object_prefix(object_name)
        item_iter = self._listing_iter(account_name, versions_cont,
                                       listing_prefix, req)

        auth_token_header = {'X-Auth-Token': req.headers.get('X-Auth-Token')}
        authed = False
        for previous_version in item_iter:
            if not authed:
                # validate the write access to the versioned container before
                # making any backend requests
                if 'swift.authorize' in req.environ:
                    container_info = get_container_info(
                        req.environ, self.app, swift_source='VW')
                    req.acl = container_info.get('write_acl')
                    aresp = req.environ['swift.authorize'](req)
                    if aresp:
                        return aresp
                    authed = True

            if previous_version['content_type'] == DELETE_MARKER_CONTENT_TYPE:
                # check whether we have data in the versioned container
                obj_head_headers = {'X-Newest': 'True'}
                obj_head_headers.update(auth_token_header)
                head_req = make_pre_authed_request(
                    req.environ, path=wsgi_quote(req.path_info), method='HEAD',
                    headers=obj_head_headers, swift_source='VW')
                hresp = head_req.get_response(self.app)
                drain_and_close(hresp)

                if hresp.status_int != HTTP_NOT_FOUND:
                    self._check_response_error(req, hresp)
                    # if there's an existing object, then just let the delete
                    # through (i.e., restore to the delete-marker state):
                    break

                # no data currently in the container (delete marker is current)
                for version_to_restore in item_iter:
                    if version_to_restore['content_type'] == \
                            DELETE_MARKER_CONTENT_TYPE:
                        # Nothing to restore
                        break
                    obj_to_restore = bytes_to_wsgi(
                        version_to_restore['name'].encode('utf-8'))
                    req.environ['QUERY_STRING'] = ''
                    restored_path = self._restore_data(
                        req, versions_cont, api_version, account_name,
                        container_name, object_name, obj_to_restore)
                    if not restored_path:
                        continue

                    old_del_req = make_pre_authed_request(
                        req.environ, path=wsgi_quote(restored_path),
                        method='DELETE', headers=auth_token_header,
                        swift_source='VW')
                    del_resp = old_del_req.get_response(self.app)
                    drain_and_close(del_resp)
                    if del_resp.status_int != HTTP_NOT_FOUND:
                        self._check_response_error(req, del_resp)
                        # else, well, it existed long enough to do the
                        # copy; we won't worry too much
                    break
                prev_obj_name = bytes_to_wsgi(
                    previous_version['name'].encode('utf-8'))
                marker_path = "/%s/%s/%s/%s" % (
                    api_version, account_name, versions_cont,
                    prev_obj_name)
                # done restoring, redirect the delete to the marker
                req = make_pre_authed_request(
                    req.environ, path=wsgi_quote(marker_path), method='DELETE',
                    headers=auth_token_header, swift_source='VW')
            else:
                # there are older versions so copy the previous version to the
                # current object and delete the previous version
                prev_obj_name = bytes_to_wsgi(
                    previous_version['name'].encode('utf-8'))
                req.environ['QUERY_STRING'] = ''
                restored_path = self._restore_data(
                    req, versions_cont, api_version, account_name,
                    container_name, object_name, prev_obj_name)
                if not restored_path:
                    continue

                # redirect the original DELETE to the source of the reinstated
                # version object - we already auth'd original req so make a
                # pre-authed request
                req = make_pre_authed_request(
                    req.environ, path=wsgi_quote(restored_path),
                    method='DELETE', headers=auth_token_header,
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
        mode = location = ''
        for key, val in self._response_headers:
            if key.lower() == SYSMETA_VERSIONS_LOC:
                location = val
            elif key.lower() == SYSMETA_VERSIONS_MODE:
                mode = val

        if location:
            if mode == 'history':
                self._response_headers.extend([
                    (CLIENT_HISTORY_LOC.title(), location)])
            else:
                self._response_headers.extend([
                    (CLIENT_VERSIONS_LOC.title(), location)])

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
        if CLIENT_VERSIONS_LOC in req.headers and \
                CLIENT_HISTORY_LOC in req.headers:
            if not req.headers[CLIENT_HISTORY_LOC]:
                # defer to versions location entirely
                del req.headers[CLIENT_HISTORY_LOC]
            elif req.headers[CLIENT_VERSIONS_LOC]:
                raise HTTPBadRequest(
                    request=req, content_type='text/plain',
                    body='Only one of %s or %s may be specified' % (
                        CLIENT_VERSIONS_LOC, CLIENT_HISTORY_LOC))
            else:
                # history location is present and versions location is
                # present but empty -- clean it up
                del req.headers[CLIENT_VERSIONS_LOC]

        if CLIENT_VERSIONS_LOC in req.headers or \
                CLIENT_HISTORY_LOC in req.headers:
            if CLIENT_VERSIONS_LOC in req.headers:
                val = req.headers[CLIENT_VERSIONS_LOC]
                mode = 'stack'
            else:
                val = req.headers[CLIENT_HISTORY_LOC]
                mode = 'history'

            if not val:
                # empty value is the same as X-Remove-Versions-Location
                req.headers['X-Remove-Versions-Location'] = 'x'
            elif not config_true_value(enabled) and \
                    req.method in ('PUT', 'POST'):
                # differently from previous version, we are actually
                # returning an error if user tries to set versions location
                # while feature is explicitly disabled.
                raise HTTPPreconditionFailed(
                    request=req, content_type='text/plain',
                    body='Versioned Writes is disabled')
            else:
                # OK, we received a value, have versioning enabled, and aren't
                # trying to set two modes at once. Validate the value and
                # translate to sysmeta.
                location = check_container_format(req, val)
                req.headers[SYSMETA_VERSIONS_LOC] = location
                req.headers[SYSMETA_VERSIONS_MODE] = mode

                # reset original header on container server to maintain sanity
                # now only sysmeta is source of Versions Location
                req.headers[CLIENT_VERSIONS_LOC] = ''

                # if both add and remove headers are in the same request
                # adding location takes precedence over removing
                for header in ['X-Remove-Versions-Location',
                               'X-Remove-History-Location']:
                    if header in req.headers:
                        del req.headers[header]

        if any(req.headers.get(header) for header in [
                'X-Remove-Versions-Location',
                'X-Remove-History-Location']):
            req.headers.update({CLIENT_VERSIONS_LOC: '',
                                SYSMETA_VERSIONS_LOC: '',
                                SYSMETA_VERSIONS_MODE: ''})
            for header in ['X-Remove-Versions-Location',
                           'X-Remove-History-Location']:
                if header in req.headers:
                    del req.headers[header]

        # send request and translate sysmeta headers from response
        vw_ctx = VersionedWritesContext(self.app, self.logger)
        return vw_ctx.handle_container_request(req.environ, start_response)

    def object_request(self, req, api_version, account, container, obj,
                       allow_versioned_writes):
        """
        Handle request for object resource.

        Note that account, container, obj should be unquoted by caller
        if the url path is under url encoding (e.g. %FF)

        :param req: swift.common.swob.Request instance
        :param api_version: should be v1 unless swift bumps api version
        :param account: account name string
        :param container: container name string
        :param object: object name string
        """
        resp = None
        is_enabled = config_true_value(allow_versioned_writes)
        container_info = get_container_info(
            req.environ, self.app, swift_source='VW')

        # To maintain backwards compatibility, container version
        # location could be stored as sysmeta or not, need to check both.
        # If stored as sysmeta, check if middleware is enabled. If sysmeta
        # is not set, but versions property is set in container_info, then
        # for backwards compatibility feature is enabled.
        versions_cont = container_info.get(
            'sysmeta', {}).get('versions-location')
        versioning_mode = container_info.get(
            'sysmeta', {}).get('versions-mode', 'stack')
        if not versions_cont:
            versions_cont = container_info.get('versions')
            # if allow_versioned_writes is not set in the configuration files
            # but 'versions' is configured, enable feature to maintain
            # backwards compatibility
            if not allow_versioned_writes and versions_cont:
                is_enabled = True

        if is_enabled and versions_cont:
            versions_cont = wsgi_unquote(str_to_wsgi(
                versions_cont)).split('/')[0]
            vw_ctx = VersionedWritesContext(self.app, self.logger)
            if req.method == 'PUT':
                resp = vw_ctx.handle_obj_versions_put(
                    req, versions_cont, api_version, account,
                    obj)
            # handle DELETE
            elif versioning_mode == 'history':
                resp = vw_ctx.handle_obj_versions_delete_push(
                    req, versions_cont, api_version, account,
                    container, obj)
            else:
                resp = vw_ctx.handle_obj_versions_delete_pop(
                    req, versions_cont, api_version, account,
                    container, obj)

        if resp:
            return resp
        else:
            return self.app

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            (api_version, account, container, obj) = req.split_path(3, 4, True)
            is_cont_or_obj_req = True
        except ValueError:
            is_cont_or_obj_req = False
        if not is_cont_or_obj_req:
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
        elif (obj and req.method in ('PUT', 'DELETE')):
            try:
                return self.object_request(
                    req, api_version, account, container, obj,
                    allow_versioned_writes)(env, start_response)
            except HTTPException as error_response:
                return error_response(env, start_response)
        else:
            return self.app(env, start_response)
