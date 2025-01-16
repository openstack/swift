# Copyright (c) 2010-2012 OpenStack Foundation
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

import json

from urllib.parse import unquote

from swift.common.utils import public, private, csv_append, Timestamp, \
    config_true_value, cache_from_env, filter_namespaces, NamespaceBoundList
from swift.common.constraints import check_metadata, CONTAINER_LISTING_LIMIT
from swift.common.http import HTTP_ACCEPTED, is_success
from swift.common.request_helpers import get_sys_meta_prefix, get_param, \
    constrain_req_limit, validate_container_params
from swift.proxy.controllers.base import Controller, delay_denial, NodeIter, \
    cors_validation, set_info_cache, clear_info_cache, get_container_info, \
    record_cache_op_metrics, get_cache_key, headers_from_container_info, \
    update_headers, set_namespaces_in_cache, get_namespaces_from_cache
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, HTTPNotFound, \
    HTTPServiceUnavailable, str_to_wsgi, wsgi_to_str, Response


class ContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'

    # Ensure these are all lowercase
    pass_through_headers = ['x-container-read', 'x-container-write',
                            'x-container-sync-key', 'x-container-sync-to',
                            'x-versions-location']

    def __init__(self, app, account_name, container_name, **kwargs):
        super(ContainerController, self).__init__(app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)

    def _x_remove_headers(self):
        st = self.server_type.lower()
        return ['x-remove-%s-read' % st,
                'x-remove-%s-write' % st,
                'x-remove-versions-location',
                'x-remove-%s-sync-key' % st,
                'x-remove-%s-sync-to' % st]

    def _convert_policy_to_index(self, req):
        """
        Helper method to convert a policy name (from a request from a client)
        to a policy index (for a request to a backend).

        :param req: incoming request
        """
        policy_name = req.headers.get('X-Storage-Policy')
        if not policy_name:
            return
        policy = POLICIES.get_by_name(policy_name)
        if not policy:
            raise HTTPBadRequest(request=req,
                                 content_type="text/plain",
                                 body=("Invalid %s '%s'"
                                       % ('X-Storage-Policy', policy_name)))
        if policy.is_deprecated:
            body = 'Storage Policy %r is deprecated' % (policy.name)
            raise HTTPBadRequest(request=req, body=body)
        return int(policy)

    def clean_acls(self, req):
        if 'swift.clean_acl' in req.environ:
            for header in ('x-container-read', 'x-container-write'):
                if header in req.headers:
                    try:
                        req.headers[header] = \
                            req.environ['swift.clean_acl'](header,
                                                           req.headers[header])
                    except ValueError as err:
                        return HTTPBadRequest(request=req, body=str(err))
        return None

    def _clear_container_info_cache(self, req):
        clear_info_cache(req.environ,
                         self.account_name, self.container_name)
        clear_info_cache(req.environ,
                         self.account_name, self.container_name, 'listing')
        # TODO: should we also purge updating shards from cache?

    def _GETorHEAD_from_backend(self, req):
        part = self.app.container_ring.get_part(
            self.account_name, self.container_name)
        concurrency = self.app.container_ring.replica_count \
            if self.app.get_policy_options(None).concurrent_gets else 1
        node_iter = NodeIter(
            'container', self.app, self.app.container_ring, part,
            self.logger, req)
        resp = self.GETorHEAD_base(
            req, 'Container', node_iter, part,
            req.swift_entity_path, concurrency)
        return resp

    def _filter_complete_listing(self, req, namespaces):
        """
        Filter complete list of namespaces to return only those specified by
        the request constraints.

        :param req: a :class:`~swift.common.swob.Request`.
        :param namespaces: a list of :class:`~swift.common.utils.Namespace`.
        :return: a list of :class:`~swift.common.utils.Namespace`.
        """
        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        includes = get_param(req, 'includes')
        reverse = config_true_value(get_param(req, 'reverse'))
        if reverse:
            marker, end_marker = end_marker, marker
        namespaces = filter_namespaces(
            namespaces, includes, marker, end_marker)
        if reverse:
            namespaces.reverse()
        return namespaces

    def _get_listing_namespaces_from_cache(self, req, headers):
        """
        Try to fetch shard namespace data from cache and, if successful, return
        a list of Namespaces. Also return the cache state.

        :param req: an instance of ``swob.Request``.
        :return: a tuple comprising (a list instance of ``Namespace`` objects
            or ``None`` if no namespaces were found in cache, the cache state).
        """
        cache_key = get_cache_key(self.account_name, self.container_name,
                                  shard='listing')
        skip_chance = self.app.container_listing_shard_ranges_skip_cache
        ns_bound_list, cache_state = get_namespaces_from_cache(
            req, cache_key, skip_chance)
        if not ns_bound_list:
            return None, None, cache_state

        # Namespaces found in cache so there is no need to go to backend,
        # but we need to build response headers: mimic
        # GetOrHeadHandler.get_working_response...
        # note: server sets charset with content_type but proxy
        # GETorHEAD_base does not, so don't set it here either
        namespaces = ns_bound_list.get_namespaces()
        self.logger.debug('Found %d shards in cache for %s',
                          len(namespaces), req.path_qs)
        headers.update({'x-backend-record-type': 'shard',
                        'x-backend-record-shard-format': 'namespace',
                        'x-backend-cached-results': 'true'})
        resp = Response(request=req)
        update_headers(resp, headers)
        resp.last_modified = Timestamp(headers['x-put-timestamp']).ceil()
        resp.environ['swift_x_timestamp'] = headers.get('x-timestamp')
        resp.accept_ranges = 'bytes'
        resp.content_type = 'application/json'
        namespaces = self._filter_complete_listing(req, namespaces)
        return resp, namespaces, cache_state

    def _set_listing_namespaces_in_cache(self, req, namespaces):
        """
        Store a list of namespaces in both infocache and memcache.

        Note: the returned list of namespaces may not be identical to the given
        list. Any gaps in the given namespaces will be 'lost' as a result of
        compacting the list of namespaces to a NamespaceBoundList for caching.
        That is ok. When the cached NamespaceBoundList is transformed back to
        Namespaces to perform a listing, the Namespace before each gap will
        have expanded to include the gap, which means that the backend GET to
        that shard will have an end_marker beyond that shard's upper bound, and
        equal to the next available shard's lower. At worst, some misplaced
        objects, in the gap above the shard's upper, may be included in the
        shard's response.

        :param req: the request object.
        :param namespaces:  a list of :class:`~swift.common.utils.Namespace`
            objects.
        :return: a list of :class:`~swift.common.utils.Namespace` objects.
        """
        cache_key = get_cache_key(self.account_name, self.container_name,
                                  shard='listing')
        ns_bound_list = NamespaceBoundList.parse(namespaces)
        # cache in infocache even if no namespaces returned; this
        # is unexpected but use that result for this request
        set_cache_state = set_namespaces_in_cache(
            req, cache_key, ns_bound_list,
            self.app.recheck_listing_shard_ranges)
        if set_cache_state == 'set':
            self.logger.info(
                'Caching listing namespaces for %s (%d namespaces)',
                cache_key, len(ns_bound_list.bounds))
        # return the de-gapped namespaces
        return ns_bound_list.get_namespaces()

    def _get_listing_namespaces_from_backend(self, req, cache_enabled):
        """
        Fetch shard namespace data from the backend and, if successful, return
        a list of Namespaces.

        :param req: an instance of ``swob.Request``.
        :param cache_enabled: a boolean which should be True if memcache is
            available to cache the returned data, False otherwise.
        :return: a list instance of ``Namespace`` objects or ``None`` if no
            namespace data was returned from the backend.
        """
        # Instruct the backend server to 'automatically' return namespaces
        # of shards in a 'listing' state if the container is sharded, and
        # that the more compact 'namespace' format is sufficient. Older
        # container servers may still respond with the 'full' shard range
        # format.
        req.headers['X-Backend-Record-Type'] = 'auto'
        req.headers['X-Backend-Record-Shard-Format'] = 'namespace'
        # 'x-backend-include-deleted' is not expected in 'auto' requests to
        # the proxy (it's not supported for objects and is used by the
        # sharder when explicitly fetching 'shard' record type), but we
        # explicitly set it to false here just in case. A newer container
        # server would ignore it when returning namespaces, but an older
        # container server would include unwanted deleted shard range.
        req.headers['X-Backend-Include-Deleted'] = 'false'
        params = req.params
        params['states'] = 'listing'
        req.params = params
        if cache_enabled:
            # Instruct the backend server to ignore name constraints in
            # request params if returning namespaces so that the response
            # can potentially be cached, but only if the container state is
            # 'sharded'. We don't attempt to cache namespaces for a
            # 'sharding' container as they may include the container itself
            # as a 'gap filler' for shards that have not yet cleaved;
            # listings from 'gap filler' namespaces are likely to become
            # stale as the container continues to cleave objects to its
            # shards and caching them is therefore more likely to result in
            # stale or incomplete listings on subsequent container GETs.
            req.headers['x-backend-override-shard-name-filter'] = 'sharded'
        resp = self._GETorHEAD_from_backend(req)
        resp_record_type = resp.headers.get(
            'x-backend-record-type', '').lower()
        sharding_state = resp.headers.get(
            'x-backend-sharding-state', '').lower()
        complete_listing = config_true_value(resp.headers.pop(
            'x-backend-override-shard-name-filter', False))
        if resp_record_type == 'shard':
            data = self._parse_listing_response(req, resp)
            namespaces = self._parse_namespaces(req, data, resp)
            # given that we sent
            # 'x-backend-override-shard-name-filter=sharded' we should only
            # receive back 'x-backend-override-shard-name-filter=true' if
            # the sharding state is 'sharded', but check them both
            # anyway...
            if (namespaces and
                    sharding_state == 'sharded' and
                    complete_listing):
                namespaces = self._set_listing_namespaces_in_cache(
                    req, namespaces)
                namespaces = self._filter_complete_listing(req, namespaces)
        else:
            namespaces = None
        return resp, namespaces

    def _record_shard_listing_cache_metrics(self, cache_state, resp, info):
        """
        Record a single cache operation by shard listing into its
        corresponding metrics.

        :param  cache_state: the state of this cache operation, includes
                  infocache_hit, memcache hit, miss, error, skip, force_skip
                  and disabled.
        :param  resp: the response from either backend or cache hit.
        :param  info: the cached container info.
        """
        should_record = False
        if is_success(resp.status_int):
            if resp.headers.get('X-Backend-Record-Type', '') == 'shard':
                # Here we either got namespaces by hitting the cache, or we
                # got namespaces from backend successfully for cache_state
                # other than cache hit. Note: it's possible that later we find
                # that namespaces can't be parsed.
                should_record = True
        elif (info and is_success(info['status'])
                and info.get('sharding_state') == 'sharded'):
            # The shard listing request failed when getting namespaces from
            # backend.
            # Note: In the absence of 'info' we cannot assume the container is
            # sharded, so we don't increment the metric if 'info' is None. Even
            # when we have valid info, we can't be sure that the container is
            # sharded, but we assume info was correct and increment the failure
            # metrics.
            should_record = True
        # else:
        #  The request failed, but in the absence of info we cannot assume
        #  the container is sharded, so we don't increment the metric.

        if should_record:
            record_cache_op_metrics(
                self.logger, self.server_type.lower(), 'shard_listing',
                cache_state, resp)

    def _GET_auto(self, req):
        # This is an object listing but the backend may be sharded.
        # Only lookup container info from cache and skip the backend HEAD,
        # since we are going to GET the backend container anyway.
        info = get_container_info(
            req.environ, self.app, swift_source=None, cache_only=True)
        memcache = cache_from_env(req.environ, True)
        cache_enabled = self.app.recheck_listing_shard_ranges > 0 and memcache
        resp = namespaces = None
        if cache_enabled:
            # if the container is sharded we may look for namespaces in cache
            headers = headers_from_container_info(info)
            if config_true_value(req.headers.get('x-newest', False)):
                cache_state = 'force_skip'
                self.logger.debug(
                    'Skipping shard cache lookup (x-newest) for %s',
                    req.path_qs)
            elif (headers and is_success(info['status']) and
                  info.get('sharding_state') == 'sharded'):
                # container is sharded so we may have the namespaces cached,
                # but only use cached namespaces if all required response
                # headers are also available from cache.
                resp, namespaces, cache_state = \
                    self._get_listing_namespaces_from_cache(req, headers)
            else:
                # container metadata didn't support a cache lookup, this could
                # be the case that container metadata was not in cache and we
                # don't know if the container was sharded, or the case that the
                # sharding state in metadata indicates the container was
                # unsharded.
                cache_state = 'bypass'
        else:
            cache_state = 'disabled'

        if not namespaces:
            resp, namespaces = self._get_listing_namespaces_from_backend(
                req, cache_enabled)
        self._record_shard_listing_cache_metrics(cache_state, resp, info)

        if namespaces is not None:
            # we got namespaces, so the container must be sharded; now build
            # the listing from shards
            # NB: the filtered namespaces list may be empty but we still need
            # to build a response body with an empty list of objects
            resp = self._get_from_shards(req, resp, namespaces)

        return resp

    def _get_or_head_pre_check(self, req):
        ai = self.account_info(self.account_name, req)
        auto_account = self.account_name.startswith(
            self.app.auto_create_account_prefix)
        if not (auto_account or ai[1]):
            if 'swift.authorize' in req.environ:
                aresp = req.environ['swift.authorize'](req)
                if aresp:
                    # Don't cache this. It doesn't reflect the state of the
                    # container, just that the user can't access it.
                    return aresp
            # Don't cache this. The lack of account will be cached, and that
            # is sufficient.
            return HTTPNotFound(request=req)
        return None

    def _get_or_head_post_check(self, req, resp):
        if not config_true_value(
                resp.headers.get('X-Backend-Cached-Results')):
            # Cache container metadata. We just made a request to a storage
            # node and got up-to-date information for the container.
            resp.headers['X-Backend-Recheck-Container-Existence'] = str(
                self.app.recheck_container_existence)
            set_info_cache(req.environ, self.account_name,
                           self.container_name, resp)

        if 'swift.authorize' in req.environ:
            req.acl = wsgi_to_str(resp.headers.get('x-container-read'))
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                # Don't cache this. It doesn't reflect the state of the
                # container, just that the user can't access it.
                return aresp
        if not req.environ.get('swift_owner', False):
            for key in self.app.swift_owner_headers:
                if key in resp.headers:
                    del resp.headers[key]
        # Expose sharding state in reseller requests
        if req.environ.get('reseller_request', False):
            resp.headers['X-Container-Sharding'] = config_true_value(
                resp.headers.get(get_sys_meta_prefix('container') + 'Sharding',
                                 'False'))
        return resp

    @public
    @delay_denial
    @cors_validation
    def GET(self, req):
        """Handler for HTTP GET requests."""
        # early checks for request validity
        validate_container_params(req)
        aresp = self._get_or_head_pre_check(req)
        if aresp:
            return aresp

        # Always request json format from the backend. listing_formats
        # middleware will take care of what the client gets back.
        # The read-modify-write of params here is because the
        # Request.params getter dynamically generates a dict of params from
        # the query string; the setter must be called for new params to
        # update the query string.
        params = req.params
        params['format'] = 'json'
        req.params = params

        # x-backend-record-type may be sent via internal client e.g. from
        # the sharder or in probe tests
        record_type = req.headers.get('X-Backend-Record-Type', '').lower()
        if record_type in ('object', 'shard'):
            # Go direct to the backend for HEADs, and GETs that *explicitly*
            # specify a record type. We won't be reading/writing namespaces in
            # cache nor building listings from shards. This path is used by
            # the sharder, manage_shard_ranges and other tools that fetch shard
            # ranges, and by the proxy itself when explicitly requesting
            # objects while recursively building a listing from shards.
            # Note: shard record type could be namespace or full format
            resp = self._GETorHEAD_from_backend(req)
        else:
            # Requests that do not explicitly specify a record type, or specify
            # 'auto', default to returning an object listing. The listing may
            # be built from shards and may involve reading/writing namespaces
            # in cache. This path is used for client requests and by the proxy
            # itself while recursively building a listing from shards.
            resp = self._GET_auto(req)
            resp.headers.pop('X-Backend-Record-Type', None)
            resp.headers.pop('X-Backend-Record-Shard-Format', None)

        return self._get_or_head_post_check(req, resp)

    def _get_from_shards(self, req, resp, namespaces):
        """
        Construct an object listing using shards described by the list of
        namespaces.

        :param req: an instance of :class:`~swift.common.swob.Request`.
        :param resp: an instance of :class:`~swift.common.swob.Response`.
        :param namespaces: a list of :class:`~swift.common.utils.Namespace`.
        :return: an instance of :class:`~swift.common.swob.Response`. If an
            error is encountered while building the listing an instance of
            ``HTTPServiceUnavailable`` may be returned. Otherwise, the given
            ``resp`` is returned with a body that is an object listing.
        """
        # The history of containers that have returned namespaces is
        # maintained in the request environ so that loops can be avoided by
        # forcing an object listing if the same container is visited again.
        # This can happen in at least two scenarios:
        #   1. a container has filled a gap in its namespaces with a
        #      namespace pointing to itself
        #   2. a root container returns a (stale) namespace pointing to a
        #      shard that has shrunk into the root, in which case the shrunken
        #      shard may return the root's namespace.
        shard_listing_history = req.environ.setdefault(
            'swift.shard_listing_history', [])
        policy_key = 'X-Backend-Storage-Policy-Index'
        if not (shard_listing_history or policy_key in req.headers):
            # We're handling the original request to the root container: set
            # the root policy index in the request, unless it is already set,
            # so that shards will return listings for that policy index.
            # Note: we only get here if the root responded with namespaces,
            # or if the namespaces were cached and the cached root container
            # info has sharding_state==sharded; in both cases we can assume
            # that the response is "modern enough" to include
            # 'X-Backend-Storage-Policy-Index'.
            req.headers[policy_key] = resp.headers[policy_key]
        shard_listing_history.append((self.account_name, self.container_name))
        self.logger.debug('GET listing from %s shards for: %s',
                          len(namespaces), req.path_qs)

        objects = []
        req_limit = constrain_req_limit(req, CONTAINER_LISTING_LIMIT)
        params = req.params.copy()
        params.pop('states', None)
        req.headers.pop('X-Backend-Record-Type', None)
        reverse = config_true_value(params.get('reverse'))
        marker = wsgi_to_str(params.get('marker'))
        end_marker = wsgi_to_str(params.get('end_marker'))
        prefix = wsgi_to_str(params.get('prefix'))

        limit = req_limit
        all_resp_status = []
        for i, namespace in enumerate(namespaces):
            params['limit'] = limit
            # Always set marker to ensure that object names less than or equal
            # to those already in the listing are not fetched; if the listing
            # is empty then the original request marker, if any, is used. This
            # allows misplaced objects below the expected namespace to be
            # included in the listing.
            last_name = ''
            last_name_was_subdir = False
            if objects:
                last_name_was_subdir = 'subdir' in objects[-1]
                if last_name_was_subdir:
                    last_name = objects[-1]['subdir']
                else:
                    last_name = objects[-1]['name']

                params['marker'] = str_to_wsgi(last_name)
            elif marker:
                params['marker'] = str_to_wsgi(marker)
            else:
                params['marker'] = ''
            # Always set end_marker to ensure that misplaced objects beyond the
            # expected namespace are not fetched. This prevents a misplaced
            # object obscuring correctly placed objects in the next shard
            # range.
            if end_marker and end_marker in namespace:
                params['end_marker'] = str_to_wsgi(end_marker)
            elif reverse:
                params['end_marker'] = str_to_wsgi(namespace.lower_str)
            else:
                params['end_marker'] = str_to_wsgi(namespace.end_marker)

            headers = {}
            if ((namespace.account, namespace.container) in
                    shard_listing_history):
                # directed back to same container - force GET of objects
                headers['X-Backend-Record-Type'] = 'object'
            else:
                headers['X-Backend-Record-Type'] = 'auto'
            if config_true_value(req.headers.get('x-newest', False)):
                headers['X-Newest'] = 'true'

            if prefix:
                if prefix > namespace:
                    continue
                try:
                    just_past = prefix[:-1] + chr(ord(prefix[-1]) + 1)
                except ValueError:
                    pass
                else:
                    if just_past < namespace:
                        continue

            if last_name_was_subdir and str(
                namespace.lower if reverse else namespace.upper
            ).startswith(last_name):
                continue

            self.logger.debug(
                'Getting listing part %d from shard %s %s with %s',
                i, namespace, namespace.name, headers)
            objs, shard_resp = self._get_container_listing(
                req, namespace.account, namespace.container,
                headers=headers, params=params)
            all_resp_status.append(shard_resp.status_int)

            sharding_state = shard_resp.headers.get('x-backend-sharding-state',
                                                    'unknown')

            if objs is None:
                # give up if any non-success response from shard containers
                self.logger.error(
                    'Aborting listing from shards due to bad response: %r'
                    % all_resp_status)
                return HTTPServiceUnavailable(request=req)
            shard_policy = shard_resp.headers.get(
                'X-Backend-Record-Storage-Policy-Index',
                shard_resp.headers[policy_key]
            )
            if shard_policy != req.headers[policy_key]:
                self.logger.error(
                    'Aborting listing from shards due to bad shard policy '
                    'index: %s (expected %s)',
                    shard_policy, req.headers[policy_key])
                return HTTPServiceUnavailable(request=req)
            self.logger.debug(
                'Found %d objects in shard (state=%s), total = %d',
                len(objs), sharding_state, len(objs) + len(objects))

            if not objs:
                # tolerate empty shard containers
                continue

            objects.extend(objs)
            limit -= len(objs)

            if limit <= 0:
                break
            last_name = objects[-1].get('name',
                                        objects[-1].get('subdir', u''))
            if end_marker and reverse and end_marker >= last_name:
                break
            if end_marker and not reverse and end_marker <= last_name:
                break

        resp.body = json.dumps(objects).encode('ascii')
        constrained = any(req.params.get(constraint) for constraint in (
            'marker', 'end_marker', 'path', 'prefix', 'delimiter'))
        if not constrained and len(objects) < req_limit:
            self.logger.debug('Setting object count to %s' % len(objects))
            # prefer the actual listing stats over the potentially outdated
            # root stats. This condition is only likely when a sharded
            # container is shrinking or in tests; typically a sharded container
            # will have more than CONTAINER_LISTING_LIMIT objects so any
            # unconstrained listing will be capped by the limit and total
            # object stats cannot therefore be inferred from the listing.
            resp.headers['X-Container-Object-Count'] = len(objects)
            resp.headers['X-Container-Bytes-Used'] = sum(
                [o['bytes'] for o in objects])
        return resp

    @public
    @delay_denial
    @cors_validation
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        aresp = self._get_or_head_pre_check(req)
        if aresp:
            return aresp
        resp = self._GETorHEAD_from_backend(req)
        return self._get_or_head_post_check(req, resp)

    @public
    @cors_validation
    def PUT(self, req):
        """HTTP PUT request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        policy_index = self._convert_policy_to_index(req)
        if not req.environ.get('swift_owner'):
            for key in self.app.swift_owner_headers:
                req.headers.pop(key, None)
        if req.environ.get('reseller_request', False) and \
                'X-Container-Sharding' in req.headers:
            req.headers[get_sys_meta_prefix('container') + 'Sharding'] = \
                str(config_true_value(req.headers['X-Container-Sharding']))
        length_limit = self.get_name_length_limit()
        if len(self.container_name) > length_limit:
            body = 'Container name length of %d longer than %d' % (
                len(self.container_name), length_limit)
            resp = HTTPBadRequest(request=req, body=body)
            return resp
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts and self.app.account_autocreate:
            if not self.autocreate_account(req, self.account_name):
                return HTTPServiceUnavailable(request=req)
            account_partition, accounts, container_count = \
                self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        if 0 < self.app.max_containers_per_account <= container_count and \
                self.account_name not in self.app.max_containers_whitelist:
            container_info = \
                self.container_info(self.account_name, self.container_name,
                                    req)
            if not is_success(container_info.get('status')):
                body = 'Reached container limit of %s' % (
                    self.app.max_containers_per_account, )
                resp = HTTPForbidden(request=req, body=body)
                return resp
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self._backend_requests(req, len(containers),
                                         account_partition, accounts,
                                         policy_index)
        resp = self.make_requests(
            req, self.app.container_ring,
            container_partition, 'PUT', req.swift_entity_path, headers)
        self._clear_container_info_cache(req)
        return resp

    @public
    @cors_validation
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        if not req.environ.get('swift_owner'):
            for key in self.app.swift_owner_headers:
                req.headers.pop(key, None)
        if req.environ.get('reseller_request', False) and \
                'X-Container-Sharding' in req.headers:
            req.headers[get_sys_meta_prefix('container') + 'Sharding'] = \
                str(config_true_value(req.headers['X-Container-Sharding']))
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self.generate_request_headers(req, transfer=True)
        self._clear_container_info_cache(req)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'POST',
            req.swift_entity_path, [headers] * len(containers))
        return resp

    @public
    @cors_validation
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self._backend_requests(req, len(containers),
                                         account_partition, accounts)
        self._clear_container_info_cache(req)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'DELETE',
            req.swift_entity_path, headers)
        # Indicates no server had the container
        if resp.status_int == HTTP_ACCEPTED:
            return HTTPNotFound(request=req)
        return resp

    @private
    def UPDATE(self, req):
        """HTTP UPDATE request handler.

        Method to perform bulk operations on container DBs,
        similar to a merge_items REPLICATE request.

        Not client facing; internal clients or middlewares must include
        ``X-Backend-Allow-Private-Methods: true`` header to access.
        """
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        # Since this isn't client facing, expect callers to supply an index
        policy_index = req.headers['X-Backend-Storage-Policy-Index']
        headers = self._backend_requests(
            req, len(containers), account_partition=None, accounts=[],
            policy_index=policy_index)
        return self.make_requests(
            req, self.app.container_ring, container_partition, 'UPDATE',
            req.swift_entity_path, headers, body=req.body)

    def _backend_requests(self, req, n_outgoing, account_partition, accounts,
                          policy_index=None):
        additional = {'X-Timestamp': Timestamp.now().internal}
        if policy_index is None:
            additional['X-Backend-Storage-Policy-Default'] = \
                int(POLICIES.default)
        else:
            additional['X-Backend-Storage-Policy-Index'] = str(policy_index)
        headers = [self.generate_request_headers(req, transfer=True,
                                                 additional=additional)
                   for _junk in range(n_outgoing)]

        for i, account in enumerate(accounts):
            i = i % len(headers)

            headers[i]['X-Account-Partition'] = account_partition
            headers[i]['X-Account-Host'] = csv_append(
                headers[i].get('X-Account-Host'),
                '%(ip)s:%(port)s' % account)
            headers[i]['X-Account-Device'] = csv_append(
                headers[i].get('X-Account-Device'),
                account['device'])

        return headers
