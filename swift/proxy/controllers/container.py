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

from swift import gettext_ as _
import json
import math

import six
from six.moves.urllib.parse import unquote

from swift.common.utils import public, private, csv_append, Timestamp, \
    config_true_value, ShardRange, cache_from_env, filter_shard_ranges
from swift.common.constraints import check_metadata, CONTAINER_LISTING_LIMIT
from swift.common.http import HTTP_ACCEPTED, is_success
from swift.common.request_helpers import get_sys_meta_prefix, get_param, \
    constrain_req_limit, validate_container_params
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, set_info_cache, clear_info_cache, _get_info_from_caches, \
    get_cache_key, headers_from_container_info, update_headers
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, HTTPNotFound, \
    HTTPServiceUnavailable, str_to_wsgi, wsgi_to_str, bytes_to_wsgi, Response


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
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name, 'listing')
        # TODO: should we also purge updating shards from cache?

    def _GETorHEAD_from_backend(self, req):
        part = self.app.container_ring.get_part(
            self.account_name, self.container_name)
        concurrency = self.app.container_ring.replica_count \
            if self.app.get_policy_options(None).concurrent_gets else 1
        node_iter = self.app.iter_nodes(self.app.container_ring, part)
        resp = self.GETorHEAD_base(
            req, _('Container'), node_iter, part,
            req.swift_entity_path, concurrency)
        return resp

    def _filter_resp_shard_ranges(self, req, cached_ranges):
        # filter returned shard ranges according to request constraints
        marker = get_param(req, 'marker', '')
        end_marker = get_param(req, 'end_marker')
        includes = get_param(req, 'includes')
        reverse = config_true_value(get_param(req, 'reverse'))
        if reverse:
            marker, end_marker = end_marker, marker
        shard_ranges = [
            ShardRange.from_dict(shard_range)
            for shard_range in cached_ranges]
        shard_ranges = filter_shard_ranges(shard_ranges, includes, marker,
                                           end_marker)
        if reverse:
            shard_ranges.reverse()
        return json.dumps([dict(sr) for sr in shard_ranges]).encode('ascii')

    def _GET_using_cache(self, req):
        # It may be possible to fulfil the request from cache: we only reach
        # here if request record_type is 'shard' or 'auto', so if the container
        # state is 'sharded' then look for cached shard ranges. However, if
        # X-Newest is true then we always fetch from the backend servers.
        get_newest = config_true_value(req.headers.get('x-newest', False))
        if get_newest:
            self.app.logger.debug(
                'Skipping shard cache lookup (x-newest) for %s', req.path_qs)
            info = None
        else:
            info = _get_info_from_caches(self.app, req.environ,
                                         self.account_name,
                                         self.container_name)
        if (info and is_success(info['status']) and
                info.get('sharding_state') == 'sharded'):
            # container is sharded so we may have the shard ranges cached
            headers = headers_from_container_info(info)
            if headers:
                # only use cached values if all required headers available
                infocache = req.environ.setdefault('swift.infocache', {})
                memcache = cache_from_env(req.environ, True)
                cache_key = get_cache_key(self.account_name,
                                          self.container_name,
                                          shard='listing')
                cached_ranges = infocache.get(cache_key)
                if cached_ranges is None and memcache:
                    cached_ranges = memcache.get(cache_key)
                if cached_ranges is not None:
                    infocache[cache_key] = tuple(cached_ranges)
                    # shard ranges can be returned from cache
                    self.app.logger.debug('Found %d shards in cache for %s',
                                          len(cached_ranges), req.path_qs)
                    headers.update({'x-backend-record-type': 'shard',
                                    'x-backend-cached-results': 'true'})
                    shard_range_body = self._filter_resp_shard_ranges(
                        req, cached_ranges)
                    # mimic GetOrHeadHandler.get_working_response...
                    # note: server sets charset with content_type but proxy
                    # GETorHEAD_base does not, so don't set it here either
                    resp = Response(request=req, body=shard_range_body)
                    update_headers(resp, headers)
                    resp.last_modified = math.ceil(
                        float(headers['x-put-timestamp']))
                    resp.environ['swift_x_timestamp'] = headers.get(
                        'x-timestamp')
                    resp.accept_ranges = 'bytes'
                    resp.content_type = 'application/json'
                    return resp

        # The request was not fulfilled from cache so send to the backend
        # server, but instruct the backend server to ignore name constraints in
        # request params if returning shard ranges so that the response can
        # potentially be cached. Only do this if the container state is
        # 'sharded'. We don't attempt to cache shard ranges for a 'sharding'
        # container as they may include the container itself as a 'gap filler'
        # for shard ranges that have not yet cleaved; listings from 'gap
        # filler' shard ranges are likely to become stale as the container
        # continues to cleave objects to its shards and caching them is
        # therefore more likely to result in stale or incomplete listings on
        # subsequent container GETs.
        req.headers['x-backend-override-shard-name-filter'] = 'sharded'
        resp = self._GETorHEAD_from_backend(req)

        sharding_state = resp.headers.get(
            'x-backend-sharding-state', '').lower()
        resp_record_type = resp.headers.get(
            'x-backend-record-type', '').lower()
        complete_listing = config_true_value(resp.headers.pop(
            'x-backend-override-shard-name-filter', False))
        # given that we sent 'x-backend-override-shard-name-filter=sharded' we
        # should only receive back 'x-backend-override-shard-name-filter=true'
        # if the sharding state is 'sharded', but check them both anyway...
        if (resp_record_type == 'shard' and
                sharding_state == 'sharded' and
                complete_listing):
            # backend returned unfiltered listing state shard ranges so parse
            # them and replace response body with filtered listing
            cache_key = get_cache_key(self.account_name, self.container_name,
                                      shard='listing')
            data = self._parse_listing_response(req, resp)
            backend_shard_ranges = self._parse_shard_ranges(req, data, resp)
            if backend_shard_ranges is not None:
                cached_ranges = [dict(sr) for sr in backend_shard_ranges]
                if resp.headers.get('x-backend-sharding-state') == 'sharded':
                    # cache in infocache even if no shard ranges returned; this
                    # is unexpected but use that result for this request
                    infocache = req.environ.setdefault('swift.infocache', {})
                    infocache[cache_key] = tuple(cached_ranges)
                    memcache = cache_from_env(req.environ, True)
                    if memcache and cached_ranges:
                        # cache in memcache only if shard ranges as expected
                        self.app.logger.debug('Caching %d shards for %s',
                                              len(cached_ranges), req.path_qs)
                        memcache.set(
                            cache_key, cached_ranges,
                            time=self.app.recheck_listing_shard_ranges)

                # filter returned shard ranges according to request constraints
                resp.body = self._filter_resp_shard_ranges(req, cached_ranges)

        return resp

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
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

        # The read-modify-write of params here is because the Request.params
        # getter dynamically generates a dict of params from the query string;
        # the setter must be called for new params to update the query string.
        params = req.params
        params['format'] = 'json'
        # x-backend-record-type may be sent via internal client e.g. from
        # the sharder or in probe tests
        record_type = req.headers.get('X-Backend-Record-Type', '').lower()
        if not record_type:
            record_type = 'auto'
            req.headers['X-Backend-Record-Type'] = 'auto'
            params['states'] = 'listing'
        req.params = params

        memcache = cache_from_env(req.environ, True)
        if (req.method == 'GET' and
                record_type != 'object' and
                self.app.recheck_listing_shard_ranges > 0 and
                memcache and
                get_param(req, 'states') == 'listing' and
                not config_true_value(
                    req.headers.get('x-backend-include-deleted', False))):
            # This GET might be served from cache or might populate cache.
            # 'x-backend-include-deleted' is not usually expected in requests
            # to the proxy (it is used from sharder to container servers) but
            # it is included in the conditions just in case because we don't
            # cache deleted shard ranges.
            resp = self._GET_using_cache(req)
        else:
            resp = self._GETorHEAD_from_backend(req)

        resp_record_type = resp.headers.get('X-Backend-Record-Type', '')
        if all((req.method == "GET", record_type == 'auto',
               resp_record_type.lower() == 'shard')):
            resp = self._get_from_shards(req, resp)

        if not config_true_value(
                resp.headers.get('X-Backend-Cached-Results')):
            # Cache container metadata. We just made a request to a storage
            # node and got up-to-date information for the container.
            resp.headers['X-Backend-Recheck-Container-Existence'] = str(
                self.app.recheck_container_existence)
            set_info_cache(self.app, req.environ, self.account_name,
                           self.container_name, resp)
        if 'swift.authorize' in req.environ:
            req.acl = resp.headers.get('x-container-read')
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

    def _get_from_shards(self, req, resp):
        # Construct listing using shards described by the response body.
        # The history of containers that have returned shard ranges is
        # maintained in the request environ so that loops can be avoided by
        # forcing an object listing if the same container is visited again.
        # This can happen in at least two scenarios:
        #   1. a container has filled a gap in its shard ranges with a
        #      shard range pointing to itself
        #   2. a root container returns a (stale) shard range pointing to a
        #      shard that has shrunk into the root, in which case the shrunken
        #      shard may return the root's shard range.
        shard_listing_history = req.environ.setdefault(
            'swift.shard_listing_history', [])
        shard_listing_history.append((self.account_name, self.container_name))
        shard_ranges = [ShardRange.from_dict(data)
                        for data in json.loads(resp.body)]
        self.app.logger.debug('GET listing from %s shards for: %s',
                              len(shard_ranges), req.path_qs)
        if not shard_ranges:
            # can't find ranges or there was a problem getting the ranges. So
            # return what we have.
            return resp

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
        for i, shard_range in enumerate(shard_ranges):
            params['limit'] = limit
            # Always set marker to ensure that object names less than or equal
            # to those already in the listing are not fetched; if the listing
            # is empty then the original request marker, if any, is used. This
            # allows misplaced objects below the expected shard range to be
            # included in the listing.
            if objects:
                last_name = objects[-1].get('name',
                                            objects[-1].get('subdir', u''))
                params['marker'] = bytes_to_wsgi(last_name.encode('utf-8'))
            elif marker:
                params['marker'] = str_to_wsgi(marker)
            else:
                params['marker'] = ''
            # Always set end_marker to ensure that misplaced objects beyond the
            # expected shard range are not fetched. This prevents a misplaced
            # object obscuring correctly placed objects in the next shard
            # range.
            if end_marker and end_marker in shard_range:
                params['end_marker'] = str_to_wsgi(end_marker)
            elif reverse:
                params['end_marker'] = str_to_wsgi(shard_range.lower_str)
            else:
                params['end_marker'] = str_to_wsgi(shard_range.end_marker)

            headers = {}
            if ((shard_range.account, shard_range.container) in
                    shard_listing_history):
                # directed back to same container - force GET of objects
                headers['X-Backend-Record-Type'] = 'object'
            if config_true_value(req.headers.get('x-newest', False)):
                headers['X-Newest'] = 'true'

            if prefix:
                if prefix > shard_range:
                    continue
                try:
                    just_past = prefix[:-1] + chr(ord(prefix[-1]) + 1)
                except ValueError:
                    pass
                else:
                    if just_past < shard_range:
                        continue

            self.app.logger.debug(
                'Getting listing part %d from shard %s %s with %s',
                i, shard_range, shard_range.name, headers)
            objs, shard_resp = self._get_container_listing(
                req, shard_range.account, shard_range.container,
                headers=headers, params=params)
            all_resp_status.append(shard_resp.status_int)

            sharding_state = shard_resp.headers.get('x-backend-sharding-state',
                                                    'unknown')

            if objs is None:
                # give up if any non-success response from shard containers
                self.app.logger.error(
                    'Aborting listing from shards due to bad response: %r'
                    % all_resp_status)
                return HTTPServiceUnavailable(request=req)

            self.app.logger.debug(
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
            if six.PY2:
                last_name = last_name.encode('utf8')
            if end_marker and reverse and end_marker >= last_name:
                break
            if end_marker and not reverse and end_marker <= last_name:
                break

        resp.body = json.dumps(objects).encode('ascii')
        constrained = any(req.params.get(constraint) for constraint in (
            'marker', 'end_marker', 'path', 'prefix', 'delimiter'))
        if not constrained and len(objects) < req_limit:
            self.app.logger.debug('Setting object count to %s' % len(objects))
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
    def GET(self, req):
        """Handler for HTTP GET requests."""
        # early checks for request validity
        validate_container_params(req)
        return self.GETorHEAD(req)

    @public
    @delay_denial
    @cors_validation
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

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
        ``X-Backend-Allow-Method: UPDATE`` header to access.
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
