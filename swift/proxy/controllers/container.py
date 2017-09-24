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

from six.moves.urllib.parse import unquote, urlencode
from swift import gettext_ as _
import json

from swift.common.utils import public, csv_append, Timestamp, \
    config_true_value, account_to_shard_account
from swift.common.constraints import check_metadata, CONTAINER_LISTING_LIMIT
from swift.common import constraints
from swift.common.http import HTTP_ACCEPTED, is_success, \
    HTTP_PRECONDITION_FAILED
from swift.common.request_helpers import get_sys_meta_prefix
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, set_info_cache, clear_info_cache
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPNotFound
from swift.container.backend import DB_STATE_SHARDING, DB_STATE_UNSHARDED, \
    DB_STATE_SHARDED


class ContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'

    # Ensure these are all lowercase
    pass_through_headers = ['x-container-read', 'x-container-write',
                            'x-container-sync-key', 'x-container-sync-to',
                            'x-versions-location', 'x-container-sharding']

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

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        ai = self.account_info(self.account_name, req)
        if not ai[1]:
            if 'swift.authorize' in req.environ:
                aresp = req.environ['swift.authorize'](req)
                if aresp:
                    # Don't cache this. It doesn't reflect the state of the
                    # container, just that the user can't access it.
                    return aresp
            # Don't cache this. The lack of account will be cached, and that
            # is sufficient.
            return HTTPNotFound(request=req)
        part = self.app.container_ring.get_part(
            self.account_name, self.container_name)
        concurrency = self.app.container_ring.replica_count \
            if self.app.concurrent_gets else 1
        node_iter = self.app.iter_nodes(self.app.container_ring, part)
        params = req.params
        params['format'] = 'json'
        req.params = params
        resp = self.GETorHEAD_base(
            req, _('Container'), node_iter, part,
            req.swift_entity_path, concurrency)
        sharding_state = \
            int(resp.headers.get('X-Backend-Sharding-State',
                                 DB_STATE_UNSHARDED))
        if req.method == "GET" and params.get('items') != 'shard' and \
                sharding_state in (DB_STATE_SHARDING, DB_STATE_SHARDED):
            # TODO: think about whether this actually does what we want for
            # items=all -- and whether we should even support that here
            new_resp = self._get_sharded(req, resp, sharding_state)
            if new_resp:
                resp = new_resp

        # Cache this. We just made a request to a storage node and got
        # up-to-date information for the container.
        resp.headers['X-Backend-Recheck-Container-Existence'] = str(
            self.app.recheck_container_existence)
        # TODO: seems like a good idea to not set cache for shard/all
        # requests, but revisit this at some point
        if params.get('items') not in ('shard', 'all'):
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

    def _get_sharded(self, req, resp, sharding_state):
        # if sharding, we need to visit all the shards before the upto and
        # merge with this response.
        # TODO: this results in a more up to date listing but currently
        # use staler data
        # upto = None
        # if sharding_state == DB_STATE_SHARDING:
        #     def filter_key(x):
        #         x[0].startswith('X-Container-Sysmeta-Shard-Last-')
        #
        #     uptos = filter(filter_key, req.headers.items())
        #     if uptos:
        #         upto = max(uptos, key=lambda x: x[-1])[0]

        limit = req.params.get('limit', CONTAINER_LISTING_LIMIT)
        shard_account = account_to_shard_account(self.account_name)
        # In whatever case we need the list of ShardRanges that contain this
        # range
        ranges = self._get_shard_ranges(req, self.account_name,
                                        self.container_name)
        if not ranges:
            # can't find ranges or there was a problem getting the ranges. So
            # return what we have.
            return None

        def get_objects(account, container, parameters):
            path = '/%s/%s' % (account, container)
            part, nodes = self.app.container_ring.get_nodes(account, container)
            req_headers = [self.generate_request_headers(req, transfer=True)
                           for _junk in range(len(nodes))]
            response = self.make_requests(req, self.app.container_ring,
                                          part, "GET", path, req_headers,
                                          urlencode(parameters))

            if is_success(response.status_int):
                try:
                    return (response.headers, json.loads(response.body),
                            response)
                except ValueError:
                    pass
            return None, None, response

        def merge_old_new(shard_range, params):
            if shard_range is None:
                return get_objects(self.account_name, self.container_name,
                                   params)
            try:
                params['items'] = 'all'
                # need some extra limit because we are getting deleted objects
                params['limit'] = min(limit * 2, CONTAINER_LISTING_LIMIT)

                hdrs, objs, tmp_resp = get_objects(
                    shard_account, shard_range.name, params)
                if hdrs is None and tmp_resp:
                    return tmp_resp
            finally:
                params.pop('items', None)
                params['limit'] = limit

            # now get the headers from the old db + holding (shard range) db.
            old_hdrs, old_objs, old_resp = \
                get_objects(self.account_name, self.container_name, params)

            if not is_success(old_resp.status_int):
                # just use the new response
                result_objs = [r for r in objs if r['deleted'] == 0]
                result_hdrs = hdrs
            else:
                items = dict([(r['name'], r) for r in old_objs])
                for item in objs:
                    if item.get('deleted', 0) == 1:
                        if item['name'] in items:
                            del items[item['name']]
                            continue
                    items[item['name']] = item
                result_objs = sorted([r for r in items.values()],
                                     key=lambda i: i['name'])
                if config_true_value(params.get('reverse')):
                    result_objs.reverse()

                result_hdrs = old_hdrs
            if len(result_objs) > params['limit']:
                result_objs = result_objs[:params['limit']]

            return result_hdrs, result_objs, old_resp

        headers = resp.headers.copy()
        # Expose sharding state in reseller requests
        if req.environ.get('reseller_request', False):
            headers['X-Container-Sharding'] = config_true_value(
                headers.get(get_sys_meta_prefix('container') + 'Sharding',
                            'False'))
        objects = []
        params = req.params.copy()
        reverse = config_true_value(params.get('reverse'))
        marker = params.get('marker')
        end_marker = params.get('end_marker')
        sharding = sharding_state == DB_STATE_SHARDING
        params['format'] = 'json'
        num_ranges = len(ranges)
        shard_range = None
        for i in range(num_ranges + 1):
            if sharding and reverse and i == 0:
                # special case if we are still sharding as data may exist in
                # the old DB after where we're up to (because reverse).
                if marker and (marker < ranges[0] or marker in ranges[0]):
                    continue
                if end_marker > ranges[0]:
                    params['end_marker'] = end_marker
                elif ranges[0].upper:
                    params['end_marker'] = ranges[0].upper + '\x00'
            elif sharding and not reverse and i == num_ranges:
                # we are in another edge case where the we need to check more
                # in the old DB
                if (end_marker and shard_range and
                        (end_marker < shard_range.upper or
                         end_marker in shard_range)):
                    continue
                params['end_marker'] = end_marker
                params['marker'] = shard_range.upper
                shard_range = None
            else:
                try:
                    # TODO: how big can this be? pop(0) is quadratic...
                    shard_range = ranges.pop(0)
                except IndexError:
                    break

                if marker and marker in shard_range:
                    params['marker'] = marker
                else:
                    params['marker'] = shard_range.upper or '' if reverse \
                        else shard_range.lower or ''
                    if params['marker'] and reverse:
                        params['marker'] += '\x00'

                if end_marker and end_marker in shard_range:
                    params['end_marker'] = end_marker
                else:
                    params['end_marker'] = shard_range.lower or '' if reverse \
                        else shard_range.upper or ''
                    if params['end_marker'] and not reverse:
                        params['end_marker'] += '\x00'

            # now we have all those params set up. Let's get some objects
            if sharding:
                hdrs, objs, tmp_resp = merge_old_new(shard_range, params)
            else:
                hdrs, objs, tmp_resp = get_objects(
                    shard_account, shard_range.name, params)

            if hdrs is None and tmp_resp:
                return tmp_resp

            if not objs:
                # TODO: Can we be more aggressive here, and break instead?
                continue

            objects.extend(objs)
            limit -= len(objs)
            params['limit'] = limit

            if limit <= 0:
                break
            elif end_marker and reverse and end_marker >= objects[-1]['name']:
                break
            elif end_marker and not reverse and end_marker <= \
                    objects[-1]['name']:
                break

        resp.body = json.dumps(objects)
        return resp

    @public
    @delay_denial
    @cors_validation
    def GET(self, req):
        """Handler for HTTP GET requests."""
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
                config_true_value(req.headers['X-Container-Sharding'])
        if len(self.container_name) > constraints.MAX_CONTAINER_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Container name length of %d longer than %d' % \
                        (len(self.container_name),
                         constraints.MAX_CONTAINER_NAME_LENGTH)
            return resp
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts and self.app.account_autocreate:
            self.autocreate_account(req, self.account_name)
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
                resp = HTTPForbidden(request=req)
                resp.body = 'Reached container limit of %s' % \
                    self.app.max_containers_per_account
                return resp
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self._backend_requests(req, len(containers),
                                         account_partition, accounts,
                                         policy_index)
        resp = self.make_requests(
            req, self.app.container_ring,
            container_partition, 'PUT', req.swift_entity_path, headers)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
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
                config_true_value(req.headers['X-Container-Sharding'])
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'POST',
            req.swift_entity_path, [headers] * len(containers))
        return resp

    def _delete_sharded(self, req, sharding_state):
        # TODO propergate the DELETE to all shards. If one returns a 409 then
        # we back off (what do we do with the containers that were deleted).
        # Also we need some kind of force delete when sending to the root
        # container while in the sharding state, as there will be a readonly
        # (non-empty) container.
        return HTTPBadRequest(req)

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
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'DELETE',
            req.swift_entity_path, headers)
        # Indicates no server had the container
        if resp.status_int == HTTP_ACCEPTED:
            return HTTPNotFound(request=req)

        sharding_state = resp.headers.get('X-Backend-Sharding-State')
        if resp.status_int == HTTP_PRECONDITION_FAILED and sharding_state:
            if sharding_state in (DB_STATE_SHARDING, DB_STATE_SHARDED):
                # We need to first attempt to delete the container shards then
                # the container
                resp = self._delete_sharded(req, )
        return resp

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
