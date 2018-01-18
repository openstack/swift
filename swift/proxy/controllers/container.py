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

from six.moves.urllib.parse import unquote
from swift import gettext_ as _
import json

from swift.common.utils import public, csv_append, Timestamp, \
    config_true_value, ShardRange
from swift.common.constraints import check_metadata, CONTAINER_LISTING_LIMIT
from swift.common import constraints
from swift.common.http import HTTP_ACCEPTED, is_success
from swift.common.request_helpers import get_sys_meta_prefix
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, set_info_cache, clear_info_cache
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPNotFound, HTTPServerError
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
        # TODO: if cached container info tells us container is sharded then
        # skip straight to _get_sharded
        resp = self.GETorHEAD_base(
            req, _('Container'), node_iter, part,
            req.swift_entity_path, concurrency)
        sharding_state = \
            int(resp.headers.get('X-Backend-Sharding-State',
                                 DB_STATE_UNSHARDED))
        record_type = req.headers.get('X-Backend-Record-Type', '').lower()
        if (req.method == "GET" and record_type != 'shard' and
                params.get('scope') != 'root' and
                sharding_state in (DB_STATE_SHARDING, DB_STATE_SHARDED)):
            resp = self._get_from_shards(req, resp)

        # Cache this. We just made a request to a storage node and got
        # up-to-date information for the container.
        resp.headers['X-Backend-Recheck-Container-Existence'] = str(
            self.app.recheck_container_existence)
        # TODO: seems like a good idea to not set cache for shard/all
        # requests, but revisit this at some point
        if record_type != 'shard' and params.get('items') != 'all':
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
        # get the list of ShardRanges that contain the requested listing range
        # by using original request params
        ranges = self._get_shard_ranges(
            req, self.account_name, self.container_name,
            state=ShardRange.STATES[ShardRange.ACTIVE])
        if not ranges:
            # can't find ranges or there was a problem getting the ranges. So
            # return what we have.
            return resp

        objects = []
        limit = int(req.params.get('limit', CONTAINER_LISTING_LIMIT))
        params = req.params.copy()
        reverse = config_true_value(params.get('reverse'))
        marker = params.get('marker')
        end_marker = params.get('end_marker')
        params['scope'] = 'root'  # avoid fetching shards again

        for shard_range in ranges:
            params['limit'] = limit
            # always set marker and end_marker to ensure that misplaced objects
            # outside of the expected shard range are not fetched
            # TODO: perhaps check that we have not strayed beyond end_marker
            # before getting objects from another shard?
            if marker and marker in shard_range:
                params['marker'] = marker
            else:
                params['marker'] = str(shard_range.upper) if reverse \
                    else shard_range.lower
                if params['marker'] and reverse:
                    params['marker'] += '\x00'

            if end_marker and end_marker in shard_range:
                params['end_marker'] = end_marker
            else:
                params['end_marker'] = str(shard_range.lower) if reverse \
                    else shard_range.upper
                if params['end_marker'] and not reverse:
                    params['end_marker'] += '\x00'

            objs, shard_resp = self._get_container_listing(
                req, shard_range.account, shard_range.container, params=params)

            if objs is None:
                # TODO: unit tests failed shard GET scenario
                return shard_resp

            if not objs:
                # TODO: Can we be more aggressive here, and break instead?
                continue

            objects.extend(objs)
            limit -= len(objs)

            # TODO: do we need these checks?
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
            if not self.autocreate_account(req, self.account_name):
                return HTTPServerError(request=req)
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
