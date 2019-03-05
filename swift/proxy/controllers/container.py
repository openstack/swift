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

from six.moves.urllib.parse import unquote

from swift.common.utils import public, csv_append, Timestamp, \
    config_true_value, ShardRange
from swift.common.constraints import check_metadata, CONTAINER_LISTING_LIMIT
from swift.common.http import HTTP_ACCEPTED, is_success
from swift.common.request_helpers import get_sys_meta_prefix
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, set_info_cache, clear_info_cache
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPNotFound, HTTPServiceUnavailable, str_to_wsgi, wsgi_to_bytes


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
        part = self.app.container_ring.get_part(
            self.account_name, self.container_name)
        concurrency = self.app.container_ring.replica_count \
            if self.app.concurrent_gets else 1
        node_iter = self.app.iter_nodes(self.app.container_ring, part)
        params = req.params
        params['format'] = 'json'
        record_type = req.headers.get('X-Backend-Record-Type', '').lower()
        if not record_type:
            record_type = 'auto'
            req.headers['X-Backend-Record-Type'] = 'auto'
            params['states'] = 'listing'
        req.params = params
        resp = self.GETorHEAD_base(
            req, _('Container'), node_iter, part,
            req.swift_entity_path, concurrency)
        resp_record_type = resp.headers.get('X-Backend-Record-Type', '')
        if all((req.method == "GET", record_type == 'auto',
               resp_record_type.lower() == 'shard')):
            resp = self._get_from_shards(req, resp)

        # Cache this. We just made a request to a storage node and got
        # up-to-date information for the container.
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
        # construct listing using shards described by the response body
        shard_ranges = [ShardRange.from_dict(data)
                        for data in json.loads(resp.body)]
        self.app.logger.debug('GET listing from %s shards for: %s',
                              len(shard_ranges), req.path_qs)
        if not shard_ranges:
            # can't find ranges or there was a problem getting the ranges. So
            # return what we have.
            return resp

        objects = []
        req_limit = int(req.params.get('limit', CONTAINER_LISTING_LIMIT))
        params = req.params.copy()
        params.pop('states', None)
        req.headers.pop('X-Backend-Record-Type', None)
        reverse = config_true_value(params.get('reverse'))
        marker = params.get('marker')
        end_marker = params.get('end_marker')

        limit = req_limit
        for shard_range in shard_ranges:
            params['limit'] = limit
            # Always set marker to ensure that object names less than or equal
            # to those already in the listing are not fetched; if the listing
            # is empty then the original request marker, if any, is used. This
            # allows misplaced objects below the expected shard range to be
            # included in the listing.
            if objects:
                last_name = objects[-1].get('name',
                                            objects[-1].get('subdir', u''))
                params['marker'] = last_name.encode('utf-8')
            elif marker:
                params['marker'] = marker
            else:
                params['marker'] = ''
            # Always set end_marker to ensure that misplaced objects beyond the
            # expected shard range are not fetched. This prevents a misplaced
            # object obscuring correctly placed objects in the next shard
            # range.
            if end_marker and end_marker in shard_range:
                params['end_marker'] = end_marker
            elif reverse:
                params['end_marker'] = str_to_wsgi(shard_range.lower_str)
            else:
                params['end_marker'] = str_to_wsgi(shard_range.end_marker)

            if (shard_range.account == self.account_name and
                    shard_range.container == self.container_name):
                # directed back to same container - force GET of objects
                headers = {'X-Backend-Record-Type': 'object'}
            else:
                headers = None
            self.app.logger.debug('Getting from %s %s with %s',
                                  shard_range, shard_range.name, headers)
            objs, shard_resp = self._get_container_listing(
                req, shard_range.account, shard_range.container,
                headers=headers, params=params)

            if not objs:
                # tolerate errors or empty shard containers
                continue

            objects.extend(objs)
            limit -= len(objs)

            if limit <= 0:
                break
            if (end_marker and reverse and
                (wsgi_to_bytes(end_marker) >=
                 objects[-1]['name'].encode('utf-8'))):
                break
            if (end_marker and not reverse and
                (wsgi_to_bytes(end_marker) <=
                 objects[-1]['name'].encode('utf-8'))):
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
            resp = HTTPBadRequest(request=req)
            resp.body = 'Container name length of %d longer than %d' % \
                        (len(self.container_name), length_limit)
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
                str(config_true_value(req.headers['X-Container-Sharding']))
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
