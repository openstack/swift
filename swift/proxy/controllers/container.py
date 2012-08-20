# Copyright (c) 2010-2012 OpenStack, LLC.
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

# NOTE: swift_conn
# You'll see swift_conn passed around a few places in this file. This is the
# source httplib connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

import time
from urllib import unquote
from random import shuffle

from webob.exc import HTTPBadRequest, HTTPForbidden, HTTPNotFound

from swift.common.utils import normalize_timestamp, public
from swift.common.constraints import check_metadata, MAX_CONTAINER_NAME_LENGTH
from swift.common.http import HTTP_ACCEPTED
from swift.proxy.controllers.base import Controller, delay_denial, \
    get_container_memcache_key


class ContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'

    # Ensure these are all lowercase
    pass_through_headers = ['x-container-read', 'x-container-write',
                            'x-container-sync-key', 'x-container-sync-to',
                            'x-versions-location']

    def __init__(self, app, account_name, container_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)

    def clean_acls(self, req):
        if 'swift.clean_acl' in req.environ:
            for header in ('x-container-read', 'x-container-write'):
                if header in req.headers:
                    try:
                        req.headers[header] = \
                            req.environ['swift.clean_acl'](header,
                                                           req.headers[header])
                    except ValueError, err:
                        return HTTPBadRequest(request=req, body=str(err))
        return None

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        if not self.account_info(self.account_name)[1]:
            return HTTPNotFound(request=req)
        part, nodes = self.app.container_ring.get_nodes(
                        self.account_name, self.container_name)
        shuffle(nodes)
        resp = self.GETorHEAD_base(req, _('Container'), part, nodes,
                req.path_info, len(nodes))

        if self.app.memcache:
            # set the memcache container size for ratelimiting
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.set(cache_key,
              {'status': resp.status_int,
               'read_acl': resp.headers.get('x-container-read'),
               'write_acl': resp.headers.get('x-container-write'),
               'sync_key': resp.headers.get('x-container-sync-key'),
               'container_size': resp.headers.get('x-container-object-count'),
               'versions': resp.headers.get('x-versions-location')},
                                  timeout=self.app.recheck_container_existence)

        if 'swift.authorize' in req.environ:
            req.acl = resp.headers.get('x-container-read')
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not req.environ.get('swift_owner', False):
            for key in ('x-container-read', 'x-container-write',
                        'x-container-sync-key', 'x-container-sync-to'):
                if key in resp.headers:
                    del resp.headers[key]
        return resp

    @public
    @delay_denial
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    @delay_denial
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        if len(self.container_name) > MAX_CONTAINER_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Container name length of %d longer than %d' % \
                        (len(self.container_name), MAX_CONTAINER_NAME_LENGTH)
            return resp
        account_partition, accounts, container_count = \
            self.account_info(self.account_name,
                              autocreate=self.app.account_autocreate)
        if self.app.max_containers_per_account > 0 and \
                container_count >= self.app.max_containers_per_account and \
                self.account_name not in self.app.max_containers_whitelist:
            resp = HTTPForbidden(request=req)
            resp.body = 'Reached container limit of %s' % \
                        self.app.max_containers_per_account
            return resp
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = []
        for account in accounts:
            nheaders = {'X-Timestamp': normalize_timestamp(time.time()),
                        'x-trans-id': self.trans_id,
                        'X-Account-Host': '%(ip)s:%(port)s' % account,
                        'X-Account-Partition': account_partition,
                        'X-Account-Device': account['device'],
                        'Connection': 'close'}
            self.transfer_headers(req.headers, nheaders)
            headers.append(nheaders)
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        resp = self.make_requests(req, self.app.container_ring,
                container_partition, 'PUT', req.path_info, headers)
        return resp

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        account_partition, accounts, container_count = \
            self.account_info(self.account_name,
                              autocreate=self.app.account_autocreate)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'x-trans-id': self.trans_id,
                   'Connection': 'close'}
        self.transfer_headers(req.headers, headers)
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        resp = self.make_requests(req, self.app.container_ring,
                container_partition, 'POST', req.path_info,
                [headers] * len(containers))
        return resp

    @public
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        account_partition, accounts, container_count = \
            self.account_info(self.account_name)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = []
        for account in accounts:
            headers.append({'X-Timestamp': normalize_timestamp(time.time()),
                           'X-Trans-Id': self.trans_id,
                           'X-Account-Host': '%(ip)s:%(port)s' % account,
                           'X-Account-Partition': account_partition,
                           'X-Account-Device': account['device'],
                           'Connection': 'close'})
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        resp = self.make_requests(req, self.app.container_ring,
                    container_partition, 'DELETE', req.path_info, headers)
        # Indicates no server had the container
        if resp.status_int == HTTP_ACCEPTED:
            return HTTPNotFound(request=req)
        return resp
