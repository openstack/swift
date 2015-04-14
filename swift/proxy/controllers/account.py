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
from urllib import unquote

from swift.account.utils import account_listing_response
from swift.common.request_helpers import get_listing_content_type
from swift.common.middleware.acl import parse_acl, format_acl
from swift.common.utils import public
from swift.common.constraints import check_metadata
from swift.common import constraints
from swift.common.http import HTTP_NOT_FOUND, HTTP_GONE
from swift.proxy.controllers.base import Controller, clear_info_cache
from swift.common.swob import HTTPBadRequest, HTTPMethodNotAllowed
from swift.common.request_helpers import get_sys_meta_prefix


class AccountController(Controller):
    """WSGI controller for account requests"""
    server_type = 'Account'

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        if not self.app.allow_account_management:
            self.allowed_methods.remove('PUT')
            self.allowed_methods.remove('DELETE')

    def add_acls_from_sys_metadata(self, resp):
        if resp.environ['REQUEST_METHOD'] in ('HEAD', 'GET', 'PUT', 'POST'):
            prefix = get_sys_meta_prefix('account') + 'core-'
            name = 'access-control'
            (extname, intname) = ('x-account-' + name, prefix + name)
            acl_dict = parse_acl(version=2, data=resp.headers.pop(intname))
            if acl_dict:  # treat empty dict as empty header
                resp.headers[extname] = format_acl(
                    version=2, acl_dict=acl_dict)

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        if len(self.account_name) > constraints.MAX_ACCOUNT_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Account name length of %d longer than %d' % \
                        (len(self.account_name),
                         constraints.MAX_ACCOUNT_NAME_LENGTH)
            return resp

        partition = self.app.account_ring.get_part(self.account_name)
        node_iter = self.app.iter_nodes(self.app.account_ring, partition)
        resp = self.GETorHEAD_base(
            req, _('Account'), node_iter, partition,
            req.swift_entity_path.rstrip('/'))
        if resp.status_int == HTTP_NOT_FOUND:
            if resp.headers.get('X-Account-Status', '').lower() == 'deleted':
                resp.status = HTTP_GONE
            elif self.app.account_autocreate:
                resp = account_listing_response(self.account_name, req,
                                                get_listing_content_type(req))
        if req.environ.get('swift_owner'):
            self.add_acls_from_sys_metadata(resp)
        else:
            for header in self.app.swift_owner_headers:
                resp.headers.pop(header, None)
        return resp

    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        if not self.app.allow_account_management:
            return HTTPMethodNotAllowed(
                request=req,
                headers={'Allow': ', '.join(self.allowed_methods)})
        error_response = check_metadata(req, 'account')
        if error_response:
            return error_response
        if len(self.account_name) > constraints.MAX_ACCOUNT_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Account name length of %d longer than %d' % \
                        (len(self.account_name),
                         constraints.MAX_ACCOUNT_NAME_LENGTH)
            return resp
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(self.app, req.environ, self.account_name)
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'PUT',
            req.swift_entity_path, [headers] * len(accounts))
        self.add_acls_from_sys_metadata(resp)
        return resp

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        if len(self.account_name) > constraints.MAX_ACCOUNT_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Account name length of %d longer than %d' % \
                        (len(self.account_name),
                         constraints.MAX_ACCOUNT_NAME_LENGTH)
            return resp
        error_response = check_metadata(req, 'account')
        if error_response:
            return error_response
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(self.app, req.environ, self.account_name)
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'POST',
            req.swift_entity_path, [headers] * len(accounts))
        if resp.status_int == HTTP_NOT_FOUND and self.app.account_autocreate:
            self.autocreate_account(req, self.account_name)
            resp = self.make_requests(
                req, self.app.account_ring, account_partition, 'POST',
                req.swift_entity_path, [headers] * len(accounts))
        self.add_acls_from_sys_metadata(resp)
        return resp

    @public
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        # Extra safety in case someone typos a query string for an
        # account-level DELETE request that was really meant to be caught by
        # some middleware.
        if req.query_string:
            return HTTPBadRequest(request=req)
        if not self.app.allow_account_management:
            return HTTPMethodNotAllowed(
                request=req,
                headers={'Allow': ', '.join(self.allowed_methods)})
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = self.generate_request_headers(req)
        clear_info_cache(self.app, req.environ, self.account_name)
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'DELETE',
            req.swift_entity_path, [headers] * len(accounts))
        return resp
