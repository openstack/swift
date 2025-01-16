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

from urllib.parse import unquote

from swift.account.utils import account_listing_response
from swift.common.middleware.acl import parse_acl, format_acl
from swift.common.utils import public
from swift.common.constraints import check_metadata
from swift.common.http import HTTP_NOT_FOUND, HTTP_GONE
from swift.proxy.controllers.base import Controller, clear_info_cache, \
    set_info_cache, NodeIter
from swift.common.middleware import listing_formats
from swift.common.swob import HTTPBadRequest, HTTPMethodNotAllowed
from swift.common.request_helpers import get_sys_meta_prefix


class AccountController(Controller):
    """WSGI controller for account requests"""
    server_type = 'Account'

    def __init__(self, app, account_name, **kwargs):
        super(AccountController, self).__init__(app)
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
        length_limit = self.get_name_length_limit()
        if len(self.account_name) > length_limit:
            resp = HTTPBadRequest(request=req)
            resp.body = b'Account name length of %d longer than %d' % \
                        (len(self.account_name), length_limit)
            # Don't cache this. We know the account doesn't exist because
            # the name is bad; we don't need to cache that because it's
            # really cheap to recompute.
            return resp

        partition = self.app.account_ring.get_part(self.account_name)
        concurrency = self.app.account_ring.replica_count \
            if self.app.get_policy_options(None).concurrent_gets else 1
        node_iter = NodeIter(
            'account', self.app, self.app.account_ring, partition,
            self.logger, req)
        params = req.params
        params['format'] = 'json'
        req.params = params
        resp = self.GETorHEAD_base(
            req, 'Account', node_iter, partition,
            req.swift_entity_path.rstrip('/'), concurrency)
        if resp.status_int == HTTP_NOT_FOUND:
            if resp.headers.get('X-Account-Status', '').lower() == 'deleted':
                resp.status = HTTP_GONE
            elif self.app.account_autocreate:
                # This is kind of a lie; we pretend like the account is
                # there, but it's not. We'll create it as soon as something
                # tries to write to it, but we don't need databases on disk
                # to tell us that nothing's there.
                #
                # We set a header so that certain consumers can tell it's a
                # fake listing. The important one is the PUT of a container
                # to an autocreate account; the proxy checks to see if the
                # account exists before actually performing the PUT and
                # creates the account if necessary. If we feed it a perfect
                # lie, it'll just try to create the container without
                # creating the account, and that'll fail.
                resp = account_listing_response(
                    self.account_name, req,
                    listing_formats.get_listing_content_type(req))
                resp.headers['X-Backend-Fake-Account-Listing'] = 'yes'

        # Cache this. We just made a request to a storage node and got
        # up-to-date information for the account.
        resp.headers['X-Backend-Recheck-Account-Existence'] = str(
            self.app.recheck_account_existence)
        set_info_cache(req.environ, self.account_name, None, resp)

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
        length_limit = self.get_name_length_limit()
        if len(self.account_name) > length_limit:
            resp = HTTPBadRequest(request=req)
            resp.body = b'Account name length of %d longer than %d' % \
                        (len(self.account_name), length_limit)
            return resp
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(req.environ, self.account_name)
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'PUT',
            req.swift_entity_path, [headers] * len(accounts))
        self.add_acls_from_sys_metadata(resp)
        return resp

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        length_limit = self.get_name_length_limit()
        if len(self.account_name) > length_limit:
            resp = HTTPBadRequest(request=req)
            resp.body = b'Account name length of %d longer than %d' % \
                        (len(self.account_name), length_limit)
            return resp
        error_response = check_metadata(req, 'account')
        if error_response:
            return error_response
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(req.environ, self.account_name)
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
        clear_info_cache(req.environ, self.account_name)
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'DELETE',
            req.swift_entity_path, [headers] * len(accounts))
        return resp
