# Copyright (c) 2010 OpenStack, LLC.
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

try:
    import simplejson as json
except ImportError:
    import json
from httplib import HTTPConnection, HTTPSConnection
from time import gmtime, strftime, time
from traceback import format_exc
from urllib import quote, unquote
from urlparse import urlparse
from uuid import uuid4

from eventlet.timeout import Timeout
from eventlet import TimeoutError
from webob import Response, Request
from webob.exc import HTTPAccepted, HTTPBadRequest, HTTPConflict, \
    HTTPCreated, HTTPForbidden, HTTPNoContent, HTTPNotFound, \
    HTTPServiceUnavailable, HTTPUnauthorized

from swift.common.bufferedhttp import http_connect_raw as http_connect
from swift.common.middleware.acl import clean_acl, parse_acl, referrer_allowed
from swift.common.utils import cache_from_env, get_logger, split_path


class Swauth(object):
    """
    Scalable authentication and authorization system that uses Swift as its
    backing store.

    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf)
        self.log_headers = conf.get('log_headers') == 'True'
        self.reseller_prefix = conf.get('reseller_prefix', 'AUTH').strip()
        if self.reseller_prefix and self.reseller_prefix[-1] != '_':
            self.reseller_prefix += '_'
        self.auth_prefix = conf.get('auth_prefix', '/auth/')
        if not self.auth_prefix:
            self.auth_prefix = '/auth/'
        if self.auth_prefix[0] != '/':
            self.auth_prefix = '/' + self.auth_prefix
        if self.auth_prefix[-1] != '/':
            self.auth_prefix += '/'
        self.auth_account = '%s.auth' % self.reseller_prefix
        self.default_swift_cluster = conf.get('default_swift_cluster',
            'local#http://127.0.0.1:8080/v1')
        # This setting is a little messy because of the options it has to
        # provide. The basic format is cluster_name#url, such as the default
        # value of local#http://127.0.0.1:8080/v1.
        # If the URL given to the user needs to differ from the url used by
        # Swauth to create/delete accounts, there's a more complex format:
        # cluster_name#url#url, such as
        # local#https://public.com:8080/v1#http://private.com:8080/v1.
        cluster_parts = self.default_swift_cluster.split('#', 2)
        self.dsc_name = cluster_parts[0]
        if len(cluster_parts) == 3:
            self.dsc_url = cluster_parts[1].rstrip('/')
            self.dsc_url2 = cluster_parts[2].rstrip('/')
        elif len(cluster_parts) == 2:
            self.dsc_url = self.dsc_url2 = cluster_parts[1].rstrip('/')
        else:
            raise Exception('Invalid cluster format')
        self.dsc_parsed = urlparse(self.dsc_url)
        if self.dsc_parsed.scheme not in ('http', 'https'):
            raise Exception('Cannot handle protocol scheme %s for url %s' %
                            (self.dsc_parsed.scheme, repr(self.dsc_url)))
        self.dsc_parsed2 = urlparse(self.dsc_url2)
        if self.dsc_parsed2.scheme not in ('http', 'https'):
            raise Exception('Cannot handle protocol scheme %s for url %s' %
                            (self.dsc_parsed2.scheme, repr(self.dsc_url2)))
        self.super_admin_key = conf.get('super_admin_key')
        if not self.super_admin_key:
            msg = _('No super_admin_key set in conf file! Exiting.')
            try:
                self.logger.critical(msg)
            except Exception:
                pass
            raise ValueError(msg)
        self.token_life = int(conf.get('token_life', 86400))
        self.timeout = int(conf.get('node_timeout', 10))
        self.itoken = None
        self.itoken_expires = None

    def __call__(self, env, start_response):
        """
        Accepts a standard WSGI application call, authenticating the request
        and installing callback hooks for authorization and ACL header
        validation. For an authenticated request, REMOTE_USER will be set to a
        comma separated list of the user's groups.

        With a non-empty reseller prefix, acts as the definitive auth service
        for just tokens and accounts that begin with that prefix, but will deny
        requests outside this prefix if no other auth middleware overrides it.

        With an empty reseller prefix, acts as the definitive auth service only
        for tokens that validate to a non-empty set of groups. For all other
        requests, acts as the fallback auth service when no other auth
        middleware overrides it.

        Alternatively, if the request matches the self.auth_prefix, the request
        will be routed through the internal auth request handler (self.handle).
        This is to handle creating users, accounts, granting tokens, etc.
        """
        if 'HTTP_X_CF_TRANS_ID' not in env:
            env['HTTP_X_CF_TRANS_ID'] = 'tx' + str(uuid4())
        if env.get('PATH_INFO', '').startswith(self.auth_prefix):
            return self.handle(env, start_response)
        token = env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))
        if token and token.startswith(self.reseller_prefix):
            # Note: Empty reseller_prefix will match all tokens.
            groups = self.get_groups(env, token)
            if groups:
                env['REMOTE_USER'] = groups
                user = groups and groups.split(',', 1)[0] or ''
                # We know the proxy logs the token, so we augment it just a bit
                # to also log the authenticated user.
                env['HTTP_X_AUTH_TOKEN'] = '%s,%s' % (user, token)
                env['swift.authorize'] = self.authorize
                env['swift.clean_acl'] = clean_acl
            else:
                # Unauthorized token
                if self.reseller_prefix:
                    # Because I know I'm the definitive auth for this token, I
                    # can deny it outright.
                    return HTTPUnauthorized()(env, start_response)
                # Because I'm not certain if I'm the definitive auth for empty
                # reseller_prefixed tokens, I won't overwrite swift.authorize.
                elif 'swift.authorize' not in env:
                    env['swift.authorize'] = self.denied_response
        else:
            if self.reseller_prefix:
                # With a non-empty reseller_prefix, I would like to be called
                # back for anonymous access to accounts I know I'm the
                # definitive auth for.
                try:
                    version, rest = split_path(env.get('PATH_INFO', ''),
                                               1, 2, True)
                except ValueError:
                    return HTTPNotFound()(env, start_response)
                if rest and rest.startswith(self.reseller_prefix):
                    # Handle anonymous access to accounts I'm the definitive
                    # auth for.
                    env['swift.authorize'] = self.authorize
                    env['swift.clean_acl'] = clean_acl
                # Not my token, not my account, I can't authorize this request,
                # deny all is a good idea if not already set...
                elif 'swift.authorize' not in env:
                    env['swift.authorize'] = self.denied_response
            # Because I'm not certain if I'm the definitive auth for empty
            # reseller_prefixed accounts, I won't overwrite swift.authorize.
            elif 'swift.authorize' not in env:
                env['swift.authorize'] = self.authorize
                env['swift.clean_acl'] = clean_acl
        return self.app(env, start_response)

    def get_groups(self, env, token):
        """
        Get groups for the given token.

        :param env: The current WSGI environment dictionary.
        :param token: Token to validate and return a group string for.

        :returns: None if the token is invalid or a string containing a comma
                  separated list of groups the authenticated user is a member
                  of. The first group in the list is also considered a unique
                  identifier for that user.
        """
        groups = None
        memcache_client = cache_from_env(env)
        if memcache_client:
            memcache_key = '%s/auth/%s' % (self.reseller_prefix, token)
            cached_auth_data = memcache_client.get(memcache_key)
            if cached_auth_data:
                expires, groups = cached_auth_data
                if expires < time():
                    groups = None
        if not groups:
            path = quote('/v1/%s/.token_%s/%s' %
                         (self.auth_account, token[-1], token))
            resp = self.make_request(env, 'GET', path).get_response(self.app)
            if resp.status_int // 100 != 2:
                return None
            detail = json.loads(resp.body)
            if detail['expires'] < time():
                self.make_request(env, 'DELETE', path).get_response(self.app)
                return None
            groups = [g['name'] for g in detail['groups']]
            if '.admin' in groups:
                groups.remove('.admin')
                groups.append(detail['account_id'])
            groups = ','.join(groups)
            if memcache_client:
                memcache_client.set(memcache_key, (detail['expires'], groups),
                                    timeout=float(detail['expires'] - time()))
        return groups

    def authorize(self, req):
        """
        Returns None if the request is authorized to continue or a standard
        WSGI response callable if not.
        """
        try:
            version, account, container, obj = split_path(req.path, 1, 4, True)
        except ValueError:
            return HTTPNotFound(request=req)
        if not account or not account.startswith(self.reseller_prefix):
            return self.denied_response(req)
        user_groups = (req.remote_user or '').split(',')
        if '.reseller_admin' in user_groups and \
                account != self.reseller_prefix and \
                account[len(self.reseller_prefix)].isalnum():
            return None
        if account in user_groups and \
                (req.method not in ('DELETE', 'PUT') or container):
            # If the user is admin for the account and is not trying to do an
            # account DELETE or PUT...
            return None
        referrers, groups = parse_acl(getattr(req, 'acl', None))
        if referrer_allowed(req.referer, referrers):
            return None
        if not req.remote_user:
            return self.denied_response(req)
        for user_group in user_groups:
            if user_group in groups:
                return None
        return self.denied_response(req)

    def denied_response(self, req):
        """
        Returns a standard WSGI response callable with the status of 403 or 401
        depending on whether the REMOTE_USER is set or not.
        """
        if req.remote_user:
            return HTTPForbidden(request=req)
        else:
            return HTTPUnauthorized(request=req)

    def handle(self, env, start_response):
        """
        WSGI entry point for auth requests (ones that match the
        self.auth_prefix).
        Wraps env in webob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            req = Request(env)
            if self.auth_prefix:
                req.path_info_pop()
            req.bytes_transferred = '-'
            req.client_disconnect = False
            if 'x-storage-token' in req.headers and \
                    'x-auth-token' not in req.headers:
                req.headers['x-auth-token'] = req.headers['x-storage-token']
            if 'eventlet.posthooks' in env:
                env['eventlet.posthooks'].append(
                    (self.posthooklogger, (req,), {}))
                return self.handle_request(req)(env, start_response)
            else:
                # Lack of posthook support means that we have to log on the
                # start of the response, rather than after all the data has
                # been sent. This prevents logging client disconnects
                # differently than full transmissions.
                response = self.handle_request(req)(env, start_response)
                self.posthooklogger(env, req)
                return response
        except (Exception, TimeoutError):
            print "EXCEPTION IN handle: %s: %s" % (format_exc(), env)
            start_response('500 Server Error',
                           [('Content-Type', 'text/plain')])
            return ['Internal server error.\n']

    def handle_request(self, req):
        """
        Entry point for auth requests (ones that match the self.auth_prefix).
        Should return a WSGI-style callable (such as webob.Response).

        :param req: webob.Request object
        """
        req.start_time = time()
        handler = None
        try:
            version, account, user, _ = split_path(req.path_info, minsegs=1,
                                                maxsegs=4, rest_with_last=True)
        except ValueError:
            return HTTPNotFound(request=req)
        if version in ('v1', 'v1.0', 'auth'):
            if req.method == 'GET':
                handler = self.handle_get_token
        elif version == 'v2':
            req.path_info_pop()
            if req.method == 'GET':
                if not account and not user:
                    handler = self.handle_get_reseller
                elif account:
                    if not user:
                        handler = self.handle_get_account
                    elif account == '.token':
                        req.path_info_pop()
                        handler = self.handle_validate_token
                    else:
                        handler = self.handle_get_user
            elif req.method == 'PUT':
                if not user:
                    handler = self.handle_put_account
                else:
                    handler = self.handle_put_user
            elif req.method == 'DELETE':
                if not user:
                    handler = self.handle_delete_account
                else:
                    handler = self.handle_delete_user
            elif req.method == 'POST':
                if account == '.prep':
                    handler = self.handle_prep
                elif user == '.services':
                    handler = self.handle_set_services
        if not handler:
            req.response = HTTPBadRequest(request=req)
        else:
            req.response = handler(req)
        return req.response

    def handle_prep(self, req):
        """
        Handles the POST v2/.prep call for preparing the backing store Swift
        cluster for use with the auth subsystem. Can only be called by
        .super_admin.

        :param req: The webob.Request to process.
        :returns: webob.Response, 204 on success
        """
        if not self.is_super_admin(req):
            return HTTPForbidden(request=req)
        path = quote('/v1/%s' % self.auth_account)
        resp = self.make_request(req.environ, 'PUT',
                                 path).get_response(self.app)
        if resp.status_int // 100 != 2:
            raise Exception('Could not create the main auth account: %s %s' %
                            (path, resp.status))
        path = quote('/v1/%s/.account_id' % self.auth_account)
        resp = self.make_request(req.environ, 'PUT',
                                 path).get_response(self.app)
        if resp.status_int // 100 != 2:
            raise Exception('Could not create container: %s %s' %
                            (path, resp.status))
        for container in xrange(16):
            path = quote('/v1/%s/.token_%x' % (self.auth_account, container))
            resp = self.make_request(req.environ, 'PUT',
                                     path).get_response(self.app)
            if resp.status_int // 100 != 2:
                raise Exception('Could not create container: %s %s' %
                                (path, resp.status))
        return HTTPNoContent(request=req)

    def handle_get_reseller(self, req):
        """
        Handles the GET v2 call for getting general reseller information
        (currently just a list of accounts). Can only be called by a
        .reseller_admin.

        On success, a JSON dictionary will be returned with a single `accounts`
        key whose value is list of dicts. Each dict represents an account and
        currently only contains the single key `name`. For example::

            {"accounts": [{"name": "reseller"}, {"name": "test"},
                          {"name": "test2"}]}

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success with a JSON dictionary as
                  explained above.
        """
        if not self.is_reseller_admin(req):
            return HTTPForbidden(request=req)
        listing = []
        marker = ''
        while True:
            path = '/v1/%s?format=json&marker=%s' % (quote(self.auth_account),
                                                     quote(marker))
            resp = self.make_request(req.environ, 'GET',
                                     path).get_response(self.app)
            if resp.status_int // 100 != 2:
                raise Exception('Could not list main auth account: %s %s' %
                                (path, resp.status))
            sublisting = json.loads(resp.body)
            if not sublisting:
                break
            for container in sublisting:
                if container['name'][0] != '.':
                    listing.append({'name': container['name']})
            marker = sublisting[-1]['name']
        return Response(body=json.dumps({'accounts': listing}))

    def handle_get_account(self, req):
        """
        Handles the GET v2/<account> call for getting account information.
        Can only be called by an account .admin.

        On success, a JSON dictionary will be returned containing the keys
        `account_id`, `services`, and `users`. The `account_id` is the value
        used when creating service accounts. The `services` value is a dict as
        described in the :func:`handle_get_token` call. The `users` value is a
        list of dicts, each dict representing a user and currently only
        containing the single key `name`. For example::

             {"account_id": "AUTH_018c3946-23f8-4efb-a8fb-b67aae8e4162",
              "services": {"storage": {"default": "local",
                                       "local": "http://127.0.0.1:8080/v1/AUTH_018c3946-23f8-4efb-a8fb-b67aae8e4162"}},
              "users": [{"name": "tester"}, {"name": "tester3"}]}

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success with a JSON dictionary as
                  explained above.
        """
        account = req.path_info_pop()
        if req.path_info or not account.isalnum():
            return HTTPBadRequest(request=req)
        if not self.is_account_admin(req, account):
            return HTTPForbidden(request=req)
        path = quote('/v1/%s/%s/.services' % (self.auth_account, account))
        resp = self.make_request(req.environ, 'GET',
                                 path).get_response(self.app)
        if resp.status_int == 404:
            return HTTPNotFound(request=req)
        if resp.status_int // 100 != 2:
            raise Exception('Could not obtain the .services object: %s %s' %
                            (path, resp.status))
        services = json.loads(resp.body)
        listing = []
        marker = ''
        while True:
            path = '/v1/%s?format=json&marker=%s' % (quote('%s/%s' %
                (self.auth_account, account)), quote(marker))
            resp = self.make_request(req.environ, 'GET',
                                     path).get_response(self.app)
            if resp.status_int == 404:
                return HTTPNotFound(request=req)
            if resp.status_int // 100 != 2:
                raise Exception('Could not list in main auth account: %s %s' %
                                (path, resp.status))
            account_id = resp.headers['X-Container-Meta-Account-Id']
            sublisting = json.loads(resp.body)
            if not sublisting:
                break
            for obj in sublisting:
                if obj['name'][0] != '.':
                    listing.append({'name': obj['name']})
            marker = sublisting[-1]['name']
        return Response(body=json.dumps({'account_id': account_id,
                                    'services': services, 'users': listing}))

    def handle_set_services(self, req):
        """
        Handles the POST v2/<account>/.services call for setting services
        information. Can only be called by a reseller .admin.

        In the :func:`handle_get_account` (GET v2/<account>) call, a section of
        the returned JSON dict is `services`. This section looks something like
        this::

              "services": {"storage": {"default": "local",
                                       "local": "http://127.0.0.1:8080/v1/AUTH_018c3946-23f8-4efb-a8fb-b67aae8e4162"}}

        Making use of this section is described in :func:`handle_get_token`.

        This function allows setting values within this section for the
        <account>, allowing the addition of new service end points or updating
        existing ones.

        The body of the POST request should contain a JSON dict with the
        following format::

            {"service_name": {"end_point_name": "end_point_value"}}

        There can be multiple services and multiple end points in the same
        call.

        Any new services or end points will be added to the existing set of
        services and end points. Any existing services with the same service
        name will be merged with the new end points. Any existing end points
        with the same end point name will have their values updated.

        The updated services dictionary will be returned on success.

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success with the udpated services JSON
                  dict as described above
        """
        if not self.is_reseller_admin(req):
            return HTTPForbidden(request=req)
        account = req.path_info_pop()
        if req.path_info != '/.services' or not account.isalnum():
            return HTTPBadRequest(request=req)
        try:
            new_services = json.loads(req.body)
        except ValueError, err:
            return HTTPBadRequest(body=str(err))
        # Get the current services information
        path = quote('/v1/%s/%s/.services' % (self.auth_account, account))
        resp = self.make_request(req.environ, 'GET',
                                 path).get_response(self.app)
        if resp.status_int == 404:
            return HTTPNotFound(request=req)
        if resp.status_int // 100 != 2:
            raise Exception('Could not obtain services info: %s %s' %
                            (path, resp.status))
        services = json.loads(resp.body)
        for new_service, value in new_services.iteritems():
            if new_service in services:
                services[new_service].update(value)
            else:
                services[new_service] = value
        # Save the new services information
        services = json.dumps(services)
        resp = self.make_request(req.environ, 'PUT', path,
                                 services).get_response(self.app)
        if resp.status_int // 100 != 2:
            raise Exception('Could not save .services object: %s %s' %
                            (path, resp.status))
        return Response(request=req, body=services)

    def handle_put_account(self, req):
        """
        Handles the PUT v2/<account> call for adding an account to the auth
        system. Can only be called by a .reseller_admin.

        By default, a newly created UUID4 will be used with the reseller prefix
        as the account id used when creating corresponding service accounts.
        However, you can provide an X-Account-Suffix header to replace the
        UUID4 part.

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success.
        """
        if not self.is_reseller_admin(req):
            return HTTPForbidden(request=req)
        account = req.path_info_pop()
        if req.path_info or not account.isalnum():
            return HTTPBadRequest(request=req)
        # Ensure the container in the main auth account exists (this
        # container represents the new account)
        path = quote('/v1/%s/%s' % (self.auth_account, account))
        resp = self.make_request(req.environ, 'HEAD',
                                 path).get_response(self.app)
        if resp.status_int == 404:
            resp = self.make_request(req.environ, 'PUT',
                                     path).get_response(self.app)
            if resp.status_int // 100 != 2:
                raise Exception('Could not create account within main auth '
                    'account: %s %s' % (path, resp.status))
        elif resp.status_int // 100 == 2:
            if 'x-container-meta-account-id' in resp.headers:
                # Account was already created
                return HTTPAccepted(request=req)
        else:
            raise Exception('Could not verify account within main auth '
                'account: %s %s' % (path, resp.status))
        account_suffix = req.headers.get('x-account-suffix')
        if not account_suffix:
            account_suffix = str(uuid4())
        # Create the new account in the Swift cluster
        path = quote('%s/%s%s' % (self.dsc_parsed2.path,
                                  self.reseller_prefix, account_suffix))
        try:
            conn = self.get_conn()
            conn.request('PUT', path,
                        headers={'X-Auth-Token': self.get_itoken(req.environ)})
            resp = conn.getresponse()
            resp.read()
            if resp.status // 100 != 2:
                raise Exception('Could not create account on the Swift '
                    'cluster: %s %s %s' % (path, resp.status, resp.reason))
        except (Exception, TimeoutError):
            self.logger.error(_('ERROR: Exception while trying to communicate '
                'with %(scheme)s://%(host)s:%(port)s/%(path)s'),
                {'scheme': self.dsc_parsed2.scheme,
                 'host': self.dsc_parsed2.hostname,
                 'port': self.dsc_parsed2.port, 'path': path})
            raise
        # Record the mapping from account id back to account name
        path = quote('/v1/%s/.account_id/%s%s' %
                     (self.auth_account, self.reseller_prefix, account_suffix))
        resp = self.make_request(req.environ, 'PUT', path,
                                 account).get_response(self.app)
        if resp.status_int // 100 != 2:
            raise Exception('Could not create account id mapping: %s %s' %
                            (path, resp.status))
        # Record the cluster url(s) for the account
        path = quote('/v1/%s/%s/.services' % (self.auth_account, account))
        services = {'storage': {}}
        services['storage'][self.dsc_name] = '%s/%s%s' % (self.dsc_url,
            self.reseller_prefix, account_suffix)
        services['storage']['default'] = self.dsc_name
        resp = self.make_request(req.environ, 'PUT', path,
                                 json.dumps(services)).get_response(self.app)
        if resp.status_int // 100 != 2:
            raise Exception('Could not create .services object: %s %s' %
                            (path, resp.status))
        # Record the mapping from account name to the account id
        path = quote('/v1/%s/%s' % (self.auth_account, account))
        resp = self.make_request(req.environ, 'POST', path,
            headers={'X-Container-Meta-Account-Id': '%s%s' %
            (self.reseller_prefix, account_suffix)}).get_response(self.app)
        if resp.status_int // 100 != 2:
            raise Exception('Could not record the account id on the account: '
                            '%s %s' % (path, resp.status))
        return HTTPCreated(request=req)

    def handle_delete_account(self, req):
        """
        Handles the DELETE v2/<account> call for removing an account from the
        auth system. Can only be called by a .reseller_admin.

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success.
        """
        if not self.is_reseller_admin(req):
            return HTTPForbidden(request=req)
        account = req.path_info_pop()
        if req.path_info or not account.isalnum():
            return HTTPBadRequest(request=req)
        # Make sure the account has no users and get the account_id
        marker = ''
        while True:
            path = '/v1/%s?format=json&marker=%s' % (quote('%s/%s' %
                (self.auth_account, account)), quote(marker))
            resp = self.make_request(req.environ, 'GET',
                                     path).get_response(self.app)
            if resp.status_int == 404:
                return HTTPNotFound(request=req)
            if resp.status_int // 100 != 2:
                raise Exception('Could not list in main auth account: %s %s' %
                                (path, resp.status))
            account_id = resp.headers['x-container-meta-account-id']
            sublisting = json.loads(resp.body)
            if not sublisting:
                break
            for obj in sublisting:
                if obj['name'][0] != '.':
                    return HTTPConflict(request=req)
            marker = sublisting[-1]['name']
        # Obtain the listing of services the account is on.
        path = quote('/v1/%s/%s/.services' % (self.auth_account, account))
        resp = self.make_request(req.environ, 'GET',
                                 path).get_response(self.app)
        if resp.status_int // 100 != 2 and resp.status_int != 404:
            raise Exception('Could not obtain .services object: %s %s' %
                            (path, resp.status))
        if resp.status_int // 100 == 2:
            services = json.loads(resp.body)
            # Delete the account on each cluster it is on.
            deleted_any = False
            for name, url in services['storage'].iteritems():
                if name != 'default':
                    parsed = urlparse(url)
                    conn = self.get_conn(parsed)
                    conn.request('DELETE', parsed.path,
                        headers={'X-Auth-Token': self.get_itoken(req.environ)})
                    resp = conn.getresponse()
                    resp.read()
                    if resp.status == 409:
                        if deleted_any:
                            raise Exception('Managed to delete one or more '
                                'service end points, but failed with: '
                                '%s %s %s' % (url, resp.status, resp.reason))
                        else:
                            return HTTPConflict(request=req)
                    if resp.status // 100 != 2 and resp.status != 404:
                        raise Exception('Could not delete account on the '
                            'Swift cluster: %s %s %s' %
                            (url, resp.status, resp.reason))
                    deleted_any = True
            # Delete the .services object itself.
            path = quote('/v1/%s/%s/.services' %
                         (self.auth_account, account))
            resp = self.make_request(req.environ, 'DELETE',
                                     path).get_response(self.app)
            if resp.status_int // 100 != 2 and resp.status_int != 404:
                raise Exception('Could not delete .services object: %s %s' %
                                (path, resp.status))
        # Delete the account id mapping for the account.
        path = quote('/v1/%s/.account_id/%s' %
                     (self.auth_account, account_id))
        resp = self.make_request(req.environ, 'DELETE',
                                 path).get_response(self.app)
        if resp.status_int // 100 != 2 and resp.status_int != 404:
            raise Exception('Could not delete account id mapping: %s %s' %
                            (path, resp.status))
        # Delete the account marker itself.
        path = quote('/v1/%s/%s' % (self.auth_account, account))
        resp = self.make_request(req.environ, 'DELETE',
                                 path).get_response(self.app)
        if resp.status_int // 100 != 2 and resp.status_int != 404:
            raise Exception('Could not delete account marked: %s %s' %
                            (path, resp.status))
        return HTTPNoContent(request=req)

    def handle_get_user(self, req):
        """
        Handles the GET v2/<account>/<user> call for getting user information.
        Can only be called by an account .admin.

        On success, a JSON dict will be returned as described::

            {"groups": [  # List of groups the user is a member of
                {"name": "<act>:<usr>"},
                    # The first group is a unique user identifier
                {"name": "<account>"},
                    # The second group is the auth account name
                {"name": "<additional-group>"}
                    # There may be additional groups, .admin being a special
                    # group indicating an account admin and .reseller_admin
                    # indicating a reseller admin.
             ],
             "auth": "plaintext:<key>"
             # The auth-type and key for the user; currently only plaintext is
             # implemented.
            }

        For example::

            {"groups": [{"name": "test:tester"}, {"name": "test"},
                        {"name": ".admin"}],
             "auth": "plaintext:testing"}

        If the <user> in the request is the special user `.groups`, the JSON
        dict will contain a single key of `groups` whose value is a list of
        dicts representing the active groups within the account. Each dict
        currently has the single key `name`. For example::

            {"groups": [{"name": ".admin"}, {"name": "test"},
                        {"name": "test:tester"}, {"name": "test:tester3"}]}

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success with a JSON dictionary as
                  explained above.
        """
        account = req.path_info_pop()
        user = req.path_info_pop()
        if req.path_info or not account.isalnum() or \
                (not user.isalnum() and user != '.groups'):
            return HTTPBadRequest(request=req)
        if not self.is_account_admin(req, account):
            return HTTPForbidden(request=req)
        if user == '.groups':
            # TODO: This could be very slow for accounts with a really large
            # number of users. Speed could be improved by concurrently
            # requesting user group information. Then again, I don't *know*
            # it's slow for `normal` use cases, so testing should be done.
            groups = set()
            marker = ''
            while True:
                path = '/v1/%s?format=json&marker=%s' % (quote('%s/%s' %
                    (self.auth_account, account)), quote(marker))
                resp = self.make_request(req.environ, 'GET',
                                         path).get_response(self.app)
                if resp.status_int == 404:
                    return HTTPNotFound(request=req)
                if resp.status_int // 100 != 2:
                    raise Exception('Could not list in main auth account: '
                                    '%s %s' % (path, resp.status))
                sublisting = json.loads(resp.body)
                if not sublisting:
                    break
                for obj in sublisting:
                    if obj['name'][0] != '.':
                        path = quote('/v1/%s/%s/%s' % (self.auth_account,
                                                       account, obj['name']))
                        resp = self.make_request(req.environ, 'GET',
                                                 path).get_response(self.app)
                        if resp.status_int // 100 != 2:
                            raise Exception('Could not retrieve user object: '
                                            '%s %s' % (path, resp.status))
                        groups.update(g['name']
                            for g in json.loads(resp.body)['groups'])
                marker = sublisting[-1]['name']
            body = json.dumps({'groups':
                                [{'name': g} for g in sorted(groups)]})
        else:
            path = quote('/v1/%s/%s/%s' % (self.auth_account, account, user))
            resp = self.make_request(req.environ, 'GET',
                                     path).get_response(self.app)
            if resp.status_int == 404:
                return HTTPNotFound(request=req)
            if resp.status_int // 100 != 2:
                raise Exception('Could not retrieve user object: %s %s' %
                                (path, resp.status))
            body = resp.body
        return Response(body=body)

    def handle_put_user(self, req):
        """
        Handles the PUT v2/<account>/<user> call for adding a user to an
        account.

        X-Auth-User-Key represents the user's key, X-Auth-User-Admin may be set
        to `true` to create an account .admin, and X-Auth-User-Reseller-Admin
        may be set to `true` to create a .reseller_admin.

        Can only be called by an account .admin unless the user is to be a
        .reseller_admin, in which case the request must be by .super_admin.

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success.
        """
        # Validate path info
        account = req.path_info_pop()
        user = req.path_info_pop()
        key = req.headers.get('x-auth-user-key')
        admin = req.headers.get('x-auth-user-admin') == 'true'
        reseller_admin = \
            req.headers.get('x-auth-user-reseller-admin') == 'true'
        if reseller_admin:
            admin = True
        if req.path_info or not account.isalnum() or not user.isalnum() or \
                not key:
            return HTTPBadRequest(request=req)
        if reseller_admin:
            if not self.is_super_admin(req):
                return HTTPForbidden(request=req)
        elif not self.is_account_admin(req, account):
            return HTTPForbidden(request=req)
        # Create the object in the main auth account (this object represents
        # the user)
        path = quote('/v1/%s/%s/%s' % (self.auth_account, account, user))
        groups = ['%s:%s' % (account, user), account]
        if admin:
            groups.append('.admin')
        if reseller_admin:
            groups.append('.reseller_admin')
        resp = self.make_request(req.environ, 'PUT', path, json.dumps({'auth':
            'plaintext:%s' % key,
            'groups': [{'name': g} for g in groups]})).get_response(self.app)
        if resp.status_int == 404:
            return HTTPNotFound(request=req)
        if resp.status_int // 100 != 2:
            raise Exception('Could not create user object: %s %s' %
                            (path, resp.status))
        return HTTPCreated(request=req)

    def handle_delete_user(self, req):
        """
        Handles the DELETE v2/<account>/<user> call for deleting a user from an
        account.

        Can only be called by an account .admin.

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success.
        """
        # Validate path info
        account = req.path_info_pop()
        user = req.path_info_pop()
        if req.path_info or not account.isalnum() or not user.isalnum():
            return HTTPBadRequest(request=req)
        if not self.is_account_admin(req, account):
            return HTTPForbidden(request=req)
        # Delete the user's existing token, if any.
        path = quote('/v1/%s/%s/%s' % (self.auth_account, account, user))
        resp = self.make_request(req.environ, 'HEAD',
                                 path).get_response(self.app)
        if resp.status_int == 404:
            return HTTPNotFound(request=req)
        elif resp.status_int // 100 != 2:
            raise Exception('Could not obtain user details: %s %s' %
                            (path, resp.status))
        candidate_token = resp.headers.get('x-object-meta-auth-token')
        if candidate_token:
            path = quote('/v1/%s/.token_%s/%s' %
                (self.auth_account, candidate_token[-1], candidate_token))
            resp = self.make_request(req.environ, 'DELETE',
                                     path).get_response(self.app)
            if resp.status_int // 100 != 2 and resp.status_int != 404:
                raise Exception('Could not delete possibly existing token: '
                                '%s %s' % (path, resp.status))
        # Delete the user entry itself.
        path = quote('/v1/%s/%s/%s' % (self.auth_account, account, user))
        resp = self.make_request(req.environ, 'DELETE',
                                 path).get_response(self.app)
        if resp.status_int // 100 != 2 and resp.status_int != 404:
            raise Exception('Could not delete the user object: %s %s' %
                            (path, resp.status))
        return HTTPNoContent(request=req)

    def handle_get_token(self, req):
        """
        Handles the various `request for token and service end point(s)` calls.
        There are various formats to support the various auth servers in the
        past. Examples::

            GET <auth-prefix>/v1/<act>/auth
                X-Auth-User: <act>:<usr>  or  X-Storage-User: <usr>
                X-Auth-Key: <key>         or  X-Storage-Pass: <key>
            GET <auth-prefix>/auth
                X-Auth-User: <act>:<usr>  or  X-Storage-User: <act>:<usr>
                X-Auth-Key: <key>         or  X-Storage-Pass: <key>
            GET <auth-prefix>/v1.0
                X-Auth-User: <act>:<usr>  or  X-Storage-User: <act>:<usr>
                X-Auth-Key: <key>         or  X-Storage-Pass: <key>

        On successful authentication, the response will have X-Auth-Token and
        X-Storage-Token set to the token to use with Swift and X-Storage-URL
        set to the URL to the default Swift cluster to use.

        The response body will be set to the account's services JSON object as
        described here::

            {"storage": {     # Represents the Swift storage service end points
                "default": "cluster1", # Indicates which cluster is the default
                "cluster1": "<URL to use with Swift>",
                    # A Swift cluster that can be used with this account,
                    # "cluster1" is the name of the cluster which is usually a
                    # location indicator (like "dfw" for a datacenter region).
                "cluster2": "<URL to use with Swift>"
                    # Another Swift cluster that can be used with this account,
                    # there will always be at least one Swift cluster to use or
                    # this whole "storage" dict won't be included at all.
             },
             "servers": {       # Represents the Nova server service end points
                # Expected to be similar to the "storage" dict, but not
                # implemented yet.
             },
             # Possibly other service dicts, not implemented yet.
            }

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success with data set as explained
                  above.
        """
        # Validate the request info
        try:
            pathsegs = split_path(req.path_info, minsegs=1, maxsegs=3,
                                  rest_with_last=True)
        except ValueError:
            return HTTPNotFound(request=req)
        if pathsegs[0] == 'v1' and pathsegs[2] == 'auth':
            account = pathsegs[1]
            user = req.headers.get('x-storage-user')
            if not user:
                user = req.headers.get('x-auth-user')
                if not user or ':' not in user:
                    return HTTPUnauthorized(request=req)
                account2, user = user.split(':', 1)
                if account != account2:
                    return HTTPUnauthorized(request=req)
            key = req.headers.get('x-storage-pass')
            if not key:
                key = req.headers.get('x-auth-key')
        elif pathsegs[0] in ('auth', 'v1.0'):
            user = req.headers.get('x-auth-user')
            if not user:
                user = req.headers.get('x-storage-user')
            if not user or ':' not in user:
                return HTTPUnauthorized(request=req)
            account, user = user.split(':', 1)
            key = req.headers.get('x-auth-key')
            if not key:
                key = req.headers.get('x-storage-pass')
        else:
            return HTTPBadRequest(request=req)
        if not all((account, user, key)):
            return HTTPUnauthorized(request=req)
        if user == '.super_admin' and key == self.super_admin_key:
            token = self.get_itoken(req.environ)
            url = '%s/%s.auth' % (self.dsc_url, self.reseller_prefix)
            return Response(request=req,
              body=json.dumps({'storage': {'default': 'local', 'local': url}}),
              headers={'x-auth-token': token, 'x-storage-token': token,
                       'x-storage-url': url})
        # Authenticate user
        path = quote('/v1/%s/%s/%s' % (self.auth_account, account, user))
        resp = self.make_request(req.environ, 'GET',
                                 path).get_response(self.app)
        if resp.status_int == 404:
            return HTTPUnauthorized(request=req)
        if resp.status_int // 100 != 2:
            raise Exception('Could not obtain user details: %s %s' %
                            (path, resp.status))
        user_detail = json.loads(resp.body)
        if not self.credentials_match(user_detail, key):
            return HTTPUnauthorized(request=req)
        # See if a token already exists and hasn't expired
        token = None
        candidate_token = resp.headers.get('x-object-meta-auth-token')
        if candidate_token:
            path = quote('/v1/%s/.token_%s/%s' %
                (self.auth_account, candidate_token[-1], candidate_token))
            resp = self.make_request(req.environ, 'GET',
                                     path).get_response(self.app)
            if resp.status_int // 100 == 2:
                token_detail = json.loads(resp.body)
                if token_detail['expires'] > time():
                    token = candidate_token
                else:
                    self.make_request(req.environ, 'DELETE',
                                      path).get_response(self.app)
            elif resp.status_int != 404:
                raise Exception('Could not detect whether a token already '
                                'exists: %s %s' % (path, resp.status))
        # Create a new token if one didn't exist
        if not token:
            # Retrieve account id, we'll save this in the token
            path = quote('/v1/%s/%s' % (self.auth_account, account))
            resp = self.make_request(req.environ, 'HEAD',
                                     path).get_response(self.app)
            if resp.status_int // 100 != 2:
                raise Exception('Could not retrieve account id value: '
                                '%s %s' % (path, resp.status))
            account_id = \
                resp.headers['x-container-meta-account-id']
            # Generate new token
            token = '%stk%s' % (self.reseller_prefix, uuid4().hex)
            # Save token info
            path = quote('/v1/%s/.token_%s/%s' %
                         (self.auth_account, token[-1], token))
            resp = self.make_request(req.environ, 'PUT', path,
                json.dumps({'account': account, 'user': user,
                'account_id': account_id,
                'groups': user_detail['groups'],
                'expires': time() + self.token_life})).get_response(self.app)
            if resp.status_int // 100 != 2:
                raise Exception('Could not create new token: %s %s' %
                                (path, resp.status))
            # Record the token with the user info for future use.
            path = quote('/v1/%s/%s/%s' % (self.auth_account, account, user))
            resp = self.make_request(req.environ, 'POST', path,
                headers={'X-Object-Meta-Auth-Token': token}
                ).get_response(self.app)
            if resp.status_int // 100 != 2:
                raise Exception('Could not save new token: %s %s' %
                                (path, resp.status))
        # Get the services information
        path = quote('/v1/%s/%s/.services' % (self.auth_account, account))
        resp = self.make_request(req.environ, 'GET',
                                 path).get_response(self.app)
        if resp.status_int // 100 != 2:
            raise Exception('Could not obtain services info: %s %s' %
                            (path, resp.status))
        detail = json.loads(resp.body)
        url = detail['storage'][detail['storage']['default']]
        return Response(request=req, body=resp.body,
            headers={'x-auth-token': token, 'x-storage-token': token,
                     'x-storage-url': url})

    def handle_validate_token(self, req):
        """
        Handles the GET v2/.token/<token> call for validating a token, usually
        called by a service like Swift.

        On a successful validation, X-Auth-TTL will be set for how much longer
        this token is valid and X-Auth-Groups will contain a comma separated
        list of groups the user belongs to.

        The first group listed will be a unique identifier for the user the
        token represents.

        .reseller_admin is a special group that indicates the user should be
        allowed to do anything on any account.

        :param req: The webob.Request to process.
        :returns: webob.Response, 2xx on success with data set as explained
                  above.
        """
        token = req.path_info_pop()
        if req.path_info or not token.startswith(self.reseller_prefix):
            return HTTPBadRequest(request=req)
        expires = groups = None
        memcache_client = cache_from_env(req.environ)
        if memcache_client:
            memcache_key = '%s/auth/%s' % (self.reseller_prefix, token)
            cached_auth_data = memcache_client.get(memcache_key)
            if cached_auth_data:
                expires, groups = cached_auth_data
                if expires < time():
                    groups = None
        if not groups:
            path = quote('/v1/%s/.token_%s/%s' %
                         (self.auth_account, token[-1], token))
            resp = self.make_request(req.environ, 'GET',
                                     path).get_response(self.app)
            if resp.status_int // 100 != 2:
                return HTTPNotFound(request=req)
            detail = json.loads(resp.body)
            expires = detail['expires']
            if expires < time():
                self.make_request(req.environ, 'DELETE',
                                  path).get_response(self.app)
                return HTTPNotFound(request=req)
            groups = [g['name'] for g in detail['groups']]
            if '.admin' in groups:
                groups.remove('.admin')
                groups.append(detail['account_id'])
            groups = ','.join(groups)
        return HTTPNoContent(headers={'X-Auth-TTL': expires - time(),
                                      'X-Auth-Groups': groups})

    def make_request(self, env, method, path, body=None, headers=None):
        """
        Makes a new webob.Request based on the current env but with the
        parameters specified.

        :param env: Current WSGI environment dictionary
        :param method: HTTP method of new request
        :param path: HTTP path of new request
        :param body: HTTP body of new request; None by default
        :param headers: Extra HTTP headers of new request; None by default

        :returns: webob.Request object
        """
        newenv = {'REQUEST_METHOD': method}
        for name in ('swift.cache', 'HTTP_X_CF_TRANS_ID'):
            if name in env:
                newenv[name] = env[name]
        if not headers:
            headers = {}
        if body:
            return Request.blank(path, environ=newenv, body=body,
                                 headers=headers)
        else:
            return Request.blank(path, environ=newenv, headers=headers)

    def get_conn(self, urlparsed=None):
        """
        Returns an HTTPConnection based on the urlparse result given or the
        default Swift cluster (internal url) urlparse result.

        :param urlparsed: The result from urlparse.urlparse or None to use the
                          default Swift cluster's value
        """
        if not urlparsed:
            urlparsed = self.dsc_parsed2
        if urlparsed.scheme == 'http':
            return HTTPConnection(urlparsed.netloc)
        else:
            return HTTPSConnection(urlparsed.netloc)

    def get_itoken(self, env):
        """
        Returns the current internal token to use for the auth system's own
        actions with other services. Each process will create its own
        itoken and the token will be deleted and recreated based on the
        token_life configuration value. The itoken information is stored in
        memcache because the auth process that is asked by Swift to validate
        the token may not be the same as the auth process that created the
        token.
        """
        if not self.itoken or self.itoken_expires < time():
            self.itoken = '%sitk%s' % (self.reseller_prefix, uuid4().hex)
            memcache_key = '%s/auth/%s' % (self.reseller_prefix, self.itoken)
            self.itoken_expires = time() + self.token_life - 60
            memcache_client = cache_from_env(env)
            if not memcache_client:
                raise Exception(
                    'No memcache set up; required for Swauth middleware')
            memcache_client.set(memcache_key, (self.itoken_expires,
                '.auth,.reseller_admin,%s.auth' % self.reseller_prefix),
                timeout=self.token_life)
        return self.itoken

    def get_admin_detail(self, req):
        """
        Returns the dict for the user specified as the admin in the request
        with the addition of an `account` key set to the admin user's account.

        :param req: The webob request to retrieve X-Auth-Admin-User and
                    X-Auth-Admin-Key from.
        :returns: The dict for the admin user with the addition of the
                  `account` key.
        """
        if ':' not in req.headers.get('x-auth-admin-user', ''):
            return None
        admin_account, admin_user = \
            req.headers.get('x-auth-admin-user').split(':', 1)
        path = quote('/v1/%s/%s/%s' % (self.auth_account, admin_account,
                                       admin_user))
        resp = self.make_request(req.environ, 'GET',
                                 path).get_response(self.app)
        if resp.status_int == 404:
            return None
        if resp.status_int // 100 != 2:
            raise Exception('Could not get admin user object: %s %s' %
                            (path, resp.status))
        admin_detail = json.loads(resp.body)
        admin_detail['account'] = admin_account
        return admin_detail

    def credentials_match(self, user_detail, key):
        """
        Returns True if the key is valid for the user_detail. Currently, this
        only supports plaintext key matching.

        :param user_detail: The dict for the user.
        :param key: The key to validate for the user.
        :returns: True if the key is valid for the user, False if not.
        """
        return user_detail and user_detail.get('auth') == 'plaintext:%s' % key

    def is_super_admin(self, req):
        """
        Returns True if the admin specified in the request represents the
        .super_admin.

        :param req: The webob.Request to check.
        :param returns: True if .super_admin.
        """
        return req.headers.get('x-auth-admin-user') == '.super_admin' and \
               req.headers.get('x-auth-admin-key') == self.super_admin_key

    def is_reseller_admin(self, req, admin_detail=None):
        """
        Returns True if the admin specified in the request represents a
        .reseller_admin.

        :param req: The webob.Request to check.
        :param admin_detail: The previously retrieved dict from
                             :func:`get_admin_detail` or None for this function
                             to retrieve the admin_detail itself.
        :param returns: True if .reseller_admin.
        """
        if self.is_super_admin(req):
            return True
        if not admin_detail:
            admin_detail = self.get_admin_detail(req)
        if not self.credentials_match(admin_detail,
                                      req.headers.get('x-auth-admin-key')):
            return False
        return '.reseller_admin' in (g['name'] for g in admin_detail['groups'])

    def is_account_admin(self, req, account):
        """
        Returns True if the admin specified in the request represents a .admin
        for the account specified.

        :param req: The webob.Request to check.
        :param account: The account to check for .admin against.
        :param returns: True if .admin.
        """
        if self.is_super_admin(req):
            return True
        admin_detail = self.get_admin_detail(req)
        if admin_detail:
            if self.is_reseller_admin(req, admin_detail=admin_detail):
                return True
            if not self.credentials_match(admin_detail,
                                          req.headers.get('x-auth-admin-key')):
                return False
            return admin_detail and admin_detail['account'] == account and \
                   '.admin' in (g['name'] for g in admin_detail['groups'])
        return False

    def posthooklogger(self, env, req):
        response = getattr(req, 'response', None)
        if not response:
            return
        trans_time = '%.4f' % (time() - req.start_time)
        the_request = quote(unquote(req.path))
        if req.query_string:
            the_request = the_request + '?' + req.query_string
        # remote user for zeus
        client = req.headers.get('x-cluster-client-ip')
        if not client and 'x-forwarded-for' in req.headers:
            # remote user for other lbs
            client = req.headers['x-forwarded-for'].split(',')[0].strip()
        logged_headers = None
        if self.log_headers:
            logged_headers = '\n'.join('%s: %s' % (k, v)
                                       for k, v in req.headers.items())
        status_int = response.status_int
        if getattr(req, 'client_disconnect', False) or \
                getattr(response, 'client_disconnect', False):
            status_int = 499
        self.logger.info(' '.join(quote(str(x)) for x in (client or '-',
            req.remote_addr or '-', strftime('%d/%b/%Y/%H/%M/%S', gmtime()),
            req.method, the_request, req.environ['SERVER_PROTOCOL'],
            status_int, req.referer or '-', req.user_agent or '-',
            req.headers.get('x-auth-token',
                req.headers.get('x-auth-admin-user', '-')),
            getattr(req, 'bytes_transferred', 0) or '-',
            getattr(response, 'bytes_transferred', 0) or '-',
            req.headers.get('etag', '-'),
            req.headers.get('x-cf-trans-id', '-'), logged_headers or '-',
            trans_time)))


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return Swauth(app, conf)
    return auth_filter
