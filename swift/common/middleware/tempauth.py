# Copyright (c) 2011 OpenStack, LLC.
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

from time import gmtime, strftime, time
from traceback import format_exc
from urllib import quote, unquote
from uuid import uuid4
from hashlib import sha1
import hmac
import base64

from eventlet import Timeout
from webob import Response, Request
from webob.exc import HTTPBadRequest, HTTPForbidden, HTTPNotFound, \
    HTTPUnauthorized

from swift.common.middleware.acl import clean_acl, parse_acl, referrer_allowed
from swift.common.utils import cache_from_env, get_logger, get_remote_client, \
    split_path, TRUE_VALUES


class TempAuth(object):
    """
    Test authentication and authorization system.

    Add to your pipeline in proxy-server.conf, such as::

        [pipeline:main]
        pipeline = catch_errors cache tempauth proxy-server

    Set account auto creation to true in proxy-server.conf::

        [app:proxy-server]
        account_autocreate = true

    And add a tempauth filter section, such as::

        [filter:tempauth]
        use = egg:swift#tempauth
        user_admin_admin = admin .admin .reseller_admin
        user_test_tester = testing .admin
        user_test2_tester2 = testing2 .admin
        user_test_tester3 = testing3

    See the proxy-server.conf-sample for more information.

    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, log_route='tempauth')
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
        self.token_life = int(conf.get('token_life', 86400))
        self.allowed_sync_hosts = [h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.allow_overrides = \
            conf.get('allow_overrides', 't').lower() in TRUE_VALUES
        self.users = {}
        for conf_key in conf:
            if conf_key.startswith('user_'):
                values = conf[conf_key].split()
                if not values:
                    raise ValueError('%s has no key set' % conf_key)
                key = values.pop(0)
                if values and '://' in values[-1]:
                    url = values.pop()
                else:
                    url = 'https://' if 'cert_file' in conf else 'http://'
                    ip = conf.get('bind_ip', '127.0.0.1')
                    if ip == '0.0.0.0':
                        ip = '127.0.0.1'
                    url += ip
                    url += ':' + conf.get('bind_port', '8080') + '/v1/' + \
                           self.reseller_prefix + conf_key.split('_')[1]
                self.users[conf_key.split('_', 1)[1].replace('_', ':')] = {
                    'key': key, 'url': url, 'groups': values}

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
        This is to handle granting tokens, etc.
        """
        if self.allow_overrides and env.get('swift.authorize_override', False):
            return self.app(env, start_response)
        if env.get('PATH_INFO', '').startswith(self.auth_prefix):
            return self.handle(env, start_response)
        s3 = env.get('HTTP_AUTHORIZATION')
        token = env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))
        if s3 or (token and token.startswith(self.reseller_prefix)):
            # Note: Empty reseller_prefix will match all tokens.
            groups = self.get_groups(env, token)
            if groups:
                env['REMOTE_USER'] = groups
                user = groups and groups.split(',', 1)[0] or ''
                # We know the proxy logs the token, so we augment it just a bit
                # to also log the authenticated user.
                env['HTTP_X_AUTH_TOKEN'] = \
                    '%s,%s' % (user, 's3' if s3 else token)
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
        if not memcache_client:
            raise Exception('Memcache required')
        memcache_token_key = '%s/token/%s' % (self.reseller_prefix, token)
        cached_auth_data = memcache_client.get(memcache_token_key)
        if cached_auth_data:
            expires, groups = cached_auth_data
            if expires < time():
                groups = None

        if env.get('HTTP_AUTHORIZATION'):
            account_user, sign = \
                env['HTTP_AUTHORIZATION'].split(' ')[1].rsplit(':', 1)
            if account_user not in self.users:
                return None
            account, user = account_user.split(':', 1)
            account_id = self.users[account_user]['url'].rsplit('/', 1)[-1]
            path = env['PATH_INFO']
            env['PATH_INFO'] = path.replace(account_user, account_id, 1)
            msg = base64.urlsafe_b64decode(unquote(token))
            key = self.users[account_user]['key']
            s = base64.encodestring(hmac.new(key, msg, sha1).digest()).strip()
            if s != sign:
                return None
            groups = [account, account_user]
            groups.extend(self.users[account_user]['groups'])
            if '.admin' in groups:
                groups.remove('.admin')
                groups.append(account_id)
            groups = ','.join(groups)

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
                account[len(self.reseller_prefix)] != '.':
            req.environ['swift_owner'] = True
            return None
        if account in user_groups and \
                (req.method not in ('DELETE', 'PUT') or container):
            # If the user is admin for the account and is not trying to do an
            # account DELETE or PUT...
            req.environ['swift_owner'] = True
            return None
        if (req.environ.get('swift_sync_key') and
            req.environ['swift_sync_key'] ==
                req.headers.get('x-container-sync-key', None) and
            'x-timestamp' in req.headers and
            (req.remote_addr in self.allowed_sync_hosts or
             get_remote_client(req) in self.allowed_sync_hosts)):
            return None
        referrers, groups = parse_acl(getattr(req, 'acl', None))
        if referrer_allowed(req.referer, referrers):
            if obj or '.rlistings' in groups:
                return None
            return self.denied_response(req)
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
        except (Exception, Timeout):
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
            version, account, user, _junk = split_path(req.path_info,
                minsegs=1, maxsegs=4, rest_with_last=True)
        except ValueError:
            return HTTPNotFound(request=req)
        if version in ('v1', 'v1.0', 'auth'):
            if req.method == 'GET':
                handler = self.handle_get_token
        if not handler:
            req.response = HTTPBadRequest(request=req)
        else:
            req.response = handler(req)
        return req.response

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
        # Authenticate user
        account_user = account + ':' + user
        if account_user not in self.users:
            return HTTPUnauthorized(request=req)
        if self.users[account_user]['key'] != key:
            return HTTPUnauthorized(request=req)
        # Get memcache client
        memcache_client = cache_from_env(req.environ)
        if not memcache_client:
            raise Exception('Memcache required')
        # See if a token already exists and hasn't expired
        token = None
        memcache_user_key = '%s/user/%s' % (self.reseller_prefix, account_user)
        candidate_token = memcache_client.get(memcache_user_key)
        if candidate_token:
            memcache_token_key = \
                '%s/token/%s' % (self.reseller_prefix, candidate_token)
            cached_auth_data = memcache_client.get(memcache_token_key)
            if cached_auth_data:
                expires, groups = cached_auth_data
                if expires > time():
                    token = candidate_token
        # Create a new token if one didn't exist
        if not token:
            # Generate new token
            token = '%stk%s' % (self.reseller_prefix, uuid4().hex)
            expires = time() + self.token_life
            groups = [account, account_user]
            groups.extend(self.users[account_user]['groups'])
            if '.admin' in groups:
                groups.remove('.admin')
                account_id = self.users[account_user]['url'].rsplit('/', 1)[-1]
                groups.append(account_id)
            groups = ','.join(groups)
            # Save token
            memcache_token_key = '%s/token/%s' % (self.reseller_prefix, token)
            memcache_client.set(memcache_token_key, (expires, groups),
                                timeout=float(expires - time()))
            # Record the token with the user info for future use.
            memcache_user_key = \
                '%s/user/%s' % (self.reseller_prefix, account_user)
            memcache_client.set(memcache_user_key, token,
                                timeout=float(expires - time()))
        return Response(request=req,
            headers={'x-auth-token': token, 'x-storage-token': token,
                     'x-storage-url': self.users[account_user]['url']})

    def posthooklogger(self, env, req):
        if not req.path.startswith(self.auth_prefix):
            return
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
            req.environ.get('swift.trans_id', '-'), logged_headers or '-',
            trans_time)))


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return TempAuth(app, conf)
    return auth_filter
