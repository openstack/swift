# Copyright (c) 2010-2011 OpenStack, LLC.
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

from time import time

from eventlet.timeout import Timeout
from webob.exc import HTTPForbidden, HTTPUnauthorized, HTTPNotFound

from swift.common.bufferedhttp import http_connect_raw as http_connect
from swift.common.middleware.acl import clean_acl, parse_acl, referrer_allowed
from swift.common.utils import cache_from_env, split_path, TRUE_VALUES


class DevAuth(object):
    """Auth Middleware that uses the dev auth server."""

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.reseller_prefix = conf.get('reseller_prefix', 'AUTH').strip()
        if self.reseller_prefix and self.reseller_prefix[-1] != '_':
            self.reseller_prefix += '_'
        self.auth_host = conf.get('ip', '127.0.0.1')
        self.auth_port = int(conf.get('port', 11000))
        self.ssl = conf.get('ssl', 'false').lower() in TRUE_VALUES
        self.auth_prefix = conf.get('prefix', '/')
        self.timeout = int(conf.get('node_timeout', 10))

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
        """
        s3 = env.get('HTTP_AUTHORIZATION')
        token = env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))
        if s3 or (token and token.startswith(self.reseller_prefix)):
            # Note: Empty reseller_prefix will match all tokens.
            # Attempt to auth my token with my auth server
            groups = self.get_groups(env, token,
                                     memcache_client=cache_from_env(env))
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

    def get_groups(self, env, token, memcache_client=None):
        """
        Get groups for the given token.

        If memcache_client is set, token credentials will be cached
        appropriately.

        With a cache miss, or no memcache_client, the configurated external
        authentication server will be queried for the group information.

        :param token: Token to validate and return a group string for.
        :param memcache_client: Memcached client to use for caching token
                                credentials; None if no caching is desired.
        :returns: None if the token is invalid or a string containing a comma
                  separated list of groups the authenticated user is a member
                  of. The first group in the list is also considered a unique
                  identifier for that user.
        """
        groups = None
        key = '%s/token/%s' % (self.reseller_prefix, token)
        cached_auth_data = memcache_client and memcache_client.get(key)
        if cached_auth_data:
            start, expiration, groups = cached_auth_data
            if time() - start > expiration:
                groups = None

        headers = {}
        if env.get('HTTP_AUTHORIZATION'):
            groups = None
            headers["Authorization"] = env.get('HTTP_AUTHORIZATION')

        if not groups:
            with Timeout(self.timeout):
                conn = http_connect(self.auth_host, self.auth_port, 'GET',
                                    '%stoken/%s' % (self.auth_prefix, token),
                                    headers, ssl=self.ssl)

                resp = conn.getresponse()
                resp.read()
                conn.close()
            if resp.status // 100 != 2:
                return None
            expiration = float(resp.getheader('x-auth-ttl'))
            groups = resp.getheader('x-auth-groups')
            if memcache_client:
                memcache_client.set(key, (time(), expiration, groups),
                                    timeout=expiration)

        if env.get('HTTP_AUTHORIZATION'):
            account, user, sign = \
                env['HTTP_AUTHORIZATION'].split(' ')[-1].split(':')
            cfaccount = resp.getheader('x-auth-account-suffix')
            path = env['PATH_INFO']
            env['PATH_INFO'] = \
                path.replace("%s:%s" % (account, user), cfaccount, 1)

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
        if '.reseller_admin' in user_groups:
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


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return DevAuth(app, conf)
    return auth_filter
