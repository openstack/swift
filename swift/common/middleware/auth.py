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

from time import time

from eventlet.timeout import Timeout
from webob.exc import HTTPForbidden, HTTPUnauthorized

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
        self.ssl = \
            conf.get('ssl', 'false').lower() in TRUE_VALUES
        self.timeout = int(conf.get('node_timeout', 10))

    def __call__(self, env, start_response):
        """
        Accepts a standard WSGI application call, authenticating the request
        and installing callback hooks for authorization and ACL header
        validation. For an authenticated request, REMOTE_USER will be set to a
        comma separated list of the user's groups.

        With out a reseller prefix, I act as the default auth service for
        all requests, but I won't overwrite swift.authorize unless my auth
        explictly grants groups to this token

        As a reseller, I must respect that my auth server is not
        authorative for all tokens, but I can set swift.authorize to
        denied_reponse if it's not already set
        """
        token = env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))
        if token and token.startswith(self.reseller_prefix):
            # N.B. no reseller_prefix will match all tokens!
            # attempt to auth my token with my auth server
            groups = self.get_groups(token,
                                     memcache_client=cache_from_env(env))
            if groups:
                env['REMOTE_USER'] = groups
                user = groups and groups.split(',', 1)[0] or ''
                env['HTTP_X_AUTH_TOKEN'] = '%s,%s' % (user, token)
                env['swift.authorize'] = self.authorize
                env['swift.clean_acl'] = clean_acl
            else:
                # unauthorized token
                if self.reseller_prefix:
                    # because I own this token, I can deny it outright
                    return HTTPUnauthorized()(env, start_response)
                elif 'swift.authorize' not in env:
                    # default auth won't over-write swift.authorize
                    env['swift.authorize'] = self.denied_response
        else:
            if self.reseller_prefix:
                # As a reseller, I would like to be calledback for annoynomous
                # access to my accounts
                version, rest = split_path(env.get('PATH_INFO', ''),
                                           1, 2, True)
                if rest and rest.startswith(self.reseller_prefix):
                    # handle annoynomous access to my reseller's accounts
                    env['swift.authorize'] = self.authorize
                    env['swift.clean_acl'] = clean_acl
                elif 'swift.authorize' not in env:
                    # not my token, not my account, I can't authorize this
                    # request, deny all is a good idea if not already set...
                    env['swift.authorize'] = self.denied_response
            elif 'swift.authorize' not in env:
                # As a default auth, I'm willing to handle annoynomous requests
                # for all accounts, but I won't over-write swift.authorize
                env['swift.authorize'] = self.authorize
                env['swift.clean_acl'] = clean_acl

        return self.app(env, start_response)

    def get_groups(self, token, memcache_client=None):
        """
        Get groups for the given token, may use a memcache_client if set, and
        update cache/expire old values; otherwise call auth_host and return
        'x-auth-groups'
        """
        groups = None
        if memcache_client:
            key = '%s/token/%s' % (self.reseller_prefix, token)
            cached_auth_data = memcache_client.get(key)
            if cached_auth_data:
                start, expiration, groups = cached_auth_data
                if time() - start > expiration:
                    groups = None

            def set_memcache(expiration, groups, key=key):
                memcache_client.set(key, (time(), expiration, groups),
                                    timeout=expiration)
        else:
            set_memcache = lambda *args: None
        if not groups:
            with Timeout(self.timeout):
                conn = http_connect(self.auth_host, self.auth_port, 'GET',
                                    '/token/%s' % token, ssl=self.ssl)
                resp = conn.getresponse()
                resp.read()
                conn.close()
            if resp.status // 100 != 2:
                return None
            expiration = float(resp.getheader('x-auth-ttl'))
            groups = resp.getheader('x-auth-groups')
            set_memcache(expiration, groups)
        return groups

    def authorize(self, req):
        """
        Returns None if the request is authorized to continue or a standard
        WSGI response callable if not.
        """
        version, account, container, obj = split_path(req.path, 1, 4, True)
        if not account or not account.startswith(self.reseller_prefix):
            return self.denied_response(req)
        user_groups = (req.remote_user or '').split(',')
        if '.reseller_admin' in user_groups:
            return None
        if account in user_groups and (req.method != 'PUT' or container):
            # If the user is admin for the account and is not trying to do an
            # account PUT...
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
