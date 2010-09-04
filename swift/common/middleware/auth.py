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
from swift.common.utils import cache_from_env, split_path


class DevAuth(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.auth_host = conf.get('ip', '127.0.0.1')
        self.auth_port = int(conf.get('port', 11000))
        self.ssl = \
            conf.get('ssl', 'false').lower() in ('true', 'on', '1', 'yes')
        self.timeout = int(conf.get('node_timeout', 10))

    def __call__(self, env, start_response):
        user = None
        token = env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))
        if token:
            memcache_client = cache_from_env(env)
            key = 'devauth/%s' % token
            cached_auth_data = memcache_client.get(key)
            if cached_auth_data:
                start, expiration, user = cached_auth_data
                if time() - start > expiration:
                    user = None
            if not user:
                with Timeout(self.timeout):
                    conn = http_connect(self.auth_host, self.auth_port, 'GET',
                                        '/token/%s' % token, ssl=self.ssl)
                    resp = conn.getresponse()
                    resp.read()
                    conn.close()
                if resp.status // 100 != 2:
                    return HTTPUnauthorized()(env, start_response)
                expiration = float(resp.getheader('x-auth-ttl'))
                user = resp.getheader('x-auth-user')
                memcache_client.set(key, (time(), expiration, user),
                                    timeout=expiration)
        env['REMOTE_USER'] = user
        env['swift.authorize'] = self.authorize
        env['swift.clean_acl'] = clean_acl
        return self.app(env, start_response)

    def authorize(self, req):
        version, account, container, obj = split_path(req.path, 1, 4, True)
        if not account:
            return self.denied_response(req)
        if req.remote_user and account in req.remote_user.split(','):
            return None
        referrers, groups = parse_acl(getattr(req, 'acl', None))
        if referrer_allowed(req, referrers):
            return None
        if not req.remote_user:
            return self.denied_response(req)
        for user_group in req.remote_user.split(','):
            if user_group in groups:
                return None
        return self.denied_response(req)

    def denied_response(self, req):
        if req.remote_user:
            return HTTPForbidden(request=req)
        else:
            return HTTPUnauthorized(request=req)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    def auth_filter(app):
        return DevAuth(app, conf)
    return auth_filter
