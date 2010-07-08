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

from ConfigParser import ConfigParser, NoOptionError
import os
import time

from webob.request import Request
from webob.exc import HTTPUnauthorized, HTTPPreconditionFailed
from eventlet.timeout import Timeout

from swift.common.utils import split_path
from swift.common.bufferedhttp import http_connect_raw as http_connect


class DevAuthMiddleware(object):
    """
    Auth Middleware that uses the dev auth server
    """
    def __init__(self, app, conf, memcache_client, logger):
        self.app = app
        self.memcache_client = memcache_client
        self.logger = logger
        self.conf = conf
        self.auth_host = conf.get('bind_ip', '127.0.0.1')
        self.auth_port = int(conf.get('bind_port', 11000))
        self.timeout = int(conf.get('node_timeout', 10))

    def __call__(self, env, start_response):
        req = Request(env)
        if req.path != '/healthcheck':
            if 'x-storage-token' in req.headers and \
                    'x-auth-token' not in req.headers:
                req.headers['x-auth-token'] = req.headers['x-storage-token']
            version, account, container, obj = split_path(req.path, 1, 4, True)
            if account is None:
                return HTTPPreconditionFailed(request=req, body='Bad URL')(
                    env, start_response)
            if not req.headers.get('x-auth-token'):
                return HTTPPreconditionFailed(request=req,
                    body='Missing Auth Token')(env, start_response)
                if account is None:
                    return HTTPPreconditionFailed(
                        request=req, body='Bad URL')(env, start_response)
            if not self.auth(account, req.headers['x-auth-token']):
                return HTTPUnauthorized(request=req)(env, start_response)

        # If we get here, then things should be good.
        return self.app(env, start_response)

    def auth(self, account, token):
        """
        Dev authorization implmentation

        :param account: account name
        :param token: auth token

        :returns: True if authorization is successful, False otherwise
        """
        key = 'auth/%s/%s' % (account, token)
        now = time.time()
        cached_auth_data = self.memcache_client.get(key)
        if cached_auth_data:
            start, expiration = cached_auth_data
            if now - start <= expiration:
                return True
        try:
            with Timeout(self.timeout):
                conn = http_connect(self.auth_host, self.auth_port, 'GET',
                    '/token/%s/%s' % (account, token))
                resp = conn.getresponse()
                resp.read()
                conn.close()
                if resp.status == 204:
                    validated = float(resp.getheader('x-auth-ttl'))
                else:
                    validated = False
        except:
            self.logger.exception('ERROR with auth')
            return False
        if not validated:
            return False
        else:
            val = (now, validated)
            self.memcache_client.set(key, val, timeout=validated)
            return True
