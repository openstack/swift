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

try:
    import simplejson as json
except ImportError:
    import json

import cgi
import urllib

from webob import Response, Request
from webob.exc import HTTPMovedPermanently, HTTPNotFound, HTTPUnauthorized

from swift.common.utils import split_path, TRUE_VALUES


# To use:
#   Put the staticweb filter just after the auth filter.
#   Make the container publicly readable:
#       st post -r '.r:*' container
#   You should be able to get objects and do direct container listings
#   now, though they'll be in the REST API format.
#   Set an index file directive:
#       st post -m 'index:index.html' container
#   You should be able to hit paths that have an index.html without
#   needing to type the index.html part and listings will now be HTML.
#   Turn off listings:
#       st post -r '.r:*,.rnolisting' container
#   Set an error file:
#       st post -m 'error:error.html' container
#   Now 401's should load s 401error.html, 404's should load
#   404error.html, etc.
#
#   This mode is normally only active for anonymous requests. If you
#   want to use it with authenticated requests, set the X-Web-Mode:
#   true header.
#
# TODO: Make new headers instead of using user metadata.
# TODO: Tests.
# TODO: Docs.
# TODO: get_container_info can be memcached.
# TODO: Blueprint.

class StaticWeb(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf

    def start_response(self, status, headers, exc_info=None):
        self.response_status = status
        self.response_headers = headers
        self.response_exc_info = exc_info

    def error_response(self, response, env, start_response):
        if not self.error:
            start_response(self.response_status, self.response_headers,
                           self.response_exc_info)
            return response
        save_response_status = self.response_status
        save_response_headers = self.response_headers
        save_response_exc_info = self.response_exc_info
        tmp_env = dict(env)
        self.strip_ifs(tmp_env)
        tmp_env['PATH_INFO'] = '/%s/%s/%s/%s%s' % (self.version, self.account,
            self.container, self.get_status_int(), self.error)
        tmp_env['REQUEST_METHOD'] = 'GET'
        resp = self.app(tmp_env, self.start_response)
        if self.get_status_int() // 100 == 2:
            start_response(self.response_status, self.response_headers,
                           self.response_exc_info)
            return resp
        start_response(save_response_status, save_response_headers,
                       save_response_exc_info)
        return response

    def get_status_int(self):
        return int(self.response_status.split(' ', 1)[0])

    def get_header(self, headers, name, default_value=None):
        for header, value in headers:
            if header.lower() == name:
                return value
        return default_value

    def strip_ifs(self, env):
        for key in [k for k in env.keys() if k.startswith('HTTP_IF_')]:
            del env[key]

    def get_container_info(self, env, start_response):
        self.index = self.error = None
        tmp_env = {'REQUEST_METHOD': 'HEAD', 'HTTP_USER_AGENT': 'StaticWeb'}
        for name in ('swift.cache', 'HTTP_X_CF_TRANS_ID'):
            if name in env:
                tmp_env[name] = env[name]
        req = Request.blank('/%s/%s/%s' % (self.version, self.account,
            self.container), environ=tmp_env)
        resp = req.get_response(self.app)
        if resp.status_int // 100 == 2:
            self.index = resp.headers.get('x-container-meta-index', '').strip()
            self.error = resp.headers.get('x-container-meta-error', '').strip()

    def listing(self, env, start_response, prefix=None):
        tmp_env = dict(env)
        self.strip_ifs(tmp_env)
        tmp_env['REQUEST_METHOD'] = 'GET'
        tmp_env['PATH_INFO'] = \
            '/%s/%s/%s' % (self.version, self.account, self.container)
        tmp_env['QUERY_STRING'] = 'delimiter=/&format=json'
        if prefix:
            tmp_env['QUERY_STRING'] += '&prefix=%s' % urllib.quote(prefix)
        resp = self.app(tmp_env, self.start_response)
        if self.get_status_int() // 100 != 2:
            return self.error_response(resp, env, start_response)
        listing = json.loads(''.join(resp))
        if not listing:
            resp = HTTPNotFound()(env, self.start_response)
            return self.error_response(resp, env, start_response)
        headers = {'Content-Type': 'text/html'}
        body = '<html><head><title>Listing of%s</title></head><body>' \
               '<p><b>Listing of %s</b></p><p>\n' % \
               (cgi.escape(env['PATH_INFO']), cgi.escape(env['PATH_INFO']))
        if prefix:
            body += '<a href="../">../</a><br />'
        for item in listing:
            if 'subdir' in item:
                subdir = item['subdir']
                if prefix:
                    subdir = subdir[len(prefix):]
                body += '<a href="%s">%s</a><br />' % \
                        (urllib.quote(subdir), cgi.escape(subdir))
        for item in listing:
            if 'name' in item:
                name = item['name']
                if prefix:
                    name = name[len(prefix):]
                body += '<a href="%s">%s</a><br />' % \
                        (urllib.quote(name), cgi.escape(name))
        body += '</p></body></html>\n'
        return Response(headers=headers, body=body)(env, start_response)

    def handle_container(self, env, start_response):
        self.get_container_info(env, start_response)
        if not self.index:
            return self.app(env, start_response)
        if env['PATH_INFO'][-1] != '/':
            return HTTPMovedPermanently(
                location=env['PATH_INFO'] + '/')(env, start_response)
        tmp_env = dict(env)
        tmp_env['PATH_INFO'] += self.index
        resp = self.app(tmp_env, self.start_response)
        status_int = self.get_status_int()
        if status_int == 404:
            return self.listing(env, start_response)
        elif self.get_status_int() // 100 not in (2, 3):
            return self.error_response(resp, env, start_response)
        start_response(self.response_status, self.response_headers,
                       self.response_exc_info)
        return resp

    def handle_object(self, env, start_response):
        tmp_env = dict(env)
        resp = self.app(tmp_env, self.start_response)
        status_int = self.get_status_int()
        if status_int // 100 in (2, 3):
            start_response(self.response_status, self.response_headers,
                           self.response_exc_info)
            return resp
        if status_int != 404:
            return self.error_response(resp, env, start_response)
        self.get_container_info(env, start_response)
        if not self.index:
            return self.app(env, start_response)
        tmp_env = dict(env)
        if tmp_env['PATH_INFO'][-1] != '/':
            tmp_env['PATH_INFO'] += '/'
        tmp_env['PATH_INFO'] += self.index
        resp = self.app(tmp_env, self.start_response)
        status_int = self.get_status_int()
        if status_int // 100 in (2, 3):
            if env['PATH_INFO'][-1] != '/':
                return HTTPMovedPermanently(
                    location=env['PATH_INFO'] + '/')(env, start_response)
            start_response(self.response_status, self.response_headers,
                           self.response_exc_info)
            return resp
        elif status_int == 404:
            if env['PATH_INFO'][-1] != '/':
                tmp_env = dict(env)
                self.strip_ifs(tmp_env)
                tmp_env['REQUEST_METHOD'] = 'GET'
                tmp_env['PATH_INFO'] = '/%s/%s/%s' % (self.version,
                    self.account, self.container)
                tmp_env['QUERY_STRING'] = 'limit=1&format=json&delimiter' \
                    '=/&limit=1&prefix=%s' % urllib.quote(self.obj + '/')
                resp = self.app(tmp_env, self.start_response)
                if self.get_status_int() // 100 != 2 or \
                        not json.loads(''.join(resp)):
                    resp = HTTPNotFound()(env, self.start_response)
                    return self.error_response(resp, env, start_response)
                return HTTPMovedPermanently(location=env['PATH_INFO'] +
                    '/')(env, start_response)
            return self.listing(env, start_response, self.obj)

    def __call__(self, env, start_response):
        if env['REQUEST_METHOD'] not in ('HEAD', 'GET') or \
                (env.get('REMOTE_USER') and
                 not env.get('HTTP_X_WEB_MODE', '') in TRUE_VALUES):
            return self.app(env, start_response)
        (self.version, self.account, self.container, self.obj) = \
            split_path(env['PATH_INFO'], 2, 4, True)
        if self.obj:
            return self.handle_object(env, start_response)
        elif self.container:
            return self.handle_container(env, start_response)
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def staticweb_filter(app):
        return StaticWeb(app, conf)
    return staticweb_filter
