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

from webob import Response
from webob.exc import HTTPMovedPermanently, HTTPNotFound, HTTPUnauthorized

from swift.common.utils import split_path


# To use:
#   Put the staticweb filter just after the auth filter.
#   Make the container publicly readable:
#       st post -r '.r:*' container
#   You should be able to get objects now, but not do listings.
#   Set an index file:
#       st post -m 'index:index.html' container
#   You should be able to hit path's that have an index.html without needing to
#   type the index.html part, but still not do listings.
#   Turn on listings:
#       st post -m 'index:allow_listings,index.html' container
#   Set an error file:
#       st post -m 'error:error.html' container
#   Now 404's should load 404error.html

# TODO: Tests
# TODO: Docs
# TODO: Make a disallow_listings to restrict direct container listings. These
#       have to stay on by default because of swauth and any other similar
#       middleware. The lower level indirect listings can stay disabled by
#       default.
# TODO: Accept header negotiation: make static web work with authed as well
# TODO: get_container_info can be memcached
# TODO: Blueprint

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
        tmp_env = dict(env)
        self.strip_ifs(tmp_env)
        tmp_env['PATH_INFO'] = '/%s/%s/%s/%s%s' % (self.version, self.account,
            self.container, self.get_status_int(), self.error)
        tmp_env['REQUEST_METHOD'] = 'GET'
        return self.app(tmp_env, start_response)

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
        self.index_allow_listings = False
        tmp_env = dict(env)
        self.strip_ifs(tmp_env)
        tmp_env['REQUEST_METHOD'] = 'HEAD'
        tmp_env['PATH_INFO'] = \
            '/%s/%s/%s' % (self.version, self.account, self.container)
        resp = self.app(tmp_env, self.start_response)
        if self.get_status_int() // 100 != 2:
            return
        self.index = self.get_header(self.response_headers,
                                     'x-container-meta-index', '').strip()
        self.error = self.get_header(self.response_headers,
                                     'x-container-meta-error', '').strip()
        if not self.index:
            return
        if self.index.lower() == 'allow_listings':
            self.index_allow_listings = True
        elif self.index.lower().startswith('allow_listings,'):
            self.index = self.index[len('allow_listings,'):]
            self.index_allow_listings = True

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
        if not self.index and not self.index_allow_listings:
            return self.app(env, start_response)
        if env['PATH_INFO'][-1] != '/':
            return HTTPMovedPermanently(
                location=env['PATH_INFO'] + '/')(env, start_response)
        if not self.index and self.index_allow_listings:
            return self.listing(env, start_response)
        tmp_env = dict(env)
        tmp_env['PATH_INFO'] += self.index
        resp = self.app(tmp_env, self.start_response)
        status_int = self.get_status_int()
        if status_int == 404 and self.index_allow_listings:
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
        if not self.index and not self.index_allow_listings:
            return self.app(env, start_response)
        if self.index:
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
            elif status_int == 404 and self.index_allow_listings:
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
        return self.app(env, start_response)

    def __call__(self, env, start_response):
        if env.get('REMOTE_USER') or \
                env['REQUEST_METHOD'] not in ('HEAD', 'GET'):
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
