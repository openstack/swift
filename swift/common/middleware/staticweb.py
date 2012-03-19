# Copyright (c) 2010-2012 OpenStack, LLC.
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

"""
This StaticWeb WSGI middleware will serve container data as a static web site
with index file and error file resolution and optional file listings. This mode
is normally only active for anonymous requests. If you want to use it with
authenticated requests, set the ``X-Web-Mode: true`` header on the request.

The ``staticweb`` filter should be added to the pipeline in your
``/etc/swift/proxy-server.conf`` file just after any auth middleware. Also, the
configuration section for the ``staticweb`` middleware itself needs to be
added. For example::

    [DEFAULT]
    ...

    [pipeline:main]
    pipeline = healthcheck cache tempauth staticweb proxy-server

    ...

    [filter:staticweb]
    use = egg:swift#staticweb
    # Seconds to cache container x-container-meta-web-* header values.
    # cache_timeout = 300
    # You can override the default log routing for this filter here:
    # set log_name = staticweb
    # set log_facility = LOG_LOCAL0
    # set log_level = INFO
    # set access_log_name = staticweb
    # set access_log_facility = LOG_LOCAL0
    # set access_log_level = INFO
    # set log_headers = False

Any publicly readable containers (for example, ``X-Container-Read: .r:*``, see
`acls`_ for more information on this) will be checked for
X-Container-Meta-Web-Index and X-Container-Meta-Web-Error header values::

    X-Container-Meta-Web-Index  <index.name>
    X-Container-Meta-Web-Error  <error.name.suffix>

If X-Container-Meta-Web-Index is set, any <index.name> files will be served
without having to specify the <index.name> part. For instance, setting
``X-Container-Meta-Web-Index: index.html`` will be able to serve the object
.../pseudo/path/index.html with just .../pseudo/path or .../pseudo/path/

If X-Container-Meta-Web-Error is set, any errors (currently just 401
Unauthorized and 404 Not Found) will instead serve the
.../<status.code><error.name.suffix> object. For instance, setting
``X-Container-Meta-Web-Error: error.html`` will serve .../404error.html for
requests for paths not found.

For psuedo paths that have no <index.name>, this middleware can serve HTML file
listings if you set the ``X-Container-Meta-Web-Listings: true`` metadata item
on the container.

If listings are enabled, the listings can have a custom style sheet by setting
the X-Container-Meta-Web-Listings-CSS header. For instance, setting
``X-Container-Meta-Web-Listings-CSS: listing.css`` will make listings link to
the .../listing.css style sheet. If you "view source" in your browser on a
listing page, you will see the well defined document structure that can be
styled.

Example usage of this middleware via ``swift``:

    Make the container publicly readable::

        swift post -r '.r:*' container

    You should be able to get objects directly, but no index.html resolution or
    listings.

    Set an index file directive::

        swift post -m 'web-index:index.html' container

    You should be able to hit paths that have an index.html without needing to
    type the index.html part.

    Turn on listings::

        swift post -m 'web-listings: true' container

    Now you should see object listings for paths and pseudo paths that have no
    index.html.

    Enable a custom listings style sheet::

        swift post -m 'web-listings-css:listings.css' container

    Set an error file::

        swift post -m 'web-error:error.html' container

    Now 401's should load 401error.html, 404's should load 404error.html, etc.
"""


try:
    import simplejson as json
except ImportError:
    import json

import cgi
import time
from urllib import unquote, quote as urllib_quote

from webob import Response, Request
from webob.exc import HTTPMovedPermanently, HTTPNotFound

from swift.common.utils import cache_from_env, get_logger, human_readable, \
                               split_path, TRUE_VALUES


def quote(value, safe='/'):
    """
    Patched version of urllib.quote that encodes utf-8 strings before quoting
    """
    if isinstance(value, unicode):
        value = value.encode('utf-8')
    return urllib_quote(value, safe)


class StaticWeb(object):
    """
    The Static Web WSGI middleware filter; serves container data as a static
    web site. See `staticweb`_ for an overview.

    :param app: The next WSGI application/filter in the paste.deploy pipeline.
    :param conf: The filter configuration dict.
    """

    def __init__(self, app, conf):
        #: The next WSGI application/filter in the paste.deploy pipeline.
        self.app = app
        #: The filter configuration dict.
        self.conf = conf
        #: The seconds to cache the x-container-meta-web-* headers.,
        self.cache_timeout = int(conf.get('cache_timeout', 300))
        #: Logger for this filter.
        self.logger = get_logger(conf, log_route='staticweb')
        access_log_conf = {}
        for key in ('log_facility', 'log_name', 'log_level'):
            value = conf.get('access_' + key, conf.get(key, None))
            if value:
                access_log_conf[key] = value
        #: Web access logger for this filter.
        self.access_logger = get_logger(access_log_conf,
                                        log_route='staticweb-access')
        #: Indicates whether full HTTP headers should be logged or not.
        self.log_headers = conf.get('log_headers') == 'True'
        # Results from the last call to self._start_response.
        self._response_status = None
        self._response_headers = None
        self._response_exc_info = None
        # Results from the last call to self._get_container_info.
        self._index = self._error = self._listings = self._listings_css = None

    def _start_response(self, status, headers, exc_info=None):
        """
        Saves response info without sending it to the remote client.
        Uses the same semantics as the usual WSGI start_response.
        """
        self._response_status = status
        self._response_headers = headers
        self._response_exc_info = exc_info

    def _error_response(self, response, env, start_response):
        """
        Sends the error response to the remote client, possibly resolving a
        custom error response body based on x-container-meta-web-error.

        :param response: The error response we should default to sending.
        :param env: The original request WSGI environment.
        :param start_response: The WSGI start_response hook.
        """
        self._log_response(env, self._get_status_int())
        if not self._error:
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return response
        save_response_status = self._response_status
        save_response_headers = self._response_headers
        save_response_exc_info = self._response_exc_info
        tmp_env = self._get_escalated_env(env)
        tmp_env['REQUEST_METHOD'] = 'GET'
        tmp_env['PATH_INFO'] = '/%s/%s/%s/%s%s' % (self.version, self.account,
            self.container, self._get_status_int(), self._error)
        resp = self.app(tmp_env, self._start_response)
        if self._get_status_int() // 100 == 2:
            start_response(save_response_status, self._response_headers,
                           self._response_exc_info)
            return resp
        start_response(save_response_status, save_response_headers,
                       save_response_exc_info)
        return response

    def _get_status_int(self):
        """
        Returns the HTTP status int from the last called self._start_response
        result.
        """
        return int(self._response_status.split(' ', 1)[0])

    def _get_escalated_env(self, env):
        """
        Returns a new fresh WSGI environment with escalated privileges to do
        backend checks, listings, etc. that the remote user wouldn't be able to
        accomplish directly.
        """
        new_env = {'REQUEST_METHOD': 'GET',
            'HTTP_USER_AGENT': '%s StaticWeb' % env.get('HTTP_USER_AGENT')}
        for name in ('eventlet.posthooks', 'swift.trans_id', 'REMOTE_USER',
                     'SCRIPT_NAME', 'SERVER_NAME', 'SERVER_PORT',
                     'SERVER_PROTOCOL', 'swift.cache'):
            if name in env:
                new_env[name] = env[name]
        return new_env

    def _get_container_info(self, env, start_response):
        """
        Retrieves x-container-meta-web-index, x-container-meta-web-error,
        x-container-meta-web-listings, and x-container-meta-web-listings-css
        from memcache or from the cluster and stores the result in memcache and
        in self._index, self._error, self._listings, and self._listings_css.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        """
        self._index = self._error = self._listings = self._listings_css = None
        memcache_client = cache_from_env(env)
        if memcache_client:
            memcache_key = '/staticweb/%s/%s/%s' % (self.version, self.account,
                                                    self.container)
            cached_data = memcache_client.get(memcache_key)
            if cached_data:
                (self._index, self._error, self._listings,
                 self._listings_css) = cached_data
                return
        tmp_env = self._get_escalated_env(env)
        tmp_env['REQUEST_METHOD'] = 'HEAD'
        req = Request.blank('/%s/%s/%s' % (self.version, self.account,
            self.container), environ=tmp_env)
        resp = req.get_response(self.app)
        if resp.status_int // 100 == 2:
            self._index = \
                resp.headers.get('x-container-meta-web-index', '').strip()
            self._error = \
                resp.headers.get('x-container-meta-web-error', '').strip()
            self._listings = \
                resp.headers.get('x-container-meta-web-listings', '').strip()
            self._listings_css = \
                resp.headers.get('x-container-meta-web-listings-css',
                                 '').strip()
            if memcache_client:
                memcache_client.set(memcache_key,
                    (self._index, self._error, self._listings,
                     self._listings_css),
                    timeout=self.cache_timeout)

    def _listing(self, env, start_response, prefix=None):
        """
        Sends an HTML object listing to the remote client.

        :param env: The original WSGI environment dict.
        :param start_response: The original WSGI start_response hook.
        :param prefix: Any prefix desired for the container listing.
        """
        if self._listings.lower() not in TRUE_VALUES:
            resp = HTTPNotFound()(env, self._start_response)
            return self._error_response(resp, env, start_response)
        tmp_env = self._get_escalated_env(env)
        tmp_env['REQUEST_METHOD'] = 'GET'
        tmp_env['PATH_INFO'] = \
            '/%s/%s/%s' % (self.version, self.account, self.container)
        tmp_env['QUERY_STRING'] = 'delimiter=/&format=json'
        if prefix:
            tmp_env['QUERY_STRING'] += '&prefix=%s' % quote(prefix)
        else:
            prefix = ''
        resp = self.app(tmp_env, self._start_response)
        if self._get_status_int() // 100 != 2:
            return self._error_response(resp, env, start_response)
        listing = json.loads(''.join(resp))
        if not listing:
            resp = HTTPNotFound()(env, self._start_response)
            return self._error_response(resp, env, start_response)
        headers = {'Content-Type': 'text/html; charset=UTF-8'}
        body = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 ' \
                'Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">\n' \
               '<html>\n' \
               ' <head>\n' \
               '  <title>Listing of %s</title>\n' % \
               cgi.escape(env['PATH_INFO'])
        if self._listings_css:
            body += '  <link rel="stylesheet" type="text/css" ' \
                        'href="%s" />\n' % (self._build_css_path(prefix))
        else:
            body += '  <style type="text/css">\n' \
                    '   h1 {font-size: 1em; font-weight: bold;}\n' \
                    '   th {text-align: left; padding: 0px 1em 0px 1em;}\n' \
                    '   td {padding: 0px 1em 0px 1em;}\n' \
                    '   a {text-decoration: none;}\n' \
                    '  </style>\n'
        body += ' </head>\n' \
                ' <body>\n' \
                '  <h1 id="title">Listing of %s</h1>\n' \
                '  <table id="listing">\n' \
                '   <tr id="heading">\n' \
                '    <th class="colname">Name</th>\n' \
                '    <th class="colsize">Size</th>\n' \
                '    <th class="coldate">Date</th>\n' \
                '   </tr>\n' % \
                cgi.escape(env['PATH_INFO'])
        if prefix:
            body += '   <tr id="parent" class="item">\n' \
                    '    <td class="colname"><a href="../">../</a></td>\n' \
                    '    <td class="colsize">&nbsp;</td>\n' \
                    '    <td class="coldate">&nbsp;</td>\n' \
                    '   </tr>\n'
        for item in listing:
            if 'subdir' in item:
                subdir = item['subdir']
                if prefix:
                    subdir = subdir[len(prefix):]
                body += '   <tr class="item subdir">\n' \
                        '    <td class="colname"><a href="%s">%s</a></td>\n' \
                        '    <td class="colsize">&nbsp;</td>\n' \
                        '    <td class="coldate">&nbsp;</td>\n' \
                        '   </tr>\n' % \
                        (quote(subdir), cgi.escape(subdir))
        for item in listing:
            if 'name' in item:
                name = item['name']
                if prefix:
                    name = name[len(prefix):]
                body += '   <tr class="item %s">\n' \
                        '    <td class="colname"><a href="%s">%s</a></td>\n' \
                        '    <td class="colsize">%s</td>\n' \
                        '    <td class="coldate">%s</td>\n' \
                        '   </tr>\n' % \
                        (' '.join('type-' + cgi.escape(t.lower(), quote=True)
                                  for t in item['content_type'].split('/')),
                         quote(name), cgi.escape(name),
                         human_readable(item['bytes']),
                         cgi.escape(item['last_modified']).split('.')[0].
                            replace('T', ' '))
        body += '  </table>\n' \
                ' </body>\n' \
                '</html>\n'
        resp = Response(headers=headers, body=body)
        self._log_response(env, resp.status_int)
        return resp(env, start_response)

    def _build_css_path(self, prefix=''):
        """
        Constructs a relative path from a given prefix within the container.
        URLs and paths starting with '/' are not modified.

        :param prefix: The prefix for the container listing.
        """
        if self._listings_css.startswith(('/', 'http://', 'https://')):
            css_path = quote(self._listings_css, ':/')
        else:
            css_path = '../' * prefix.count('/') + quote(self._listings_css)
        return css_path

    def _handle_container(self, env, start_response):
        """
        Handles a possible static web request for a container.

        :param env: The original WSGI environment dict.
        :param start_response: The original WSGI start_response hook.
        """
        self._get_container_info(env, start_response)
        if not self._listings and not self._index:
            if env.get('HTTP_X_WEB_MODE', 'f').lower() in TRUE_VALUES:
                return HTTPNotFound()(env, start_response)
            return self.app(env, start_response)
        if env['PATH_INFO'][-1] != '/':
            resp = HTTPMovedPermanently(
                location=(env['PATH_INFO'] + '/'))
            self._log_response(env, resp.status_int)
            return resp(env, start_response)
        if not self._index:
            return self._listing(env, start_response)
        tmp_env = dict(env)
        tmp_env['HTTP_USER_AGENT'] = \
            '%s StaticWeb' % env.get('HTTP_USER_AGENT')
        tmp_env['PATH_INFO'] += self._index
        resp = self.app(tmp_env, self._start_response)
        status_int = self._get_status_int()
        if status_int == 404:
            return self._listing(env, start_response)
        elif self._get_status_int() // 100 not in (2, 3):
            return self._error_response(resp, env, start_response)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp

    def _handle_object(self, env, start_response):
        """
        Handles a possible static web request for an object. This object could
        resolve into an index or listing request.

        :param env: The original WSGI environment dict.
        :param start_response: The original WSGI start_response hook.
        """
        tmp_env = dict(env)
        tmp_env['HTTP_USER_AGENT'] = \
            '%s StaticWeb' % env.get('HTTP_USER_AGENT')
        resp = self.app(tmp_env, self._start_response)
        status_int = self._get_status_int()
        if status_int // 100 in (2, 3):
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return resp
        if status_int != 404:
            return self._error_response(resp, env, start_response)
        self._get_container_info(env, start_response)
        if not self._listings and not self._index:
            return self.app(env, start_response)
        status_int = 404
        if self._index:
            tmp_env = dict(env)
            tmp_env['HTTP_USER_AGENT'] = \
                '%s StaticWeb' % env.get('HTTP_USER_AGENT')
            if tmp_env['PATH_INFO'][-1] != '/':
                tmp_env['PATH_INFO'] += '/'
            tmp_env['PATH_INFO'] += self._index
            resp = self.app(tmp_env, self._start_response)
            status_int = self._get_status_int()
            if status_int // 100 in (2, 3):
                if env['PATH_INFO'][-1] != '/':
                    resp = HTTPMovedPermanently(
                        location=env['PATH_INFO'] + '/')
                    self._log_response(env, resp.status_int)
                    return resp(env, start_response)
                start_response(self._response_status, self._response_headers,
                               self._response_exc_info)
                return resp
        if status_int == 404:
            if env['PATH_INFO'][-1] != '/':
                tmp_env = self._get_escalated_env(env)
                tmp_env['REQUEST_METHOD'] = 'GET'
                tmp_env['PATH_INFO'] = '/%s/%s/%s' % (self.version,
                    self.account, self.container)
                tmp_env['QUERY_STRING'] = 'limit=1&format=json&delimiter' \
                    '=/&limit=1&prefix=%s' % quote(self.obj + '/')
                resp = self.app(tmp_env, self._start_response)
                if self._get_status_int() // 100 != 2 or \
                        not json.loads(''.join(resp)):
                    resp = HTTPNotFound()(env, self._start_response)
                    return self._error_response(resp, env, start_response)
                resp = HTTPMovedPermanently(location=env['PATH_INFO'] +
                    '/')
                self._log_response(env, resp.status_int)
                return resp(env, start_response)
            return self._listing(env, start_response, self.obj)

    def __call__(self, env, start_response):
        """
        Main hook into the WSGI paste.deploy filter/app pipeline.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        """
        env['staticweb.start_time'] = time.time()
        try:
            (self.version, self.account, self.container, self.obj) = \
                split_path(env['PATH_INFO'], 2, 4, True)
        except ValueError:
            return self.app(env, start_response)
        memcache_client = cache_from_env(env)
        if memcache_client:
            if env['REQUEST_METHOD'] in ('PUT', 'POST'):
                if not self.obj and self.container:
                    memcache_key = '/staticweb/%s/%s/%s' % \
                        (self.version, self.account, self.container)
                    memcache_client.delete(memcache_key)
                return self.app(env, start_response)
        if (env['REQUEST_METHOD'] not in ('HEAD', 'GET') or
            (env.get('REMOTE_USER') and
             env.get('HTTP_X_WEB_MODE', 'f').lower() not in TRUE_VALUES) or
            (not env.get('REMOTE_USER') and
             env.get('HTTP_X_WEB_MODE', 't').lower() not in TRUE_VALUES)):
            return self.app(env, start_response)
        if self.obj:
            return self._handle_object(env, start_response)
        elif self.container:
            return self._handle_container(env, start_response)
        return self.app(env, start_response)

    def _log_response(self, env, status_int):
        """
        Logs an access line for StaticWeb responses; use when the next app in
        the pipeline will not be handling the final response to the remote
        user.

        Assumes that the request and response bodies are 0 bytes or very near 0
        so no bytes transferred are tracked or logged.

        This does mean that the listings responses that actually do transfer
        content will not be logged with any bytes transferred, but in counter
        to that the full bytes for the underlying listing will be logged by the
        proxy even if the remote client disconnects early for the StaticWeb
        listing.

        I didn't think the extra complexity of getting the bytes transferred
        exactly correct for these requests was worth it, but perhaps someone
        else will think it is.

        To get things exact, this filter would need to use an
        eventlet.posthooks logger like the proxy does and any log processing
        systems would need to ignore some (but not all) proxy requests made by
        StaticWeb if they were just interested in the bytes transferred to the
        remote client.
        """
        trans_time = '%.4f' % (time.time() -
                               env.get('staticweb.start_time', time.time()))
        the_request = quote(unquote(env['PATH_INFO']))
        if env.get('QUERY_STRING'):
            the_request = the_request + '?' + env['QUERY_STRING']
        # remote user for zeus
        client = env.get('HTTP_X_CLUSTER_CLIENT_IP')
        if not client and 'HTTP_X_FORWARDED_FOR' in env:
            # remote user for other lbs
            client = env['HTTP_X_FORWARDED_FOR'].split(',')[0].strip()
        logged_headers = None
        if self.log_headers:
            logged_headers = '\n'.join('%s: %s' % (k, v)
                for k, v in req.headers.items())
        self.access_logger.info(' '.join(quote(str(x)) for x in (
            client or '-',
            env.get('REMOTE_ADDR', '-'),
            time.strftime('%d/%b/%Y/%H/%M/%S', time.gmtime()),
            env['REQUEST_METHOD'],
            the_request,
            env['SERVER_PROTOCOL'],
            status_int,
            env.get('HTTP_REFERER', '-'),
            env.get('HTTP_USER_AGENT', '-'),
            env.get('HTTP_X_AUTH_TOKEN', '-'),
            '-',
            '-',
            env.get('HTTP_ETAG', '-'),
            env.get('swift.trans_id', '-'),
            logged_headers or '-',
            trans_time)))


def filter_factory(global_conf, **local_conf):
    """ Returns a Static Web WSGI filter for use with paste.deploy. """
    conf = global_conf.copy()
    conf.update(local_conf)

    def staticweb_filter(app):
        return StaticWeb(app, conf)
    return staticweb_filter
