# Copyright (c) 2010-2016 OpenStack Foundation
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
is normally only active for anonymous requests. When using keystone for
authentication set ``delay_auth_decision = true`` in the authtoken middleware
configuration in your ``/etc/swift/proxy-server.conf`` file.  If you want to
use it with authenticated requests, set the ``X-Web-Mode: true`` header on the
request.

The ``staticweb`` filter should be added to the pipeline in your
``/etc/swift/proxy-server.conf`` file just after any auth middleware. Also, the
configuration section for the ``staticweb`` middleware itself needs to be
added. For example::

    [DEFAULT]
    ...

    [pipeline:main]
    pipeline = catch_errors healthcheck proxy-logging cache ratelimit tempauth
               staticweb proxy-logging proxy-server

    ...

    [filter:staticweb]
    use = egg:swift#staticweb

Any publicly readable containers (for example, ``X-Container-Read: .r:*``, see
:ref:`acls` for more information on this) will be checked for
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

For pseudo paths that have no <index.name>, this middleware can serve HTML file
listings if you set the ``X-Container-Meta-Web-Listings: true`` metadata item
on the container. Note that the listing must be authorized; you may want a
container ACL like ``X-Container-Read: .r:*,.rlistings``.

If listings are enabled, the listings can have a custom style sheet by setting
the X-Container-Meta-Web-Listings-CSS header. For instance, setting
``X-Container-Meta-Web-Listings-CSS: listing.css`` will make listings link to
the .../listing.css style sheet. If you "view source" in your browser on a
listing page, you will see the well defined document structure that can be
styled.

Additionally, prefix-based :ref:`tempurl` parameters may be used to authorize
requests instead of making the whole container publicly readable. This gives
clients dynamic discoverability of the objects available within that prefix.

.. note::

    ``temp_url_prefix`` values should typically end with a slash (``/``) when
    used with StaticWeb. StaticWeb's redirects will not carry over any TempURL
    parameters, as they likely indicate that the user created an overly-broad
    TempURL.

By default, the listings will be rendered with a label of
"Listing of /v1/account/container/path".  This can be altered by
setting a ``X-Container-Meta-Web-Listings-Label: <label>``.  For example,
if the label is set to "example.com", a label of
"Listing of example.com/path" will be used instead.

The content-type of directory marker objects can be modified by setting
the ``X-Container-Meta-Web-Directory-Type`` header.  If the header is not set,
application/directory is used by default.  Directory marker objects are
0-byte objects that represent directories to create a simulated hierarchical
structure.

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

        swift post -r '.r:*,.rlistings' container
        swift post -m 'web-listings: true' container

    Now you should see object listings for paths and pseudo paths that have no
    index.html.

    Enable a custom listings style sheet::

        swift post -m 'web-listings-css:listings.css' container

    Set an error file::

        swift post -m 'web-error:error.html' container

    Now 401's should load 401error.html, 404's should load 404error.html, etc.

    Set Content-Type of directory marker object::

        swift post -m 'web-directory-type:text/directory' container

    Now 0-byte objects with a content-type of text/directory will be treated
    as directories rather than objects.
"""


import html
import json
import time

from urllib.parse import urlparse

from swift.common.utils import human_readable, split_path, config_true_value, \
    quote, get_logger
from swift.common.registry import register_swift_info
from swift.common.wsgi import make_env, WSGIContext
from swift.common.http import is_success, is_redirection, HTTP_NOT_FOUND
from swift.common.swob import Response, HTTPMovedPermanently, HTTPNotFound, \
    Request, wsgi_quote, wsgi_to_str, str_to_wsgi
from swift.common.middleware.tempurl import get_temp_url_info
from swift.proxy.controllers.base import get_container_info


class _StaticWebContext(WSGIContext):
    """
    The Static Web WSGI middleware filter; serves container data as a
    static web site. See `staticweb`_ for an overview.

    This _StaticWebContext is used by StaticWeb with each request
    that might need to be handled to make keeping contextual
    information about the request a bit simpler than storing it in
    the WSGI env.

    :param staticweb: The staticweb middleware object in use.
    :param version: A WSGI string representation of the swift api version.
    :param account: A WSGI string representation of the account name.
    :param container: A WSGI string representation of the container name.
    :param obj: A WSGI string representation of the object name.
    """

    def __init__(self, staticweb, version, account, container, obj):
        WSGIContext.__init__(self, staticweb.app)
        self.version = version
        self.account = account
        self.container = container
        self.obj = obj
        self.app = staticweb.app
        self.url_scheme = staticweb.url_scheme
        self.url_host = staticweb.url_host
        self.agent = '%(orig)s StaticWeb'
        # Results from the last call to self._get_container_info.
        self._index = self._error = self._listings = self._listings_css = \
            self._dir_type = self._listings_label = None

    def _error_response(self, response, env, start_response):
        """
        Sends the error response to the remote client, possibly resolving a
        custom error response body based on x-container-meta-web-error.

        :param response: The error response we should default to sending.
        :param env: The original request WSGI environment.
        :param start_response: The WSGI start_response hook.
        """
        if not self._error:
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return response
        save_response_status = self._response_status
        save_response_headers = self._response_headers
        save_response_exc_info = self._response_exc_info
        resp = self._app_call(make_env(
            env, 'GET', '/%s/%s/%s/%s%s' % (
                self.version, self.account, self.container,
                self._get_status_int(), self._error),
            self.agent, swift_source='SW'))
        if is_success(self._get_status_int()):
            start_response(save_response_status, self._response_headers,
                           self._response_exc_info)
            return resp
        start_response(save_response_status, save_response_headers,
                       save_response_exc_info)
        return response

    def _get_container_info(self, env):
        """
        Retrieves x-container-meta-web-index, x-container-meta-web-error,
        x-container-meta-web-listings, x-container-meta-web-listings-css,
        and x-container-meta-web-directory-type from memcache or from the
        cluster and stores the result in memcache and in self._index,
        self._error, self._listings, self._listings_css and self._dir_type.

        :param env: The WSGI environment dict.
        :return: The container_info dict.
        """
        self._index = self._error = self._listings = self._listings_css = \
            self._dir_type = None
        container_info = get_container_info(
            env, self.app, swift_source='SW')
        if is_success(container_info['status']):
            meta = container_info.get('meta', {})
            self._index = meta.get('web-index', '').strip()
            self._error = meta.get('web-error', '').strip()
            self._listings = meta.get('web-listings', '').strip()
            self._listings_label = meta.get('web-listings-label', '').strip()
            self._listings_css = meta.get('web-listings-css', '').strip()
            self._dir_type = meta.get('web-directory-type', '').strip()
        return container_info

    def _listing(self, env, start_response, prefix=''):
        """
        Sends an HTML object listing to the remote client.

        :param env: The original WSGI environment dict.
        :param start_response: The original WSGI start_response hook.
        :param prefix: Any WSGI-str prefix desired for the container listing.
        """
        label = wsgi_to_str(env['PATH_INFO'])
        if self._listings_label:
            groups = wsgi_to_str(env['PATH_INFO']).split('/')
            label = '{0}/{1}'.format(self._listings_label,
                                     '/'.join(groups[4:]))

        if not config_true_value(self._listings):
            body = '<!DOCTYPE html>\n' \
                '<html>\n' \
                '<head>\n' \
                '<title>Listing of %s</title>\n' % html.escape(label)
            if self._listings_css:
                body += '  <link rel="stylesheet" type="text/css" ' \
                    'href="%s" />\n' % self._build_css_path(prefix or '')
            else:
                body += '  <style type="text/css">\n' \
                    '   h1 {font-size: 1em; font-weight: bold;}\n' \
                    '   p {font-size: 2}\n' \
                    '  </style>\n'
            body += '</head>\n<body>' \
                '  <h1>Web Listing Disabled</h1>' \
                '   <p>The owner of this web site has disabled web listing.' \
                '   <p>If you are the owner of this web site, you can enable' \
                '   web listing by setting X-Container-Meta-Web-Listings.</p>'
            if self._index:
                body += '<h1>Index File Not Found</h1>' \
                    ' <p>The owner of this web site has set ' \
                    ' <b>X-Container-Meta-Web-Index: %s</b>. ' \
                    ' However, this file is not found.</p>' % self._index
            body += ' </body>\n</html>\n'
            resp = HTTPNotFound(body=body)(env, self._start_response)
            return self._error_response(resp, env, start_response)
        tmp_env = make_env(
            env, 'GET', '/%s/%s/%s' % (
                self.version, self.account, self.container),
            self.agent, swift_source='SW')
        tmp_env['QUERY_STRING'] = 'delimiter=/'
        if prefix:
            tmp_env['QUERY_STRING'] += '&prefix=%s' % wsgi_quote(prefix)
        else:
            prefix = ''
        resp = self._app_call(tmp_env)
        if not is_success(self._get_status_int()):
            return self._error_response(resp, env, start_response)
        listing = None
        body = b''.join(resp)
        if body:
            listing = json.loads(body)
        if prefix and not listing:
            resp = HTTPNotFound()(env, self._start_response)
            return self._error_response(resp, env, start_response)

        tempurl_qs = tempurl_prefix = ''
        if env.get('REMOTE_USER') == '.wsgi.tempurl':
            sig, expires, tempurl_prefix, _filename, inline, ip_range = \
                get_temp_url_info(env)
            if tempurl_prefix is None:
                tempurl_prefix = ''
            else:
                parts = [
                    'temp_url_prefix=%s' % quote(tempurl_prefix),
                    'temp_url_expires=%s' % quote(str(expires)),
                    'temp_url_sig=%s' % sig,
                ]
                if ip_range:
                    parts.append('temp_url_ip_range=%s' % quote(ip_range))
                if inline:
                    parts.append('inline')
                tempurl_qs = '?' + '&amp;'.join(parts)

        headers = {'Content-Type': 'text/html; charset=UTF-8',
                   'X-Backend-Content-Generator': 'staticweb'}
        body = '<!DOCTYPE html>\n' \
               '<html>\n' \
               ' <head>\n' \
               '  <title>Listing of %s</title>\n' % \
               html.escape(label)
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
                '   </tr>\n' % html.escape(label)
        if len(prefix) > len(tempurl_prefix):
            body += '   <tr id="parent" class="item">\n' \
                    '    <td class="colname"><a href="../%s">../</a></td>\n' \
                    '    <td class="colsize">&nbsp;</td>\n' \
                    '    <td class="coldate">&nbsp;</td>\n' \
                    '   </tr>\n' % tempurl_qs
        for item in listing:
            if 'subdir' in item:
                subdir = item['subdir']
                if prefix:
                    subdir = subdir[len(wsgi_to_str(prefix)):]
                body += '   <tr class="item subdir">\n' \
                        '    <td class="colname"><a href="%s">%s</a></td>\n' \
                        '    <td class="colsize">&nbsp;</td>\n' \
                        '    <td class="coldate">&nbsp;</td>\n' \
                        '   </tr>\n' % \
                        (quote(subdir) + tempurl_qs, html.escape(subdir))
        for item in listing:
            if 'name' in item:
                name = item['name']
                if prefix:
                    name = name[len(wsgi_to_str(prefix)):]
                content_type = item['content_type']
                bytes = human_readable(item['bytes'])
                last_modified = (
                    html.escape(item['last_modified']).
                    split('.')[0].replace('T', ' '))
                body += '   <tr class="item %s">\n' \
                        '    <td class="colname"><a href="%s">%s</a></td>\n' \
                        '    <td class="colsize">%s</td>\n' \
                        '    <td class="coldate">%s</td>\n' \
                        '   </tr>\n' % \
                        (' '.join('type-' + html.escape(t.lower())
                                  for t in content_type.split('/')),
                         quote(name) + tempurl_qs, html.escape(name),
                         bytes, last_modified)
        body += '  </table>\n' \
                ' </body>\n' \
                '</html>\n'
        resp = Response(headers=headers, body=body)
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

    def _redirect_with_slash(self, env_, start_response):
        env = {}
        env.update(env_)
        if self.url_scheme:
            env['wsgi.url_scheme'] = self.url_scheme
        if self.url_host:
            env['HTTP_HOST'] = self.url_host
        resp = HTTPMovedPermanently(
            location=wsgi_quote(env['PATH_INFO'] + '/'))
        return resp(env, start_response)

    def handle_container(self, env, start_response):
        """
        Handles a possible static web request for a container.

        :param env: The original WSGI environment dict.
        :param start_response: The original WSGI start_response hook.
        """
        container_info = self._get_container_info(env)
        req = Request(env)
        req.acl = container_info['read_acl']
        # we checked earlier that swift.authorize is set in env
        aresp = env['swift.authorize'](req)
        if aresp:
            resp = aresp(env, self._start_response)
            return self._error_response(resp, env, start_response)

        if not self._listings and not self._index:
            if config_true_value(env.get('HTTP_X_WEB_MODE', 'f')):
                return HTTPNotFound()(env, start_response)
            return self.app(env, start_response)
        if not env['PATH_INFO'].endswith('/'):
            return self._redirect_with_slash(env, start_response)
        if not self._index:
            return self._listing(env, start_response)
        tmp_env = dict(env)
        tmp_env['HTTP_USER_AGENT'] = \
            '%s StaticWeb' % env.get('HTTP_USER_AGENT')
        tmp_env['swift.source'] = 'SW'
        tmp_env['PATH_INFO'] += str_to_wsgi(self._index)
        resp = self._app_call(tmp_env)
        status_int = self._get_status_int()
        if status_int == HTTP_NOT_FOUND:
            return self._listing(env, start_response)
        elif not is_success(self._get_status_int()) and \
                not is_redirection(self._get_status_int()):
            return self._error_response(resp, env, start_response)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp

    def handle_object(self, env, start_response):
        """
        Handles a possible static web request for an object. This object could
        resolve into an index or listing request.

        :param env: The original WSGI environment dict.
        :param start_response: The original WSGI start_response hook.
        """
        tmp_env = dict(env)
        tmp_env['HTTP_USER_AGENT'] = \
            '%s StaticWeb' % env.get('HTTP_USER_AGENT')
        tmp_env['swift.source'] = 'SW'
        resp = self._app_call(tmp_env)
        status_int = self._get_status_int()
        self._get_container_info(env)
        if is_success(status_int) or is_redirection(status_int):
            # Treat directory marker objects as not found
            if not self._dir_type:
                self._dir_type = 'application/directory'
            content_length = self._response_header_value('content-length')
            content_length = int(content_length) if content_length else 0
            if self._response_header_value('content-type') == self._dir_type \
                    and content_length <= 1:
                status_int = HTTP_NOT_FOUND
            else:
                start_response(self._response_status, self._response_headers,
                               self._response_exc_info)
                return resp
        if status_int != HTTP_NOT_FOUND:
            # Retaining the previous code's behavior of not using custom error
            # pages for non-404 errors.
            self._error = None
            return self._error_response(resp, env, start_response)
        if not self._listings and not self._index:
            start_response(self._response_status, self._response_headers,
                           self._response_exc_info)
            return resp
        status_int = HTTP_NOT_FOUND
        if self._index:
            tmp_env = dict(env)
            tmp_env['HTTP_USER_AGENT'] = \
                '%s StaticWeb' % env.get('HTTP_USER_AGENT')
            tmp_env['swift.source'] = 'SW'
            if not tmp_env['PATH_INFO'].endswith('/'):
                tmp_env['PATH_INFO'] += '/'
            tmp_env['PATH_INFO'] += str_to_wsgi(self._index)
            resp = self._app_call(tmp_env)
            status_int = self._get_status_int()
            if is_success(status_int) or is_redirection(status_int):
                if not env['PATH_INFO'].endswith('/'):
                    return self._redirect_with_slash(env, start_response)
                start_response(self._response_status, self._response_headers,
                               self._response_exc_info)
                return resp
        if status_int == HTTP_NOT_FOUND:
            if not env['PATH_INFO'].endswith('/'):
                tmp_env = make_env(
                    env, 'GET', '/%s/%s/%s' % (
                        self.version, self.account, self.container),
                    self.agent, swift_source='SW')
                tmp_env['QUERY_STRING'] = 'limit=1&delimiter=/&prefix=%s' % (
                    quote(wsgi_to_str(self.obj) + '/'), )
                resp = self._app_call(tmp_env)
                body = b''.join(resp)
                if not is_success(self._get_status_int()) or not body or \
                        not json.loads(body):
                    resp = HTTPNotFound()(env, self._start_response)
                    return self._error_response(resp, env, start_response)
                return self._redirect_with_slash(env, start_response)
            return self._listing(env, start_response, self.obj)


class StaticWeb(object):
    """
    The Static Web WSGI middleware filter; serves container data as a static
    web site. See `staticweb`_ for an overview.

    The proxy logs created for any subrequests made will have swift.source set
    to "SW".

    :param app: The next WSGI application/filter in the paste.deploy pipeline.
    :param conf: The filter configuration dict.
    """

    def __init__(self, app, conf):
        #: The next WSGI application/filter in the paste.deploy pipeline.
        self.app = app
        #: The filter configuration dict. Only used in tests.
        self.conf = conf
        self.logger = get_logger(conf, log_route='staticweb')

        # We expose a more general "url_base" parameter in case we want
        # to incorporate the path prefix later. Currently it is discarded.
        url_base = conf.get('url_base', None)
        self.url_scheme = None
        self.url_host = None
        if url_base:
            parsed = urlparse(url_base)
            self.url_scheme = parsed.scheme
            self.url_host = parsed.netloc

    def __call__(self, env, start_response):
        """
        Main hook into the WSGI paste.deploy filter/app pipeline.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        """
        env['staticweb.start_time'] = time.time()
        if 'swift.authorize' not in env:
            self.logger.warning(
                'No authentication middleware authorized request yet. '
                'Skipping staticweb')
            return self.app(env, start_response)
        try:
            (version, account, container, obj) = \
                split_path(env['PATH_INFO'], 2, 4, True)
        except ValueError:
            return self.app(env, start_response)
        if env['REQUEST_METHOD'] not in ('HEAD', 'GET'):
            return self.app(env, start_response)
        if env.get('REMOTE_USER') and env['REMOTE_USER'] != '.wsgi.tempurl' \
                and not config_true_value(env.get('HTTP_X_WEB_MODE', 'f')):
            return self.app(env, start_response)
        if not container:
            return self.app(env, start_response)
        context = _StaticWebContext(self, version, account, container, obj)
        if obj:
            return context.handle_object(env, start_response)
        return context.handle_container(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Returns a Static Web WSGI filter for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)
    register_swift_info('staticweb')

    def staticweb_filter(app):
        return StaticWeb(app, conf)
    return staticweb_filter
