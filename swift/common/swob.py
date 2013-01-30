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
Implementation of WSGI Request and Response objects.

This library has a very similar API to Webob.  It wraps WSGI request
environments and response values into objects that are more friendly to
interact with.
"""

from collections import defaultdict
from cStringIO import StringIO
import UserDict
import time
from functools import partial
from datetime import datetime, timedelta, tzinfo
from email.utils import parsedate
import urlparse
import urllib2
import re
import random
import functools
import inspect

from swift.common.utils import reiterate, split_path


RESPONSE_REASONS = {
    100: ('Continue', ''),
    200: ('OK', ''),
    201: ('Created', ''),
    202: ('Accepted', 'The request is accepted for processing.'),
    204: ('No Content', ''),
    206: ('Partial Content', ''),
    301: ('Moved Permanently', 'The resource has moved permanently.'),
    302: ('Found', ''),
    304: ('Not Modified', ''),
    307: ('Temporary Redirect', 'The resource has moved temporarily.'),
    400: ('Bad Request', 'The server could not comply with the request since '
          'it is either malformed or otherwise incorrect.'),
    401: ('Unauthorized', 'This server could not verify that you are '
          'authorized to access the document you requested.'),
    402: ('Payment Required', 'Access was denied for financial reasons.'),
    403: ('Forbidden', 'Access was denied to this resource.'),
    404: ('Not Found', 'The resource could not be found.'),
    405: ('Method Not Allowed', 'The method is not allowed for this '
          'resource.'),
    406: ('Not Acceptable', 'The resource is not available in a format '
          'acceptable to your browser.'),
    408: ('Request Timeout', 'The server has waited too long for the request '
          'to be sent by the client.'),
    409: ('Conflict', 'There was a conflict when trying to complete '
          'your request.'),
    410: ('Gone', 'This resource is no longer available.'),
    411: ('Length Required', 'Content-Length header required.'),
    412: ('Precondition Failed', 'A precondition for this request was not '
          'met.'),
    413: ('Request Entity Too Large', 'The body of your request was too '
          'large for this server.'),
    414: ('Request URI Too Long', 'The request URI was too long for this '
          'server.'),
    415: ('Unsupported Media Type', 'The request media type is not '
          'supported by this server.'),
    416: ('Requested Range Not Satisfiable', 'The Range requested is not '
          'available.'),
    417: ('Expectation Failed', 'Expectation failed.'),
    422: ('Unprocessable Entity', 'Unable to process the contained '
          'instructions'),
    499: ('Client Disconnect', 'The client was disconnected during request.'),
    500: ('Internal Error', 'The server has either erred or is incapable of '
          'performing the requested operation.'),
    501: ('Not Implemented', 'The requested method is not implemented by '
          'this server.'),
    502: ('Bad Gateway', 'Bad gateway.'),
    503: ('Service Unavailable', 'The server is currently unavailable. '
          'Please try again at a later time.'),
    504: ('Gateway Timeout', 'A timeout has occurred speaking to a '
          'backend server.'),
    507: ('Insufficient Storage', 'There was not enough space to save the '
          'resource. Drive: %(drive)s'),
}


class _UTC(tzinfo):
    """
    A tzinfo class for datetime objects that returns a 0 timedelta (UTC time)
    """
    def dst(self, dt):
        return timedelta(0)
    utcoffset = dst

    def tzname(self, dt):
        return 'UTC'
UTC = _UTC()


def _datetime_property(header):
    """
    Set and retrieve the datetime value of self.headers[header]
    (Used by both request and response)
    The header is parsed on retrieval and a datetime object is returned.
    The header can be set using a datetime, numeric value, or str.
    If a value of None is given, the header is deleted.

    :param header: name of the header, e.g. "Content-Length"
    """
    def getter(self):
        value = self.headers.get(header, None)
        if value is not None:
            try:
                parts = parsedate(self.headers[header])[:7]
                date = datetime(*(parts + (UTC,)))
            except Exception:
                return None
            if date.year < 1970:
                raise ValueError('Somehow an invalid year')
            return date

    def setter(self, value):
        if isinstance(value, (float, int, long)):
            self.headers[header] = time.strftime(
                "%a, %d %b %Y %H:%M:%S GMT", time.gmtime(value))
        elif isinstance(value, datetime):
            self.headers[header] = value.strftime("%a, %d %b %Y %H:%M:%S GMT")
        else:
            self.headers[header] = value

    return property(getter, setter,
                    doc=("Retrieve and set the %s header as a datetime, "
                         "set it with a datetime, int, or str") % header)


def _header_property(header):
    """
    Set and retrieve the value of self.headers[header]
    (Used by both request and response)
    If a value of None is given, the header is deleted.

    :param header: name of the header, e.g. "Content-Length"
    """
    def getter(self):
        return self.headers.get(header, None)

    def setter(self, value):
        self.headers[header] = value

    return property(getter, setter,
                    doc="Retrieve and set the %s header" % header)


def _header_int_property(header):
    """
    Set and retrieve the value of self.headers[header]
    (Used by both request and response)
    On retrieval, it converts values to integers.
    If a value of None is given, the header is deleted.

    :param header: name of the header, e.g. "Content-Length"
    """
    def getter(self):
        val = self.headers.get(header, None)
        if val is not None:
            val = int(val)
        return val

    def setter(self, value):
        self.headers[header] = value

    return property(getter, setter,
                    doc="Retrieve and set the %s header as an int" % header)


class HeaderEnvironProxy(UserDict.DictMixin):
    """
    A dict-like object that proxies requests to a wsgi environ,
    rewriting header keys to environ keys.

    For example, headers['Content-Range'] sets and gets the value of
    headers.environ['HTTP_CONTENT_RANGE']
    """
    def __init__(self, environ):
        self.environ = environ

    def _normalize(self, key):
        key = 'HTTP_' + key.replace('-', '_').upper()
        if key == 'HTTP_CONTENT_LENGTH':
            return 'CONTENT_LENGTH'
        if key == 'HTTP_CONTENT_TYPE':
            return 'CONTENT_TYPE'
        return key

    def __getitem__(self, key):
        return self.environ[self._normalize(key)]

    def __setitem__(self, key, value):
        if value is None:
            self.environ.pop(self._normalize(key), None)
        elif isinstance(value, unicode):
            self.environ[self._normalize(key)] = value.encode('utf-8')
        else:
            self.environ[self._normalize(key)] = str(value)

    def __contains__(self, key):
        return self._normalize(key) in self.environ

    def __delitem__(self, key):
        del self.environ[self._normalize(key)]

    def keys(self):
        keys = [key[5:].replace('_', '-').title()
                for key in self.environ.iterkeys() if key.startswith('HTTP_')]
        if 'CONTENT_LENGTH' in self.environ:
            keys.append('Content-Length')
        if 'CONTENT_TYPE' in self.environ:
            keys.append('Content-Type')
        return keys


class HeaderKeyDict(dict):
    """
    A dict that lower-cases all keys on the way in, so as to be
    case-insensitive.
    """
    def __init__(self, *args, **kwargs):
        for arg in args:
            self.update(arg)
        self.update(kwargs)

    def update(self, other):
        if hasattr(other, 'keys'):
            for key in other.keys():
                self[key.lower()] = other[key]
        else:
            for key, value in other:
                self[key.lower()] = value

    def __getitem__(self, key):
        return dict.get(self, key.lower())

    def __setitem__(self, key, value):
        if value is None:
            self.pop(key.lower(), None)
        elif isinstance(value, unicode):
            return dict.__setitem__(self, key.lower(), value.encode('utf-8'))
        else:
            return dict.__setitem__(self, key.lower(), str(value))

    def __contains__(self, key):
        return dict.__contains__(self, key.lower())

    def __delitem__(self, key):
        return dict.__delitem__(self, key.lower())

    def get(self, key, default=None):
        return dict.get(self, key.lower(), default)


def _resp_status_property():
    """
    Set and retrieve the value of Response.status
    On retrieval, it concatenates status_int and title.
    When set to a str, it splits status_int and title apart.
    When set to an integer, retrieves the correct title for that
    response code from the RESPONSE_REASONS dict.
    """
    def getter(self):
        return '%s %s' % (self.status_int, self.title)

    def setter(self, value):
        if isinstance(value, (int, long)):
            self.status_int = value
            self.explanation = self.title = RESPONSE_REASONS[value][0]
        else:
            if isinstance(value, unicode):
                value = value.encode('utf-8')
            self.status_int = int(value.split(' ', 1)[0])
            self.explanation = self.title = value.split(' ', 1)[1]

    return property(getter, setter,
                    doc="Retrieve and set the Response status, e.g. '200 OK'")


def _resp_body_property():
    """
    Set and retrieve the value of Response.body
    If necessary, it will consume Response.app_iter to create a body.
    On assignment, encodes unicode values to utf-8, and sets the content-length
    to the length of the str.
    """
    def getter(self):
        if not self._body:
            if not self._app_iter:
                return ''
            self._body = ''.join(self._app_iter)
            self._app_iter = None
        return self._body

    def setter(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(value, str):
            self.content_length = len(value)
            self._app_iter = None
        self._body = value

    return property(getter, setter,
                    doc="Retrieve and set the Response body str")


def _resp_etag_property():
    """
    Set and retrieve Response.etag
    This may be broken for etag use cases other than Swift's.
    Quotes strings when assigned and unquotes when read, for compatibility
    with webob.
    """
    def getter(self):
        etag = self.headers.get('etag', None)
        if etag:
            etag = etag.replace('"', '')
        return etag

    def setter(self, value):
        if value is None:
            self.headers['etag'] = None
        else:
            self.headers['etag'] = '"%s"' % value

    return property(getter, setter,
                    doc="Retrieve and set the response Etag header")


def _resp_content_type_property():
    """
    Set and retrieve Response.content_type
    Strips off any charset when retrieved -- that is accessible
    via Response.charset.
    """
    def getter(self):
        if 'content-type' in self.headers:
            return self.headers.get('content-type').split(';')[0]

    def setter(self, value):
        self.headers['content-type'] = value

    return property(getter, setter,
                    doc="Retrieve and set the response Content-Type header")


def _resp_charset_property():
    """
    Set and retrieve Response.charset
    On retrieval, separates the charset from the content-type.
    On assignment, removes any existing charset from the content-type and
    appends the new one.
    """
    def getter(self):
        if '; charset=' in self.headers['content-type']:
            return self.headers['content-type'].split('; charset=')[1]

    def setter(self, value):
        if 'content-type' in self.headers:
            self.headers['content-type'] = self.headers['content-type'].split(
                ';')[0]
            if value:
                self.headers['content-type'] += '; charset=' + value

    return property(getter, setter,
                    doc="Retrieve and set the response charset")


def _resp_app_iter_property():
    """
    Set and retrieve Response.app_iter
    Mostly a pass-through to Response._app_iter; it's a property so it can zero
    out an existing content-length on assignment.
    """
    def getter(self):
        return self._app_iter

    def setter(self, value):
        if isinstance(value, (list, tuple)):
            self.content_length = sum(map(len, value))
        elif value is not None:
            self.content_length = None
            self._body = None
        self._app_iter = value

    return property(getter, setter,
                    doc="Retrieve and set the response app_iter")


def _req_fancy_property(cls, header, even_if_nonexistent=False):
    """
    Set and retrieve "fancy" properties.
    On retrieval, these properties return a class that takes the value of the
    header as the only argument to their constructor.
    For assignment, those classes should implement a __str__ that converts them
    back to their header values.

    :param header: name of the header, e.g. "Accept"
    :param even_if_nonexistent: Return a value even if the header does not
        exist.  Classes using this should be prepared to accept None as a
        parameter.
    """
    def getter(self):
        try:
            if header in self.headers or even_if_nonexistent:
                return cls(self.headers.get(header))
        except ValueError:
            return None

    def setter(self, value):
        self.headers[header] = value

    return property(getter, setter, doc=("Retrieve and set the %s "
                    "property in the WSGI environ, as a %s object") %
                    (header, cls.__name__))


class Range(object):
    """
    Wraps a Request's Range header as a friendly object.
    After initialization, "range.ranges" is populated with a list
    of (start, end) tuples denoting the requested ranges.

    If there were any syntactically-invalid byte-range-spec values,
    "range.ranges" will be an empty list, per the relevant RFC:

    "The recipient of a byte-range-set that includes one or more syntactically
    invalid byte-range-spec values MUST ignore the header field that includes
    that byte-range-set."

    According to the RFC 2616 specification, the following cases will be all
    considered as syntactically invalid, thus, a ValueError is thrown so that
    the range header will be ignored. If the range value contains at least
    one of the following cases, the entire range is considered invalid,
    ValueError will be thrown so that the header will be ignored.

    1. value not starts with bytes=
    2. range value start is greater than the end, eg. bytes=5-3
    3. range does not have start or end, eg. bytes=-
    4. range does not have hyphen, eg. bytes=45
    5. range value is non numeric
    6. any combination of the above

    Every syntactically valid range will be added into the ranges list
    even when some of the ranges may not be satisfied by underlying content.

    :param headerval: value of the header as a str
    """
    def __init__(self, headerval):
        headerval = headerval.replace(' ', '')
        if not headerval.lower().startswith('bytes='):
            raise ValueError('Invalid Range header: %s' % headerval)
        self.ranges = []
        for rng in headerval[6:].split(','):
            # Check if the range has required hyphen.
            if rng.find('-') == -1:
                raise ValueError('Invalid Range header: %s' % headerval)
            start, end = rng.split('-', 1)
            if start:
                # when start contains non numeric value, this also causes
                # ValueError
                start = int(start)
            else:
                start = None
            if end:
                # when end contains non numeric value, this also causes
                # ValueError
                end = int(end)
                if start is not None and end < start:
                    raise ValueError('Invalid Range header: %s' % headerval)
            else:
                end = None
                if start is None:
                    raise ValueError('Invalid Range header: %s' % headerval)
            self.ranges.append((start, end))

    def __str__(self):
        string = 'bytes='
        for start, end in self.ranges:
            if start is not None:
                string += str(start)
            string += '-'
            if end is not None:
                string += str(end)
            string += ','
        return string.rstrip(',')

    def ranges_for_length(self, length):
        """
        This method is used to return multiple ranges for a given length
        which should represent the length of the underlying content.
        The constructor method __init__ made sure that any range in ranges
        list is syntactically valid. So if length is None or size of the
        ranges is zero, then the Range header should be ignored which will
        eventually make the response to be 200.

        If an empty list is returned by this method, it indicates that there
        are unsatisfiable ranges found in the Range header, 416 will be
        returned.

        if a returned list has at least one element, the list indicates that
        there is at least one range valid and the server should serve the
        request with a 206 status code.

        The start value of each range represents the starting position in
        the content, the end value represents the ending position. This
        method purposely adds 1 to the end number because the spec defines
        the Range to be inclusive.

        The Range spec can be found at the following link:
        http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.1

        :param length: length of the underlying content
        """
        # not syntactically valid ranges, must ignore
        if length is None or not self.ranges or self.ranges == []:
            return None
        all_ranges = []
        for single_range in self.ranges:
            begin, end = single_range
            # The possible values for begin and end are
            # None, 0, or a positive numeric number
            if begin is None:
                if end == 0:
                    # this is the bytes=-0 case
                    continue
                elif end > length:
                    # This is the case where the end is greater than the
                    # content length, as the RFC 2616 stated, the entire
                    # content should be returned.
                    all_ranges.append((0, length))
                else:
                    all_ranges.append((length - end, length))
                continue
            # begin can only be 0 and numeric value from this point on
            if end is None:
                if begin < length:
                    all_ranges.append((begin, length))
                else:
                    # the begin position is greater than or equal to the
                    # content length; skip and move on to the next range
                    continue
            # end can only be 0 or numeric value
            elif begin < length:
                # the begin position is valid, take the min of end + 1 or
                # the total length of the content
                all_ranges.append((begin, min(end + 1, length)))

        return all_ranges


class Match(object):
    """
    Wraps a Request's If-None-Match header as a friendly object.

    :param headerval: value of the header as a str
    """
    def __init__(self, headerval):
        self.tags = set()
        for tag in headerval.split(', '):
            if tag.startswith('"') and tag.endswith('"'):
                self.tags.add(tag[1:-1])
            else:
                self.tags.add(tag)

    def __contains__(self, val):
        return '*' in self.tags or val in self.tags


class Accept(object):
    """
    Wraps a Request's Accept header as a friendly object.

    :param headerval: value of the header as a str
    """
    token = r'[^()<>@,;:\"/\[\]?={}\x00-\x20\x7f]+'  # RFC 2616 2.2
    acc_pattern = re.compile(r'^\s*(' + token + r')/(' + token +
                             r')(;\s*q=([\d.]+))?\s*$')

    def __init__(self, headerval):
        self.headerval = headerval

    def _get_types(self):
        types = []
        if not self.headerval:
            return []
        for typ in self.headerval.split(','):
            type_parms = self.acc_pattern.findall(typ)
            if not type_parms:
                raise ValueError('Invalid accept header')
            typ, subtype, parms, quality = type_parms[0]
            quality = float(quality or '1.0')
            pattern = '^' + \
                (self.token if typ == '*' else re.escape(typ)) + '/' + \
                (self.token if subtype == '*' else re.escape(subtype)) + '$'
            types.append((pattern, quality, '*' not in (typ, subtype)))
        # sort candidates by quality, then whether or not there were globs
        types.sort(reverse=True, key=lambda t: (t[1], t[2]))
        return [t[0] for t in types]

    def best_match(self, options):
        """
        Returns the item from "options" that best matches the accept header.
        Returns None if no available options are acceptable to the client.

        :param options: a list of content-types the server can respond with
        """
        try:
            types = self._get_types()
        except ValueError:
            return None
        if not types and options:
            return options[0]
        for pattern in types:
            for option in options:
                if re.match(pattern, option):
                    return option
        return None

    def __repr__(self):
        return self.headerval


def _req_environ_property(environ_field):
    """
    Set and retrieve value of the environ_field entry in self.environ.
    (Used by both request and response)
    """
    def getter(self):
        return self.environ.get(environ_field, None)

    def setter(self, value):
        self.environ[environ_field] = value

    return property(getter, setter, doc=("Get and set the %s property "
                    "in the WSGI environment") % environ_field)


def _req_body_property():
    """
    Set and retrieve the Request.body parameter.  It consumes wsgi.input and
    returns the results.  On assignment, uses a StringIO to create a new
    wsgi.input.
    """
    def getter(self):
        body = self.environ['wsgi.input'].read()
        self.environ['wsgi.input'] = StringIO(body)
        return body

    def setter(self, value):
        self.environ['wsgi.input'] = StringIO(value)
        self.environ['CONTENT_LENGTH'] = str(len(value))

    return property(getter, setter, doc="Get and set the request body str")


def _host_url_property():
    """
    Retrieves the best guess that can be made for an absolute location up to
    the path, for example: https://host.com:1234
    """
    def getter(self):
        if 'HTTP_HOST' in self.environ:
            host = self.environ['HTTP_HOST']
        else:
            host = '%s:%s' % (self.environ['SERVER_NAME'],
                              self.environ['SERVER_PORT'])
        scheme = self.environ.get('wsgi.url_scheme', 'http')
        if scheme == 'http' and host.endswith(':80'):
            host, port = host.rsplit(':', 1)
        elif scheme == 'https' and host.endswith(':443'):
            host, port = host.rsplit(':', 1)
        return '%s://%s' % (scheme, host)

    return property(getter, doc="Get url for request/response up to path")


class Request(object):
    """
    WSGI Request object.
    """
    range = _req_fancy_property(Range, 'range')
    if_none_match = _req_fancy_property(Match, 'if-none-match')
    accept = _req_fancy_property(Accept, 'accept', True)
    method = _req_environ_property('REQUEST_METHOD')
    referrer = referer = _req_environ_property('HTTP_REFERER')
    script_name = _req_environ_property('SCRIPT_NAME')
    path_info = _req_environ_property('PATH_INFO')
    host = _req_environ_property('HTTP_HOST')
    host_url = _host_url_property()
    remote_addr = _req_environ_property('REMOTE_ADDR')
    remote_user = _req_environ_property('REMOTE_USER')
    user_agent = _req_environ_property('HTTP_USER_AGENT')
    query_string = _req_environ_property('QUERY_STRING')
    if_match = _req_environ_property('HTTP_IF_MATCH')
    body_file = _req_environ_property('wsgi.input')
    content_length = _header_int_property('content-length')
    if_modified_since = _datetime_property('if-modified-since')
    if_unmodified_since = _datetime_property('if-unmodified-since')
    body = _req_body_property()
    charset = None
    _params_cache = None
    acl = _req_environ_property('swob.ACL')

    def __init__(self, environ):
        self.environ = environ
        self.headers = HeaderEnvironProxy(self.environ)

    @classmethod
    def blank(cls, path, environ=None, headers=None, body=None):
        """
        Create a new request object with the given parameters, and an
        environment otherwise filled in with non-surprising default values.
        """
        headers = headers or {}
        environ = environ or {}
        parsed_path = urlparse.urlparse(path)
        server_name = 'localhost'
        if parsed_path.netloc:
            server_name = parsed_path.netloc.split(':', 1)[0]

        server_port = parsed_path.port
        if server_port is None:
            server_port = {'http': 80,
                           'https': 443}.get(parsed_path.scheme, 80)
        if parsed_path.scheme and parsed_path.scheme not in ['http', 'https']:
            raise TypeError('Invalid scheme: %s' % parsed_path.scheme)
        env = {
            'REQUEST_METHOD': 'GET',
            'SCRIPT_NAME': '',
            'QUERY_STRING': parsed_path.query,
            'PATH_INFO': urllib2.unquote(parsed_path.path),
            'SERVER_NAME': server_name,
            'SERVER_PORT': str(server_port),
            'HTTP_HOST': '%s:%d' % (server_name, server_port),
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': parsed_path.scheme or 'http',
            'wsgi.errors': StringIO(''),
            'wsgi.multithread': False,
            'wsgi.multiprocess': False
        }
        env.update(environ)
        if body is not None:
            env['wsgi.input'] = StringIO(body)
            env['CONTENT_LENGTH'] = str(len(body))
        elif 'wsgi.input' not in env:
            env['wsgi.input'] = StringIO('')
        req = Request(env)
        for key, val in headers.iteritems():
            req.headers[key] = val
        return req

    @property
    def params(self):
        "Provides QUERY_STRING parameters as a dictionary"
        if self._params_cache is None:
            if 'QUERY_STRING' in self.environ:
                self._params_cache = dict(
                    urlparse.parse_qsl(self.environ['QUERY_STRING'], True))
            else:
                self._params_cache = {}
        return self._params_cache
    str_params = params

    @property
    def path_qs(self):
        """The path of the request, without host but with query string."""
        path = self.path
        if self.query_string:
            path += '?' + self.query_string
        return path

    @property
    def path(self):
        "Provides the full path of the request, excluding the QUERY_STRING"
        return urllib2.quote(self.environ.get('SCRIPT_NAME', '') +
                             self.environ['PATH_INFO'])

    @property
    def url(self):
        "Provides the full url of the request"
        return self.host_url + self.path_qs

    def path_info_pop(self):
        """
        Takes one path portion (delineated by slashes) from the
        path_info, and appends it to the script_name.  Returns
        the path segment.
        """
        path_info = self.path_info
        if not path_info or path_info[0] != '/':
            return None
        try:
            slash_loc = path_info.index('/', 1)
        except ValueError:
            slash_loc = len(path_info)
        self.script_name += path_info[:slash_loc]
        self.path_info = path_info[slash_loc:]
        return path_info[1:slash_loc]

    def copy_get(self):
        """
        Makes a copy of the request, converting it to a GET.
        """
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'GET',
            'CONTENT_LENGTH': '0',
            'wsgi.input': StringIO(''),
        })
        return Request(env)

    def call_application(self, application):
        """
        Calls the application with this request's environment.  Returns the
        status, headers, and app_iter for the response as a tuple.

        :param application: the WSGI application to call
        """
        output = []
        captured = []

        def start_response(status, headers, exc_info=None):
            captured[:] = [status, headers, exc_info]
            return output.append
        app_iter = application(self.environ, start_response)
        if not app_iter:
            app_iter = output
        if not captured:
            app_iter = reiterate(app_iter)
        return (captured[0], captured[1], app_iter)

    def get_response(self, application):
        """
        Calls the application with this request's environment.  Returns a
        Response object that wraps up the application's result.

        :param application: the WSGI application to call
        """
        status, headers, app_iter = self.call_application(application)
        return Response(status=status, headers=dict(headers),
                        app_iter=app_iter, request=self)

    def split_path(self, minsegs=1, maxsegs=None, rest_with_last=False):
        """
        Validate and split the Request's path.

        **Examples**::

            ['a'] = split_path('/a')
            ['a', None] = split_path('/a', 1, 2)
            ['a', 'c'] = split_path('/a/c', 1, 2)
            ['a', 'c', 'o/r'] = split_path('/a/c/o/r', 1, 3, True)

        :param path: HTTP Request path to be split
        :param minsegs: Minimum number of segments to be extracted
        :param maxsegs: Maximum number of segments to be extracted
        :param rest_with_last: If True, trailing data will be returned as part
                               of last segment.  If False, and there is
                               trailing data, raises ValueError.
        :returns: list of segments with a length of maxsegs (non-existant
                  segments will return as None)
        :raises: ValueError if given an invalid path
        """
        return split_path(
            self.environ.get('SCRIPT_NAME', '') + self.environ['PATH_INFO'],
            minsegs, maxsegs, rest_with_last)


def content_range_header_value(start, stop, size):
    return 'bytes %s-%s/%s' % (start, (stop - 1), size)


def content_range_header(start, stop, size):
    return "Content-Range: " + content_range_header_value(start, stop, size)


def multi_range_iterator(ranges, content_type, boundary, size, sub_iter_gen):
    for start, stop in ranges:
        yield ''.join(['\r\n--', boundary, '\r\n',
                       'Content-Type: ', content_type, '\r\n'])
        yield content_range_header(start, stop, size) + '\r\n\r\n'
        sub_iter = sub_iter_gen(start, stop)
        for chunk in sub_iter:
            yield chunk
    yield '\r\n--' + boundary + '--\r\n'


class Response(object):
    """
    WSGI Response object.
    """
    content_length = _header_int_property('content-length')
    content_type = _resp_content_type_property()
    content_range = _header_property('content-range')
    etag = _resp_etag_property()
    status = _resp_status_property()
    body = _resp_body_property()
    host_url = _host_url_property()
    last_modified = _datetime_property('last-modified')
    location = _header_property('location')
    accept_ranges = _header_property('accept-ranges')
    charset = _resp_charset_property()
    app_iter = _resp_app_iter_property()

    def __init__(self, body=None, status=200, headers={}, app_iter=None,
                 request=None, conditional_response=False, **kw):
        self.headers = HeaderKeyDict(
            [('Content-Type', 'text/html; charset=UTF-8')])
        self.conditional_response = conditional_response
        self.request = request
        self.body = body
        self.app_iter = app_iter
        self.status = status
        self.boundary = "%.32x" % random.randint(0, 256 ** 16)
        if request:
            self.environ = request.environ
        else:
            self.environ = {}
        self.headers.update(headers)
        for key, value in kw.iteritems():
            setattr(self, key, value)

    def _prepare_for_ranges(self, ranges):
        """
        Prepare the Response for multiple ranges.
        """

        content_size = self.content_length
        content_type = self.content_type
        self.content_type = ''.join(['multipart/byteranges;',
                                     'boundary=', self.boundary])

        # This section calculate the total size of the targeted response
        # The value 12 is the length of total bytes of hyphen, new line
        # form feed for each section header. The value 8 is the length of
        # total bytes of hyphen, new line, form feed characters for the
        # closing boundary which appears only once
        section_header_fixed_len = 12 + (len(self.boundary) +
                                         len('Content-Type: ') +
                                         len(content_type) +
                                         len('Content-Range: bytes '))
        body_size = 0
        for start, end in ranges:
            body_size += section_header_fixed_len
            body_size += len(str(start) + '-' + str(end - 1) + '/' +
                             str(content_size)) + (end - start)
        body_size += 8 + len(self.boundary)
        self.content_length = body_size
        self.content_range = None
        return content_size, content_type

    def _response_iter(self, app_iter, body):
        if self.request and self.request.method == 'HEAD':
            # We explicitly do NOT want to set self.content_length to 0 here
            return ['']
        if self.conditional_response and self.request and \
                self.request.range and self.request.range.ranges and \
                not self.content_range:
            ranges = self.request.range.ranges_for_length(self.content_length)
            if ranges == []:
                self.status = 416
                self.content_length = 0
                return ['']
            elif ranges:
                range_size = len(ranges)
                if range_size > 0:
                    # There is at least one valid range in the request, so try
                    # to satisfy the request
                    if range_size == 1:
                        start, end = ranges[0]
                        if app_iter and hasattr(app_iter, 'app_iter_range'):
                            self.status = 206
                            self.content_range = content_range_header_value(
                                start, end, self.content_length)
                            self.content_length = (end - start)
                            return app_iter.app_iter_range(start, end)
                        elif body:
                            self.status = 206
                            self.content_range = content_range_header_value(
                                start, end, self.content_length)
                            self.content_length = (end - start)
                            return [body[start:end]]
                    elif range_size > 1:
                        if app_iter and hasattr(app_iter, 'app_iter_ranges'):
                            self.status = 206
                            content_size, content_type = \
                                self._prepare_for_ranges(ranges)
                            return app_iter.app_iter_ranges(ranges,
                                                            content_type,
                                                            self.boundary,
                                                            content_size)
                        elif body:
                            self.status = 206
                            content_size, content_type, = \
                                self._prepare_for_ranges(ranges)

                            def _body_slicer(start, stop):
                                yield body[start:stop]
                            return multi_range_iterator(ranges, content_type,
                                                        self.boundary,
                                                        content_size,
                                                        _body_slicer)
        if app_iter:
            return app_iter
        if body is not None:
            return [body]
        if self.status_int in RESPONSE_REASONS:
            title, exp = RESPONSE_REASONS[self.status_int]
            if exp:
                body = '<html><h1>%s</h1><p>%s</p></html>' % (title, exp)
                if '%(' in body:
                    body = body % defaultdict(lambda: 'unknown', self.__dict__)
                self.content_length = len(body)
                return [body]
        return ['']

    def absolute_location(self):
        """
        Attempt to construct an absolute location.
        """
        if not self.location.startswith('/'):
            return self.location
        return self.host_url + self.location

    def __call__(self, env, start_response):
        if not self.request:
            self.request = Request(env)
        self.environ = env
        app_iter = self._response_iter(self.app_iter, self._body)
        if 'location' in self.headers:
            self.location = self.absolute_location()
        start_response(self.status, self.headers.items())
        return app_iter


class HTTPException(Response, Exception):

    def __init__(self, *args, **kwargs):
        Response.__init__(self, *args, **kwargs)
        Exception.__init__(self, self.status)


def wsgify(func):
    """
    A decorator for translating functions which take a swob Request object and
    return a Response object into WSGI callables.  Also catches any raised
    HTTPExceptions and treats them as a returned Response.
    """
    argspec = inspect.getargspec(func)
    if argspec.args and argspec.args[0] == 'self':
        @functools.wraps(func)
        def _wsgify_self(self, env, start_response):
            try:
                return func(self, Request(env))(env, start_response)
            except HTTPException, err_resp:
                return err_resp(env, start_response)
        return _wsgify_self
    else:
        @functools.wraps(func)
        def _wsgify_bare(env, start_response):
            try:
                return func(Request(env))(env, start_response)
            except HTTPException, err_resp:
                return err_resp(env, start_response)
        return _wsgify_bare


class StatusMap(object):
    """
    A dict-like object that returns HTTPException subclasses/factory functions
    where the given key is the status code.
    """
    def __getitem__(self, key):
        return partial(HTTPException, status=key)
status_map = StatusMap()


HTTPOk = status_map[200]
HTTPCreated = status_map[201]
HTTPAccepted = status_map[202]
HTTPNoContent = status_map[204]
HTTPMovedPermanently = status_map[301]
HTTPFound = status_map[302]
HTTPNotModified = status_map[304]
HTTPBadRequest = status_map[400]
HTTPUnauthorized = status_map[401]
HTTPForbidden = status_map[403]
HTTPMethodNotAllowed = status_map[405]
HTTPNotFound = status_map[404]
HTTPNotAcceptable = status_map[406]
HTTPRequestTimeout = status_map[408]
HTTPConflict = status_map[409]
HTTPLengthRequired = status_map[411]
HTTPPreconditionFailed = status_map[412]
HTTPRequestEntityTooLarge = status_map[413]
HTTPRequestedRangeNotSatisfiable = status_map[416]
HTTPUnprocessableEntity = status_map[422]
HTTPClientDisconnect = status_map[499]
HTTPServerError = status_map[500]
HTTPInternalServerError = status_map[500]
HTTPBadGateway = status_map[502]
HTTPServiceUnavailable = status_map[503]
HTTPInsufficientStorage = status_map[507]
