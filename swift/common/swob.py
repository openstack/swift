# Copyright (c) 2010-2012 OpenStack Foundation
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

Why Swob and not just use WebOb?
By Michael Barton

We used webob for years. The main problem was that the interface
wasn't stable. For a while, each of our several test suites required
a slightly different version of webob to run, and none of them worked
with the then-current version. It was a huge headache, so we just
scrapped it.

This is kind of a ton of code, but it's also been a huge relief to
not have to scramble to add a bunch of code branches all over the
place to keep Swift working every time webob decides some interface
needs to change.
"""

from collections import defaultdict
from collections.abc import MutableMapping
import time
from functools import partial
from datetime import datetime
from email.utils import parsedate
import re
import random
import functools
from io import BytesIO

from io import StringIO
import urllib

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import UTC, reiterate, split_path, Timestamp, pairs, \
    close_if_possible, closing_if_possible, config_true_value, friendly_close
from swift.common.exceptions import InvalidTimestamp


RESPONSE_REASONS = {
    100: ('Continue', ''),
    200: ('OK', ''),
    201: ('Created', ''),
    202: ('Accepted', 'The request is accepted for processing.'),
    204: ('No Content', ''),
    206: ('Partial Content', ''),
    301: ('Moved Permanently', 'The resource has moved permanently.'),
    302: ('Found', 'The resource has moved temporarily.'),
    303: ('See Other', 'The response to the request can be found under a '
          'different URI.'),
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
    529: ('Too Many Backend Requests', 'The server is incapable of performing '
          'the requested operation due to too many requests. Slow down.')
}

MAX_RANGE_OVERLAPS = 2
MAX_NONASCENDING_RANGES = 8
MAX_RANGES = 50


class WsgiBytesIO(BytesIO):
    """
    This class adds support for the additional wsgi.input methods defined on
    eventlet.wsgi.Input to the BytesIO class which would otherwise be a fine
    stand-in for the file-like object in the WSGI environment.
    """

    def set_hundred_continue_response_headers(self, headers):
        pass

    def send_hundred_continue_response(self):
        pass


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
                return datetime(*(parts + (UTC,)))
            except Exception:
                return None

    def setter(self, value):
        if isinstance(value, (float, int)):
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

    :param header: name of the header, e.g. "Transfer-Encoding"
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


def header_to_environ_key(header_name):
    # Why the to/from wsgi dance? Headers that include something like b'\xff'
    # on the wire get translated to u'\u00ff' on py3, which gets upper()ed to
    # u'\u0178', which is nonsense in a WSGI string.
    # Note that we have to only get as far as bytes because something like
    # b'\xc3\x9f' on the wire would be u'\u00df' as a native string on py3,
    # which would upper() to 'SS'.
    real_header = wsgi_to_bytes(header_name)
    header_name = 'HTTP_' + bytes_to_wsgi(
        real_header.upper()).replace('-', '_')
    if header_name == 'HTTP_CONTENT_LENGTH':
        return 'CONTENT_LENGTH'
    if header_name == 'HTTP_CONTENT_TYPE':
        return 'CONTENT_TYPE'
    return header_name


class HeaderEnvironProxy(MutableMapping):
    """
    A dict-like object that proxies requests to a wsgi environ,
    rewriting header keys to environ keys.

    For example, headers['Content-Range'] sets and gets the value of
    headers.environ['HTTP_CONTENT_RANGE']
    """
    def __init__(self, environ):
        self.environ = environ

    def __iter__(self):
        for k in self.keys():
            yield k

    def __len__(self):
        return len(self.keys())

    def __getitem__(self, key):
        return self.environ[header_to_environ_key(key)]

    def __setitem__(self, key, value):
        if value is None:
            self.environ.pop(header_to_environ_key(key), None)
        elif isinstance(value, bytes):
            self.environ[header_to_environ_key(key)] = value.decode('latin1')
        else:
            self.environ[header_to_environ_key(key)] = str(value)

    def __contains__(self, key):
        return header_to_environ_key(key) in self.environ

    def __delitem__(self, key):
        del self.environ[header_to_environ_key(key)]

    def keys(self):
        # See the to/from WSGI comment in header_to_environ_key
        keys = [
            bytes_to_wsgi(wsgi_to_bytes(key[5:]).replace(b'_', b'-').title())
            for key in self.environ if key.startswith('HTTP_')]
        if 'CONTENT_LENGTH' in self.environ:
            keys.append('Content-Length')
        if 'CONTENT_TYPE' in self.environ:
            keys.append('Content-Type')
        return keys


def wsgi_to_bytes(wsgi_str):
    if wsgi_str is None:
        return None
    return wsgi_str.encode('latin1')


def wsgi_to_str(wsgi_str):
    if wsgi_str is None:
        return None
    return wsgi_to_bytes(wsgi_str).decode('utf8', errors='surrogateescape')


def bytes_to_wsgi(byte_str):
    return byte_str.decode('latin1')


def str_to_wsgi(native_str):
    return bytes_to_wsgi(native_str.encode('utf8', errors='surrogateescape'))


def wsgi_quote(wsgi_str, safe='/'):
    if not isinstance(wsgi_str, str) or any(ord(x) > 255 for x in wsgi_str):
        raise TypeError('Expected a WSGI string; got %r' % wsgi_str)
    return urllib.parse.quote(wsgi_str, safe=safe, encoding='latin-1')


def wsgi_unquote(wsgi_str):
    if not isinstance(wsgi_str, str) or any(ord(x) > 255 for x in wsgi_str):
        raise TypeError('Expected a WSGI string; got %r' % wsgi_str)
    return urllib.parse.unquote(wsgi_str, encoding='latin-1')


def wsgi_quote_plus(wsgi_str):
    if not isinstance(wsgi_str, str) or any(ord(x) > 255 for x in wsgi_str):
        raise TypeError('Expected a WSGI string; got %r' % wsgi_str)
    return urllib.parse.quote_plus(wsgi_str, encoding='latin-1')


def wsgi_unquote_plus(wsgi_str):
    if not isinstance(wsgi_str, str) or any(ord(x) > 255 for x in wsgi_str):
        raise TypeError('Expected a WSGI string; got %r' % wsgi_str)
    return urllib.parse.unquote_plus(wsgi_str, encoding='latin-1')


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
        if isinstance(value, int):
            self.status_int = value
            self.explanation = self.title = RESPONSE_REASONS[value][0]
        else:
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
                return b''
            with closing_if_possible(self._app_iter):
                self._body = b''.join(self._app_iter)
            self._app_iter = None
        return self._body

    def setter(self, value):
        if isinstance(value, str):
            raise TypeError('WSGI responses must be bytes')
        if isinstance(value, bytes):
            self.content_length = len(value)
            close_if_possible(self._app_iter)
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
            for i, item in enumerate(value):
                if not isinstance(item, bytes):
                    raise TypeError('WSGI responses must be bytes; '
                                    'got %s for item %d' % (type(item), i))
            self.content_length = sum(map(len, value))
        elif value is not None:
            self.content_length = None
            self._body = None
        close_if_possible(self._app_iter)
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

    If there were any syntactically-invalid byte-range-spec values, the
    constructor will raise a ValueError, per the relevant RFC:

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
        if not headerval:
            raise ValueError('Invalid Range header: %r' % headerval)
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
                # We could just rely on int() raising the ValueError, but
                # this catches things like '--0'
                if not end.isdigit():
                    raise ValueError('Invalid Range header: %s' % headerval)
                end = int(end)
                if end < 0:
                    raise ValueError('Invalid Range header: %s' % headerval)
                elif start is not None and end < start:
                    raise ValueError('Invalid Range header: %s' % headerval)
            else:
                end = None
                if start is None:
                    raise ValueError('Invalid Range header: %s' % headerval)
            self.ranges.append((start, end))

    def __str__(self):
        string = 'bytes='
        for i, (start, end) in enumerate(self.ranges):
            if start is not None:
                string += str(start)
            string += '-'
            if end is not None:
                string += str(end)
            if i < len(self.ranges) - 1:
                string += ','
        return string

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

        # RFC 7233 section 6.1 ("Denial-of-Service Attacks Using Range") says:
        #
        # Unconstrained multiple range requests are susceptible to denial-of-
        # service attacks because the effort required to request many
        # overlapping ranges of the same data is tiny compared to the time,
        # memory, and bandwidth consumed by attempting to serve the requested
        # data in many parts.  Servers ought to ignore, coalesce, or reject
        # egregious range requests, such as requests for more than two
        # overlapping ranges or for many small ranges in a single set,
        # particularly when the ranges are requested out of order for no
        # apparent reason.  Multipart range requests are not designed to
        # support random access.
        #
        # We're defining "egregious" here as:
        #
        # * more than 50 requested ranges OR
        # * more than 2 overlapping ranges OR
        # * more than 8 non-ascending-order ranges
        if len(all_ranges) > MAX_RANGES:
            return []

        overlaps = 0
        for ((start1, end1), (start2, end2)) in pairs(all_ranges):
            if ((start1 < start2 < end1) or (start1 < end2 < end1) or
               (start2 < start1 < end2) or (start2 < end1 < end2)):
                overlaps += 1
                if overlaps > MAX_RANGE_OVERLAPS:
                    return []

        ascending = True
        for start1, start2 in zip(all_ranges, all_ranges[1:]):
            if start1 > start2:
                ascending = False
                break
        if not ascending and len(all_ranges) >= MAX_NONASCENDING_RANGES:
            return []

        return all_ranges


def normalize_etag(tag):
    if tag and tag.startswith('"') and tag.endswith('"') and tag != '"':
        return tag[1:-1]
    return tag


class Match(object):
    """
    Wraps a Request's If-[None-]Match header as a friendly object.

    :param headerval: value of the header as a str
    """
    def __init__(self, headerval):
        self.tags = set()
        for tag in headerval.split(','):
            tag = tag.strip()
            if not tag:
                continue
            self.tags.add(normalize_etag(tag))

    def __contains__(self, val):
        return '*' in self.tags or normalize_etag(val) in self.tags

    def __repr__(self):
        return '%s(%r)' % (
            self.__class__.__name__, ', '.join(sorted(self.tags)))


class Accept(object):
    """
    Wraps a Request's Accept header as a friendly object.

    :param headerval: value of the header as a str
    """

    # RFC 2616 section 2.2
    token = r'[^()<>@,;:\"/\[\]?={}\x00-\x20\x7f]+'  # nosec B105
    qdtext = r'[^"]'
    quoted_pair = r'(?:\\.)'
    quoted_string = r'"(?:' + qdtext + r'|' + quoted_pair + r')*"'
    extension = (r'(?:\s*;\s*(?:' + token + r")\s*=\s*" + r'(?:' + token +
                 r'|' + quoted_string + r'))')
    acc = (r'^\s*(' + token + r')/(' + token +
           r')(' + extension + r'*?\s*)$')
    acc_pattern = re.compile(acc)

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
            typ, subtype, parms = type_parms[0]
            parms = [p.strip() for p in parms.split(';') if p.strip()]

            seen_q_already = False
            quality = 1.0

            for parm in parms:
                name, value = parm.split('=')
                name = name.strip()
                value = value.strip()
                if name == 'q':
                    if seen_q_already:
                        raise ValueError('Multiple "q" params')
                    seen_q_already = True
                    quality = float(value)

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
        :raises ValueError: if the header is malformed
        """
        types = self._get_types()
        if not types and options:
            return options[0]
        for pattern in types:
            for option in options:
                if re.match(pattern, option):
                    return option
        return None

    def __repr__(self):
        return self.headerval


def _req_environ_property(environ_field, is_wsgi_string_field=True):
    """
    Set and retrieve value of the environ_field entry in self.environ.
    (Used by Request)
    """
    def getter(self):
        return self.environ.get(environ_field, None)

    def setter(self, value):
        if is_wsgi_string_field:
            # Check that input is valid before setting
            if isinstance(value, str):
                value.encode('latin1').decode('utf-8')
            if isinstance(value, bytes):
                value = value.decode('latin1')
        self.environ[environ_field] = value

    return property(getter, setter, doc=("Get and set the %s property "
                    "in the WSGI environment") % environ_field)


def _req_body_property():
    """
    Set and retrieve the Request.body parameter.  It consumes wsgi.input and
    returns the results.  On assignment, uses a WsgiBytesIO to create a new
    wsgi.input.
    """
    def getter(self):
        body = self.environ['wsgi.input'].read()
        self.environ['wsgi.input'] = WsgiBytesIO(body)
        return body

    def setter(self, value):
        if not isinstance(value, bytes):
            value = value.encode('utf8')
        self.environ['wsgi.input'] = WsgiBytesIO(value)
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


def is_chunked(headers):
    te = None
    for key in headers:
        if key.lower() == 'transfer-encoding':
            te = headers.get(key)
    if te:
        encodings = te.split(',')
        if len(encodings) > 1:
            raise AttributeError('Unsupported Transfer-Coding header'
                                 ' value specified in Transfer-Encoding'
                                 ' header')
        # If there are more than one transfer encoding value, the last
        # one must be chunked, see RFC 2616 Sec. 3.6
        if encodings[-1].lower() == 'chunked':
            return True
        else:
            raise ValueError('Invalid Transfer-Encoding header value')
    else:
        return False


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
    if_match = _req_fancy_property(Match, 'if-match')
    body_file = _req_environ_property('wsgi.input',
                                      is_wsgi_string_field=False)
    content_length = _header_int_property('content-length')
    if_modified_since = _datetime_property('if-modified-since')
    if_unmodified_since = _datetime_property('if-unmodified-since')
    body = _req_body_property()
    charset = None
    _params_cache = None
    _timestamp = None
    acl = _req_environ_property('swob.ACL', is_wsgi_string_field=False)

    def __init__(self, environ):
        self.environ = environ
        self.headers = HeaderEnvironProxy(self.environ)

    @classmethod
    def blank(cls, path, environ=None, headers=None, body=None, **kwargs):
        """
        Create a new request object with the given parameters, and an
        environment otherwise filled in with non-surprising default values.

        :param path: encoded, parsed, and unquoted into PATH_INFO
        :param environ: WSGI environ dictionary
        :param headers: HTTP headers
        :param body: stuffed in a WsgiBytesIO and hung on wsgi.input
        :param kwargs: any environ key with an property setter
        """
        headers = headers or {}
        environ = environ or {}
        if isinstance(path, bytes):
            path = path.decode('latin1')
        else:
            # Check that the input is valid
            path.encode('latin1')

        parsed_path = urllib.parse.urlparse(path)
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
            'PATH_INFO': wsgi_unquote(parsed_path.path),
            'SERVER_NAME': server_name,
            'SERVER_PORT': str(server_port),
            'HTTP_HOST': '%s:%d' % (server_name, server_port),
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': parsed_path.scheme or 'http',
            'wsgi.errors': StringIO(),
            'wsgi.multithread': False,
            'wsgi.multiprocess': False
        }
        env.update(environ)
        if body is not None:
            if not isinstance(body, bytes):
                body = body.encode('utf8')
            env['wsgi.input'] = WsgiBytesIO(body)
            env['CONTENT_LENGTH'] = str(len(body))
        elif 'wsgi.input' not in env:
            env['wsgi.input'] = WsgiBytesIO()
        req = Request(env)
        for key, val in headers.items():
            req.headers[key] = val
        for key, val in kwargs.items():
            prop = getattr(Request, key, None)
            if prop and isinstance(prop, property):
                try:
                    setattr(req, key, val)
                except AttributeError:
                    pass
                else:
                    continue
            raise TypeError("got unexpected keyword argument %r" % key)
        return req

    @property
    def params(self):
        "Provides QUERY_STRING parameters as a dictionary"
        if self._params_cache is None:
            if 'QUERY_STRING' in self.environ:
                self._params_cache = dict(urllib.parse.parse_qsl(
                    self.environ['QUERY_STRING'],
                    keep_blank_values=True, encoding='latin-1'))
            else:
                self._params_cache = {}
        return self._params_cache
    str_params = params

    @params.setter
    def params(self, param_pairs):
        self._params_cache = None
        self.query_string = urllib.parse.urlencode(param_pairs,
                                                   encoding='latin-1')

    def ensure_x_timestamp(self):
        """
        Similar to :attr:`timestamp`, but the ``X-Timestamp`` header will be
        set if not present.

        :raises HTTPBadRequest: if X-Timestamp is already set but not a valid
                                :class:`~swift.common.utils.Timestamp`
        :returns: the request's X-Timestamp header,
                  as a :class:`~swift.common.utils.Timestamp`
        """
        # The container sync feature includes an x-timestamp header with
        # requests. If present this is checked and preserved, otherwise a fresh
        # timestamp is added.
        if 'HTTP_X_TIMESTAMP' in self.environ:
            try:
                self._timestamp = Timestamp(self.environ['HTTP_X_TIMESTAMP'])
            except ValueError:
                raise HTTPBadRequest(
                    request=self, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % self.environ['HTTP_X_TIMESTAMP'])
        else:
            self._timestamp = Timestamp.now()
        # Always normalize it to the internal form
        self.environ['HTTP_X_TIMESTAMP'] = self._timestamp.internal
        return self._timestamp

    @property
    def timestamp(self):
        """
        Provides HTTP_X_TIMESTAMP as a :class:`~swift.common.utils.Timestamp`
        """
        if self._timestamp is None:
            try:
                raw_timestamp = self.environ['HTTP_X_TIMESTAMP']
            except KeyError:
                raise InvalidTimestamp('Missing X-Timestamp header')
            try:
                self._timestamp = Timestamp(raw_timestamp)
            except ValueError:
                raise InvalidTimestamp('Invalid X-Timestamp header')
        return self._timestamp

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
        return wsgi_quote(self.environ.get('SCRIPT_NAME', '') +
                          self.environ['PATH_INFO'])

    @property
    def swift_entity_path(self):
        """
        Provides the (native string) account/container/object path,
        sans API version.

        This can be useful when constructing a path to send to a backend
        server, as that path will need everything after the "/v1".
        """
        _ver, entity_path = self.split_path(1, 2, rest_with_last=True)
        if entity_path is not None:
            return '/' + wsgi_to_str(entity_path)

    @property
    def is_chunked(self):
        return is_chunked(self.headers)

    @property
    def url(self):
        "Provides the full url of the request"
        return self.host_url + self.path_qs

    @property
    def allow_reserved_names(self):
        return config_true_value(self.environ.get(
            'HTTP_X_BACKEND_ALLOW_RESERVED_NAMES'))

    def as_referer(self):
        return self.method + ' ' + self.url

    def path_info_pop(self):
        """
        Takes one path portion (delineated by slashes) from the
        path_info, and appends it to the script_name.  Returns
        the path segment.
        """
        path_info = self.path_info
        if not path_info or not path_info.startswith('/'):
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
            'wsgi.input': WsgiBytesIO(),
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
        if not captured:
            raise RuntimeError('application never called start_response')
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

        :param minsegs: Minimum number of segments to be extracted
        :param maxsegs: Maximum number of segments to be extracted
        :param rest_with_last: If True, trailing data will be returned as part
                               of last segment.  If False, and there is
                               trailing data, raises ValueError.
        :returns: list of segments with a length of maxsegs (non-existent
                  segments will return as None)
        :raises ValueError: if given an invalid path
        """
        return split_path(
            self.environ.get('SCRIPT_NAME', '') + self.environ['PATH_INFO'],
            minsegs, maxsegs, rest_with_last)

    def message_length(self):
        """
        Properly determine the message length for this request. It will return
        an integer if the headers explicitly contain the message length, or
        None if the headers don't contain a length. The ValueError exception
        will be raised if the headers are invalid.

        :raises ValueError: if either transfer-encoding or content-length
            headers have bad values
        :raises AttributeError: if the last value of the transfer-encoding
            header is not "chunked"
        """
        if not is_chunked(self.headers):
            # Because we are not using chunked transfer encoding we can pay
            # attention to the content-length header.
            fsize = self.headers.get('content-length', None)
            if fsize is not None:
                try:
                    fsize = int(fsize)
                except ValueError:
                    raise ValueError('Invalid Content-Length header value')
        else:
            fsize = None
        return fsize


def content_range_header_value(start, stop, size):
    return 'bytes %s-%s/%s' % (start, (stop - 1), size)


def content_range_header(start, stop, size):
    value = content_range_header_value(start, stop, size)
    return b"Content-Range: " + value.encode('ascii')


def multi_range_iterator(ranges, content_type, boundary, size, sub_iter_gen):
    for start, stop in ranges:
        yield b''.join([b'--', boundary, b'\r\n',
                       b'Content-Type: ', content_type, b'\r\n'])
        yield content_range_header(start, stop, size) + b'\r\n\r\n'
        sub_iter = sub_iter_gen(start, stop)
        for chunk in sub_iter:
            yield chunk
        yield b'\r\n'
    yield b'--' + boundary + b'--'


class Response(object):
    """
    WSGI Response object.
    """
    content_length = _header_int_property('content-length')
    content_type = _resp_content_type_property()
    content_range = _header_property('content-range')
    etag = _resp_etag_property()
    status = _resp_status_property()
    status_int = None
    body = _resp_body_property()
    host_url = _host_url_property()
    last_modified = _datetime_property('last-modified')
    location = _header_property('location')
    accept_ranges = _header_property('accept-ranges')
    charset = _resp_charset_property()
    app_iter = _resp_app_iter_property()

    def __init__(self, body=None, status=200, headers=None, app_iter=None,
                 request=None, conditional_response=False,
                 conditional_etag=None, **kw):
        self.headers = HeaderKeyDict(
            [('Content-Type', 'text/html; charset=UTF-8')])
        self.conditional_response = conditional_response
        self._conditional_etag = conditional_etag
        self.request = request
        self._app_iter = None
        # Allow error messages to come as natural strings on py3.
        if isinstance(body, str):
            body = body.encode('utf8')
        self.body = body
        self.app_iter = app_iter
        self.response_iter = None
        self.status = status
        self.boundary = b"%.32x" % random.randint(0, 256 ** 16)
        if request:
            self.environ = request.environ
        else:
            self.environ = {}
        if headers:
            if self._body and 'Content-Length' in headers:
                # If body is not empty, prioritize actual body length over
                # content_length in headers
                del headers['Content-Length']
            self.headers.update(headers)
        if self.status_int == 401 and 'www-authenticate' not in self.headers:
            self.headers.update({'www-authenticate': self.www_authenticate()})
        for key, value in kw.items():
            setattr(self, key, value)
        # When specifying both 'content_type' and 'charset' in the kwargs,
        # charset needs to be applied *after* content_type, otherwise charset
        # can get wiped out when content_type sorts later in dict order.
        if 'charset' in kw and 'content_type' in kw:
            self.charset = kw['charset']

    @property
    def conditional_etag(self):
        """
        The conditional_etag keyword argument for Response will allow the
        conditional match value of a If-Match request to be compared to a
        non-standard value.

        This is available for Storage Policies that do not store the client
        object data verbatim on the storage nodes, but still need support
        conditional requests.

        It's most effectively used with X-Backend-Etag-Is-At which would
        define the additional Metadata key(s) where the original ETag of the
        clear-form client request data may be found.
        """
        if self._conditional_etag is not None:
            return self._conditional_etag
        else:
            return self.etag

    def _prepare_for_ranges(self, ranges):
        """
        Prepare the Response for multiple ranges.
        """

        content_size = self.content_length
        content_type = self.headers['content-type'].encode('utf8')
        self.content_type = b''.join([b'multipart/byteranges;',
                                      b'boundary=', self.boundary])

        # This section calculates the total size of the response.
        section_header_fixed_len = sum([
            # --boundary\r\n
            2, len(self.boundary), 2,
            # Content-Type: <type>\r\n
            len('Content-Type: '), len(content_type), 2,
            # Content-Range: <value>\r\n; <value> accounted for later
            len('Content-Range: '), 2,
            # \r\n at end of headers
            2])

        body_size = 0
        for start, end in ranges:
            body_size += section_header_fixed_len

            # length of the value of Content-Range, not including the \r\n
            # since that's already accounted for
            cr = content_range_header_value(start, end, content_size)
            body_size += len(cr)

            # the actual bytes (note: this range is half-open, i.e. begins
            # with byte <start> and ends with byte <end - 1>, so there's no
            # fencepost error here)
            body_size += (end - start)

            # \r\n prior to --boundary
            body_size += 2

        # --boundary-- terminates the message
        body_size += len(self.boundary) + 4

        self.content_length = body_size
        self.content_range = None
        return content_size, content_type

    def _get_conditional_response_status(self):
        """Checks for a conditional response from an If-Match
        or If-Modified. request. If so, returns the correct status code
        (304 or 412).
        :returns: conditional response status (304 or 412) or None
        """
        if self.conditional_etag and self.request.if_none_match and \
                self.conditional_etag in self.request.if_none_match:
            return 304

        if self.conditional_etag and self.request.if_match and \
                self.conditional_etag not in self.request.if_match:
            return 412

        if self.status_int == 404 and self.request.if_match \
                and '*' in self.request.if_match:
            # If none of the entity tags match, or if "*" is given and no
            # current entity exists, the server MUST NOT perform the
            # requested method, and MUST return a 412 (Precondition
            # Failed) response. [RFC 2616 section 14.24]
            return 412

        if self.last_modified and self.request.if_modified_since \
                and self.last_modified <= self.request.if_modified_since:
            return 304

        if self.last_modified and self.request.if_unmodified_since \
                and self.last_modified > self.request.if_unmodified_since:
            return 412

        return None

    def _response_iter(self, app_iter, body):
        if self.conditional_response and self.request:
            empty_resp = self._get_conditional_response_status()
            if empty_resp is not None:
                self.status = empty_resp
                self.content_length = 0
                # the existing successful response and it's app_iter have been
                # determined to not meet the conditions of the reqeust, the
                # response app_iter should be closed but not drained.
                close_if_possible(app_iter)
                return [b'']

        if self.request and self.request.method == 'HEAD':
            # We explicitly do NOT want to set self.content_length to 0 here
            friendly_close(app_iter)  # be friendly to our app_iter
            return [b'']

        if self.conditional_response and self.request and \
                self.request.range and self.request.range.ranges and \
                not self.content_range:
            ranges = self.request.range.ranges_for_length(self.content_length)
            if ranges == []:
                self.status = 416
                close_if_possible(app_iter)
                self.headers['Content-Range'] = \
                    'bytes */%d' % self.content_length
                # Setting body + app_iter to None makes us emit the default
                # body text from RESPONSE_REASONS.
                body = None
                app_iter = None
            elif self.content_length == 0:
                # If ranges_for_length found ranges but our content length
                # is 0, then that means we got a suffix-byte-range request
                # (e.g. "bytes=-512"). This is asking for *up to* the last N
                # bytes of the file. If we had any bytes to send at all,
                # we'd return a 206 with an appropriate Content-Range header,
                # but we can't construct a Content-Range header because we
                # have no byte indices because we have no bytes.
                #
                # The only reasonable thing to do is to return a 200 with
                # the whole object (all zero bytes of it). This is also what
                # Apache and Nginx do, so if we're wrong, at least we're in
                # good company.
                pass
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
                body = '<html><h1>%s</h1><p>%s</p></html>' % (
                    title,
                    exp % defaultdict(lambda: 'unknown', self.__dict__))
                body = body.encode('utf8')
                self.content_length = len(body)
                return [body]
        return [b'']

    def fix_conditional_response(self):
        """
        You may call this once you have set the content_length to the whole
        object length and body or app_iter to reset the content_length
        properties on the request.

        It is ok to not call this method, the conditional response will be
        maintained for you when you __call__ the response.
        """
        self.response_iter = self._response_iter(self.app_iter, self._body)

    def absolute_location(self):
        """
        Attempt to construct an absolute location.
        """
        if not self.location.startswith('/'):
            return self.location
        return self.host_url + self.location

    def www_authenticate(self):
        """
        Construct a suitable value for WWW-Authenticate response header

        If we have a request and a valid-looking path, the realm
        is the account; otherwise we set it to 'unknown'.
        """
        try:
            vrs, realm, rest = self.request.split_path(2, 3, True)
            if realm in ('v1.0', 'auth'):
                realm = 'unknown'
        except (AttributeError, ValueError):
            realm = 'unknown'
        return 'Swift realm="%s"' % wsgi_quote(realm)

    @property
    def is_success(self):
        return self.status_int // 100 == 2

    def __call__(self, env, start_response):
        """
        Respond to the WSGI request.

        .. warning::

            This will translate any relative Location header value to an
            absolute URL using the WSGI environment's HOST_URL as a
            prefix, as RFC 2616 specifies.

            However, it is quite common to use relative redirects,
            especially when it is difficult to know the exact HOST_URL
            the browser would have used when behind several CNAMEs, CDN
            services, etc. All modern browsers support relative
            redirects.

            To skip over RFC enforcement of the Location header value,
            you may set ``env['swift.leave_relative_location'] = True``
            in the WSGI environment.
        """
        if not self.request:
            self.request = Request(env)
        self.environ = env

        if not self.response_iter:
            self.response_iter = self._response_iter(self.app_iter, self._body)

        if 'location' in self.headers and \
                not env.get('swift.leave_relative_location'):
            self.location = self.absolute_location()
        start_response(self.status, list(self.headers.items()))
        return self.response_iter


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
    @functools.wraps(func)
    def _wsgify(*args):
        env, start_response = args[-2:]
        new_args = args[:-2] + (Request(env), )
        try:
            return func(*new_args)(env, start_response)
        except HTTPException as err_resp:
            return err_resp(env, start_response)
    return _wsgify


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
HTTPPartialContent = status_map[206]
HTTPMovedPermanently = status_map[301]
HTTPFound = status_map[302]
HTTPSeeOther = status_map[303]
HTTPNotModified = status_map[304]
HTTPTemporaryRedirect = status_map[307]
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
HTTPNotImplemented = status_map[501]
HTTPBadGateway = status_map[502]
HTTPServiceUnavailable = status_map[503]
HTTPInsufficientStorage = status_map[507]
HTTPTooManyBackendRequests = status_map[529]
