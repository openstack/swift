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
Monkey Patch httplib.HTTPResponse to buffer reads of headers. This can improve
performance when making large numbers of small HTTP requests.  This module
also provides helper functions to make HTTP connections using
BufferedHTTPResponse.

.. warning::

    If you use this, be sure that the libraries you are using do not access
    the socket directly (xmlrpclib, I'm looking at you :/), and instead
    make all calls through httplib.
"""

from swift.common import constraints
import logging
import time
import socket

import eventlet
from eventlet.green.httplib import CONTINUE, HTTPConnection, HTTPMessage, \
    HTTPResponse, HTTPSConnection, _UNKNOWN
from six.moves.urllib.parse import quote
import six

if six.PY2:
    httplib = eventlet.import_patched('httplib')
else:
    httplib = eventlet.import_patched('http.client')
httplib._MAXHEADERS = constraints.MAX_HEADER_COUNT


class BufferedHTTPResponse(HTTPResponse):
    """HTTPResponse class that buffers reading of headers"""

    def __init__(self, sock, debuglevel=0, strict=0,
                 method=None):          # pragma: no cover
        self.sock = sock
        # sock is an eventlet.greenio.GreenSocket
        if six.PY2:
            # sock.fd is a socket._socketobject
            # sock.fd._sock is a _socket.socket object, which is what we want.
            self._real_socket = sock.fd._sock
        else:
            # sock.fd is a socket.socket, which should have a _real_close
            self._real_socket = sock.fd
        self.fp = sock.makefile('rb')
        self.debuglevel = debuglevel
        self.strict = strict
        self._method = method

        self.headers = self.msg = None

        # from the Status-Line of the response
        self.version = _UNKNOWN         # HTTP-Version
        self.status = _UNKNOWN          # Status-Code
        self.reason = _UNKNOWN          # Reason-Phrase

        self.chunked = _UNKNOWN         # is "chunked" being used?
        self.chunk_left = _UNKNOWN      # bytes left to read in current chunk
        self.length = _UNKNOWN          # number of bytes left in response
        self.will_close = _UNKNOWN      # conn will close at end of response
        self._readline_buffer = b''

    def expect_response(self):
        if self.fp:
            self.fp.close()
            self.fp = None
        self.fp = self.sock.makefile('rb', 0)
        version, status, reason = self._read_status()
        if status != CONTINUE:
            self._read_status = lambda: (version, status, reason)
            self.begin()
        else:
            self.status = status
            self.reason = reason.strip()
            self.version = 11
            if six.PY2:
                # Under py2, HTTPMessage.__init__ reads the headers
                # which advances fp
                self.msg = HTTPMessage(self.fp, 0)
                # immediately kill msg.fp to make sure it isn't read again
                self.msg.fp = None
            else:
                # py3 has a separate helper for it
                self.headers = self.msg = httplib.parse_headers(self.fp)

    def read(self, amt=None):
        if not self._readline_buffer:
            return HTTPResponse.read(self, amt)

        if amt is None:
            # Unbounded read: send anything we have buffered plus whatever
            # is left.
            buffered = self._readline_buffer
            self._readline_buffer = b''
            return buffered + HTTPResponse.read(self, amt)
        elif amt <= len(self._readline_buffer):
            # Bounded read that we can satisfy entirely from our buffer
            res = self._readline_buffer[:amt]
            self._readline_buffer = self._readline_buffer[amt:]
            return res
        else:
            # Bounded read that wants more bytes than we have
            smaller_amt = amt - len(self._readline_buffer)
            buf = self._readline_buffer
            self._readline_buffer = b''
            return buf + HTTPResponse.read(self, smaller_amt)

    def readline(self, size=1024):
        # You'd think Python's httplib would provide this, but it doesn't.
        # It does, however, provide a comment in the HTTPResponse class:
        #
        #  # XXX It would be nice to have readline and __iter__ for this,
        #  # too.
        #
        # Yes, it certainly would.
        while (b'\n' not in self._readline_buffer
               and len(self._readline_buffer) < size):
            read_size = size - len(self._readline_buffer)
            chunk = HTTPResponse.read(self, read_size)
            if not chunk:
                break
            self._readline_buffer += chunk

        line, newline, rest = self._readline_buffer.partition(b'\n')
        self._readline_buffer = rest
        return line + newline

    def nuke_from_orbit(self):
        """
        Terminate the socket with extreme prejudice.

        Closes the underlying socket regardless of whether or not anyone else
        has references to it. Use this when you are certain that nobody else
        you care about has a reference to this socket.
        """
        if self._real_socket:
            if six.PY2:
                # this is idempotent; see sock_close in Modules/socketmodule.c
                # in the Python source for details.
                self._real_socket.close()
            else:
                # Hopefully this is equivalent?
                # TODO: verify that this does everything ^^^^ does for py2
                self._real_socket._real_close()
        self._real_socket = None
        self.close()

    def close(self):
        HTTPResponse.close(self)
        self.sock = None
        self._real_socket = None


class BufferedHTTPConnection(HTTPConnection):
    """HTTPConnection class that uses BufferedHTTPResponse"""
    response_class = BufferedHTTPResponse

    def connect(self):
        self._connected_time = time.time()
        ret = HTTPConnection.connect(self)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return ret

    def putrequest(self, method, url, skip_host=0, skip_accept_encoding=0):
        '''Send a request to the server.

        :param method: specifies an HTTP request method, e.g. 'GET'.
        :param url: specifies the object being requested, e.g. '/index.html'.
        :param skip_host: if True does not add automatically a 'Host:' header
        :param skip_accept_encoding: if True does not add automatically an
           'Accept-Encoding:' header
        '''
        self._method = method
        self._path = url
        return HTTPConnection.putrequest(self, method, url, skip_host,
                                         skip_accept_encoding)

    def getexpect(self):
        kwargs = {'method': self._method}
        if hasattr(self, 'strict'):
            kwargs['strict'] = self.strict
        response = BufferedHTTPResponse(self.sock, **kwargs)
        response.expect_response()
        return response

    def getresponse(self):
        response = HTTPConnection.getresponse(self)
        logging.debug("HTTP PERF: %(time).5f seconds to %(method)s "
                      "%(host)s:%(port)s %(path)s)",
                      {'time': time.time() - self._connected_time,
                       'method': self._method, 'host': self.host,
                       'port': self.port, 'path': self._path})
        return response


def http_connect(ipaddr, port, device, partition, method, path,
                 headers=None, query_string=None, ssl=False):
    """
    Helper function to create an HTTPConnection object. If ssl is set True,
    HTTPSConnection will be used. However, if ssl=False, BufferedHTTPConnection
    will be used, which is buffered for backend Swift services.

    :param ipaddr: IPv4 address to connect to
    :param port: port to connect to
    :param device: device of the node to query
    :param partition: partition on the device
    :param method: HTTP method to request ('GET', 'PUT', 'POST', etc.)
    :param path: request path
    :param headers: dictionary of headers
    :param query_string: request query string
    :param ssl: set True if SSL should be used (default: False)
    :returns: HTTPConnection object
    """
    if isinstance(path, six.text_type):
        path = path.encode("utf-8")
    if isinstance(device, six.text_type):
        device = device.encode("utf-8")
    if isinstance(partition, six.text_type):
        partition = partition.encode('utf-8')
    elif isinstance(partition, six.integer_types):
        partition = str(partition).encode('ascii')
    path = quote(b'/' + device + b'/' + partition + path)
    return http_connect_raw(
        ipaddr, port, method, path, headers, query_string, ssl)


def http_connect_raw(ipaddr, port, method, path, headers=None,
                     query_string=None, ssl=False):
    """
    Helper function to create an HTTPConnection object. If ssl is set True,
    HTTPSConnection will be used. However, if ssl=False, BufferedHTTPConnection
    will be used, which is buffered for backend Swift services.

    :param ipaddr: IPv4 address to connect to
    :param port: port to connect to
    :param method: HTTP method to request ('GET', 'PUT', 'POST', etc.)
    :param path: request path
    :param headers: dictionary of headers
    :param query_string: request query string
    :param ssl: set True if SSL should be used (default: False)
    :returns: HTTPConnection object
    """
    if not port:
        port = 443 if ssl else 80
    if ssl:
        conn = HTTPSConnection('%s:%s' % (ipaddr, port))
    else:
        conn = BufferedHTTPConnection('%s:%s' % (ipaddr, port))
    if query_string:
        path += '?' + query_string
    conn.path = path
    conn.putrequest(method, path, skip_host=(headers and 'Host' in headers))
    if headers:
        for header, value in headers.items():
            conn.putheader(header, str(value))
    conn.endheaders()
    return conn
