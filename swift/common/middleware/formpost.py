# Copyright (c) 2011 OpenStack, LLC.
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
FormPost Middleware

Translates a browser form post into a regular Swift object PUT.

The format of the form is::

    <form action="<swift-url>" method="POST"
          enctype="multipart/form-data">
      <input type="hidden" name="redirect" value="<redirect-url>" />
      <input type="hidden" name="max_file_size" value="<bytes>" />
      <input type="hidden" name="max_file_count" value="<count>" />
      <input type="hidden" name="expires" value="<unix-timestamp>" />
      <input type="hidden" name="signature" value="<hmac>" />
      <input type="file" name="file1" /><br />
      <input type="submit" />
    </form>

The <swift-url> is the URL to the Swift desination, such as::

    https://swift-cluster.example.com/v1/AUTH_account/container/object_prefix

The name of each file uploaded will be appended to the <swift-url>
given. So, you can upload directly to the root of container with a
url like::

    https://swift-cluster.example.com/v1/AUTH_account/container/

Optionally, you can include an object prefix to better separate
different users' uploads, such as::

    https://swift-cluster.example.com/v1/AUTH_account/container/object_prefix

Note the form method must be POST and the enctype must be set as
"multipart/form-data".

The redirect attribute is the URL to redirect the browser to after
the upload completes. The URL will have status and message query
parameters added to it, indicating the HTTP status code for the
upload (2xx is success) and a possible message for further
information if there was an error (such as "max_file_size exceeded").

The max_file_size attribute must be included and indicates the
largest single file upload that can be done, in bytes.

The max_file_count attribute must be included and indicates the
maximum number of files that can be uploaded with the form. Include
additional ``<input type="file" name="filexx" />`` attributes if
desired.

The expires attribute is the Unix timestamp before which the form
must be submitted before it's invalidated.

The signature attribute is the HMAC-SHA1 signature of the form. Here is
sample code for computing the signature::

    import hmac
    from hashlib import sha1
    from time import time
    path = '/v1/account/container/object_prefix'
    redirect = 'https://myserver.com/some-page'
    max_file_size = 104857600
    max_file_count = 10
    expires = int(time() + 600)
    key = 'mykey'
    hmac_body = '%s\\n%s\\n%s\\n%s\\n%s' % (path, redirect,
        max_file_size, max_file_count, expires)
    signature = hmac.new(key, hmac_body, sha1).hexdigest()

The key is the value of the X-Account-Meta-Temp-URL-Key header on the
account.

Be certain to use the full path, from the /v1/ onward.

The command line tool ``swift-form-signature`` may be used (mostly
just when testing) to compute expires and signature.

Also note that the file attributes must be after the other attributes
in order to be processed correctly. If attributes come after the
file, they won't be sent with the subrequest (there is no way to
parse all the attributes on the server-side without reading the whole
thing into memory -- to service many requests, some with large files,
there just isn't enough memory on the server, so attributes following
the file are simply ignored).
"""

__all__ = ['FormPost', 'filter_factory', 'READ_CHUNK_SIZE', 'MAX_VALUE_LENGTH']

import hmac
import re
import rfc822
from hashlib import sha1
from StringIO import StringIO
from time import gmtime, strftime, time
from time import time
from urllib import quote, unquote

from swift.common.utils import get_logger, streq_const_time


#: The size of data to read from the form at any given time.
READ_CHUNK_SIZE = 4096

#: The maximum size of any attribute's value. Any additional data will be
#: truncated.
MAX_VALUE_LENGTH = 4096

#: Regular expression to match form attributes.
ATTRIBUTES_RE = re.compile(r'(\w+)=(".*?"|[^";]+)(; ?|$)')


class FormInvalid(Exception):
    pass


def _parse_attrs(header):
    """
    Given the value of a header like:
    Content-Disposition: form-data; name="somefile"; filename="test.html"

    Return data like
    ("form-data", {"name": "somefile", "filename": "test.html"})

    :param header: Value of a header (the part after the ': ').
    :returns: (value name, dict) of the attribute data parsed (see above).
    """
    attributes = {}
    attrs = ''
    if '; ' in header:
        header, attrs = header.split('; ', 1)
    m = True
    while m:
        m = ATTRIBUTES_RE.match(attrs)
        if m:
            attrs = attrs[len(m.group(0)):]
            attributes[m.group(1)] = m.group(2).strip('"')
    return header, attributes


class _IterRequestsFileLikeObject(object):

    def __init__(self, wsgi_input, boundary, input_buffer):
        self.no_more_data_for_this_file = False
        self.no_more_files = False
        self.wsgi_input = wsgi_input
        self.boundary = boundary
        self.input_buffer = input_buffer

    def read(self, length=None):
        if not length:
            length = READ_CHUNK_SIZE
        if self.no_more_data_for_this_file:
            return ''

        # read enough data to know whether we're going to run
        # into a boundary in next [length] bytes
        if len(self.input_buffer) < length + len(self.boundary) + 2:
            to_read = length + len(self.boundary) + 2
            while to_read > 0:
                chunk = self.wsgi_input.read(to_read)
                to_read -= len(chunk)
                self.input_buffer += chunk
                if not chunk:
                    self.no_more_files = True
                    break

        boundary_pos = self.input_buffer.find(self.boundary)

        # boundary does not exist in the next (length) bytes
        if boundary_pos == -1 or boundary_pos > length:
            ret = self.input_buffer[:length]
            self.input_buffer = self.input_buffer[length:]
        # if it does, just return data up to the boundary
        else:
            ret, self.input_buffer = self.input_buffer.split(self.boundary, 1)
            self.no_more_files = self.input_buffer.startswith('--')
            self.no_more_data_for_this_file = True
            self.input_buffer = self.input_buffer[2:]
        return ret

    def readline(self):
        if self.no_more_data_for_this_file:
            return ''
        boundary_pos = newline_pos = -1
        while newline_pos < 0 and boundary_pos < 0:
            chunk = self.wsgi_input.read(READ_CHUNK_SIZE)
            self.input_buffer += chunk
            newline_pos = self.input_buffer.find('\r\n')
            boundary_pos = self.input_buffer.find(self.boundary)
            if not chunk:
                self.no_more_files = True
                break
        # found a newline
        if newline_pos >= 0 and \
                (boundary_pos < 0 or newline_pos < boundary_pos):
            # Use self.read to ensure any logic there happens...
            ret = ''
            to_read = newline_pos + 2
            while to_read > 0:
                chunk = self.read(to_read)
                # Should never happen since we're reading from input_buffer,
                # but just for completeness...
                if not chunk:
                    break
                to_read -= len(chunk)
                ret += chunk
            return ret
        else:  # no newlines, just return up to next boundary
            return self.read(len(self.input_buffer))


def _iter_requests(wsgi_input, boundary):
    """
    Given a multi-part mime encoded input file object and boundary,
    yield file-like objects for each part.

    :param wsgi_input: The file-like object to read from.
    :param boundary: The mime boundary to separate new file-like
                     objects on.
    :returns: A generator of file-like objects for each part.
    """
    boundary = '--' + boundary
    if wsgi_input.readline().strip() != boundary:
        raise FormInvalid('invalid starting boundary')
    boundary = '\r\n' + boundary
    input_buffer = ''
    done = False
    while not done:
        it = _IterRequestsFileLikeObject(wsgi_input, boundary, input_buffer)
        yield it
        done = it.no_more_files
        input_buffer = it.input_buffer


class _CappedFileLikeObject(object):
    """
    A file-like object wrapping another file-like object that raises
    an EOFError if the amount of data read exceeds a given
    max_file_size.

    :param fp: The file-like object to wrap.
    :param max_file_size: The maximum bytes to read before raising an
                          EOFError.
    """

    def __init__(self, fp, max_file_size):
        self.fp = fp
        self.max_file_size = max_file_size
        self.amount_read = 0

    def read(self, size=None):
        ret = self.fp.read(size)
        self.amount_read += len(ret)
        if self.amount_read > self.max_file_size:
            raise EOFError('max_file_size exceeded')
        return ret

    def readline(self):
        ret = self.fp.readline()
        self.amount_read += len(ret)
        if self.amount_read > self.max_file_size:
            raise EOFError('max_file_size exceeded')
        return ret


class FormPost(object):
    """
    FormPost Middleware

    See above for a full description.

    :param app: The next WSGI filter or app in the paste.deploy
                chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf):
        #: The next WSGI application/filter in the paste.deploy pipeline.
        self.app = app
        #: The filter configuration dict.
        self.conf = conf
        #: The logger to use with this middleware.
        self.logger = get_logger(conf, log_route='formpost')

    def __call__(self, env, start_response):
        """
        Main hook into the WSGI paste.deploy filter/app pipeline.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        :returns: Response as per WSGI.
        """
        if env['REQUEST_METHOD'] == 'POST':
            try:
                content_type, attrs = \
                    _parse_attrs(env.get('CONTENT_TYPE') or '')
                if content_type == 'multipart/form-data' and \
                        'boundary' in attrs:
                    resp_status = [0]

                    def _start_response(status, headers, exc_info=None):
                        resp_status[0] = int(status.split(' ', 1)[0])
                        start_response(status, headers, exc_info)

                    self._log_request(env, resp_status)
                    return self._translate_form(env, start_response,
                                                attrs['boundary'])
            except (FormInvalid, EOFError), err:
                self._log_request(env, 400)
                body = 'FormPost: %s' % err
                start_response('400 Bad Request',
                    (('Content-Type', 'text/plain'),
                     ('Content-Length', str(len(body)))))
                return [body]
        return self.app(env, start_response)

    def _translate_form(self, env, start_response, boundary):
        """
        Translates the form data into subrequests and issues a
        response.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        :returns: Response as per WSGI.
        """
        key = self._get_key(env)
        status = message = ''
        attributes = {}
        file_count = 0
        for fp in _iter_requests(env['wsgi.input'], boundary):
            hdrs = rfc822.Message(fp, 0)
            disp, attrs = \
                _parse_attrs(hdrs.getheader('Content-Disposition', ''))
            if disp == 'form-data' and attrs.get('filename'):
                file_count += 1
                try:
                    if file_count > int(attributes.get('max_file_count') or 0):
                        status = '400 Bad Request'
                        message = 'max file count exceeded'
                        break
                except ValueError:
                    raise FormInvalid('max_file_count not an integer')
                attributes['filename'] = attrs['filename'] or 'filename'
                if 'content-type' not in attributes and 'content-type' in hdrs:
                    attributes['content-type'] = \
                        hdrs['Content-Type'] or 'application/octet-stream'
                status, message = self._perform_subrequest(env, start_response,
                                                           attributes, fp, key)
                if status[:1] != '2':
                    break
            else:
                data = ''
                mxln = MAX_VALUE_LENGTH
                while mxln:
                    chunk = fp.read(mxln)
                    if not chunk:
                        break
                    mxln -= len(chunk)
                    data += chunk
                while fp.read(READ_CHUNK_SIZE):
                    pass
                if 'name' in attrs:
                    attributes[attrs['name'].lower()] = data.rstrip('\r\n--')
        if not status:
            status = '400 Bad Request'
            message = 'no files to process'
        if not attributes.get('redirect'):
            body = status
            if message:
                body = status + '\r\nFormPost: ' + message.title()
            start_response(status, [('Content-Type', 'text/plain'),
                                    ('Content-Length', len(body))])
            return [body]
        status = status.split(' ', 1)[0]
        body = '<html><body><p><a href="%s?status=%s&message=%s">Click to ' \
               'continue...</a></p></body></html>' % \
               (attributes['redirect'], quote(status), quote(message))
        start_response('303 See Other',
            [('Location', '%s?status=%s&message=%s' %
                (attributes['redirect'], quote(status), quote(message))),
             ('Content-Length', str(len(body)))])
        return [body]

    def _perform_subrequest(self, env, start_response, attributes, fp, key):
        """
        Performs the subrequest and returns a new response.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        :param attributes: dict of the attributes of the form so far.
        :param fp: The file-like object containing the request body.
        :param key: The account key to validate the signature with.
        :returns: Response as per WSGI.
        """
        if not key:
            return '401 Unauthorized', 'invalid signature'
        try:
            max_file_size = int(attributes.get('max_file_size') or 0)
        except ValueError:
            raise FormInvalid('max_file_size not an integer')
        subenv = {'REQUEST_METHOD': 'PUT',
                  'SCRIPT_NAME': '',
                  'SERVER_NAME': env['SERVER_NAME'],
                  'SERVER_PORT': env['SERVER_PORT'],
                  'SERVER_PROTOCOL': env['SERVER_PROTOCOL'],
                  'HTTP_TRANSFER_ENCODING': 'chunked',
                  'wsgi.input': _CappedFileLikeObject(fp, max_file_size),
                  'swift.cache': env['swift.cache']}
        subenv['PATH_INFO'] = env['PATH_INFO']
        if subenv['PATH_INFO'][-1] != '/' and \
                subenv['PATH_INFO'].count('/') < 4:
            subenv['PATH_INFO'] += '/'
        subenv['PATH_INFO'] += attributes['filename'] or 'filename'
        if 'content-type' in attributes:
            subenv['CONTENT_TYPE'] = \
                attributes['content-type'] or 'application/octet-stream'
        try:
            if int(attributes.get('expires') or 0) < time():
                return '401 Unauthorized', 'form expired'
        except ValueError:
            raise FormInvalid('expired not an integer')
        hmac_body = '%s\n%s\n%s\n%s\n%s' % (
                        env['PATH_INFO'],
                        attributes.get('redirect') or '',
                        attributes.get('max_file_size') or '0',
                        attributes.get('max_file_count') or '0',
                        attributes.get('expires') or '0'
                    )
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        if not streq_const_time(sig, (attributes.get('signature') or
                                      'invalid')):
            return '401 Unauthorized', 'invalid signature'
        subenv['swift.authorize'] = lambda req: None
        subenv['swift.authorize_override'] = True
        subenv['REMOTE_USER'] = '.wsgi.formpost'
        substatus = [None]

        def _start_response(status, headers, exc_info=None):
            substatus[0] = status

        self.app(subenv, _start_response)
        return substatus[0], ''

    def _get_key(self, env):
        """
        Returns the X-Account-Meta-Temp-URL-Key header value for the
        account, or None if none is set.

        :param env: The WSGI environment for the request.
        :returns: X-Account-Meta-Temp-URL-Key str value, or None.
        """
        parts = env['PATH_INFO'].split('/', 4)
        if len(parts) < 4 or parts[0] or parts[1] != 'v1' or not parts[2] or \
                not parts[3]:
            return None
        account = parts[2]
        key = None
        memcache = env.get('swift.cache')
        if memcache:
            key = memcache.get('temp-url-key/%s' % account)
        if not key:
            newenv = {'REQUEST_METHOD': 'HEAD', 'SCRIPT_NAME': '',
                      'PATH_INFO': '/v1/' + account, 'CONTENT_LENGTH': '0',
                      'SERVER_PROTOCOL': 'HTTP/1.0',
                      'HTTP_USER_AGENT': 'FormPost', 'wsgi.version': (1, 0),
                      'wsgi.url_scheme': 'http', 'wsgi.input': StringIO('')}
            for name in ('SERVER_NAME', 'SERVER_PORT', 'wsgi.errors',
                         'wsgi.multithread', 'wsgi.multiprocess',
                         'wsgi.run_once', 'swift.cache', 'swift.trans_id'):
                if name in env:
                    newenv[name] = env[name]
            newenv['swift.authorize'] = lambda req: None
            newenv['swift.authorize_override'] = True
            newenv['REMOTE_USER'] = '.wsgi.formpost'
            key = [None]

            def _start_response(status, response_headers, exc_info=None):
                for h, v in response_headers:
                    if h.lower() == 'x-account-meta-temp-url-key':
                        key[0] = v

            self.app(newenv, _start_response)
            key = key[0]
            if key and memcache:
                memcache.set('temp-url-key/%s' % account, key, timeout=60)
        return key

    def _log_request(self, env, response_status_int):
        """
        Used when a request might not be logged by the underlying
        WSGI application, but we'd still like to record what
        happened. An early 401 Unauthorized is a good example of
        this.

        :param env: The WSGI environment for the request.
        :param response_status_int: The HTTP status we'll be replying
                                    to the request with.
        """
        the_request = quote(unquote(env.get('PATH_INFO') or '/'))
        if env.get('QUERY_STRING'):
            the_request = the_request + '?' + env['QUERY_STRING']
        client = env.get('HTTP_X_CLUSTER_CLIENT_IP')
        if not client and 'HTTP_X_FORWARDED_FOR' in env:
            # remote host for other lbs
            client = env['HTTP_X_FORWARDED_FOR'].split(',')[0].strip()
        if not client:
            client = env.get('REMOTE_ADDR')
        self.logger.info(' '.join(quote(str(x)) for x in (
            client or '-',
            env.get('REMOTE_ADDR') or '-',
            strftime('%d/%b/%Y/%H/%M/%S', gmtime()),
            env.get('REQUEST_METHOD') or 'GET',
            the_request,
            env.get('SERVER_PROTOCOL') or '1.0',
            response_status_int,
            env.get('HTTP_REFERER') or '-',
            (env.get('HTTP_USER_AGENT') or '-') + ' FormPOST',
            env.get('HTTP_X_AUTH_TOKEN') or '-',
            '-',
            '-',
            '-',
            env.get('swift.trans_id') or '-',
            '-',
            '-',
        )))


def filter_factory(global_conf, **local_conf):
    """ Returns the WSGI filter for use with paste.deploy. """
    conf = global_conf.copy()
    conf.update(local_conf)
    return lambda app: FormPost(app, conf)
