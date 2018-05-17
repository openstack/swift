# Copyright (c) 2011 OpenStack Foundation
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

r"""
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

Optionally, if you want the uploaded files to be temporary you can set
x-delete-at or x-delete-after attributes by adding one of these as a
form input::

    <input type="hidden" name="x_delete_at" value="<unix-timestamp>" />
    <input type="hidden" name="x_delete_after" value="<seconds>" />

The <swift-url> is the URL of the Swift destination, such as::

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

The redirect attribute is the URL to redirect the browser to after the upload
completes. This is an optional parameter. If you are uploading the form via an
XMLHttpRequest the redirect should not be included. The URL will have status
and message query parameters added to it, indicating the HTTP status code for
the upload (2xx is success) and a possible message for further information if
there was an error (such as "max_file_size exceeded").

The max_file_size attribute must be included and indicates the
largest single file upload that can be done, in bytes.

The max_file_count attribute must be included and indicates the
maximum number of files that can be uploaded with the form. Include
additional ``<input type="file" name="filexx" />`` attributes if
desired.

The expires attribute is the Unix timestamp before which the form
must be submitted before it is invalidated.

The signature attribute is the HMAC-SHA1 signature of the form. Here is
sample code for computing the signature::

    import hmac
    from hashlib import sha1
    from time import time
    path = '/v1/account/container/object_prefix'
    redirect = 'https://srv.com/some-page'  # set to '' if redirect not in form
    max_file_size = 104857600
    max_file_count = 10
    expires = int(time() + 600)
    key = 'mykey'
    hmac_body = '%s\n%s\n%s\n%s\n%s' % (path, redirect,
        max_file_size, max_file_count, expires)
    signature = hmac.new(key, hmac_body, sha1).hexdigest()

The key is the value of either the account (X-Account-Meta-Temp-URL-Key,
X-Account-Meta-Temp-Url-Key-2) or the container
(X-Container-Meta-Temp-URL-Key, X-Container-Meta-Temp-Url-Key-2) TempURL keys.

Be certain to use the full path, from the /v1/ onward.
Note that x_delete_at and x_delete_after are not used in signature generation
as they are both optional attributes.

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
from hashlib import sha1
from time import time

from six.moves.urllib.parse import quote
from swift.common.exceptions import MimeInvalid
from swift.common.middleware.tempurl import get_tempurl_keys_from_metadata
from swift.common.utils import streq_const_time, register_swift_info, \
    parse_content_disposition, parse_mime_headers, \
    iter_multipart_mime_documents, reiterate, close_if_possible
from swift.common.wsgi import make_pre_authed_env
from swift.common.swob import HTTPUnauthorized
from swift.proxy.controllers.base import get_account_info, get_container_info


#: The size of data to read from the form at any given time.
READ_CHUNK_SIZE = 4096

#: The maximum size of any attribute's value. Any additional data will be
#: truncated.
MAX_VALUE_LENGTH = 4096


class FormInvalid(Exception):
    pass


class FormUnauthorized(Exception):
    pass


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
        self.file_size_exceeded = False

    def read(self, size=None):
        ret = self.fp.read(size)
        self.amount_read += len(ret)
        if self.amount_read > self.max_file_size:
            self.file_size_exceeded = True
            raise EOFError('max_file_size exceeded')
        return ret

    def readline(self):
        ret = self.fp.readline()
        self.amount_read += len(ret)
        if self.amount_read > self.max_file_size:
            self.file_size_exceeded = True
            raise EOFError('max_file_size exceeded')
        return ret


class FormPost(object):
    """
    FormPost Middleware

    See above for a full description.

    The proxy logs created for any subrequests made will have swift.source set
    to "FP".

    :param app: The next WSGI filter or app in the paste.deploy
                chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf):
        #: The next WSGI application/filter in the paste.deploy pipeline.
        self.app = app
        #: The filter configuration dict.
        self.conf = conf

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
                    parse_content_disposition(env.get('CONTENT_TYPE') or '')
                if content_type == 'multipart/form-data' and \
                        'boundary' in attrs:
                    http_user_agent = "%s FormPost" % (
                        env.get('HTTP_USER_AGENT', ''))
                    env['HTTP_USER_AGENT'] = http_user_agent.strip()
                    status, headers, body = self._translate_form(
                        env, attrs['boundary'])
                    start_response(status, headers)
                    return [body]
            except MimeInvalid:
                body = 'FormPost: invalid starting boundary'
                start_response(
                    '400 Bad Request',
                    (('Content-Type', 'text/plain'),
                     ('Content-Length', str(len(body)))))
                return [body]
            except (FormInvalid, EOFError) as err:
                body = 'FormPost: %s' % err
                start_response(
                    '400 Bad Request',
                    (('Content-Type', 'text/plain'),
                     ('Content-Length', str(len(body)))))
                return [body]
            except FormUnauthorized as err:
                message = 'FormPost: %s' % str(err).title()
                return HTTPUnauthorized(body=message)(
                    env, start_response)
        return self.app(env, start_response)

    def _translate_form(self, env, boundary):
        """
        Translates the form data into subrequests and issues a
        response.

        :param env: The WSGI environment dict.
        :param boundary: The MIME type boundary to look for.
        :returns: status_line, headers_list, body
        """
        keys = self._get_keys(env)
        status = message = ''
        attributes = {}
        subheaders = []
        file_count = 0
        for fp in iter_multipart_mime_documents(
                env['wsgi.input'], boundary, read_chunk_size=READ_CHUNK_SIZE):
            hdrs = parse_mime_headers(fp)
            disp, attrs = parse_content_disposition(
                hdrs.get('Content-Disposition', ''))
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
                status, subheaders = \
                    self._perform_subrequest(env, attributes, fp, keys)
                if not status.startswith('2'):
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

        headers = [(k, v) for k, v in subheaders
                   if k.lower().startswith('access-control')]

        redirect = attributes.get('redirect')
        if not redirect:
            body = status
            if message:
                body = status + '\r\nFormPost: ' + message.title()
            headers.extend([('Content-Type', 'text/plain'),
                            ('Content-Length', len(body))])
            return status, headers, body
        status = status.split(' ', 1)[0]
        if '?' in redirect:
            redirect += '&'
        else:
            redirect += '?'
        redirect += 'status=%s&message=%s' % (quote(status), quote(message))
        body = '<html><body><p><a href="%s">' \
               'Click to continue...</a></p></body></html>' % redirect
        headers.extend(
            [('Location', redirect), ('Content-Length', str(len(body)))])
        return '303 See Other', headers, body

    def _perform_subrequest(self, orig_env, attributes, fp, keys):
        """
        Performs the subrequest and returns the response.

        :param orig_env: The WSGI environment dict; will only be used
                         to form a new env for the subrequest.
        :param attributes: dict of the attributes of the form so far.
        :param fp: The file-like object containing the request body.
        :param keys: The account keys to validate the signature with.
        :returns: (status_line, headers_list)
        """
        if not keys:
            raise FormUnauthorized('invalid signature')
        try:
            max_file_size = int(attributes.get('max_file_size') or 0)
        except ValueError:
            raise FormInvalid('max_file_size not an integer')
        subenv = make_pre_authed_env(orig_env, 'PUT', agent=None,
                                     swift_source='FP')
        if 'QUERY_STRING' in subenv:
            del subenv['QUERY_STRING']
        subenv['HTTP_TRANSFER_ENCODING'] = 'chunked'
        subenv['wsgi.input'] = _CappedFileLikeObject(fp, max_file_size)
        if not subenv['PATH_INFO'].endswith('/') and \
                subenv['PATH_INFO'].count('/') < 4:
            subenv['PATH_INFO'] += '/'
        subenv['PATH_INFO'] += attributes['filename'] or 'filename'
        if 'x_delete_at' in attributes:
            try:
                subenv['HTTP_X_DELETE_AT'] = int(attributes['x_delete_at'])
            except ValueError:
                raise FormInvalid('x_delete_at not an integer: '
                                  'Unix timestamp required.')
        if 'x_delete_after' in attributes:
            try:
                subenv['HTTP_X_DELETE_AFTER'] = int(
                    attributes['x_delete_after'])
            except ValueError:
                raise FormInvalid('x_delete_after not an integer: '
                                  'Number of seconds required.')
        if 'content-type' in attributes:
            subenv['CONTENT_TYPE'] = \
                attributes['content-type'] or 'application/octet-stream'
        try:
            if int(attributes.get('expires') or 0) < time():
                raise FormUnauthorized('form expired')
        except ValueError:
            raise FormInvalid('expired not an integer')
        hmac_body = '%s\n%s\n%s\n%s\n%s' % (
            orig_env['PATH_INFO'],
            attributes.get('redirect') or '',
            attributes.get('max_file_size') or '0',
            attributes.get('max_file_count') or '0',
            attributes.get('expires') or '0')

        has_valid_sig = False
        for key in keys:
            sig = hmac.new(key, hmac_body, sha1).hexdigest()
            if streq_const_time(sig, (attributes.get('signature') or
                                      'invalid')):
                has_valid_sig = True
        if not has_valid_sig:
            raise FormUnauthorized('invalid signature')

        substatus = [None]
        subheaders = [None]

        wsgi_input = subenv['wsgi.input']

        def _start_response(status, headers, exc_info=None):
            if wsgi_input.file_size_exceeded:
                raise EOFError("max_file_size exceeded")

            substatus[0] = status
            subheaders[0] = headers

        # reiterate to ensure the response started,
        # but drop any data on the floor
        close_if_possible(reiterate(self.app(subenv, _start_response)))
        return substatus[0], subheaders[0]

    def _get_keys(self, env):
        """
        Returns the X-[Account|Container]-Meta-Temp-URL-Key[-2] header values
        for the account or container, or an empty list if none are set.

        Returns 0-4 elements depending on how many keys are set in the
        account's or container's metadata.

        Also validate that the request
        path indicates a valid container; if not, no keys will be returned.

        :param env: The WSGI environment for the request.
        :returns: list of tempurl keys
        """
        parts = env['PATH_INFO'].split('/', 4)
        if len(parts) < 4 or parts[0] or parts[1] != 'v1' or not parts[2] or \
                not parts[3]:
            return []

        account_info = get_account_info(env, self.app, swift_source='FP')
        account_keys = get_tempurl_keys_from_metadata(account_info['meta'])

        container_info = get_container_info(env, self.app, swift_source='FP')
        container_keys = get_tempurl_keys_from_metadata(
            container_info.get('meta', []))

        return account_keys + container_keys


def filter_factory(global_conf, **local_conf):
    """Returns the WSGI filter for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    register_swift_info('formpost')

    return lambda app: FormPost(app, conf)
