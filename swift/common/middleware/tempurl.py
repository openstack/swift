# Copyright (c) 2011-2014 Greg Holt
# Copyright (c) 2012-2013 John Dickinson
# Copyright (c) 2012 Felipe Reyes
# Copyright (c) 2012 Peter Portante
# Copyright (c) 2012 Victor Rodionov
# Copyright (c) 2013-2014 Samuel Merritt
# Copyright (c) 2013 Chuck Thier
# Copyright (c) 2013 David Goetz
# Copyright (c) 2013 Dirk Mueller
# Copyright (c) 2013 Donagh McCabe
# Copyright (c) 2013 Fabien Boucher
# Copyright (c) 2013 Greg Lange
# Copyright (c) 2013 Kun Huang
# Copyright (c) 2013 Richard Hawkins
# Copyright (c) 2013 Tong Li
# Copyright (c) 2013 ZhiQiang Fan
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
TempURL Middleware

Allows the creation of URLs to provide temporary access to objects.

For example, a website may wish to provide a link to download a large
object in Swift, but the Swift account has no public access. The
website can generate a URL that will provide GET access for a limited
time to the resource. When the web browser user clicks on the link,
the browser will download the object directly from Swift, obviating
the need for the website to act as a proxy for the request.

If the user were to share the link with all his friends, or
accidentally post it on a forum, etc. the direct access would be
limited to the expiration time set when the website created the link.

Beyond that, the middleware provides the ability to create URLs, which
contain signatures which are valid for all objects which share a
common prefix. These prefix-based URLs are useful for sharing a set
of objects.

Restrictions can also be placed on the ip that the resource is allowed
to be accessed from. This can be useful for locking down where the urls
can be used from.

------------
Client Usage
------------

To create temporary URLs, first an ``X-Account-Meta-Temp-URL-Key``
header must be set on the Swift account. Then, an HMAC (RFC 2104)
signature is generated using the HTTP method to allow (``GET``, ``PUT``,
``DELETE``, etc.), the Unix timestamp until which the access should be allowed,
the full path to the object, and the key set on the account.

The digest algorithm to be used may be configured by the operator. By default,
HMAC-SHA1, HMAC-SHA256, and HMAC-SHA512 are supported. Check the
``tempurl.allowed_digests`` entry in the cluster's capabilities response to
see which algorithms are supported by your deployment; see
:doc:`api/discoverability` for more information. On older clusters,
the ``tempurl`` key may be present while the ``allowed_digests`` subkey
is not; in this case, only HMAC-SHA1 is supported.

For example, here is code generating the signature for a ``GET`` for 60
seconds on ``/v1/AUTH_account/container/object``::

    import hmac
    from hashlib import sha1
    from time import time
    method = 'GET'
    expires = int(time() + 60)
    path = '/v1/AUTH_account/container/object'
    key = 'mykey'
    hmac_body = '%s\n%s\n%s' % (method, expires, path)
    sig = hmac.new(key, hmac_body, sha1).hexdigest()

Be certain to use the full path, from the ``/v1/`` onward.

Let's say ``sig`` ends up equaling
``da39a3ee5e6b4b0d3255bfef95601890afd80709`` and ``expires`` ends up
``1323479485``. Then, for example, the website could provide a link to::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485

For longer hashes, a hex encoding becomes unwieldy. Base64 encoding is also
supported, and indicated by prefixing the signature with ``"<digest name>:"``.
This is *required* for HMAC-SHA512 signatures. For example, comparable code
for generating a HMAC-SHA512 signature would be::

    import base64
    import hmac
    from hashlib import sha512
    from time import time
    method = 'GET'
    expires = int(time() + 60)
    path = '/v1/AUTH_account/container/object'
    key = 'mykey'
    hmac_body = '%s\n%s\n%s' % (method, expires, path)
    sig = 'sha512:' + base64.urlsafe_b64encode(hmac.new(
        key, hmac_body, sha512).digest())

Supposing that ``sig`` ends up equaling
``sha512:ZrSijn0GyDhsv1ltIj9hWUTrbAeE45NcKXyBaz7aPbSMvROQ4jtYH4nRAmm
5ErY2X11Yc1Yhy2OMCyN3yueeXg==`` and ``expires`` ends up
``1516741234``, then the website could provide a link to::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=sha512:ZrSijn0GyDhsv1ltIj9hWUTrbAeE45NcKXyBaz7aPbSMvRO
    Q4jtYH4nRAmm5ErY2X11Yc1Yhy2OMCyN3yueeXg==&
    temp_url_expires=1516741234

You may also use ISO 8601 UTC timestamps with the format
``"%Y-%m-%dT%H:%M:%SZ"`` instead of UNIX timestamps in the URL
(but NOT in the code above for generating the signature!).
So, the above HMAC-SHA1 URL could also be formulated as::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=2011-12-10T01:11:25Z

If a prefix-based signature with the prefix ``pre`` is desired, set path to::

    path = 'prefix:/v1/AUTH_account/container/pre'

The generated signature would be valid for all objects starting
with ``pre``. The middleware detects a prefix-based temporary URL by
a query parameter called ``temp_url_prefix``. So, if ``sig`` and ``expires``
would end up like above, following URL would be valid::

    https://swift-cluster.example.com/v1/AUTH_account/container/pre/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485&
    temp_url_prefix=pre

Another valid URL::

    https://swift-cluster.example.com/v1/AUTH_account/container/pre/
    subfolder/another_object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485&
    temp_url_prefix=pre

If you wish to lock down the ip ranges from where the resource can be accessed
to the ip ``1.2.3.4``::

    import hmac
    from hashlib import sha1
    from time import time
    method = 'GET'
    expires = int(time() + 60)
    path = '/v1/AUTH_account/container/object'
    ip_range = '1.2.3.4'
    key = 'mykey'
    hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip_range, method, expires, path)
    sig = hmac.new(key, hmac_body, sha1).hexdigest()

The generated signature would only be valid from the ip ``1.2.3.4``. The
middleware detects an ip-based temporary URL by a query parameter called
``temp_url_ip_range``. So, if ``sig`` and ``expires`` would end up like
above, following URL would be valid::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485&
    temp_url_ip_range=1.2.3.4

Similarly to lock down the ip to a range of ``1.2.3.X`` so starting
from the ip ``1.2.3.0`` to ``1.2.3.255``::

    import hmac
    from hashlib import sha1
    from time import time
    method = 'GET'
    expires = int(time() + 60)
    path = '/v1/AUTH_account/container/object'
    ip_range = '1.2.3.0/24'
    key = 'mykey'
    hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip_range, method, expires, path)
    sig = hmac.new(key, hmac_body, sha1).hexdigest()

Then the following url would be valid::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485&
    temp_url_ip_range=1.2.3.0/24


Any alteration of the resource path or query arguments of a temporary URL
would result in ``401 Unauthorized``. Similarly, a ``PUT`` where ``GET`` was
the allowed method would be rejected with ``401 Unauthorized``.
However, ``HEAD`` is allowed if ``GET``, ``PUT``, or ``POST`` is allowed.

Using this in combination with browser form post translation
middleware could also allow direct-from-browser uploads to specific
locations in Swift.

TempURL supports both account and container level keys.  Each allows up to two
keys to be set, allowing key rotation without invalidating all existing
temporary URLs.  Account keys are specified by ``X-Account-Meta-Temp-URL-Key``
and ``X-Account-Meta-Temp-URL-Key-2``, while container keys are specified by
``X-Container-Meta-Temp-URL-Key`` and ``X-Container-Meta-Temp-URL-Key-2``.
Signatures are checked against account and container keys, if
present.

With ``GET`` TempURLs, a ``Content-Disposition`` header will be set on the
response so that browsers will interpret this as a file attachment to
be saved. The filename chosen is based on the object name, but you
can override this with a filename query parameter. Modifying the
above example::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485&filename=My+Test+File.pdf

If you do not want the object to be downloaded, you can cause
``Content-Disposition: inline`` to be set on the response by adding the
``inline`` parameter to the query string, like so::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485&inline

In some cases, the client might not able to present the content of the object,
but you still want the content able to save to local with the specific
filename. So you can cause ``Content-Disposition: inline; filename=...`` to be
set on the response by adding the ``inline&filename=...`` parameter to the
query string, like so::

    https://swift-cluster.example.com/v1/AUTH_account/container/object?
    temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485&inline&filename=My+Test+File.pdf

---------------------
Cluster Configuration
---------------------

This middleware understands the following configuration settings:

``incoming_remove_headers``
    A whitespace-delimited list of the headers to remove from
    incoming requests. Names may optionally end with ``*`` to
    indicate a prefix match. ``incoming_allow_headers`` is a
    list of exceptions to these removals.
    Default: ``x-timestamp``

``incoming_allow_headers``
    A whitespace-delimited list of the headers allowed as
    exceptions to ``incoming_remove_headers``. Names may
    optionally end with ``*`` to indicate a prefix match.

    Default: None

``outgoing_remove_headers``
    A whitespace-delimited list of the headers to remove from
    outgoing responses. Names may optionally end with ``*`` to
    indicate a prefix match. ``outgoing_allow_headers`` is a
    list of exceptions to these removals.

    Default: ``x-object-meta-*``

``outgoing_allow_headers``
    A whitespace-delimited list of the headers allowed as
    exceptions to ``outgoing_remove_headers``. Names may
    optionally end with ``*`` to indicate a prefix match.

    Default: ``x-object-meta-public-*``

``methods``
    A whitespace delimited list of request methods that are
    allowed to be used with a temporary URL.

    Default: ``GET HEAD PUT POST DELETE``

``allowed_digests``
    A whitespace delimited list of digest algorithms that are allowed
    to be used when calculating the signature for a temporary URL.

    Default: ``sha1 sha256 sha512``
"""

__all__ = ['TempURL', 'filter_factory',
           'DEFAULT_INCOMING_REMOVE_HEADERS',
           'DEFAULT_INCOMING_ALLOW_HEADERS',
           'DEFAULT_OUTGOING_REMOVE_HEADERS',
           'DEFAULT_OUTGOING_ALLOW_HEADERS']

import binascii
from calendar import timegm
import functools
import hashlib
import six
from os.path import basename
from time import time, strftime, strptime, gmtime
from ipaddress import ip_address, ip_network

from six.moves.urllib.parse import parse_qs
from six.moves.urllib.parse import urlencode

from swift.proxy.controllers.base import get_account_info, get_container_info
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import header_to_environ_key, HTTPUnauthorized, \
    HTTPBadRequest
from swift.common.utils import split_path, get_valid_utf8_str, \
    register_swift_info, get_hmac, streq_const_time, quote, get_logger, \
    strict_b64decode


DISALLOWED_INCOMING_HEADERS = 'x-object-manifest x-symlink-target'

#: Default headers to remove from incoming requests. Simply a whitespace
#: delimited list of header names and names can optionally end with '*' to
#: indicate a prefix match. DEFAULT_INCOMING_ALLOW_HEADERS is a list of
#: exceptions to these removals.
DEFAULT_INCOMING_REMOVE_HEADERS = 'x-timestamp'

#: Default headers as exceptions to DEFAULT_INCOMING_REMOVE_HEADERS. Simply a
#: whitespace delimited list of header names and names can optionally end with
#: '*' to indicate a prefix match.
DEFAULT_INCOMING_ALLOW_HEADERS = ''

#: Default headers to remove from outgoing responses. Simply a whitespace
#: delimited list of header names and names can optionally end with '*' to
#: indicate a prefix match. DEFAULT_OUTGOING_ALLOW_HEADERS is a list of
#: exceptions to these removals.
DEFAULT_OUTGOING_REMOVE_HEADERS = 'x-object-meta-*'

#: Default headers as exceptions to DEFAULT_OUTGOING_REMOVE_HEADERS. Simply a
#: whitespace delimited list of header names and names can optionally end with
#: '*' to indicate a prefix match.
DEFAULT_OUTGOING_ALLOW_HEADERS = 'x-object-meta-public-*'

DEFAULT_ALLOWED_DIGESTS = 'sha1 sha256 sha512'
SUPPORTED_DIGESTS = set(DEFAULT_ALLOWED_DIGESTS.split())

CONTAINER_SCOPE = 'container'
ACCOUNT_SCOPE = 'account'

EXPIRES_ISO8601_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def get_tempurl_keys_from_metadata(meta):
    """
    Extracts the tempurl keys from metadata.

    :param meta: account metadata
    :returns: list of keys found (possibly empty if no keys set)

    Example:
      meta = get_account_info(...)['meta']
      keys = get_tempurl_keys_from_metadata(meta)
    """
    return [get_valid_utf8_str(value) for key, value in meta.items()
            if key.lower() in ('temp-url-key', 'temp-url-key-2')]


def disposition_format(disposition_type, filename):
    # Content-Disposition in HTTP is defined in
    # https://tools.ietf.org/html/rfc6266 and references
    # https://tools.ietf.org/html/rfc5987#section-3.2
    # to explain the filename*= encoding format. The summary
    # is that it's the charset, then an optional (and empty) language
    # then the filename. Looks funny, but it's right.
    return '''%s; filename="%s"; filename*=UTF-8''%s''' % (
        disposition_type, quote(filename, safe=' /'), quote(filename))


def authorize_same_account(account_to_match):

    def auth_callback_same_account(req):
        try:
            _ver, acc, _rest = req.split_path(2, 3, True)
        except ValueError:
            return HTTPUnauthorized(request=req)

        if acc == account_to_match:
            return None
        else:
            return HTTPUnauthorized(request=req)

    return auth_callback_same_account


def authorize_same_container(account_to_match, container_to_match):

    def auth_callback_same_container(req):
        try:
            _ver, acc, con, _rest = req.split_path(3, 4, True)
        except ValueError:
            return HTTPUnauthorized(request=req)

        if acc == account_to_match and con == container_to_match:
            return None
        else:
            return HTTPUnauthorized(request=req)

    return auth_callback_same_container


class TempURL(object):
    """
    WSGI Middleware to grant temporary URLs specific access to Swift
    resources. See the overview for more information.

    The proxy logs created for any subrequests made will have swift.source set
    to "TU".

    :param app: The next WSGI filter or app in the paste.deploy
                chain.
    :param conf: The configuration dict for the middleware.
    """

    def __init__(self, app, conf):
        #: The next WSGI application/filter in the paste.deploy pipeline.
        self.app = app
        #: The filter configuration dict.
        self.conf = conf

        self.allowed_digests = conf.get(
            'allowed_digests', DEFAULT_ALLOWED_DIGESTS.split())

        self.disallowed_headers = set(
            header_to_environ_key(h)
            for h in DISALLOWED_INCOMING_HEADERS.split())

        headers = [header_to_environ_key(h)
                   for h in conf.get('incoming_remove_headers',
                                     DEFAULT_INCOMING_REMOVE_HEADERS.split())]
        #: Headers to remove from incoming requests. Uppercase WSGI env style,
        #: like `HTTP_X_PRIVATE`.
        self.incoming_remove_headers = \
            [h for h in headers if not h.endswith('*')]
        #: Header with match prefixes to remove from incoming requests.
        #: Uppercase WSGI env style, like `HTTP_X_SENSITIVE_*`.
        self.incoming_remove_headers_startswith = \
            [h[:-1] for h in headers if h.endswith('*')]

        headers = [header_to_environ_key(h)
                   for h in conf.get('incoming_allow_headers',
                                     DEFAULT_INCOMING_ALLOW_HEADERS.split())]
        #: Headers to allow in incoming requests. Uppercase WSGI env style,
        #: like `HTTP_X_MATCHES_REMOVE_PREFIX_BUT_OKAY`.
        self.incoming_allow_headers = \
            [h for h in headers if not h.endswith('*')]
        #: Header with match prefixes to allow in incoming requests. Uppercase
        #: WSGI env style, like `HTTP_X_MATCHES_REMOVE_PREFIX_BUT_OKAY_*`.
        self.incoming_allow_headers_startswith = \
            [h[:-1] for h in headers if h.endswith('*')]

        headers = [h.title()
                   for h in conf.get('outgoing_remove_headers',
                                     DEFAULT_OUTGOING_REMOVE_HEADERS.split())]
        #: Headers to remove from outgoing responses. Lowercase, like
        #: `x-account-meta-temp-url-key`.
        self.outgoing_remove_headers = \
            [h for h in headers if not h.endswith('*')]
        #: Header with match prefixes to remove from outgoing responses.
        #: Lowercase, like `x-account-meta-private-*`.
        self.outgoing_remove_headers_startswith = \
            [h[:-1] for h in headers if h.endswith('*')]

        headers = [h.title()
                   for h in conf.get('outgoing_allow_headers',
                                     DEFAULT_OUTGOING_ALLOW_HEADERS.split())]
        #: Headers to allow in outgoing responses. Lowercase, like
        #: `x-matches-remove-prefix-but-okay`.
        self.outgoing_allow_headers = \
            [h for h in headers if not h.endswith('*')]
        #: Header with match prefixes to allow in outgoing responses.
        #: Lowercase, like `x-matches-remove-prefix-but-okay-*`.
        self.outgoing_allow_headers_startswith = \
            [h[:-1] for h in headers if h.endswith('*')]
        #: HTTP user agent to use for subrequests.
        self.agent = '%(orig)s TempURL'

    def __call__(self, env, start_response):
        """
        Main hook into the WSGI paste.deploy filter/app pipeline.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        :returns: Response as per WSGI.
        """
        if env['REQUEST_METHOD'] == 'OPTIONS':
            return self.app(env, start_response)
        info = self._get_temp_url_info(env)
        temp_url_sig, temp_url_expires, temp_url_prefix, filename,\
            inline_disposition, temp_url_ip_range = info
        if temp_url_sig is None and temp_url_expires is None:
            return self.app(env, start_response)
        if not temp_url_sig or not temp_url_expires:
            return self._invalid(env, start_response)

        if ':' in temp_url_sig:
            hash_algorithm, temp_url_sig = temp_url_sig.split(':', 1)
            if ('-' in temp_url_sig or '_' in temp_url_sig) and not (
                    '+' in temp_url_sig or '/' in temp_url_sig):
                temp_url_sig = temp_url_sig.replace('-', '+').replace('_', '/')
            try:
                temp_url_sig = binascii.hexlify(strict_b64decode(
                    temp_url_sig + '=='))
            except ValueError:
                return self._invalid(env, start_response)
        elif len(temp_url_sig) == 40:
            hash_algorithm = 'sha1'
        elif len(temp_url_sig) == 64:
            hash_algorithm = 'sha256'
        else:
            return self._invalid(env, start_response)
        if hash_algorithm not in self.allowed_digests:
            return self._invalid(env, start_response)

        account, container, obj = self._get_path_parts(env)
        if not account:
            return self._invalid(env, start_response)

        if temp_url_ip_range:
            client_address = env.get('REMOTE_ADDR')
            if client_address is None:
                return self._invalid(env, start_response)
            try:
                allowed_ip_ranges = ip_network(six.u(temp_url_ip_range))
                if ip_address(six.u(client_address)) not in allowed_ip_ranges:
                    return self._invalid(env, start_response)
            except ValueError:
                return self._invalid(env, start_response)

        keys = self._get_keys(env)
        if not keys:
            return self._invalid(env, start_response)
        if temp_url_prefix is None:
            path = '/v1/%s/%s/%s' % (account, container, obj)
        else:
            if not obj.startswith(temp_url_prefix):
                return self._invalid(env, start_response)
            path = 'prefix:/v1/%s/%s/%s' % (account, container,
                                            temp_url_prefix)
        if env['REQUEST_METHOD'] == 'HEAD':
            hmac_vals = [
                hmac for method in ('HEAD', 'GET', 'POST', 'PUT')
                for hmac in self._get_hmacs(
                    env, temp_url_expires, path, keys, hash_algorithm,
                    request_method=method, ip_range=temp_url_ip_range)]
        else:
            hmac_vals = self._get_hmacs(
                env, temp_url_expires, path, keys, hash_algorithm,
                ip_range=temp_url_ip_range)

        is_valid_hmac = False
        hmac_scope = None
        for hmac, scope in hmac_vals:
            # While it's true that we short-circuit, this doesn't affect the
            # timing-attack resistance since the only way this will
            # short-circuit is when a valid signature is passed in.
            if streq_const_time(temp_url_sig, hmac):
                is_valid_hmac = True
                hmac_scope = scope
                break
        if not is_valid_hmac:
            return self._invalid(env, start_response)
        # disallowed headers prevent accidentally allowing upload of a pointer
        # to data that the PUT tempurl would not otherwise allow access for.
        # It should be safe to provide a GET tempurl for data that an
        # untrusted client just uploaded with a PUT tempurl.
        resp = self._clean_disallowed_headers(env, start_response)
        if resp:
            return resp
        self._clean_incoming_headers(env)

        if hmac_scope == ACCOUNT_SCOPE:
            env['swift.authorize'] = authorize_same_account(account)
        else:
            env['swift.authorize'] = authorize_same_container(account,
                                                              container)
        env['swift.authorize_override'] = True
        env['REMOTE_USER'] = '.wsgi.tempurl'
        qs = {'temp_url_sig': temp_url_sig,
              'temp_url_expires': temp_url_expires}
        if temp_url_prefix is not None:
            qs['temp_url_prefix'] = temp_url_prefix
        if filename:
            qs['filename'] = filename
        env['QUERY_STRING'] = urlencode(qs)

        def _start_response(status, headers, exc_info=None):
            headers = self._clean_outgoing_headers(headers)
            if env['REQUEST_METHOD'] in ('GET', 'HEAD') and status[0] == '2':
                # figure out the right value for content-disposition
                # 1) use the value from the query string
                # 2) use the value from the object metadata
                # 3) use the object name (default)
                out_headers = []
                existing_disposition = None
                for h, v in headers:
                    if h.lower() != 'content-disposition':
                        out_headers.append((h, v))
                    else:
                        existing_disposition = v
                if inline_disposition:
                    if filename:
                        disposition_value = disposition_format('inline',
                                                               filename)
                    else:
                        disposition_value = 'inline'
                elif filename:
                    disposition_value = disposition_format('attachment',
                                                           filename)
                elif existing_disposition:
                    disposition_value = existing_disposition
                else:
                    name = basename(env['PATH_INFO'].rstrip('/'))
                    disposition_value = disposition_format('attachment',
                                                           name)
                # this is probably just paranoia, I couldn't actually get a
                # newline into existing_disposition
                value = disposition_value.replace('\n', '%0A')
                out_headers.append(('Content-Disposition', value))

                # include Expires header for better cache-control
                out_headers.append(('Expires', strftime(
                    "%a, %d %b %Y %H:%M:%S GMT",
                    gmtime(temp_url_expires))))
                headers = out_headers
            return start_response(status, headers, exc_info)

        return self.app(env, _start_response)

    def _get_path_parts(self, env):
        """
        Return the account, container and object name for the request,
        if it's an object request and one of the configured methods;
        otherwise, None is returned.

        :param env: The WSGI environment for the request.
        :returns: (Account str, container str, object str) or
            (None, None, None).
        """
        if env['REQUEST_METHOD'] in self.conf['methods']:
            try:
                ver, acc, cont, obj = split_path(env['PATH_INFO'], 4, 4, True)
            except ValueError:
                return (None, None, None)
            if ver == 'v1' and obj.strip('/'):
                return (acc, cont, obj)
        return (None, None, None)

    def _get_temp_url_info(self, env):
        """
        Returns the provided temporary URL parameters (sig, expires, prefix,
        temp_url_ip_range), if given and syntactically valid.
        Either sig, expires or prefix could be None if not provided.
        If provided, expires is also converted to an int if possible or 0
        if not, and checked for expiration (returns 0 if expired).

        :param env: The WSGI environment for the request.
        :returns: (sig, expires, prefix, filename, inline,
            temp_url_ip_range) as described above.
        """
        temp_url_sig = temp_url_expires = temp_url_prefix = filename =\
            inline = None
        temp_url_ip_range = None
        qs = parse_qs(env.get('QUERY_STRING', ''), keep_blank_values=True)
        if 'temp_url_ip_range' in qs:
            temp_url_ip_range = qs['temp_url_ip_range'][0]
        if 'temp_url_sig' in qs:
            temp_url_sig = qs['temp_url_sig'][0]
        if 'temp_url_expires' in qs:
            try:
                temp_url_expires = int(qs['temp_url_expires'][0])
            except ValueError:
                try:
                    temp_url_expires = timegm(strptime(
                        qs['temp_url_expires'][0],
                        EXPIRES_ISO8601_FORMAT))
                except ValueError:
                    temp_url_expires = 0
            if temp_url_expires < time():
                temp_url_expires = 0
        if 'temp_url_prefix' in qs:
            temp_url_prefix = qs['temp_url_prefix'][0]
        if 'filename' in qs:
            filename = qs['filename'][0]
        if 'inline' in qs:
            inline = True
        return (temp_url_sig, temp_url_expires, temp_url_prefix, filename,
                inline, temp_url_ip_range)

    def _get_keys(self, env):
        """
        Returns the X-[Account|Container]-Meta-Temp-URL-Key[-2] header values
        for the account or container, or an empty list if none are set. Each
        value comes as a 2-tuple (key, scope), where scope is either
        CONTAINER_SCOPE or ACCOUNT_SCOPE.

        Returns 0-4 elements depending on how many keys are set in the
        account's or container's metadata.

        :param env: The WSGI environment for the request.
        :returns: [
            (X-Account-Meta-Temp-URL-Key str value, ACCOUNT_SCOPE) if set,
            (X-Account-Meta-Temp-URL-Key-2 str value, ACCOUNT_SCOPE if set,
            (X-Container-Meta-Temp-URL-Key str value, CONTAINER_SCOPE) if set,
            (X-Container-Meta-Temp-URL-Key-2 str value, CONTAINER_SCOPE if set,
        ]
        """
        account_info = get_account_info(env, self.app, swift_source='TU')
        account_keys = get_tempurl_keys_from_metadata(account_info['meta'])

        container_info = get_container_info(env, self.app, swift_source='TU')
        container_keys = get_tempurl_keys_from_metadata(
            container_info.get('meta', []))

        return ([(ak, ACCOUNT_SCOPE) for ak in account_keys] +
                [(ck, CONTAINER_SCOPE) for ck in container_keys])

    def _get_hmacs(self, env, expires, path, scoped_keys, hash_algorithm,
                   request_method=None, ip_range=None):
        """
        :param env: The WSGI environment for the request.
        :param expires: Unix timestamp as an int for when the URL
                        expires.
        :param path: The path which is used for hashing.
        :param scoped_keys: (key, scope) tuples like _get_keys() returns
        :param hash_algorithm: The hash algorithm to use.
        :param request_method: Optional override of the request in
                               the WSGI env. For example, if a HEAD
                               does not match, you may wish to
                               override with GET to still allow the
                               HEAD.
        :param ip_range: The ip range from which the resource is allowed
                         to be accessed
        :returns: a list of (hmac, scope) 2-tuples
        """
        if not request_method:
            request_method = env['REQUEST_METHOD']

        digest = functools.partial(hashlib.new, hash_algorithm)

        return [
            (get_hmac(
                request_method, path, expires, key,
                digest=digest, ip_range=ip_range
            ), scope)
            for (key, scope) in scoped_keys]

    def _invalid(self, env, start_response):
        """
        Performs the necessary steps to indicate a WSGI 401
        Unauthorized response to the request.

        :param env: The WSGI environment for the request.
        :param start_response: The WSGI start_response hook.
        :returns: 401 response as per WSGI.
        """
        if env['REQUEST_METHOD'] == 'HEAD':
            body = None
        else:
            body = '401 Unauthorized: Temp URL invalid\n'
        return HTTPUnauthorized(body=body)(env, start_response)

    def _clean_disallowed_headers(self, env, start_response):
        """
        Validate the absence of disallowed headers for "unsafe" operations.

        :returns: None for safe operations or swob.HTTPBadResponse if the
                  request includes disallowed headers.
        """
        if env['REQUEST_METHOD'] in ('GET', 'HEAD', 'OPTIONS'):
            return
        for h in env:
            if h in self.disallowed_headers:
                return HTTPBadRequest(
                    body='The header %r is not allowed in this tempurl' %
                    h[len('HTTP_'):].title().replace('_', '-'))(
                        env, start_response)

    def _clean_incoming_headers(self, env):
        """
        Removes any headers from the WSGI environment as per the
        middleware configuration for incoming requests.

        :param env: The WSGI environment for the request.
        """
        for h in env.keys():
            if h in self.incoming_allow_headers:
                continue
            for p in self.incoming_allow_headers_startswith:
                if h.startswith(p):
                    break
            else:
                if h in self.incoming_remove_headers:
                    del env[h]
                    continue
                for p in self.incoming_remove_headers_startswith:
                    if h.startswith(p):
                        del env[h]
                        break

    def _clean_outgoing_headers(self, headers):
        """
        Removes any headers as per the middleware configuration for
        outgoing responses.

        :param headers: A WSGI start_response style list of headers,
                        [('header1', 'value), ('header2', 'value),
                         ...]
        :returns: The same headers list, but with some headers
                  removed as per the middlware configuration for
                  outgoing responses.
        """
        headers = HeaderKeyDict(headers)
        for h in headers.keys():
            if h in self.outgoing_allow_headers:
                continue
            for p in self.outgoing_allow_headers_startswith:
                if h.startswith(p):
                    break
            else:
                if h in self.outgoing_remove_headers:
                    del headers[h]
                    continue
                for p in self.outgoing_remove_headers_startswith:
                    if h.startswith(p):
                        del headers[h]
                        break
        return headers.items()


def filter_factory(global_conf, **local_conf):
    """Returns the WSGI filter for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    defaults = {
        'methods': 'GET HEAD PUT POST DELETE',
        'incoming_remove_headers': DEFAULT_INCOMING_REMOVE_HEADERS,
        'incoming_allow_headers': DEFAULT_INCOMING_ALLOW_HEADERS,
        'outgoing_remove_headers': DEFAULT_OUTGOING_REMOVE_HEADERS,
        'outgoing_allow_headers': DEFAULT_OUTGOING_ALLOW_HEADERS,
        'allowed_digests': DEFAULT_ALLOWED_DIGESTS,
    }
    info_conf = {k: conf.get(k, v).split() for k, v in defaults.items()}

    allowed_digests = set(digest.lower()
                          for digest in info_conf['allowed_digests'])
    not_supported = allowed_digests - SUPPORTED_DIGESTS
    if not_supported:
        logger = get_logger(conf, log_route='tempurl')
        logger.warning('The following digest algorithms are configured but '
                       'not supported: %s', ', '.join(not_supported))
        allowed_digests -= not_supported
    if not allowed_digests:
        raise ValueError('No valid digest algorithms are configured '
                         'for tempurls')
    info_conf['allowed_digests'] = sorted(allowed_digests)

    register_swift_info('tempurl', **info_conf)
    conf.update(info_conf)

    return lambda app: TempURL(app, conf)
