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

To create such temporary URLs, first an X-Account-Meta-Temp-URL-Key
header must be set on the Swift account. Then, an HMAC-SHA1 (RFC 2104)
signature is generated using the HTTP method to allow (GET or PUT),
the Unix timestamp the access should be allowed until, the full path
to the object, and the key set on the account.

For example, here is code generating the signature for a GET for 60
seconds on /v1/AUTH_account/container/object::

    import hmac
    from hashlib import sha1
    from time import time
    method = 'GET'
    expires = int(time() + 60)
    path = '/v1/AUTH_account/container/object'
    key = 'mykey'
    hmac_body = '%s\\n%s\\n%s' % (method, expires, path)
    sig = hmac.new(key, hmac_body, sha1).hexdigest()

Be certain to use the full path, from the /v1/ onward.

Let's say the sig ends up equaling
da39a3ee5e6b4b0d3255bfef95601890afd80709 and expires ends up
1323479485. Then, for example, the website could provide a link to::

    https://swift-cluster.example.com/v1/AUTH_account/container/object
    ?temp_url_sig=da39a3ee5e6b4b0d3255bfef95601890afd80709&
    temp_url_expires=1323479485

Any alteration of the resource path or query arguments would result
in 401 Unauthorized. Similary, a PUT where GET was the allowed method
would 401. HEAD is allowed if GET or PUT is allowed.

Using this in combination with browser form post translation
middleware could also allow direct-from-browser uploads to specific
locations in Swift.

Note that changing the X-Account-Meta-Temp-URL-Key will invalidate
any previously generated temporary URLs within 60 seconds (the
memcache time for the key).

If you want to create different temporary URLs for different users,
first you need to request a user signature for each user. Using
these signature you can create unique temporary URLs for each user
and later on you can invalidate any URL as you wish.
Let's say you want to share container/object with Jack and Bob.
Fisrt, we should request user signature for each of them:

    headers ={'User-For-Share-Add':'Jack',\
              'X-Account-Meta-Temp-Url-Key':'mykey'}
    response = requests.get('http://swiftnode.com/v1/AUTH_account
                            /container/object',headers=headers)
    if response.status_code == 200:
        Jack_signature = response.headers[
                            'user-for-share-add-response']

    headers ={'User-For-Share-Add':'Bob',\
              'X-Account-Meta-Temp-Url-Key':'mykey'}
    response = requests.get('http://www.swiftnode.com/v1/AUTH_account
                            /container/object',headers=headers)
    if response.status_code == 200:
        Bob_signature = response.headers[
                                'user-for-share-add-response']
        
Using these signatures we can create temporary URLs for each of them:

    expires = int(time() + 60)
    method='GET'
    key = 'mykey'
    hmac_body = '%s\\n%s\\n%s\\n%s' % (method, expires,
    '/v1/AUTH_account/container/object',Jack_signature)
    Jack_URL = 'http://www.swiftnode.com/v1/AUTH_account/container/object
    ?temp_url_sig=%s&temp_url_expires=%d&temp_url_id=%s'%
    (hmac_body,expires,'Jack')
    hmac_body = '%s\\n%s\\n%s\\n%s' % (method, expires,
    '/v1/AUTH_account/container/object',Bob_signature)
    Bob_URL = 'http://www.swiftnode.com/v1/AUTH_account/container/object
    ?temp_url_sig=%s&temp_url_expires=%d&temp_url_id=%s'%(hmac_body,expires,'Bob')

If for example you decide to stop Bob from accessing the object you
can simply invalidate his temporary URL or just let it expire:

    headers ={'User-For-Share-Remove':'Bob',\
              'X-Account-Meta-Temp-Url-Key':'mykey'}
    response = requests.get('http://www.swiftnode.com
    /v1/AUTH_account/container/object',headers=headers)
    if response.status_code == 200:
        if response.headers['user-for-share-remove-response'] == 'Successful':
            print "Bob removed from accessing container/object"
Note removing Bob from accessing container/object won't affect his access from
other objects that you previously granted him for.

"""

__all__ = ['TempURL', 'filter_factory',
           'DEFAULT_INCOMING_REMOVE_HEADERS',
           'DEFAULT_INCOMING_ALLOW_HEADERS',
           'DEFAULT_OUTGOING_REMOVE_HEADERS',
           'DEFAULT_OUTGOING_ALLOW_HEADERS']


import hmac
from hashlib import sha1
from os.path import basename
from StringIO import StringIO
from time import gmtime, strftime, time
from urllib import quote, unquote
from urlparse import parse_qs
import uuid

from swift.common.utils import get_logger
from swift.common.wsgi import make_pre_authed_env
from swift.common.http import HTTP_UNAUTHORIZED

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


class TempURL(object):
    """
    WSGI Middleware to grant temporary URLs specific access to Swift
    resources. See the overview for more information.

    This middleware understands the following configuration settings::

        incoming_remove_headers
            The headers to remove from incoming requests. Simply a
            whitespace delimited list of header names and names can
            optionally end with '*' to indicate a prefix match.
            incoming_allow_headers is a list of exceptions to these
            removals.
            Default: x-timestamp

        incoming_allow_headers
            The headers allowed as exceptions to
            incoming_remove_headers. Simply a whitespace delimited
            list of header names and names can optionally end with
            '*' to indicate a prefix match.
            Default: None

        outgoing_remove_headers
            The headers to remove from outgoing responses. Simply a
            whitespace delimited list of header names and names can
            optionally end with '*' to indicate a prefix match.
            outgoing_allow_headers is a list of exceptions to these
            removals.
            Default: x-object-meta-*

        outgoing_allow_headers
            The headers allowed as exceptions to
            outgoing_remove_headers. Simply a whitespace delimited
            list of header names and names can optionally end with
            '*' to indicate a prefix match.
            Default: x-object-meta-public-*

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
        self.logger = get_logger(conf, log_route='tempurl')

        headers = DEFAULT_INCOMING_REMOVE_HEADERS
        if 'incoming_remove_headers' in conf:
            headers = conf['incoming_remove_headers']
        headers = \
            ['HTTP_' + h.upper().replace('-', '_') for h in headers.split()]
        #: Headers to remove from incoming requests. Uppercase WSGI env style,
        #: like `HTTP_X_PRIVATE`.
        self.incoming_remove_headers = [h for h in headers if h[-1] != '*']
        #: Header with match prefixes to remove from incoming requests.
        #: Uppercase WSGI env style, like `HTTP_X_SENSITIVE_*`.
        self.incoming_remove_headers_startswith = \
            [h[:-1] for h in headers if h[-1] == '*']

        headers = DEFAULT_INCOMING_ALLOW_HEADERS
        if 'incoming_allow_headers' in conf:
            headers = conf['incoming_allow_headers']
        headers = \
            ['HTTP_' + h.upper().replace('-', '_') for h in headers.split()]
        #: Headers to allow in incoming requests. Uppercase WSGI env style,
        #: like `HTTP_X_MATCHES_REMOVE_PREFIX_BUT_OKAY`.
        self.incoming_allow_headers = [h for h in headers if h[-1] != '*']
        #: Header with match prefixes to allow in incoming requests. Uppercase
        #: WSGI env style, like `HTTP_X_MATCHES_REMOVE_PREFIX_BUT_OKAY_*`.
        self.incoming_allow_headers_startswith = \
            [h[:-1] for h in headers if h[-1] == '*']

        headers = DEFAULT_OUTGOING_REMOVE_HEADERS
        if 'outgoing_remove_headers' in conf:
            headers = conf['outgoing_remove_headers']
        headers = [h.lower() for h in headers.split()]
        #: Headers to remove from outgoing responses. Lowercase, like
        #: `x-account-meta-temp-url-key`.
        self.outgoing_remove_headers = [h for h in headers if h[-1] != '*']
        #: Header with match prefixes to remove from outgoing responses.
        #: Lowercase, like `x-account-meta-private-*`.
        self.outgoing_remove_headers_startswith = \
            [h[:-1] for h in headers if h[-1] == '*']

        headers = DEFAULT_OUTGOING_ALLOW_HEADERS
        if 'outgoing_allow_headers' in conf:
            headers = conf['outgoing_allow_headers']
        headers = [h.lower() for h in headers.split()]
        #: Headers to allow in outgoing responses. Lowercase, like
        #: `x-matches-remove-prefix-but-okay`.
        self.outgoing_allow_headers = [h for h in headers if h[-1] != '*']
        #: Header with match prefixes to allow in outgoing responses.
        #: Lowercase, like `x-matches-remove-prefix-but-okay-*`.
        self.outgoing_allow_headers_startswith = \
            [h[:-1] for h in headers if h[-1] == '*']
        #: HTTP user agent to use for subrequests.
        self.agent = '%(orig)s TempURL'

    def __call__(self, env, start_response):
        """
        Main hook into the WSGI paste.deploy filter/app pipeline.

        :param env: The WSGI environment dict.
        :param start_response: The WSGI start_response hook.
        :returns: Response as per WSGI.
        """
        add_share_req,remove_share_req,temp_key,user_id = self._get_request_info(env)
        if add_share_req and remove_share_req:
            # cannot add and remove at the same time
            return self._invalid(env, start_response)
        if add_share_req or remove_share_req:
            account = self._get_account(env)
            if not account:
                return self._invalid(env, start_response)
            key = self._get_key(env, account)
            if not key:
                return self._invalid(env, start_response)
            if key == temp_key:
                if add_share_req:
                    return self._add_id(env, start_response,user_id)
                if remove_share_req:
                    return self._remove_id(env, start_response,user_id)
            else:
                return self._invalid(env,start_response)
            
        temp_url_sig, temp_url_expires,temp_url_id = self._get_temp_url_info(env)
        if temp_url_sig is None and temp_url_expires is None:
            return self.app(env, start_response)
        if not temp_url_sig or not temp_url_expires:
            return self._invalid(env, start_response)
        account = self._get_account(env)
        if not account:
            return self._invalid(env, start_response)
        key = self._get_key(env, account)
        if not key:
            return self._invalid(env, start_response)
        uid_sig =None
        if temp_url_id != None:
            uid_sig = self._isValid_uid(env,temp_url_id)
            if not uid_sig:
                return self._invalid(env, start_response)
        if env['REQUEST_METHOD'] == 'HEAD':
            hmac_val = self._get_hmac(env, temp_url_expires, key,uid_sig,
                                      request_method='GET')
            if temp_url_sig != hmac_val:
                hmac_val = self._get_hmac(env, temp_url_expires, key,uid_sig,
                                          request_method='PUT')
                if temp_url_sig != hmac_val:
                    return self._invalid(env, start_response)
        else:
            hmac_val = self._get_hmac(env, temp_url_expires, key,uid_sig,request_method=None)
            if temp_url_sig != hmac_val:
                return self._invalid(env,start_response)

        self._clean_incoming_headers(env)
        env['swift.authorize'] = lambda req: None
        env['swift.authorize_override'] = True
        env['REMOTE_USER'] = '.wsgi.tempurl'

        def _start_response(status, headers, exc_info=None):
            headers = self._clean_outgoing_headers(headers)
            if env['REQUEST_METHOD'] == 'GET':
                already = False
                for h, v in headers:
                    if h.lower() == 'content-disposition':
                        already = True
                        break
                if not already:
                    headers.append(('Content-Disposition',
                        'attachment; filename=%s' %
                            (quote(basename(env['PATH_INFO'])))))
            return start_response(status, headers, exc_info)

        return self.app(env, _start_response)


    def _get_account(self, env):
        """
        Returns just the account for the request, if it's an object GET, PUT,
        or HEAD request; otherwise, None is returned.

        :param env: The WSGI environment for the request.
        :returns: Account str or None.
        """
        account = None
        if env['REQUEST_METHOD'] in ('GET', 'PUT', 'HEAD'):
            parts = env['PATH_INFO'].split('/', 4)
            # Must be five parts, ['', 'v1', 'a', 'c', 'o'], must be a v1
            # request, have account, container, and object values, and the
            # object value can't just have '/'s.
            if len(parts) == 5 and not parts[0] and parts[1] == 'v1' and \
                    parts[2] and parts[3] and parts[4].strip('/'):
                account = parts[2]
        return account

    def _get_temp_url_info(self, env):
        """
        Returns the provided temporary URL parameters (sig, expires),
        if given and syntactically valid. Either sig or expires could
        be None if not provided. If provided, expires is also
        converted to an int if possible or 0 if not, and checked for
        expiration (returns 0 if expired).

        :param env: The WSGI environment for the request.
        :returns: (sig, expires) as described above.
        """
        temp_url_sig = temp_url_expires =temp_url_id= None
        qs = parse_qs(env.get('QUERY_STRING', ''))
        if 'temp_url_id' in qs:
            temp_url_id = qs['temp_url_id'][0]
        if 'temp_url_sig' in qs:
            temp_url_sig = qs['temp_url_sig'][0]
        if 'temp_url_expires' in qs:
            try:
                temp_url_expires = int(qs['temp_url_expires'][0])
            except ValueError:
                temp_url_expires = 0
            if temp_url_expires < time():
                temp_url_expires = 0
        return temp_url_sig, temp_url_expires,temp_url_id

    def _get_key(self, env, account):
        """
        Returns the X-Account-Meta-Temp-URL-Key header value for the
        account, or None if none is set.

        :param env: The WSGI environment for the request.
        :param account: Account str.
        :returns: X-Account-Meta-Temp-URL-Key str value, or None.
        """
        key = None
        memcache = env.get('swift.cache')
        if memcache:
            key = memcache.get('temp-url-key/%s' % account)
        if not key:
            newenv = make_pre_authed_env(env, 'HEAD', '/v1/' + account,
                                         self.agent)
            newenv['CONTENT_LENGTH'] = '0'
            newenv['wsgi.input'] = StringIO('')
            key = [None]

            def _start_response(status, response_headers, exc_info=None):
                for h, v in response_headers:
                    if h.lower() == 'x-account-meta-temp-url-key':
                        key[0] = v

            i = iter(self.app(newenv, _start_response))
            try:
                i.next()
            except StopIteration:
                pass
            key = key[0]
            if key and memcache:
                memcache.set('temp-url-key/%s' % account, key, timeout=60)
        return key

    def _get_hmac(self, env, expires, key,uid_sig, request_method=None):
        """
        Returns the hexdigest string of the HMAC-SHA1 (RFC 2104) for
        the request.

        :param env: The WSGI environment for the request.
        :param expires: Unix timestamp as an int for when the URL
                        expires.
        :param key: Key str, from the X-Account-Meta-Temp-URL-Key of
                    the account.
        :param request_method: Optional override of the request in
                               the WSGI env. For example, if a HEAD
                               does not match, you may wish to
                               override with GET to still allow the
                               HEAD.
        :returns: hexdigest str of the HMAC-SHA1 for the request.
        """
        if not request_method:
            request_method = env['REQUEST_METHOD']
        if not uid_sig:
            return hmac.new(key, '%s\n%s\n%s' % (request_method, expires,
                env['PATH_INFO']), sha1).hexdigest()
        else:
            return hmac.new(key, '%s\n%s\n%s\n%s' % (request_method, expires,
                            env['PATH_INFO'],uid_sig), sha1).hexdigest()

    def _invalid(self, env, start_response):
        """
        Performs the necessary steps to indicate a WSGI 401
        Unauthorized response to the request.

        :param env: The WSGI environment for the request.
        :param start_response: The WSGI start_response hook.
        :returns: 401 response as per WSGI.
        """
        self._log_request(env, HTTP_UNAUTHORIZED)
        body = '401 Unauthorized: Temp URL invalid\n'
        start_response('401 Unauthorized',
            [('Content-Type', 'text/plain'),
             ('Content-Length', str(len(body)))])
        if env['REQUEST_METHOD'] == 'HEAD':
            return []
        return [body]

    def _clean_incoming_headers(self, env):
        """
        Removes any headers from the WSGI environment as per the
        middleware configuration for incoming requests.

        :param env: The WSGI environment for the request.
        """
        for h in env.keys():
            remove = h in self.incoming_remove_headers
            if not remove:
                for p in self.incoming_remove_headers_startswith:
                    if h.startswith(p):
                        remove = True
                        break
            if remove:
                if h in self.incoming_allow_headers:
                    remove = False
            if remove:
                for p in self.incoming_allow_headers_startswith:
                    if h.startswith(p):
                        remove = False
                        break
            if remove:
                del env[h]
    def _get_request_info(self,env):
        add_share_req = None
        remove_share_req = None
        temp_key = None
        user_id = None
        if "HTTP_X_ACCOUNT_META_TEMP_URL_KEY" in env.keys():
            temp_key = env["HTTP_X_ACCOUNT_META_TEMP_URL_KEY"]
        else:
            return add_share_req,remove_share_req,temp_key,user_id
        if "HTTP_USER_FOR_SHARE_ADD" in env.keys():
            add_share_req = True
            user_id = env["HTTP_USER_FOR_SHARE_ADD"]
        if "HTTP_USER_FOR_SHARE_REMOVE" in env.keys():
            remove_share_req = True
            user_id = env["HTTP_USER_FOR_SHARE_REMOVE"]
            
        return add_share_req,remove_share_req,temp_key,user_id
            
    def _add_id(self,env,start_response,user_id):
        body = ''
        memcache = env.get('swift.cache')
        if not memcache:
            start_response('500 Internal Server Error',
                [('User-For-Share-Remove-Add','Unsuccessful'),('Content-Type', 'text/plain'),
                 ('Content-Length', str(len(body)))])
            return []
        uid = uuid.uuid4().hex
        memcache.set(env['PATH_INFO']+'/'+user_id, uid)
        start_response('200 OK',
            [('User-For-Share-Add-Response',uid),('Content-Type', 'text/plain'),
             ('Content-Length', str(len(body)))])
        return []
    def _remove_id(self,env, start_response,user_id):
        memcache = env.get('swift.cache')
        body = ''
        if memcache:
            memcache.delete(env['PATH_INFO']+'/'+user_id)
            start_response('200 OK',
                [('User-For-Share-Remove-Response','Successful'),('Content-Type', 'text/plain'),
                 ('Content-Length', str(len(body)))])
            return []
        else:
            start_response('500 Internal Server Error',
                [('User-For-Share-Remove-Response','Unsuccessful'),('Content-Type', 'text/plain'),
                 ('Content-Length', str(len(body)))])
            return []

    
    def _isValid_uid(self,env,user_id):
            memcache = env.get('swift.cache')
            uid = memcache.get(env['PATH_INFO']+'/'+user_id)
            return uid
        
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
        headers = dict(headers)
        for h in headers.keys():
            remove = h in self.outgoing_remove_headers
            if not remove:
                for p in self.outgoing_remove_headers_startswith:
                    if h.startswith(p):
                        remove = True
                        break
            if remove:
                if h in self.outgoing_allow_headers:
                    remove = False
            if remove:
                for p in self.outgoing_allow_headers_startswith:
                    if h.startswith(p):
                        remove = False
                        break
            if remove:
                del headers[h]
        return headers.items()

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
            (env.get('HTTP_USER_AGENT') or '-') + ' TempURL',
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
    return lambda app: TempURL(app, conf)
