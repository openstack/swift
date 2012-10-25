==========================
Auth Server and Middleware
==========================

--------------------------------------------
Creating Your Own Auth Server and Middleware
--------------------------------------------

The included swift/common/middleware/tempauth.py is a good example of how to
create an auth subsystem with proxy server auth middleware. The main points are
that the auth middleware can reject requests up front, before they ever get to
the Swift Proxy application, and afterwards when the proxy issues callbacks to
verify authorization.

It's generally good to separate the authentication and authorization
procedures. Authentication verifies that a request actually comes from who it
says it does. Authorization verifies the 'who' has access to the resource(s)
the request wants.

Authentication is performed on the request before it ever gets to the Swift
Proxy application. The identity information is gleaned from the request,
validated in some way, and the validation information is added to the WSGI
environment as needed by the future authorization procedure. What exactly is
added to the WSGI environment is solely dependent on what the installed
authorization procedures need; the Swift Proxy application itself needs no
specific information, it just passes it along. Convention has
environ['REMOTE_USER'] set to the authenticated user string but often more
information is needed than just that.

The included TempAuth will set the REMOTE_USER to a comma separated list of
groups the user belongs to. The first group will be the "user's group", a group
that only the user belongs to. The second group will be the "account's group",
a group that includes all users for that auth account (different than the
storage account). The third group is optional and is the storage account
string. If the user does not have admin access to the account, the third group
will be omitted.

It is highly recommended that authentication server implementers prefix their
tokens and Swift storage accounts they create with a configurable reseller
prefix (`AUTH_` by default with the included TempAuth). This prefix will avoid
conflicts with other authentication servers that might be using the same
Swift cluster. Otherwise, the Swift cluster will have to try all the resellers
until one validates a token or all fail.

A restriction with group names is that no group name should begin with a period
'.' as that is reserved for internal Swift use (such as the .r for referrer
designations as you'll see later).

Example Authentication with TempAuth:

    * Token AUTH_tkabcd is given to the TempAuth middleware in a request's
      X-Auth-Token header.
    * The TempAuth middleware validates the token AUTH_tkabcd and discovers
      it matches the "tester" user within the "test" account for the storage
      account "AUTH_storage_xyz".
    * The TempAuth middleware sets the REMOTE_USER to
      "test:tester,test,AUTH_storage_xyz"
    * Now this user will have full access (via authorization procedures later)
      to the AUTH_storage_xyz Swift storage account and access to containers in
      other storage accounts, provided the storage account begins with the same
      `AUTH_` reseller prefix and the container has an ACL specifying at least
      one of those three groups.

Authorization is performed through callbacks by the Swift Proxy server to the
WSGI environment's swift.authorize value, if one is set. The swift.authorize
value should simply be a function that takes a Request as an argument and
returns None if access is granted or returns a callable(environ,
start_response) if access is denied. This callable is a standard WSGI callable.
Generally, you should return 403 Forbidden for requests by an authenticated
user and 401 Unauthorized for an unauthenticated request. For example, here's
an authorize function that only allows GETs (in this case you'd probably return
405 Method Not Allowed, but ignore that for the moment).::

    from swift.common.swob import HTTPForbidden, HTTPUnauthorized


    def authorize(req):
        if req.method == 'GET':
            return None
        if req.remote_user:
            return HTTPForbidden(request=req)
        else:
            return HTTPUnauthorized(request=req)

Adding the swift.authorize callback is often done by the authentication
middleware as authentication and authorization are often paired together. But,
you could create separate authorization middleware that simply sets the
callback before passing on the request. To continue our example above::

    from swift.common.swob import HTTPForbidden, HTTPUnauthorized


    class Authorization(object):

        def __init__(self, app, conf):
            self.app = app
            self.conf = conf

        def __call__(self, environ, start_response):
            environ['swift.authorize'] = self.authorize
            return self.app(environ, start_response)

        def authorize(self, req):
            if req.method == 'GET':
                return None
            if req.remote_user:
                return HTTPForbidden(request=req)
            else:
                return HTTPUnauthorized(request=req)


    def filter_factory(global_conf, **local_conf):
        conf = global_conf.copy()
        conf.update(local_conf)
        def auth_filter(app):
            return Authorization(app, conf)
        return auth_filter

The Swift Proxy server will call swift.authorize after some initial work, but
before truly trying to process the request. Positive authorization at this
point will cause the request to be fully processed immediately. A denial at
this point will immediately send the denial response for most operations.

But for some operations that might be approved with more information, the
additional information will be gathered and added to the WSGI environment and
then swift.authorize will be called once more. These are called delay_denial
requests and currently include container read requests and object read and
write requests. For these requests, the read or write access control string
(X-Container-Read and X-Container-Write) will be fetched and set as the 'acl'
attribute in the Request passed to swift.authorize.

The delay_denial procedures allow skipping possibly expensive access control
string retrievals for requests that can be approved without that information,
such as administrator or account owner requests.

To further our example, we now will approve all requests that have the access
control string set to same value as the authenticated user string. Note that
you probably wouldn't do this exactly as the access control string represents a
list rather than a single user, but it'll suffice for this example::

    from swift.common.swob import HTTPForbidden, HTTPUnauthorized


    class Authorization(object):

        def __init__(self, app, conf):
            self.app = app
            self.conf = conf

        def __call__(self, environ, start_response):
            environ['swift.authorize'] = self.authorize
            return self.app(environ, start_response)

        def authorize(self, req):
            # Allow anyone to perform GET requests
            if req.method == 'GET':
                return None
            # Allow any request where the acl equals the authenticated user
            if getattr(req, 'acl', None) == req.remote_user:
                return None
            if req.remote_user:
                return HTTPForbidden(request=req)
            else:
                return HTTPUnauthorized(request=req)


    def filter_factory(global_conf, **local_conf):
        conf = global_conf.copy()
        conf.update(local_conf)
        def auth_filter(app):
            return Authorization(app, conf)
        return auth_filter

The access control string has a standard format included with Swift, though
this can be overridden if desired. The standard format can be parsed with
swift.common.middleware.acl.parse_acl which converts the string into two arrays
of strings: (referrers, groups). The referrers allow comparing the request's
Referer header to control access. The groups allow comparing the
request.remote_user (or other sources of group information) to control access.
Checking referrer access can be accomplished by using the
swift.common.middleware.acl.referrer_allowed function. Checking group access is
usually a simple string comparison.

Let's continue our example to use parse_acl and referrer_allowed. Now we'll
only allow GETs after a referrer check and any requests after a group check::

    from swift.common.middleware.acl import parse_acl, referrer_allowed
    from swift.common.swob import HTTPForbidden, HTTPUnauthorized


    class Authorization(object):

        def __init__(self, app, conf):
            self.app = app
            self.conf = conf

        def __call__(self, environ, start_response):
            environ['swift.authorize'] = self.authorize
            return self.app(environ, start_response)

        def authorize(self, req):
            if hasattr(req, 'acl'):
                referrers, groups = parse_acl(req.acl)
                if req.method == 'GET' and referrer_allowed(req, referrers):
                    return None
                if req.remote_user and groups and req.remote_user in groups:
                    return None
            if req.remote_user:
                return HTTPForbidden(request=req)
            else:
                return HTTPUnauthorized(request=req)


    def filter_factory(global_conf, **local_conf):
        conf = global_conf.copy()
        conf.update(local_conf)
        def auth_filter(app):
            return Authorization(app, conf)
        return auth_filter

The access control strings are set with PUTs and POSTs to containers
with the X-Container-Read and X-Container-Write headers. Swift allows
these strings to be set to any value, though it's very useful to
validate that the strings meet the desired format and return a useful
error to the user if they don't.

To support this validation, the Swift Proxy application will call the WSGI
environment's swift.clean_acl callback whenever one of these headers is to be
written. The callback should take a header name and value as its arguments. It
should return the cleaned value to save if valid or raise a ValueError with a
reasonable error message if not.

There is an included swift.common.middleware.acl.clean_acl that validates the
standard Swift format. Let's improve our example by making use of that::

    from swift.common.middleware.acl import \
        clean_acl, parse_acl, referrer_allowed
    from swift.common.swob import HTTPForbidden, HTTPUnauthorized


    class Authorization(object):

        def __init__(self, app, conf):
            self.app = app
            self.conf = conf

        def __call__(self, environ, start_response):
            environ['swift.authorize'] = self.authorize
            environ['swift.clean_acl'] = clean_acl
            return self.app(environ, start_response)

        def authorize(self, req):
            if hasattr(req, 'acl'):
                referrers, groups = parse_acl(req.acl)
                if req.method == 'GET' and referrer_allowed(req, referrers):
                    return None
                if req.remote_user and groups and req.remote_user in groups:
                    return None
            if req.remote_user:
                return HTTPForbidden(request=req)
            else:
                return HTTPUnauthorized(request=req)


    def filter_factory(global_conf, **local_conf):
        conf = global_conf.copy()
        conf.update(local_conf)
        def auth_filter(app):
            return Authorization(app, conf)
        return auth_filter

Now, if you want to override the format for access control strings you'll have
to provide your own clean_acl function and you'll have to do your own parsing
and authorization checking for that format. It's highly recommended you use the
standard format simply to support the widest range of external tools, but
sometimes that's less important than meeting certain ACL requirements.


----------------------------
Integrating With repoze.what
----------------------------

Here's an example of integration with repoze.what, though honestly I'm no
repoze.what expert by any stretch; this is just included here to hopefully give
folks a start on their own code if they want to use repoze.what::

    from time import time

    from eventlet.timeout import Timeout
    from repoze.what.adapters import BaseSourceAdapter
    from repoze.what.middleware import setup_auth
    from repoze.what.predicates import in_any_group, NotAuthorizedError
    from swift.common.bufferedhttp import http_connect_raw as http_connect
    from swift.common.middleware.acl import clean_acl, parse_acl, referrer_allowed
    from swift.common.utils import cache_from_env, split_path
    from swift.common.swob import HTTPForbidden, HTTPUnauthorized


    class DevAuthorization(object):

        def __init__(self, app, conf):
            self.app = app
            self.conf = conf

        def __call__(self, environ, start_response):
            environ['swift.authorize'] = self.authorize
            environ['swift.clean_acl'] = clean_acl
            return self.app(environ, start_response)

        def authorize(self, req):
            version, account, container, obj = split_path(req.path, 1, 4, True)
            if not account:
                return self.denied_response(req)
            referrers, groups = parse_acl(getattr(req, 'acl', None))
            if referrer_allowed(req, referrers):
                return None
            try:
                in_any_group(account, *groups).check_authorization(req.environ)
            except NotAuthorizedError:
                return self.denied_response(req)
            return None

        def denied_response(self, req):
            if req.remote_user:
                return HTTPForbidden(request=req)
            else:
                return HTTPUnauthorized(request=req)


    class DevIdentifier(object):

        def __init__(self, conf):
            self.conf = conf

        def identify(self, env):
            return {'token':
                    env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))}

        def remember(self, env, identity):
            return []

        def forget(self, env, identity):
            return []


    class DevAuthenticator(object):

        def __init__(self, conf):
            self.conf = conf
            self.auth_host = conf.get('ip', '127.0.0.1')
            self.auth_port = int(conf.get('port', 11000))
            self.ssl = \
                conf.get('ssl', 'false').lower() in ('true', 'on', '1', 'yes')
            self.auth_prefix = conf.get('prefix', '/')
            self.timeout = int(conf.get('node_timeout', 10))

        def authenticate(self, env, identity):
            token = identity.get('token')
            if not token:
                return None
            memcache_client = cache_from_env(env)
            key = 'devauth/%s' % token
            cached_auth_data = memcache_client.get(key)
            if cached_auth_data:
                start, expiration, user = cached_auth_data
                if time() - start <= expiration:
                    return user
            with Timeout(self.timeout):
                conn = http_connect(self.auth_host, self.auth_port, 'GET',
                        '%stoken/%s' % (self.auth_prefix, token), ssl=self.ssl)
                resp = conn.getresponse()
                resp.read()
                conn.close()
            if resp.status == 204:
                expiration = float(resp.getheader('x-auth-ttl'))
                user = resp.getheader('x-auth-user')
                memcache_client.set(key, (time(), expiration, user),
                                    timeout=expiration)
                return user
            return None


    class DevChallenger(object):

        def __init__(self, conf):
            self.conf = conf

        def challenge(self, env, status, app_headers, forget_headers):
            def no_challenge(env, start_response):
                start_response(str(status), [])
                return []
            return no_challenge


    class DevGroupSourceAdapter(BaseSourceAdapter):

        def __init__(self, *args, **kwargs):
            super(DevGroupSourceAdapter, self).__init__(*args, **kwargs)
            self.sections = {}

        def _get_all_sections(self):
            return self.sections

        def _get_section_items(self, section):
            return self.sections[section]

        def _find_sections(self, credentials):
            return credentials['repoze.what.userid'].split(',')

        def _include_items(self, section, items):
            self.sections[section] |= items

        def _exclude_items(self, section, items):
            for item in items:
                self.sections[section].remove(item)

        def _item_is_included(self, section, item):
            return item in self.sections[section]

        def _create_section(self, section):
            self.sections[section] = set()

        def _edit_section(self, section, new_section):
            self.sections[new_section] = self.sections[section]
            del self.sections[section]

        def _delete_section(self, section):
            del self.sections[section]

        def _section_exists(self, section):
            return self.sections.has_key(section)


    class DevPermissionSourceAdapter(BaseSourceAdapter):

        def __init__(self, *args, **kwargs):
            super(DevPermissionSourceAdapter, self).__init__(*args, **kwargs)
            self.sections = {}

        def _get_all_sections(self):
            return self.sections

        def _get_section_items(self, section):
            return self.sections[section]

        def _find_sections(self, group_name):
            return set([n for (n, p) in self.sections.items()
                        if group_name in p])

        def _include_items(self, section, items):
            self.sections[section] |= items

        def _exclude_items(self, section, items):
            for item in items:
                self.sections[section].remove(item)

        def _item_is_included(self, section, item):
            return item in self.sections[section]

        def _create_section(self, section):
            self.sections[section] = set()

        def _edit_section(self, section, new_section):
            self.sections[new_section] = self.sections[section]
            del self.sections[section]

        def _delete_section(self, section):
            del self.sections[section]

        def _section_exists(self, section):
            return self.sections.has_key(section)


    def filter_factory(global_conf, **local_conf):
        conf = global_conf.copy()
        conf.update(local_conf)
        def auth_filter(app):
            return setup_auth(DevAuthorization(app, conf),
                group_adapters={'all_groups': DevGroupSourceAdapter()},
                permission_adapters={'all_perms': DevPermissionSourceAdapter()},
                identifiers=[('devauth', DevIdentifier(conf))],
                authenticators=[('devauth', DevAuthenticator(conf))],
                challengers=[('devauth', DevChallenger(conf))])
        return auth_filter

-----------------------
Allowing CORS with Auth
-----------------------

Cross Origin RequestS require that the auth system allow the OPTIONS method to 
pass through without a token.  The preflight request will make an OPTIONS call 
against the object or container and will not work if the auth system stops it.
See TempAuth for an example of how OPTIONS requests are handled.
