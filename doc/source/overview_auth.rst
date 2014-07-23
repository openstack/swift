===============
The Auth System
===============

--------
TempAuth
--------

The auth system for Swift is loosely based on the auth system from the existing
Rackspace architecture -- actually from a few existing auth systems -- and is
therefore a bit disjointed. The distilled points about it are:

* The authentication/authorization part can be an external system or a
  subsystem run within Swift as WSGI middleware
* The user of Swift passes in an auth token with each request
* Swift validates each token with the external auth system or auth subsystem
  and caches the result
* The token does not change from request to request, but does expire

The token can be passed into Swift using the X-Auth-Token or the
X-Storage-Token header. Both have the same format: just a simple string
representing the token. Some auth systems use UUID tokens, some an MD5 hash of
something unique, some use "something else" but the salient point is that the
token is a string which can be sent as-is back to the auth system for
validation.

Swift will make calls to the auth system, giving the auth token to be
validated. For a valid token, the auth system responds with an overall
expiration in seconds from now. Swift will cache the token up to the expiration
time.

The included TempAuth also has the concept of admin and non-admin users
within an account.  Admin users can do anything within the account.
Non-admin users can only perform operations per container based on the
container's X-Container-Read and X-Container-Write ACLs.  Container ACLs
use the "V1" ACL syntax, which looks like this:
``name1, name2, .r:referrer1.com, .r:-bad.referrer1.com, .rlistings``
For more information on ACLs, see :mod:`swift.common.middleware.acl`.

Additionally, if the auth system sets the request environ's swift_owner key to
True, the proxy will return additional header information in some requests,
such as the X-Container-Sync-Key for a container GET or HEAD.

In addition to container ACLs, TempAuth allows account-level ACLs.  Any auth
system may use the special header ``X-Account-Access-Control`` to specify
account-level ACLs in a format specific to that auth system.  (Following the
TempAuth format is strongly recommended.)  These headers are visible and
settable only by account owners (those for whom ``swift_owner`` is true).
Behavior of account ACLs is auth-system-dependent.  In the case of TempAuth,
if an authenticated user has membership in a group which is listed in the
ACL, then the user is allowed the access level of that ACL.

Account ACLs use the "V2" ACL syntax, which is a JSON dictionary with keys
named "admin", "read-write", and "read-only".  (Note the case sensitivity.)
An example value for the ``X-Account-Access-Control`` header looks like this:
``{"admin":["a","b"],"read-only":["c"]}``  Keys may be absent (as shown).
The recommended way to generate ACL strings is as follows::

  from swift.common.middleware.acl import format_acl
  acl_data = { 'admin': ['alice'], 'read-write': ['bob', 'carol'] }
  acl_string = format_acl(version=2, acl_dict=acl_data)

Using the :func:`format_acl` method will ensure
that JSON is encoded as ASCII (using e.g. '\u1234' for Unicode).  While
it's permissible to manually send ``curl`` commands containing
``X-Account-Access-Control`` headers, you should exercise caution when
doing so, due to the potential for human error.

Within the JSON dictionary stored in ``X-Account-Access-Control``, the keys
have the following meanings:

============   ==============================================================
Access Level   Description
============   ==============================================================
read-only      These identities can read *everything* (except privileged
               headers) in the account.  Specifically, a user with read-only
               account access can get a list of containers in the account,
               list the contents of any container, retrieve any object, and
               see the (non-privileged) headers of the account, any
               container, or any object.
read-write     These identities can read or write (or create) any container.
               A user with read-write account access can create new
               containers, set any unprivileged container headers, overwrite
               objects, delete containers, etc.  A read-write user can NOT
               set account headers (or perform any PUT/POST/DELETE requests
               on the account).
admin          These identities have "swift_owner" privileges.  A user with
               admin account access can do anything the account owner can,
               including setting account headers and any privileged headers
               -- and thus granting read-only, read-write, or admin access
               to other users.
============   ==============================================================


For more details, see :mod:`swift.common.middleware.tempauth`.  For details
on the ACL format, see :mod:`swift.common.middleware.acl`.

Users with the special group ``.reseller_admin`` can operate on any account.
For an example usage please see :mod:`swift.common.middleware.tempauth`.
If a request is coming from a reseller the auth system sets the request environ
reseller_request to True. This can be used by other middlewares.

TempAuth will now allow OPTIONS requests to go through without a token.

The user starts a session by sending a ReST request to the auth system to
receive the auth token and a URL to the Swift system.

-------------
Keystone Auth
-------------

Swift is able to authenticate against OpenStack keystone via the
:mod:`swift.common.middleware.keystoneauth` middleware.

In order to use the ``keystoneauth`` middleware the ``authtoken``
middleware from keystonemiddleware will need to be configured.

The ``authtoken`` middleware performs the authentication token
validation and retrieves actual user authentication information. It
can be found in the keystonemiddleware distribution.

The ``keystoneauth`` middleware performs authorization and mapping the
``keystone`` roles to Swift's ACLs.

Configuring Swift to use Keystone
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuring Swift to use Keystone is relatively straight
forward.  The first step is to ensure that you have the auth_token
middleware installed, distributed with keystone it can either be
dropped in your python path or installed via the keystone package.

You need at first make sure you have a service endpoint of type
``object-store`` in keystone pointing to your Swift proxy. For example
having this in your ``/etc/keystone/default_catalog.templates`` ::

  catalog.RegionOne.object_store.name = Swift Service
  catalog.RegionOne.object_store.publicURL = http://swiftproxy:8080/v1/AUTH_$(tenant_id)s
  catalog.RegionOne.object_store.adminURL = http://swiftproxy:8080/
  catalog.RegionOne.object_store.internalURL = http://swiftproxy:8080/v1/AUTH_$(tenant_id)s

On your Swift Proxy server you will want to adjust your main pipeline
and add auth_token and keystoneauth in your
``/etc/swift/proxy-server.conf`` like this ::

  [pipeline:main]
  pipeline = [....] authtoken keystoneauth proxy-logging proxy-server

add the configuration for the authtoken middleware::

  [filter:authtoken]
  paste.filter_factory = keystonemiddleware.auth_token:filter_factory
  auth_host = keystonehost
  auth_port = 35357
  auth_protocol = http
  auth_uri = http://keystonehost:5000/
  admin_tenant_name = service
  admin_user = swift
  admin_password = password
  cache = swift.cache
  include_service_catalog = False

The actual values for these variables will need to be set depending on
your situation.  For more information, please refer to the Keystone
documentation on the ``auth_token`` middleware, but in short:

* Those variables beginning with ``auth_`` point to the Keystone
  Admin service.  This information is used by the middleware to actually
  query Keystone about the validity of the
  authentication tokens.
* The admin auth credentials (``admin_user``, ``admin_tenant_name``,
  ``admin_password``) will be used to retrieve an admin token. That
  token will be used to authorize user tokens behind the scenes.
* cache is set to ``swift.cache``. This means that the middleware
  will get the Swift memcache from the request environment.
* include_service_catalog defaults to True if not set. This means
  that when validating a token, the service catalog is retrieved
  and stored in the X-Service-Catalog header. Since Swift does not
  use the X-Service-Catalog header, there is no point in getting
  the service catalog. We recommend you set include_service_catalog
  to False.


.. note::

    If support is required for unvalidated users (as with anonymous
    access) or for tempurl/formpost middleware, authtoken will need
    to be configured with delay_auth_decision set to 1.

and you can finally add the keystoneauth configuration::

  [filter:keystoneauth]
  use = egg:swift#keystoneauth
  operator_roles = admin, swiftoperator

By default the only users able to give ACL or to Create other
containers are the ones who has the Keystone role specified in the
``operator_roles`` setting.

This user who have one of those role will be able to give ACLs to
other users on containers, see the documentation on ACL here
:mod:`swift.common.middleware.acl`.

Users with the Keystone role defined in ``reseller_admin_role``
(``ResellerAdmin`` by default) can operate on any account. The auth system
sets the request environ reseller_request to True if a request is coming
from a user with this role. This can be used by other middlewares.

--------------
Extending Auth
--------------

TempAuth is written as wsgi middleware, so implementing your own auth is as
easy as writing new wsgi middleware, and plugging it in to the proxy server.
The KeyStone project and the Swauth project are examples of additional auth
services.

Also, see :doc:`development_auth`.
