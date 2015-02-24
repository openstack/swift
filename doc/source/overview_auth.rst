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

Swift is able to authenticate against OpenStack Keystone_ via the
:ref:`keystoneauth` middleware.

In order to use the ``keystoneauth`` middleware the ``auth_token``
middleware from KeystoneMiddleware_ will need to be configured.

The ``authtoken`` middleware performs the authentication token
validation and retrieves actual user authentication information. It
can be found in the KeystoneMiddleware_ distribution.

The :ref:`keystoneauth` middleware performs authorization and mapping the
Keystone roles to Swift's ACLs.

.. _KeystoneMiddleware: http://docs.openstack.org/developer/keystonemiddleware/
.. _Keystone: http://docs.openstack.org/developer/keystone/

Configuring Swift to use Keystone
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuring Swift to use Keystone_
is relatively straight forward.  The first
step is to ensure that you have the ``auth_token`` middleware installed. It can
either be dropped in your python path or installed via the KeystoneMiddleware_
package.

You need at first make sure you have a service endpoint of type
``object-store`` in Keystone pointing to your Swift proxy. For example
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
  identity_uri = http://keystonehost:35357/
  admin_tenant_name = service
  admin_user = swift
  admin_password = password
  auth_uri = http://keystonehost:5000/
  cache = swift.cache
  include_service_catalog = False
  delay_auth_decision = True

The actual values for these variables will need to be set depending on
your situation, but in short:

* ``identity_uri`` points to the Keystone Admin service. This information is
  used by the middleware to actually query Keystone about the validity of the
  authentication tokens. It is not necessary to append any Keystone API version
  number to this URI.
* The admin auth credentials (``admin_user``, ``admin_tenant_name``,
  ``admin_password``) will be used to retrieve an admin token. That
  token will be used to authorize user tokens behind the scenes.
* ``auth_uri`` should point to a Keystone service from which users may
  retrieve tokens. This value is used in the `WWW-Authenticate` header that
  auth_token sends with any denial response.
* ``cache`` is set to ``swift.cache``. This means that the middleware
  will get the Swift memcache from the request environment.
* ``include_service_catalog`` defaults to ``True`` if not set. This means
  that when validating a token, the service catalog is retrieved
  and stored in the ``X-Service-Catalog`` header. Since Swift does not
  use the ``X-Service-Catalog`` header, there is no point in getting
  the service catalog. We recommend you set ``include_service_catalog``
  to ``False``.


.. note::

    The authtoken config variable ``delay_auth_decision`` must be set to
    ``True``. The default is ``False``, but that breaks public access,
    :ref:`staticweb`, :ref:`formpost`, :ref:`tempurl`, and authenticated
    capabilities requests (using :ref:`discoverability`).

and you can finally add the keystoneauth configuration. Here is a simple
configuration::

  [filter:keystoneauth]
  use = egg:swift#keystoneauth
  operator_roles = admin, swiftoperator

Use an appropriate list of roles in operator_roles. For example, in
some systems, the role ``_member_`` or ``Member`` is used to indicate
that the user is allowed to operate on project resources.

OpenStack Service Using Composite Tokens
----------------------------------------

Some Openstack services such as Cinder and Glance may use
a "service account". In this mode, you configure a separate account where
the service stores project data that it manages. This account is not used
directly by the end-user. Instead, all access is done through the service.

To access the "service" account, the service must present two tokens: one from
the end-user and another from its own service user. Only when both tokens are
present can the account be accessed. This section describes how to set the
configuration options to correctly control access to both the "normal" and
"service" accounts.

In this example, end users use the ``AUTH_`` prefix in account names,
whereas services use the ``SERVICE_`` prefix::

  [filter:keystoneauth]
  use = egg:swift#keystoneauth
  reseller_prefix = AUTH, SERVICE
  operator_roles = admin, swiftoperator
  SERVICE_service_roles = service

The actual values for these variable will need to be set depending on your
situation as follows:

* The first item in the reseller_prefix list must match Keystone's endpoint
  (see ``/etc/keystone/default_catalog.templates`` above). Normally
  this is ``AUTH``.
* The second item in the reseller_prefix list is the prefix used by the
  Openstack services(s). You must configure this value (``SERVICE`` in the
  example) with whatever the other Openstack service(s) use.
* Set the operator_roles option to contain a role or roles that end-user's
  have on project's they use.
* Set the SERVICE_service_roles value to a role or roles that only the
  Openstack service user has. Do not use a role that is assigned to
  "normal" end users. In this example, the role ``service`` is used.
  The service user is granted this role to a *single* project only. You do
  not need to make the service user a member of every project.

This configuration works as follows:

* The end-user presents a user token to an Openstack service. The service
  then makes a Swift request to the account with the ``SERVICE`` prefix.
* The service forwards the original user token with the request. It also
  adds it's own service token.
* Swift validates both tokens. When validated, the user token gives the
  ``admin`` or ``swiftoperator`` role(s). When validated, the service token
  gives the ``service`` role.
* Swift interprets the above configuration as follows:
  * Did the user token provide one of the roles listed in operator_roles?
  * Did the service token have the ``service`` role as described by the
    ``SERVICE_service_roles`` options.
* If both conditions are met, the request is granted. Otherwise, Swift
  rejects the request.

In the above example, all services share the same account. You can separate
each service into its own account. For example, the following provides a
dedicated account for each of the Glance and Cinder services. In addition,
you must assign the ``glance_service`` and ``cinder_service`` to the
appropriate service users::

  [filter:keystoneauth]
  use = egg:swift#keystoneauth
  reseller_prefix = AUTH, IMAGE, VOLUME
  operator_roles = admin, swiftoperator
  IMAGE_service_roles = glance_service
  VOLUME_service_roles = cinder_service


Access control using keystoneauth
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default the only users able to perform operations (e.g. create a container)
on an account are those having a Keystone role for the corresponding Keystone
project that matches one of the roles specified in the ``operator_roles``
option.

Users who have one of the ``operator_roles`` will be able to set container ACLs
to grant other users permission to read and/or write objects in specific
containers, using ``X-Container-Read`` and ``X-Container-Write`` headers
respectively. In addition to the ACL formats described
:mod:`here <swift.common.middleware.acl>`, keystoneauth supports ACLs using the
format::

 other_project_id:other_user_id.

where ``other_project_id`` is the UUID of a Keystone project and
``other_user_id`` is the UUID of a Keystone user. This will allow the other
user to access a container provided their token is scoped on the other
project. Both ``other_project_id`` and ``other_user_id`` may be replaced with
the wildcard character ``*`` which will match any project or user respectively.

Be sure to use Keystone UUIDs rather than names in container ACLs.

.. note::

    For backwards compatibility, keystoneauth will by default grant container
    ACLs expressed as ``other_project_name:other_user_name`` (i.e. using
    Keystone names rather than UUIDs) in the special case when both the other
    project and the other user are in Keystone's default domain and the project
    being accessed is also in the default domain.

    For further information see :ref:`keystoneauth`

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
