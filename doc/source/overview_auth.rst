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

The included TempAuth also has the concept of admin and non-admin users within
an account. Admin users can do anything within the account. Non-admin users can
only perform operations per container based on the container's X-Container-Read
and X-Container-Write ACLs. For more information on ACLs, see
:mod:`swift.common.middleware.acl`.

Additionally, if the auth system sets the request environ's swift_owner key to
True, the proxy will return additional header information in some requests,
such as the X-Container-Sync-Key for a container GET or HEAD.

TempAuth will now allow OPTIONS requests to go through without a token.

The user starts a session by sending a ReST request to the auth system to
receive the auth token and a URL to the Swift system.

-------------
Keystone Auth
-------------

Swift is able to authenticate against OpenStack keystone via the
:mod:`swift.common.middleware.keystoneauth` middleware.

In order to use the ``keystoneauth`` middleware the ``authtoken``
middleware from python-keystoneclient will need to be configured.

The ``authtoken`` middleware performs the authentication token
validation and retrieves actual user authentication information. It
can be found in the python-keystoneclient distribution.

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
  paste.filter_factory = keystoneclient.middleware.auth_token:filter_factory
  auth_host = keystonehost
  auth_port = 35357
  auth_protocol = http
  auth_uri = http://keystonehost:5000/
  admin_tenant_name = service
  admin_user = swift
  admin_password = password

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

--------------
Extending Auth
--------------

TempAuth is written as wsgi middleware, so implementing your own auth is as
easy as writing new wsgi middleware, and plugging it in to the proxy server.
The KeyStone project and the Swauth project are examples of additional auth
services.

Also, see :doc:`development_auth`.
