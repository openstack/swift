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

The user starts a session by sending a ReST request to the auth system to
receive the auth token and a URL to the Swift system.

--------------
Extending Auth
--------------

TempAuth is written as wsgi middleware, so implementing your own auth is as
easy as writing new wsgi middleware, and plugging it in to the proxy server.
The KeyStone project and the Swauth project are examples of additional auth
services.

Also, see :doc:`development_auth`.
