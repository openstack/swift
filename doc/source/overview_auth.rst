===============
The Auth System
===============

--------------
Developer Auth
--------------

The auth system for Swift is based on the auth system from the existing
Rackspace architecture -- actually from a few existing auth systems --
and is therefore a bit disjointed. The distilled points about it are:

* The authentication/authorization part is outside Swift itself
* The user of Swift passes in an auth token with each request
* Swift validates each token with the external auth system and caches the
  result
* The token does not change from request to request, but does expire

The token can be passed into Swift using the X-Auth-Token or the
X-Storage-Token header. Both have the same format: just a simple string
representing the token. Some external systems use UUID tokens, some an MD5 hash
of something unique, some use "something else" but the salient point is that
the token is a string which can be sent as-is back to the auth system for
validation.

An auth call is given the auth token and the Swift account hash. For a valid
token, the auth system responds with a session TTL and overall expiration in
seconds from now. Swift does not honor the session TTL but will cache the
token up to the expiration time. Tokens can be purged through a call to the
auth system.

The user starts a session by sending a ReST request to that auth system
to receive the auth token and a URL to the Swift system.

--------------
Extending Auth
--------------

Auth is written as wsgi middleware, so implementing your own auth is as easy
as writing new wsgi middleware, and plugging it in to the proxy server.

The current middleware is implemented in the DevAuthMiddleware class in
swift/common/auth.py, and should be a good starting place for implemeting
your own auth.

------------------
History and Future
------------------

What's established in Swift for authentication/authorization has history from
before Swift, so that won't be recorded here.
