===============
The Auth System
===============

The auth system for Swift is based on the auth system from an existing
architecture -- actually from a few existing auth systems -- and is therefore a
bit disjointed. The distilled points about it are:

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

The validation call is, for historical reasons, an XMLRPC call. There are two
types of auth systems, type 0 and type 1. With type 0, the XMLRPC call is given
the token and the Swift account name (also known as the account hash because
it's usually of the format <reseller>_<hash>). With type 1, the call is given
the container name and HTTP method as well as the token and account hash. Both
types are also given a service login and password recorded in Swift's
resellers.conf. For a valid token, both auth system types respond with a
session TTL and overall expiration in seconds from now. Swift does not honor
the session TTL but will cache the token up to the expiration time. Tokens can
be purged through a call to Swift's services server.

How the user gets the token to use with Swift is up to the reseller software
itself. For instance, with Cloud Files the user has a starting URL to an auth
system. The user starts a session by sending a ReST request to that auth system
to receive the auth token, a URL to the Swift system, and a URL to the CDN
system.

------------------
History and Future
------------------

What's established in Swift for authentication/authorization has history from
before Swift, so that won't be recorded here. It was minimally integrated with
Swift to meet project deadlines, but in the near future Swift should have a
pluggable auth/reseller system to support the above as well as other
architectures.
