===============
The Auth System
===============

--------------
Developer Auth
--------------

The auth system for Swift is loosely based on the auth system from the existing
Rackspace architecture -- actually from a few existing auth systems -- and is
therefore a bit disjointed. The distilled points about it are:

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

Swift will make calls to the external auth system, giving the auth token to be
validated. For a valid token, the auth system responds with an overall
expiration in seconds from now. Swift will cache the token up to the expiration
time. The included devauth also has the concept of admin and non-admin users
within an account. Admin users can do anything within the account. Non-admin
users can only perform operations per container based on the container's
X-Container-Read and X-Container-Write ACLs. For more information on ACLs, see
:mod:`swift.common.middleware.acl`

The user starts a session by sending a ReST request to the external auth system
to receive the auth token and a URL to the Swift system.

--------------
Extending Auth
--------------

Auth is written as wsgi middleware, so implementing your own auth is as easy
as writing new wsgi middleware, and plugging it in to the proxy server.

The current middleware is implemented in the DevAuthMiddleware class in
swift/common/middleware/auth.py, and should be a good starting place for
implementing your own auth.

Also, see :doc:`development_auth`.


------
Swauth
------

The Swauth system is an optional DevAuth replacement included at
swift/common/middleware/swauth.py; a scalable authentication and
authorization system that uses Swift itself as its backing store. This section
will describe how it stores its data.

At the topmost level, the auth system has its own Swift account it stores its
own account information within. This Swift account is known as
self.auth_account in the code and its name is in the format
self.reseller_prefix + ".auth". In this text, we'll refer to this account as
<auth_account>.

The containers whose names do not begin with a period represent the accounts
within the auth service. For example, the <auth_account>/test container would
represent the "test" account.

The objects within each container represent the users for that auth service
account. For example, the <auth_account>/test/bob object would represent the
user "bob" within the auth service account of "test". Each of these user
objects contain a JSON dictionary of the format::

    {"auth": "<auth_type>:<auth_value>", "groups": <groups_array>}

The `<auth_type>` can only be `plaintext` at this time, and the `<auth_value>`
is the plain text password itself.

The `<groups_array>` contains at least two groups. The first is a unique group
identifying that user and it's name is of the format `<user>:<account>`. The
second group is the `<account>` itself. Additional groups of `.admin` for
account administrators and `.reseller_admin` for reseller administrators may
exist. Here's an example user JSON dictionary::

    {"auth": "plaintext:testing",
     "groups": ["name": "test:tester", "name": "test", "name": ".admin"]}

To map an auth service account to a Swift storage account, the Service Account
Id string is stored in the `X-Container-Meta-Account-Id` header for the
<auth_account>/<account> container. To map back the other way, an
<auth_account>/.account_id/<account_id> object is created with the contents of
the corresponding auth service's account name.

Also, to support a future where the auth service will support multiple Swift
clusters or even multiple services for the same auth service account, an
<auth_account>/<account>/.services object is created with its contents having a
JSON dictionary of the format::

    {"storage": {"default": "local", "local": <url>}}

The "default" is always "local" right now, and "local" is always the single
Swift cluster URL; but in the future there can be more than one cluster with
various names instead of just "local", and the "default" key's value will
contain the primary cluster to use for that account. Also, there may be more
services in addition to the current "storage" service right now.

Here's an example .services dictionary at the moment::

    {"storage":
        {"default": "local",
         "local": "http://127.0.0.1:8080/v1/AUTH_8980f74b1cda41e483cbe0a925f448a9"}}

But, here's an example of what the dictionary may look like in the future::

    {"storage":
        {"default": "dfw",
         "dfw": "http://dfw.storage.com:8080/v1/AUTH_8980f74b1cda41e483cbe0a925f448a9",
         "ord": "http://ord.storage.com:8080/v1/AUTH_8980f74b1cda41e483cbe0a925f448a9",
         "sat": "http://ord.storage.com:8080/v1/AUTH_8980f74b1cda41e483cbe0a925f448a9"},
     "servers":
        {"default": "dfw",
         "dfw": "http://dfw.servers.com:8080/v1/AUTH_8980f74b1cda41e483cbe0a925f448a9",
         "ord": "http://ord.servers.com:8080/v1/AUTH_8980f74b1cda41e483cbe0a925f448a9",
         "sat": "http://ord.servers.com:8080/v1/AUTH_8980f74b1cda41e483cbe0a925f448a9"}}

Lastly, the tokens themselves are stored as objects in the
`<auth_account>/.token_[0-f]` containers. The names of the objects are the
token strings themselves, such as `AUTH_tked86bbd01864458aa2bd746879438d5a`.
The exact `.token_[0-f]` container chosen is based on the final digit of the
token name, such as `.token_a` for the token
`AUTH_tked86bbd01864458aa2bd746879438d5a`. The contents of the token objects
are JSON dictionaries of the format::

    {"account": <account>,
     "user": <user>,
     "account_id": <account_id>,
     "groups": <groups_array>,
     "expires": <time.time() value>}

The `<account>` is the auth service account's name for that token. The `<user>`
is the user within the account for that token. The `<account_id>` is the
same as the `X-Container-Meta-Account-Id` for the auth service's account,
as described above. The `<groups_array>` is the user's groups, as described
above with the user object. The "expires" value indicates when the token is no
longer valid, as compared to Python's time.time() value.

Here's an example token object's JSON dictionary::

    {"account": "test",
     "user": "tester",
     "account_id": "AUTH_8980f74b1cda41e483cbe0a925f448a9",
     "groups": ["name": "test:tester", "name": "test", "name": ".admin"],
     "expires": 1291273147.1624689}

To easily map a user to an already issued token, the token name is stored in
the user object's `X-Object-Meta-Auth-Token` header.

Here is an example full listing of an <auth_account>::

    .account_id
        AUTH_2282f516-559f-4966-b239-b5c88829e927
        AUTH_f6f57a3c-33b5-4e85-95a5-a801e67505c8
        AUTH_fea96a36-c177-4ca4-8c7e-b8c715d9d37b
    .token_0
    .token_1
    .token_2
    .token_3
    .token_4
    .token_5
    .token_6
        AUTH_tk9d2941b13d524b268367116ef956dee6
    .token_7
    .token_8
        AUTH_tk93627c6324c64f78be746f1e6a4e3f98
    .token_9
    .token_a
    .token_b
    .token_c
    .token_d
    .token_e
        AUTH_tk0d37d286af2c43ffad06e99112b3ec4e
    .token_f
        AUTH_tk766bbde93771489982d8dc76979d11cf
    reseller
        .services
        reseller
    test
        .services
        tester
        tester3
    test2
        .services
        tester2
