==============
Authentication
==============

The owner of an Object Storage account controls access to that account
and its containers and objects. An owner is the user who has the
''admin'' role for that tenant. The tenant is also known as the project
or account. As the account owner, you can modify account metadata and
create, modify, and delete containers and objects.

To identify yourself as the account owner, include an authentication
token in the ''X-Auth-Token'' header in the API request.

Depending on the token value in the ''X-Auth-Token'' header, one of the
following actions occur:

-  ''X-Auth-Token'' contains the token for the account owner.

   The request is permitted and has full access to make changes to the
   account.

-  The ''X-Auth-Token'' header is omitted or it contains a token for a
   non-owner or a token that is not valid.

   The request fails with a 401 Unauthorized or 403 Forbidden response.

   You have no access to accounts or containers, unless an access
   control list (ACL) explicitly grants access.

   The account owner can grant account and container access to users
   through access control lists (ACLs).

In addition, it is possible to provide an additional token in the
''X-Service-Token'' header. More information about how this is used is in
:doc:`../overview_backing_store`.

The following list describes the authentication services that you can
use with Object Storage:

- OpenStack Identity (keystone): For Object Storage, account is synonymous with
  project or tenant ID.

- Tempauth middleware: Object Storage includes this middleware. User and account
  management is performed in Object Storage itself.

- Swauth middleware: Stored in github, this custom middleware is modeled on 
  Tempauth. Usage is similar to Tempauth.

- Other custom middleware: Write it yourself to fit your environment.

Specifically, you use the ''X-Auth-Token'' header to pass an
authentication token to an API request.

Authentication tokens expire after a time period that the authentication
service defines. When a token expires, use of the token causes requests
to fail with a 401 Unauthorized response. To continue, you must obtain a
new token.

