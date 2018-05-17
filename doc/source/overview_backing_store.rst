
=============================================
Using Swift as Backing Store for Service Data
=============================================

----------
Background
----------

This section provides guidance to OpenStack Service developers for how to
store your users' data in Swift. An example of this is that a user requests
that Nova save a snapshot of a VM. Nova passes the request to Glance,
Glance writes the image to a Swift container as a set of objects.

Throughout this section, the following terminology and concepts are used:

* User or end-user. This is a person making a request that will result in
  an OpenStack Service making a request to Swift.

* Project (also known as Tenant). This is the unit of resource ownership.
  While data such as snapshot images or block volume backups may be
  stored as a result of an end-user's request, the reality is that these
  are project data.

* Service. This is a program or system used by end-users. Specifically, it
  is any program or system that is capable of receiving end-user's tokens and
  validating the token with the Keystone Service and has a need to store
  data in Swift. Glance and Cinder are examples of such Services.

* Service User. This is a Keystone user that has been assigned to a Service.
  This allows the Service to generate and use its own tokens so that it
  can interact with other Services as itself.

* Service Project. This is a project (tenant) that is associated with a
  Service. There may be a single project shared by many Services or there
  may be a project dedicated to each Service. In this document, the
  main purpose of the Service Project is to allow the system operator
  to configure specific roles for each Service User.

-------------------------------
Alternate Backing Store Schemes
-------------------------------

There are three schemes described here:

* Dedicated Service Account (Single Tenant)

  Your Service has a dedicated Service Project (hence a single dedicated
  Swift account). Data for all users and projects are stored in this
  account. Your Service must have a user assigned to it (the Service User).
  When you have data to store on behalf of one of your users, you use the
  Service User credentials to get a token for the Service Project and
  request Swift to store the data in the Service Project.

  With this scheme, data for all users is stored in a single account. This
  is transparent to your users and since the credentials for the Service User
  are typically not shared with anyone, your users' cannot access their
  data by making a request directly to Swift. However, since data belonging
  to all users is stored in one account, it presents a single point of
  vulnerably to accidental deletion or a leak of the service-user
  credentials.

* Multi Project (Multi Tenant)

  Data belonging to a project is stored in the Swift account
  associated with the project. Users make requests to your Service using
  a token scoped to a project in the normal way. You can then use this
  same token to store the user data in the project's Swift account.

  The effect is that data is stored in multiple projects (aka tenants).
  Hence this scheme has been known as the "multi tenant" scheme.

  With this scheme, access is controlled by Keystone. The users must
  have a role that allows them to perform the request to your Service. In
  addition, they must have a role that also allows them to store data in
  the Swift account. By default, the admin or swiftoperator roles are
  used for this purpose (specific systems may use other role names). If the
  user does not have the appropriate roles, when your Service attempts
  to access Swift, the operation will fail.

  Since you are using the user's token to access the data, it follows that
  the user can use the same token to access Swift directly -- bypassing your
  Service. When end-users are browsing containers, they will also see
  your Service's containers and objects -- and may potentially delete
  the data. Conversely, there is no single account where all data so leakage
  of credentials will only affect a single project/tenant.

* Service Prefix Account

  Data belonging to a project is stored in a Swift account associated
  with the project. This is similar to the Multi Project scheme described
  above. However, the Swift account is different than the account that
  users access. Specifically, it has a different account prefix. For example,
  for the project 1234, the user account is named AUTH_1234. Your Service uses
  a different account, for example, SERVICE_1234.

  To access the SERVICE_1234 account, you must present two tokens: the user's
  token is put in the X-Auth-Token header. You present your Service's token
  in the X-Service-Token header. Swift is configured such that only when both
  tokens are presented will it allow access. Specifically, the user cannot
  bypass your Service because they only have their own token. Conversely, your
  Service can only access the data while it has a copy of the user's token --
  the Service's token by itself will not grant access.

  The data stored in the Service Prefix Account cannot be seen by end-users.
  So they cannot delete this data -- they can only access the data if they
  make a request through your Service. The data is also more secure. To make
  an unauthorized access, someone would need to compromise both an end-user's
  and your Service User credentials. Even then, this would only expose one
  project -- not other projects.

The Service Prefix Account scheme combines features of the Dedicated Service
Account and Multi Project schemes. It has the private, dedicated,
characteristics of the Dedicated Service Account scheme but does not present
a single point of attack. Using the Service Prefix Account scheme is a little
more involved than the other schemes, so the rest of this document describes
it more detail.

-------------------------------
Service Prefix Account Overview
-------------------------------

The following diagram shows the flow through the system from the end-user,
to your Service and then onto Swift::

      client
         \
          \   <request>: <path-specific-to-the-service>
           \  x-auth-token: <user-token>
            \
          SERVICE
             \
              \    PUT: /v1/SERVICE_1234/<container>/<object>
               \   x-auth-token: <user-token>
                \  x-service-token: <service-token>
                 \
                Swift

The sequence of events and actions are as follows:

* Request arrives at your Service

* The <user-token> is validated by the keystonemiddleware.auth_token
  middleware. The user's role(s) are used to determine if the user
  can perform the request. See :doc:`overview_auth` for technical
  information on the authentication system.

* As part of this request, your Service needs to access Swift (either to
  write or read a container or object). In this example, you want to perform
  a PUT on <container>/<object>.

* In the wsgi environment, the auth_token module will have populated the
  HTTP_X_SERVICE_CATALOG item. This lists the Swift endpoint and account.
  This is something such as https://<netloc>/v1/AUTH_1234 where ``AUTH_``
  is a prefix and ``1234`` is the project id.

* The ``AUTH_`` prefix is the default value. However, your system may use a
  different prefix. To determine the actual prefix, search for the first
  underscore ('_') character in the account name. If there is no underscore
  character in the account name, this means there is no prefix.

* Your Service should have a configuration parameter that provides the
  appropriate prefix to use for storing data in Swift. There is more
  discussion of this below, but for now assume the prefix is ``SERVICE_``.

* Replace the prefix (``AUTH_`` in above examples) in the path with
  ``SERVICE_``, so the full URL to access the object becomes
  https://<netloc>/v1/SERVICE_1234/<container>/<object>.

* Make the request to Swift, using this URL. In the X-Auth-Token header place
  a copy of the <user-token>. In the X-Service-Token header, place your
  Service's token. If you use python-swiftclient you can achieve this
  by:

  * Putting the URL in the ``preauthurl`` parameter
  * Putting the <user-token> in ``preauthtoken`` parameter
  * Adding the X-Service-Token to the ``headers`` parameter


Using the HTTP_X_SERVICE_CATALOG to get Swift Account Name
----------------------------------------------------------

The auth_token middleware populates the wsgi environment with information when
it validates the user's token. The HTTP_X_SERVICE_CATALOG item is a JSON
string containing  details of the OpenStack endpoints. For Swift, this also
contains the project's Swift account name. Here is an example of a catalog
entry for Swift::

    "serviceCatalog": [
        ...
        {
            ....
            "type": "object-store",
            "endpoints": [
               ...
               {
                   ...
                   "publicURL": "https://<netloc>/v1/AUTH_1234",
                   "region": "<region-name>"
                   ...
               }
               ...
         ...
         }
    }

To get the End-user's account:

* Look for an entry with ``type`` of ``object-store``

* If there are several regions, there will be several endpoints. Use the
  appropriate region name and select the ``publicURL`` item.

* The Swift account name is the final item in the path ("AUTH_1234" in this
  example).

Getting a Service Token
-----------------------

A Service Token is no different than any other token and is requested
from Keystone using user credentials and project in the usual way. The core
requirement is that your Service User has the appropriate role. In practice:

* Your Service must have a user assigned to it (the Service User).

* Your Service has a project assigned to it (the Service Project).

* The Service User must have a role on the Service Project. This role is
  distinct from any of the normal end-user roles.

* The role used must the role configured in the /etc/swift/proxy-server.conf.
  This is the ``<prefix>_service_roles`` option. In this example, the role
  is the ``service`` role::

    [keystoneauth]
    reseller_prefix = AUTH_, SERVICE_
    SERVICE_service_role = service

The ``service`` role should only be granted to OpenStack Services. It should
not be granted to users.

Single or multiple Service Prefixes?
------------------------------------

Most of the examples used in this document used a single prefix. The
prefix, ``SERVICE`` was used. By using a single prefix, an operator is
allowing all OpenStack Services to share the same account for data
associated with a given project. For test systems or deployments well protected
on private firewalled networks, this is appropriate.

However, if one Service is compromised, that Service can access
data created by another Service. To prevent this, multiple Service Prefixes may
be used. This also requires that the operator configure multiple service
roles. For example, in a system that has Glance and Cinder, the following
Swift configuration could be used::

    [keystoneauth]
    reseller_prefix = AUTH_, IMAGE_, BLOCK_
    IMAGE_service_roles = image_service
    BLOCK_service_roles = block_service

The Service User for Glance would be granted the ``image_service`` role on its
Service Project and the Cinder Service user is granted the ``block_service``
role on its project. In this scheme, if the Cinder Service was compromised,
it would not be able to access any Glance data.

Container Naming
----------------

Since a single Service Prefix is possible, container names should be prefixed
with a unique string to prevent name clashes. We suggest you use the service
type field (as used in the service catalog). For example, The Glance Service
would use "image" as a prefix.
