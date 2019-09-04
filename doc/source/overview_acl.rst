
===========================
Access Control Lists (ACLs)
===========================

Normally to create, read and modify containers and objects, you must have the
appropriate roles on the project associated with the account, i.e., you
must be the owner of the account. However, an owner can grant access to
other users by using an Access Control List (ACL).

There are two types of ACLs:

- :ref:`container_acls`. These are specified on a container and
  apply to that container only and the objects in the container.
- :ref:`account_acls`. These are specified at the account level and
  apply to all containers and objects in the account.

.. _container_acls:

--------------
Container ACLs
--------------

Container ACLs are stored in the ``X-Container-Write`` and ``X-Container-Read``
metadata. The scope of the ACL is limited to the container where the
metadata is set and the objects in the container. In addition:

- ``X-Container-Write`` grants the ability to perform PUT, POST and DELETE
  operations on objects within a container. It does not grant the ability
  to perform POST or DELETE operations on the container itself. Some ACL
  elements also grant the ability to perform HEAD or GET operations on the
  container.

- ``X-Container-Read`` grants the ability to perform GET and HEAD
  operations on objects within a container. Some of the ACL elements also grant
  the ability to perform HEAD or GET operations on the container itself.
  However, a container ACL does not allow access to privileged metadata (such
  as ``X-Container-Sync-Key``).

Container ACLs use the "V1" ACL syntax which is a comma separated string
of elements as shown in the following example::

    .r:*,.rlistings,7ec59e87c6584c348b563254aae4c221:*

Spaces may occur between elements as shown in the following example::


    .r : *, .rlistings, 7ec59e87c6584c348b563254aae4c221:*

However, these spaces are removed from the value stored in the
``X-Container-Write`` and ``X-Container-Read`` metadata. In addition,
the ``.r:`` string can be written as ``.referrer:``, but is stored as ``.r:``.

While all auth systems use
the same syntax, the meaning of some elements
is different because of the different concepts used by different
auth systems as explained in the following sections:

- :ref:`acl_common_elements`
- :ref:`acl_keystone_elements`
- :ref:`acl_tempauth_elements`


.. _acl_common_elements:

Common ACL Elements
-------------------

The following table describes elements of an ACL that are
supported by both Keystone auth and TempAuth. These elements
should only be used with ``X-Container-Read`` (with the exception
of ``.rlistings``, an error will occur if used with
``X-Container-Write``):

============================== ================================================
Element                        Description
============================== ================================================
.r:*                           Any user has access to objects. No token is
                               required in the request.
.r:<referrer>                  The referrer is granted access to objects. The
                               referrer is identified by the ``Referer``
                               request header in the request. No token is
                               required.
.r:-<referrer>                 This syntax (with "-" prepended to the
                               referrer) is supported. However, it does not
                               deny access if another element (e.g., ``.r:*``)
                               grants access.
.rlistings                     Any user can perform a HEAD or GET operation
                               on the container provided the user also has
                               read access on objects (e.g., also has ``.r:*``
                               or ``.r:<referrer>``. No token is required.
============================== ================================================

.. _acl_keystone_elements:

Keystone Auth ACL Elements
--------------------------

The following table describes elements of an ACL that are
supported only by Keystone auth. Keystone auth also supports
the elements described in :ref:`acl_common_elements`.

A token must be included in the request for any of these ACL elements
to take effect.

============================== ================================================
Element                        Description
============================== ================================================
<project-id>:<user-id>         The specified user, provided a token
                               scoped to the project is included
                               in the request, is granted access.
                               Access to the container is also granted
                               when used in ``X-Container-Read``.
<project-id>:\*                Any user with a role in the specified Keystone
                               project has access. A token scoped to the
                               project must be included in the request.
                               Access to the container is also granted
                               when used in ``X-Container-Read``.
\*:<user-id>                   The specified user has access. A token
                               for the user (scoped to any
                               project) must be included in the request.
                               Access to the container is also granted
                               when used in ``X-Container-Read``.
\*:\*                          Any user has access.
                               Access to the container is also granted
                               when used in ``X-Container-Read``.
                               The ``*:*`` element differs from the ``.r:*``
                               element because
                               ``*:*`` requires that a valid token is
                               included in the request whereas ``.r:*``
                               does not require a token. In addition,
                               ``.r:*`` does not grant access to the
                               container listing.
<role_name>                    A user with the specified role *name* on the
                               project within which the container is stored is
                               granted access. A user token scoped to the
                               project must be included in the request. Access
                               to the container is also granted when used in
                               ``X-Container-Read``.
============================== ================================================

.. note::

    Keystone project (tenant) or user *names* (i.e.,
    ``<project-name>:<user-name>``) must no longer be
    used because with the introduction
    of domains in Keystone, names are not globally unique. You should
    use user and project *ids* instead.

    For backwards compatibility, ACLs using names will be granted by
    keystoneauth when it can be established that
    the grantee project, the grantee user and the project being
    accessed are either not yet in a domain (e.g. the ``X-Auth-Token`` has
    been obtained via the Keystone V2 API) or are all in the default domain
    to which legacy accounts would have been migrated.


.. _acl_tempauth_elements:

TempAuth ACL Elements
---------------------

The following table describes elements of an ACL that are
supported only by TempAuth. TempAuth auth also supports
the elements described in :ref:`acl_common_elements`.

============================== ================================================
Element                        Description
============================== ================================================
<user-name>                    The named user is granted access. The
                               wildcard ("*") character is not supported.
                               A token from the user must be included in the
                               request.
============================== ================================================

----------------------
Container ACL Examples
----------------------

Container ACLs may be set by including ``X-Container-Write`` and/or
``X-Container-Read`` headers with a PUT or a POST request to the container URL.
The following examples use the ``swift`` command line client which support
these headers being set via its ``--write-acl`` and ``--read-acl`` options.

Example: Public Container
-------------------------

The following allows anybody to list objects in the ``www`` container and
download objects. The users do not need to include a token in
their request. This ACL is commonly referred to as making the
container "public". It is useful when used with :ref:`staticweb`::

    swift post www --read-acl ".r:*,.rlistings"


Example: Shared Writable Container
----------------------------------

The following allows anybody to upload or download objects. However, to
download an object, the exact name of the object must be known since
users cannot list the objects in the container.
The users must include a Keystone token in the upload request. However, it does not
need to be scoped to the project associated with the container::

    swift post www --read-acl ".r:*" --write-acl "*:*"


Example: Sharing a Container with Project Members
-------------------------------------------------

The following allows any member of the ``77b8f82565f14814bece56e50c4c240f``
project to upload and download objects or to list the contents
of the ``www`` container. A token scoped to the ``77b8f82565f14814bece56e50c4c240f``
project must be included in the request::

    swift post www --read-acl "77b8f82565f14814bece56e50c4c240f:*" \
                   --write-acl "77b8f82565f14814bece56e50c4c240f:*"


Example: Sharing a Container with Users having a specified Role
---------------------------------------------------------------

The following allows any user that has been assigned the
``my_read_access_role`` on the project within which the ``www`` container is
stored to download objects or to list the contents of the ``www`` container. A
user token scoped to the project must be included in the download or list
request::

    swift post www --read-acl "my_read_access_role"


Example: Allowing a Referrer Domain to Download Objects
-------------------------------------------------------

The following allows any request from
the ``example.com`` domain to access an object in the container::

    swift post www --read-acl ".r:.example.com"

However, the request from the user **must** contain the appropriate
`Referer` header as shown in this example request::

    curl -i $publicURL/www/document --head -H "Referer: http://www.example.com/index.html"

.. note::

    The `Referer` header is included in requests by many browsers. However,
    since it is easy to create a request with any desired value in the
    `Referer` header, the referrer ACL has very weak security.


Example: Sharing a Container with Another User
----------------------------------------------

Sharing a Container with another user requires the knowledge of few
parameters regarding the users.

The sharing user must know:

- the ``OpenStack user id`` of the other user

The sharing user must communicate to the other user:

- the name of the shared container
- the ``OS_STORAGE_URL``

Usually the ``OS_STORAGE_URL`` is not exposed directly to the user
because the ``swift client`` by default automatically construct the
``OS_STORAGE_URL`` based on the User credential.

We assume that in the current directory there are the two client
environment script for the two users ``sharing.openrc`` and
``other.openrc``.

The ``sharing.openrc`` should be similar to the following:

.. code-block:: bash

    export OS_USERNAME=sharing
    # WARNING: Save the password in clear text only for testing purposes
    export OS_PASSWORD=password
    export OS_TENANT_NAME=projectName
    export OS_AUTH_URL=https://identityHost:portNumber/v2.0
    # The following lines can be omitted
    export OS_TENANT_ID=tenantIDString
    export OS_REGION_NAME=regionName
    export OS_CACERT=/path/to/cacertFile

The ``other.openrc`` should be similar to the following:

.. code-block:: bash

    export OS_USERNAME=other
    # WARNING: Save the password in clear text only for testing purposes
    export OS_PASSWORD=otherPassword
    export OS_TENANT_NAME=otherProjectName
    export OS_AUTH_URL=https://identityHost:portNumber/v2.0
    # The following lines can be omitted
    export OS_TENANT_ID=tenantIDString
    export OS_REGION_NAME=regionName
    export OS_CACERT=/path/to/cacertFile

For more information see `using the OpenStack RC file
<https://docs.openstack.org/user-guide/common/cli-set-environment-variables-using-openstack-rc.html>`_

First we figure out the other user id::

    . other.openrc
    OUID="$(openstack user show --format json "${OS_USERNAME}" | jq -r .id)"

or alternatively::

    . other.openrc
    OUID="$(openstack token issue -f json | jq -r .user_id)"

Then we figure out the storage url of the sharing user::

    sharing.openrc
    SURL="$(swift auth | awk -F = '/OS_STORAGE_URL/ {print $2}')"

Running as the sharing user create a shared container named ``shared``
in read-only mode with the other user using the proper acl::

    sharing.openrc
    swift post --read-acl "*:${OUID}" shared

Running as the sharing user create and upload a test file::

    touch void
    swift upload shared void

Running as the other user list the files in the ``shared`` container::

    other.openrc
    swift --os-storage-url="${SURL}" list shared

Running as the other user download the ``shared`` container in the
``/tmp`` directory::

    cd /tmp
    swift --os-storage-url="${SURL}" download shared


.. _account_acls:

------------
Account ACLs
------------

.. note::

    Account ACLs are not currently supported by Keystone auth

The ``X-Account-Access-Control`` header is used to specify
account-level ACLs in a format specific to the auth system.
These headers are visible and settable only by account owners (those for whom
``swift_owner`` is true).
Behavior of account ACLs is auth-system-dependent.  In the case of TempAuth,
if an authenticated user has membership in a group which is listed in the
ACL, then the user is allowed the access level of that ACL.

Account ACLs use the "V2" ACL syntax, which is a JSON dictionary with keys
named "admin", "read-write", and "read-only".  (Note the case sensitivity.)
An example value for the ``X-Account-Access-Control`` header looks like this,
where ``a``, ``b`` and ``c`` are user names::

   {"admin":["a","b"],"read-only":["c"]}

Keys may be absent (as shown in above example).

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
