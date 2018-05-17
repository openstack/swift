Object Storage API overview
---------------------------

OpenStack Object Storage is a highly available, distributed, eventually
consistent object/blob store. You create, modify, and get objects and
metadata by using the Object Storage API, which is implemented as a set
of Representational State Transfer (REST) web services.

For an introduction to OpenStack Object Storage, see the :doc:`/admin/index`.

You use the HTTPS (SSL) protocol to interact with Object Storage, and
you use standard HTTP calls to perform API operations. You can also use
language-specific APIs, which use the RESTful API, that make it easier
for you to integrate into your applications.

To assert your right to access and change data in an account, you
identify yourself to Object Storage by using an authentication token. To
get a token, you present your credentials to an authentication service.
The authentication service returns a token and the URL for the account.
Depending on which authentication service that you use, the URL for the
account appears in:

-  **OpenStack Identity Service**. The URL is defined in the service
   catalog.

-  **Tempauth**. The URL is provided in the ``X-Storage-Url`` response
   header.

In both cases, the URL is the full URL and includes the account
resource.

The Object Storage API supports the standard, non-serialized response
format, which is the default, and both JSON and XML serialized response
formats.

The Object Storage system organizes data in a hierarchy, as follows:

-  **Account**. Represents the top-level of the hierarchy.

   Your service provider creates your account and you own all resources
   in that account. The account defines a namespace for containers. A
   container might have the same name in two different accounts.

   In the OpenStack environment, *account* is synonymous with a project
   or tenant.

-  **Container**. Defines a namespace for objects. An object with the
   same name in two different containers represents two different
   objects. You can create any number of containers within an account.

   In addition to containing objects, you can also use the container to
   control access to objects by using an access control list (ACL). You
   cannot store an ACL with individual objects.

   In addition, you configure and control many other features, such as
   object versioning, at the container level.

   You can bulk-delete up to 10,000 containers in a single request.

   You can set a storage policy on a container with predefined names
   and definitions from your cloud provider.

-  **Object**. Stores data content, such as documents, images, and so
   on. You can also store custom metadata with an object.

   With the Object Storage API, you can:

   -  Store an unlimited number of objects. Each object can be as large
      as 5 GB, which is the default. You can configure the maximum
      object size.

   -  Upload and store objects of any size with large object creation.

   -  Use cross-origin resource sharing to manage object security.

   -  Compress files using content-encoding metadata.

   -  Override browser behavior for an object using content-disposition metadata.

   -  Schedule objects for deletion.

   -  Bulk-delete up to 10,000 objects in a single request.

   -  Auto-extract archive files.

   -  Generate a URL that provides time-limited **GET** access to an
      object.

   -  Upload objects directly to the Object Storage system from a
      browser by using form **POST** middleware.

   -  Create symbolic links to other objects.

The account, container, and object hierarchy affects the way you
interact with the Object Storage API.

Specifically, the resource path reflects this structure and has this
format:

.. code::

    /v1/{account}/{container}/{object}

For example, for the ``flowers/rose.jpg`` object in the ``images``
container in the ``12345678912345`` account, the resource path is:

.. code::

    /v1/12345678912345/images/flowers/rose.jpg

Notice that the object name contains the ``/`` character. This slash
does not indicate that Object Storage has a sub-hierarchy called
``flowers`` because containers do not store objects in actual
sub-folders. However, the inclusion of ``/`` or a similar convention
inside object names enables you to create pseudo-hierarchical folders
and directories.

For example, if the endpoint for Object Storage is
``objects.mycloud.com``, the returned URL is
``https://objects.mycloud.com/v1/12345678912345``.

To access a container, append the container name to the resource path.

To access an object, append the container and the object name to the
path.

If you have a large number of containers or objects, you can use query
parameters to page through large lists of containers or objects. Use the
``marker``, ``limit``, and ``end_marker`` query parameters to
control how many items are returned in a list and where the list starts
or ends. If you want to page through in reverse order, you can use the query
parameter ``reverse``, noting that your marker and end_markers should be
switched when applied to a reverse listing. I.e, for a list of objects
``[a, b, c, d, e]`` the non-reversed could be:

.. code::

  /v1/{account}/{container}/?marker=a&end_marker=d
  b
  c

However, when reversed marker and end_marker are applied to a reversed list:

.. code::

  /v1/{account}/{container}/?marker=d&end_marker=a&reverse=on
  c
  b

Object Storage HTTP requests have the following default constraints.
Your service provider might use different default values.

============================ ============= =====
Item                         Maximum value Notes
============================ ============= =====
Number of HTTP headers       90
Length of HTTP headers       4096 bytes
Length per HTTP request line 8192 bytes
Length of HTTP request       5 GB
Length of container names    256 bytes     Cannot contain the ``/`` character.
Length of object names       1024 bytes    By default, there are no character restrictions.
============================ ============= =====

You must UTF-8-encode and then URL-encode container and object names
before you call the API binding. If you use an API binding that performs
the URL-encoding for you, do not URL-encode the names before you call
the API binding. Otherwise, you double-encode these names. Check the
length restrictions against the URL-encoded string.

The API Reference describes the operations that you can perform with the
Object Storage API:

-  `Storage
   accounts <https://developer.openstack.org/api-ref/object-store/index.html#accounts>`__:
   Use to perform account-level tasks.

   Lists containers for a specified account. Creates, updates, and
   deletes account metadata. Shows account metadata.

-  `Storage
   containers <https://developer.openstack.org/api-ref/object-store/index.html#containers>`__:
   Use to perform container-level tasks.

   Lists objects in a specified container. Creates, shows details for,
   and deletes containers. Creates, updates, shows, and deletes
   container metadata.

-  `Storage
   objects <https://developer.openstack.org/api-ref/object-store/index.html#objects>`__:
   Use to perform object-level tasks.

   Creates, replaces, shows details for, and deletes objects. Copies
   objects with another object with a new or different name. Updates
   object metadata.
