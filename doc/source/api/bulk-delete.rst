.. _bulk-delete:

===========
Bulk delete
===========

To discover whether your Object Storage system supports this feature,
see :ref:`discoverability`. Alternatively, check with your service provider.

With bulk delete, you can delete up to 10,000 objects or containers
(configurable) in one request.

Bulk delete request
~~~~~~~~~~~~~~~~~~~

To perform a bulk delete operation, add the ``bulk-delete`` query
parameter to the path of a ``POST`` or ``DELETE`` operation.

.. note::

   The ``DELETE`` operation is supported for backwards compatibility.

The path is the account, such as ``/v1/12345678912345``, that contains
the objects and containers.

In the request body of the ``POST`` or ``DELETE`` operation, list the
objects or containers to be deleted. Separate each name with a newline
character. You can include a maximum of 10,000 items (configurable) in
the list.

In addition, you must:

-  UTF-8-encode and then URL-encode the names.

-  To indicate an object, specify the container and object name as:
   ``CONTAINER_NAME``/``OBJECT_NAME``.

-  To indicate a container, specify the container name as:
   ``CONTAINER_NAME``. Make sure that the container is empty. If it
   contains objects, Object Storage cannot delete the container.

-  Set the ``Content-Type`` request header to ``text/plain``.

Bulk delete response
~~~~~~~~~~~~~~~~~~~~

When Object Storage processes the request, it performs multiple
sub-operations. Even if all sub-operations fail, the operation returns a
200 status. The bulk operation returns a response body that contains
details that indicate which sub-operations have succeeded and failed.
Some sub-operations might succeed while others fail. Examine the
response body to determine the results of each delete sub-operation.

You can set the ``Accept`` request header to one of the following values
to define the response format:

``text/plain``
   Formats response as plain text. If you omit the
   ``Accept`` header, ``text/plain`` is the default.

``application/json``
   Formats response as JSON.

``application/xml`` or ``text/xml``
   Formats response as XML.

The response body contains the following information:

-  The number of files actually deleted.

-  The number of not found objects.

-  Errors. A list of object names and associated error statuses for the
   objects that failed to delete. The format depends on the value that
   you set in the ``Accept`` header.

The following bulk delete response is in ``application/xml`` format. In
this example, the ``mycontainer`` container is not empty, so it cannot
be deleted.

.. code-block:: xml

   <delete>
       <number_deleted>2</number_deleted>
       <number_not_found>4</number_not_found>
       <errors>
           <object>
               <name>/v1/12345678912345/mycontainer</name>
               <status>409 Conflict</status>
           </object>
       </errors>
   </delete>

