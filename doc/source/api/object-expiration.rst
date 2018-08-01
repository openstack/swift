=================
Object expiration
=================

You can schedule Object Storage (swift) objects to expire by setting the
``X-Delete-At`` or ``X-Delete-After`` header. Once the object is deleted,
swift will no longer serve the object and it will be deleted from the cluster
shortly thereafter.

*  Set an object to expire at an absolute time (in Unix time). You
   can get the current Unix time by running ``date +'%s'``.

   .. code-block:: console

      $ swift post CONTAINER OBJECT_FILENAME -H "X-Delete-At:UNIX_TIME"

   Verify the ``X-Delete-At`` header has posted to the object:

   .. code-block:: console

      $ swift stat CONTAINER OBJECT_FILENAME

*  Set an object to expire after a relative amount of time (in seconds):

   .. code-block:: console

      $ swift post CONTAINER OBJECT_FILENAME -H "X-Delete-After:SECONDS"

   The ``X-Delete-After`` header will be converted to ``X-Delete-At``.
   Verify the ``X-Delete-At`` header has posted to the object:

   .. code-block:: console

      $ swift stat CONTAINER OBJECT_FILENAME

   If you no longer want to expire the object, you can remove the
   ``X-Delete-At`` header:

   .. code-block:: console

      $ swift post CONTAINER OBJECT_FILENAME -H "X-Remove-Delete-At:"

.. note::

   In order for object expiration to work properly, the
   ``swift-object-expirer`` daemon will need access to all backend
   servers in the cluster. The daemon does not need access to the
   proxy-server or public network.
