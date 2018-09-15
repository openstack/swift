=================================================
Page through large lists of containers or objects
=================================================

If you have a large number of containers or objects, you can use the
``marker``, ``limit``, and ``end_marker`` parameters to control
how many items are returned in a list and where the list starts or ends.

* marker
    When you request a list of containers or objects, Object Storage
    returns a maximum of 10,000 names for each request. To get
    subsequent names, you must make another request with the
    ``marker`` parameter. Set the ``marker`` parameter to the name of
    the last item returned in the previous list. You must URL-encode the
    ``marker`` value before you send the HTTP request. Object Storage
    returns a maximum of 10,000 names starting after the last item
    returned.

* limit
    To return fewer than 10,000 names, use the ``limit`` parameter. If
    the number of names returned equals the specified ``limit`` (or
    10,000 if you omit the ``limit`` parameter), you can assume there
    are more names to list. If the number of names in the list is
    exactly divisible by the ``limit`` value, the last request has no
    content.

* end_marker
    Limits the result set to names that are less than the
    ``end_marker`` parameter value. You must URL-encode the
    ``end_marker`` value before you send the HTTP request.

To page through a large list of containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Assume the following list of container names:

.. code-block:: console

   apples
   bananas
   kiwis
   oranges
   pears

#. Use a ``limit`` of two:

   .. code-block:: console

      # curl -i $publicURL/?limit=2 -X GET -H "X-Auth-Token: $token"

   .. code-block:: console

      apples
      bananas

   Because two container names are returned, there are more names to
   list.

#. Make another request with a ``marker`` parameter set to the name of
   the last item returned:

   .. code-block:: console

      # curl -i $publicURL/?limit=2&amp;marker=bananas -X GET -H \
        “X-Auth-Token: $token"

   .. code-block:: console

      kiwis
      oranges

   Again, two items are returned, and there might be more.

#. Make another request with a ``marker`` of the last item returned:

   .. code-block:: console

      # curl -i $publicURL/?limit=2&amp;marker=oranges -X GET -H \"
        X-Auth-Token: $token"

   .. code-block:: console

      pears

   You receive a one-item response, which is fewer than the ``limit``
   number of names. This indicates that this is the end of the list.

#. Use the ``end_marker`` parameter to limit the result set to object
   names that are less than the ``end_marker`` parameter value:

   .. code-block:: console

      # curl -i $publicURL/?end_marker=oranges -X GET -H \"
       X-Auth-Token: $token"

   .. code-block:: console

      apples
      bananas
      kiwis

   You receive a result set of all container names before the
   ``end-marker`` value.
