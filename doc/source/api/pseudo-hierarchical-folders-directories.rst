===========================================
Pseudo-hierarchical folders and directories
===========================================

Although you cannot nest directories in OpenStack Object Storage, you
can simulate a hierarchical structure within a single container by
adding forward slash characters (``/``) in the object name. To navigate
the pseudo-directory structure, you can use the ``delimiter`` query
parameter. This example shows you how to use pseudo-hierarchical folders
and directories.

.. note::

   In this example, the objects reside in a container called ``backups``.
   Within that container, the objects are organized in a pseudo-directory
   called ``photos``. The container name is not displayed in the example,
   but it is a part of the object URLs. For instance, the URL of the
   picture ``me.jpg`` is
   ``https://swift.example.com/v1/CF_xer7_343/backups/photos/me.jpg``.

List pseudo-hierarchical folders request: HTTP
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To display a list of all the objects in the storage container, use
``GET`` without a ``delimiter`` or ``prefix``.

.. code-block:: console

   $ curl -X GET -i -H "X-Auth-Token: $token" \
    $publicurl/v1/AccountString/backups

The system returns status code 2xx (between 200 and 299, inclusive) and
the requested list of the objects.

.. code-block:: console

   photos/animals/cats/persian.jpg
   photos/animals/cats/siamese.jpg
   photos/animals/dogs/corgi.jpg
   photos/animals/dogs/poodle.jpg
   photos/animals/dogs/terrier.jpg
   photos/me.jpg
   photos/plants/fern.jpg
   photos/plants/rose.jpg

Use the delimiter parameter to limit the displayed results. To use
``delimiter`` with pseudo-directories, you must use the parameter slash
(``/``).

.. code-block:: console

   $ curl -X GET -i -H "X-Auth-Token: $token" \
    $publicurl/v1/AccountString/backups?delimiter=/

The system returns status code 2xx (between 200 and 299, inclusive) and
the requested matching objects. Because you use the slash, only the
pseudo-directory ``photos/`` displays. The returned values from a slash
``delimiter`` query are not real objects. The value will refer to
a real object if it does not end with a slash. The pseudo-directories
have no content-type, rather, each pseudo-directory has
its own ``subdir`` entry in the response of JSON and XML results.
For example:

.. code-block:: JSON

   [
     {
       "subdir": "photos/"
     }
   ]

.. code-block:: XML

   <?xml version="1.0" encoding="UTF-8"?>
   <container name="backups">
     <subdir name="photos/">
       <name>photos/</name>
     </subdir>
   </container>

Use the ``prefix`` and ``delimiter`` parameters to view the objects
inside a pseudo-directory, including further nested pseudo-directories.

.. code-block:: console

   $ curl -X GET -i -H "X-Auth-Token: $token" \
    $publicurl/v1/AccountString/backups?prefix=photos/&delimiter=/

The system returns status code 2xx (between 200 and 299, inclusive) and
the objects and pseudo-directories within the top level
pseudo-directory.

.. code-block:: console

   photos/animals/
   photos/me.jpg
   photos/plants/

.. code-block:: JSON

   [
     {
       "subdir": "photos/animals/"
     },
     {
       "hash": "b249a153f8f38b51e92916bbc6ea57ad",
       "last_modified": "2015-12-03T17:31:28.187370",
       "bytes": 2906,
       "name": "photos/me.jpg",
       "content_type": "image/jpeg"
     },
     {
       "subdir": "photos/plants/"
     }
   ]

.. code-block:: XML

   <?xml version="1.0" encoding="UTF-8"?>
   <container name="backups">
     <subdir name="photos/animals/">
       <name>photos/animals/</name>
     </subdir>
     <object>
       <name>photos/me.jpg</name>
       <hash>b249a153f8f38b51e92916bbc6ea57ad</hash>
       <bytes>2906</bytes>
       <content_type>image/jpeg</content_type>
       <last_modified>2015-12-03T17:31:28.187370</last_modified>
     </object>
     <subdir name="photos/plants/">
       <name>photos/plants/</name>
     </subdir>
   </container>

You can create an unlimited number of nested pseudo-directories. To
navigate through them, use a longer ``prefix`` parameter coupled with
the ``delimiter`` parameter. In this sample output, there is a
pseudo-directory called ``dogs`` within the pseudo-directory
``animals``. To navigate directly to the files contained within
``dogs``, enter the following command:

.. code-block:: console

   $ curl -X GET -i -H "X-Auth-Token: $token" \
    $publicurl/v1/AccountString/backups?prefix=photos/animals/dogs/&delimiter=/

The system returns status code 2xx (between 200 and 299, inclusive) and
the objects and pseudo-directories within the nested pseudo-directory.

.. code-block:: console

   photos/animals/dogs/corgi.jpg
   photos/animals/dogs/poodle.jpg
   photos/animals/dogs/terrier.jpg
