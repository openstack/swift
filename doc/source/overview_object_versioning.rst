=================
Object Versioning
=================

--------
Overview
--------

Object versioning in swift is implemented by setting a flag on the container
to tell swift to version all objects in the container. The flag is the
``X-Versions-Location`` header on the container, and its value is the
container where the versions are stored. It is recommended to use a different
``X-Versions-Location`` container for each container that is being versioned.

When data is ``PUT`` into a versioned container (a container with the
versioning flag turned on), the existing data in the file is redirected to a
new object and the data in the ``PUT`` request is saved as the data for the
versioned object. The new object name (for the previous version) is 
``<versions_container>/<length><object_name>/<timestamp>``, where ``length``
is the 3-character zero-padded hexidecimal length of the ``<object_name>`` and
``<timestamp>`` is the timestamp of when the previous version was created.

A ``GET`` to a versioned object will return the current version of the object
without having to do any request redirects or metadata lookups.

A ``POST`` to a versioned object will update the object metadata as normal,
but will not create a new version of the object. In other words, new versions
are only created when the content of the object changes.

A ``DELETE`` to a versioned object will only remove the current version of the
object. If you have 5 total versions of the object, you must delete the
object 5 times to completely remove the object.

Note: A large object manifest file cannot be versioned, but a large object
manifest may point to versioned segments.

--------------------------------------------------
How to Enable Object Versioning in a Swift Cluster
--------------------------------------------------

Set ``allow_versions`` to ``True`` in the container server config.

-----------------------
Examples Using ``curl``
-----------------------

First, create a container with the ``X-Versions-Location`` header or add the
header to an existing container. Also make sure the container referenced by
the ``X-Versions-Location`` exists. In this example, the name of that
container is "versions"::

    curl -i -XPUT -H "X-Auth-Token: <token>" \
        -H "X-Versions-Location: versions" http://<storage_url>/container
    curl -i -XPUT -H "X-Auth-Token: <token>" http://<storage_url>/versions

Create an object (the first version)::

    curl -i -XPUT --data-binary 1 -H "X-Auth-Token: <token>" \
        http://<storage_url>/container/myobject

Now create a new version of that object::

    curl -i -XPUT --data-binary 2 -H "X-Auth-Token: <token>" \
        http://<storage_url>/container/myobject

See a listing of the older versions of the object::

    curl -i -H "X-Auth-Token: <token>" \
        http://<storage_url>/versions?prefix=008myobject/

Now delete the current version of the object and see that the older version is
gone::

    curl -i -XDELETE -H "X-Auth-Token: <token>" \
        http://<storage_url>/container/myobject
    curl -i -H "X-Auth-Token: <token>" \
        http://<storage_url>/versions?prefix=008myobject/
