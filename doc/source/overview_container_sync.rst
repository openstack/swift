======================================
Container to Container Synchronization
======================================

--------
Overview
--------

Swift has a feature where all the contents of a container can be mirrored to
another container through background synchronization. Swift cluster operators
configure their cluster to allow/accept sync requests to/from other clusters,
and the user specifies where to sync their container to along with a secret
synchronization key.

.. note::

    If you are using the :ref:`Large Objects <large-objects>` feature and
    syncing to another cluster then you will need to ensure that manifest files
    and segment files are synced. If segment files are in a different container
    than their manifest then both the manifest's container and the segments'
    container must be synced. The target container for synced segment files
    must always have the same name as their source container in order for them
    to be resolved by synced manifests.

    Be aware that manifest files may be synced before segment files even if
    they are in the same container and were created after the segment files.

    In the case of :ref:`Static Large Objects <static-large-objects>`, a GET
    request for a manifest whose segments have yet to be completely synced will
    fail with none or only part of the large object content being returned.

    In the case of :ref:`Dynamic Large Objects <dynamic-large-objects>`, a GET
    request for a manifest whose segments have yet to be completely synced will
    either fail or return unexpected (and most likely incorrect) content.

.. note::

    If you are using encryption middleware in the cluster from which objects
    are being synced, then you should follow the instructions for
    :ref:`container_sync_client_config` to be compatible with encryption.

.. note::

    If you are using symlink middleware in the cluster from which objects
    are being synced, then you should follow the instructions for
    :ref:`symlink_container_sync_client_config` to be compatible with symlinks.

    Be aware that symlinks may be synced before their targets even if they are
    in the same container and were created after the target objects. In such
    cases, a GET for the symlink will fail with a ``404 Not Found`` error.  If
    the target has been overwritten, a GET may produce an older version (for
    dynamic links) or a ``409 Conflict`` error (for static links).

--------------------------
Configuring Container Sync
--------------------------

Create a ``container-sync-realms.conf`` file specifying the allowable clusters
and their information::

    [realm1]
    key = realm1key
    key2 = realm1key2
    cluster_clustername1 = https://host1/v1/
    cluster_clustername2 = https://host2/v1/

    [realm2]
    key = realm2key
    key2 = realm2key2
    cluster_clustername3 = https://host3/v1/
    cluster_clustername4 = https://host4/v1/


Each section name is the name of a sync realm. A sync realm is a set of
clusters that have agreed to allow container syncing with each other. Realm
names will be considered case insensitive.

``key`` is the overall cluster-to-cluster key used in combination with the
external users' key that they set on their containers'
``X-Container-Sync-Key`` metadata header values. These keys will be used to
sign each request the container sync daemon makes and used to validate each
incoming container sync request.

``key2`` is optional and is an additional key incoming requests will be checked
against. This is so you can rotate keys if you wish; you move the existing ``key``
to ``key2`` and make a new ``key`` value.

Any values in the realm section whose names begin with ``cluster_`` will
indicate the name and endpoint of a cluster and will be used by external users in
their containers' ``X-Container-Sync-To`` metadata header values with the format
``//realm_name/cluster_name/account_name/container_name``. Realm and cluster
names are considered case insensitive.

The endpoint is what the container sync daemon will use when sending out
requests to that cluster. Keep in mind this endpoint must be reachable by all
container servers, since that is where the container sync daemon runs. Note
that the endpoint ends with ``/v1/`` and that the container sync daemon will then
add the ``account/container/obj`` name after that.

Distribute this ``container-sync-realms.conf`` file to all your proxy servers
and container servers.

You also need to add the container_sync middleware to your proxy pipeline. It
needs to be after any memcache middleware and before any auth middleware. The
``[filter:container_sync]`` section only needs the ``use`` item. For example::

    [pipeline:main]
    pipeline = healthcheck proxy-logging cache container_sync tempauth proxy-logging proxy-server

    [filter:container_sync]
    use = egg:swift#container_sync

The container sync daemon will use an internal client to sync objects. Even if
you don't configure the internal client, the container sync daemon will work
with default configuration. The default configuration is the same as
``internal-client.conf-sample``. If you want to configure the internal client,
please update ``internal_client_conf_path`` in ``container-server.conf``. The
configuration file at the path will be used for the internal client.

-------------------------------------------------------
Old-Style: Configuring a Cluster's Allowable Sync Hosts
-------------------------------------------------------

This section is for the old-style of using container sync. See the previous
section, Configuring Container Sync, for the new-style.

With the old-style, the Swift cluster operator must allow synchronization with
a set of hosts before the user can enable container synchronization. First, the
backend container server needs to be given this list of hosts in the
``container-server.conf`` file::

    [DEFAULT]
    # This is a comma separated list of hosts allowed in the
    # X-Container-Sync-To field for containers.
    # allowed_sync_hosts = 127.0.0.1
    allowed_sync_hosts = host1,host2,etc.
    ...

    [container-sync]
    # You can override the default log routing for this app here (don't
    # use set!):
    # log_name = container-sync
    # log_facility = LOG_LOCAL0
    # log_level = INFO
    # Will sync, at most, each container once per interval
    # interval = 300
    # Maximum amount of time to spend syncing each container
    # container_time = 60


----------------------
Logging Container Sync
----------------------

Currently, log processing is the only way to track sync progress, problems,
and even just general activity for container synchronization. In that
light, you may wish to set the above ``log_`` options to direct the
container-sync logs to a different file for easier monitoring. Additionally, it
should be noted there is no way for an end user to monitor sync progress or
detect problems other than HEADing both containers and comparing the overall
information.



-----------------------------
Container Sync Statistics
-----------------------------

Container Sync INFO level logs contain activity metrics and accounting
information for insightful tracking.
Currently two different statistics are collected:

About once an hour or so, accumulated statistics of all operations performed
by Container Sync are reported to the log file with the following format::

    Since (time): (sync) synced [(delete) deletes, (put) puts], (skip) skipped, (fail) failed

time
    last report time
sync
    number of containers with sync turned on that were successfully synced
delete
    number of successful DELETE object requests to the target cluster
put
    number of successful PUT object request to the target cluster
skip
    number of containers whose sync has been turned off, but are not
    yet cleared from the sync store
fail
    number of containers with failure (due to exception, timeout or other
    reason)

For each container synced, per container statistics are reported with the
following format::

    Container sync report: (container), time window start: (start), time window end: %(end), puts: (puts), posts: (posts), deletes: (deletes), bytes: (bytes), sync_point1: (point1), sync_point2: (point2), total_rows: (total)

container
    account/container statistics are for
start
    report start time
end
    report end time
puts
    number of successful PUT object requests to the target container
posts
    N/A (0)
deletes
    number of successful DELETE object requests to the target container
bytes
    number of bytes sent over the network to the target container
point1
    progress indication - the container's ``x_container_sync_point1``
point2
    progress indication - the container's ``x_container_sync_point2``
total
    number of objects processed at the container

It is possible that more than one server syncs a container, therefore log files
from all servers need to be evaluated



----------------------------------------------------------
Using the ``swift`` tool to set up synchronized containers
----------------------------------------------------------

.. note::

    The ``swift`` tool is available from the `python-swiftclient`_ library.

.. note::

    You must be the account admin on the account to set synchronization targets
    and keys.

You simply tell each container where to sync to and give it a secret
synchronization key. First, let's get the account details for our two cluster
accounts::

    $ swift -A http://cluster1/auth/v1.0 -U test:tester -K testing stat -v
    StorageURL: http://cluster1/v1/AUTH_208d1854-e475-4500-b315-81de645d060e
    Auth Token: AUTH_tkd5359e46ff9e419fa193dbd367f3cd19
       Account: AUTH_208d1854-e475-4500-b315-81de645d060e
    Containers: 0
       Objects: 0
         Bytes: 0

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 stat -v
    StorageURL: http://cluster2/v1/AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c
    Auth Token: AUTH_tk816a1aaf403c49adb92ecfca2f88e430
       Account: AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c
    Containers: 0
       Objects: 0
         Bytes: 0

Now, let's make our first container and tell it to synchronize to a second
we'll make next::

    $ swift -A http://cluster1/auth/v1.0 -U test:tester -K testing post \
      -t '//realm_name/clustername2/AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c/container2' \
      -k 'secret' container1

The ``-t`` indicates the cluster to sync to, which is the realm name of the
section from ``container-sync-realms.conf``, followed by the cluster name from
that section (without the ``cluster_`` prefix), followed by the account and container
names we want to sync to. The ``-k`` specifies the secret key the two containers will share for
synchronization; this is the user key, the cluster key in
``container-sync-realms.conf`` will also be used behind the scenes.

Now, we'll do something similar for the second cluster's container::

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 post \
      -t '//realm_name/clustername1/AUTH_208d1854-e475-4500-b315-81de645d060e/container1' \
      -k 'secret' container2

That's it. Now we can upload a bunch of stuff to the first container and watch
as it gets synchronized over to the second::

    $ swift -A http://cluster1/auth/v1.0 -U test:tester -K testing \
      upload container1 .
    photo002.png
    photo004.png
    photo001.png
    photo003.png

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 \
      list container2

    [Nothing there yet, so we wait a bit...]

.. note::

    If you're an operator running :ref:`saio` and just testing, each time you
    configure a container for synchronization and place objects in the
    source container you will need to ensure that container-sync runs
    before attempting to retrieve objects from the target container.
    That is, you need to run::

      swift-init container-sync once

Now expect to see objects copied from the first container to the second::

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 \
      list container2
    photo001.png
    photo002.png
    photo003.png
    photo004.png

You can also set up a chain of synced containers if you want more than two.
You'd point 1 -> 2, then 2 -> 3, and finally 3 -> 1 for three containers.
They'd all need to share the same secret synchronization key.

.. _`python-swiftclient`: http://github.com/openstack/python-swiftclient

-----------------------------------
Using curl (or other tools) instead
-----------------------------------

So what's ``swift`` doing behind the scenes? Nothing overly complicated. It
translates the ``-t <value>`` option into an ``X-Container-Sync-To: <value>``
header and the ``-k <value>`` option into an ``X-Container-Sync-Key: <value>``
header.

For instance, when we created the first container above and told it to
synchronize to the second, we could have used this curl command::

    $ curl -i -X POST -H 'X-Auth-Token: AUTH_tkd5359e46ff9e419fa193dbd367f3cd19' \
      -H 'X-Container-Sync-To: //realm_name/clustername2/AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c/container2' \
      -H 'X-Container-Sync-Key: secret' \
      'http://cluster1/v1/AUTH_208d1854-e475-4500-b315-81de645d060e/container1'
    HTTP/1.1 204 No Content
    Content-Length: 0
    Content-Type: text/plain; charset=UTF-8
    Date: Thu, 24 Feb 2011 22:39:14 GMT

---------------------------------------------------------------------
Old-Style: Using the ``swift`` tool to set up synchronized containers
---------------------------------------------------------------------

.. note::

    The ``swift`` tool is available from the `python-swiftclient`_ library.

.. note::

    You must be the account admin on the account to set synchronization targets
    and keys.

This is for the old-style of container syncing using ``allowed_sync_hosts``.

You simply tell each container where to sync to and give it a secret
synchronization key. First, let's get the account details for our two cluster
accounts::

    $ swift -A http://cluster1/auth/v1.0 -U test:tester -K testing stat -v
    StorageURL: http://cluster1/v1/AUTH_208d1854-e475-4500-b315-81de645d060e
    Auth Token: AUTH_tkd5359e46ff9e419fa193dbd367f3cd19
       Account: AUTH_208d1854-e475-4500-b315-81de645d060e
    Containers: 0
       Objects: 0
         Bytes: 0

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 stat -v
    StorageURL: http://cluster2/v1/AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c
    Auth Token: AUTH_tk816a1aaf403c49adb92ecfca2f88e430
       Account: AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c
    Containers: 0
       Objects: 0
         Bytes: 0

Now, let's make our first container and tell it to synchronize to a second
we'll make next::

    $ swift -A http://cluster1/auth/v1.0 -U test:tester -K testing post \
      -t 'http://cluster2/v1/AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c/container2' \
      -k 'secret' container1

The ``-t`` indicates the URL to sync to, which is the ``StorageURL`` from
cluster2 we retrieved above plus the container name. The ``-k`` specifies the
secret key the two containers will share for synchronization. Now, we'll do
something similar for the second cluster's container::

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 post \
      -t 'http://cluster1/v1/AUTH_208d1854-e475-4500-b315-81de645d060e/container1' \
      -k 'secret' container2

That's it. Now we can upload a bunch of stuff to the first container and watch
as it gets synchronized over to the second::

    $ swift -A http://cluster1/auth/v1.0 -U test:tester -K testing \
      upload container1 .
    photo002.png
    photo004.png
    photo001.png
    photo003.png

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 \
      list container2

    [Nothing there yet, so we wait a bit...]
    [If you're an operator running SAIO and just testing, you may need to
     run 'swift-init container-sync once' to perform a sync scan.]

    $ swift -A http://cluster2/auth/v1.0 -U test2:tester2 -K testing2 \
      list container2
    photo001.png
    photo002.png
    photo003.png
    photo004.png

You can also set up a chain of synced containers if you want more than two.
You'd point 1 -> 2, then 2 -> 3, and finally 3 -> 1 for three containers.
They'd all need to share the same secret synchronization key.

.. _`python-swiftclient`: http://github.com/openstack/python-swiftclient

----------------------------------------------
Old-Style: Using curl (or other tools) instead
----------------------------------------------

This is for the old-style of container syncing using ``allowed_sync_hosts``.

So what's ``swift`` doing behind the scenes? Nothing overly complicated. It
translates the ``-t <value>`` option into an ``X-Container-Sync-To: <value>``
header and the ``-k <value>`` option into an ``X-Container-Sync-Key: <value>``
header.

For instance, when we created the first container above and told it to
synchronize to the second, we could have used this curl command::

    $ curl -i -X POST -H 'X-Auth-Token: AUTH_tkd5359e46ff9e419fa193dbd367f3cd19' \
      -H 'X-Container-Sync-To: http://cluster2/v1/AUTH_33cdcad8-09fb-4940-90da-0f00cbf21c7c/container2' \
      -H 'X-Container-Sync-Key: secret' \
      'http://cluster1/v1/AUTH_208d1854-e475-4500-b315-81de645d060e/container1'
    HTTP/1.1 204 No Content
    Content-Length: 0
    Content-Type: text/plain; charset=UTF-8
    Date: Thu, 24 Feb 2011 22:39:14 GMT

--------------------------------------------------
What's going on behind the scenes, in the cluster?
--------------------------------------------------

Container ring devices have a directory called ``containers``, where container
databases reside. In addition to ``containers``, each container ring device
also has a directory called ``sync-containers``. ``sync-containers`` holds
symlinks to container databases that were configured for container sync using
``x-container-sync-to`` and ``x-container-sync-key`` metadata keys.

The swift-container-sync process does the job of sending updates to the remote
container. This is done by scanning ``sync-containers`` for container
databases. For each container db found, newer rows since the last sync will
trigger PUTs or DELETEs to the other container.

``sync-containers`` is maintained as follows:
Whenever the container-server processes a PUT or a POST request that carries
``x-container-sync-to`` and ``x-container-sync-key`` metadata keys the server
creates a symlink to the container database in ``sync-containers``. Whenever
the container server deletes a synced container, the appropriate symlink
is deleted from ``sync-containers``.

In addition to the container-server, the container-replicator process does the
job of identifying containers that should be synchronized. This is done by
scanning the local devices for container databases and checking for
``x-container-sync-to`` and ``x-container-sync-key`` metadata values. If they exist
then a symlink to the container database is created in a ``sync-containers``
sub-directory on the same device.

Similarly, when the container sync metadata keys are deleted, the container
server and container-replicator would take care of deleting the symlinks
from ``sync-containers``.

.. note::

    The swift-container-sync process runs on each container server in the
    cluster and talks to the proxy servers (or load balancers) in the remote
    cluster. Therefore, the container servers must be permitted to initiate
    outbound connections to the remote proxy servers (or load balancers).

The actual syncing is slightly more complicated to make use of the three
(or number-of-replicas) main nodes for a container without each trying to
do the exact same work but also without missing work if one node happens to
be down.

Two sync points are kept in each container database. When syncing a
container, the container-sync process figures out which replica of the
container it has. In a standard 3-replica scenario, the process will
have either replica number 0, 1, or 2. This is used to figure out
which rows belong to this sync process and which ones don't.

An example may help. Assume a replica count of 3 and database row IDs
are 1..6. Also, assume that container-sync is running on this
container for the first time, hence SP1 = SP2 = -1. ::

   SP1
   SP2
    |
    v
   -1 0 1 2 3 4 5 6

First, the container-sync process looks for rows with id between SP1
and SP2. Since this is the first run, SP1 = SP2 = -1, and there aren't
any such rows. ::

   SP1
   SP2
    |
    v
   -1 0 1 2 3 4 5 6

Second, the container-sync process looks for rows with id greater than
SP1, and syncs those rows which it owns. Ownership is based on the
hash of the object name, so it's not always guaranteed to be exactly
one out of every three rows, but it usually gets close. For the sake
of example, let's say that this process ends up owning rows 2 and 5.

Once it's finished trying to sync those rows, it updates SP1 to be the
biggest row-id that it's seen, which is 6 in this example. ::

   SP2           SP1
    |             |
    v             v
   -1 0 1 2 3 4 5 6

While all that was going on, clients uploaded new objects into the
container, creating new rows in the database. ::

   SP2           SP1
    |             |
    v             v
   -1 0 1 2 3 4 5 6 7 8 9 10 11 12

On the next run, the container-sync starts off looking at rows with
ids between SP1 and SP2. This time, there are a bunch of them. The
sync process try to sync all of them. If it succeeds, it will set
SP2 to equal SP1. If it fails, it will set SP2 to the failed object
and will continue to try all other objects till SP1, setting SP2 to
the first object that failed.

Under normal circumstances, the container-sync processes
will have already taken care of synchronizing all rows, between SP1
and SP2, resulting in a set of quick checks.
However, if one of the sync
processes failed for some reason, then this is a vital fallback to
make sure all the objects in the container get synchronized. Without
this seemingly-redundant work, any container-sync failure results in
unsynchronized objects. Note that the container sync will persistently
retry to sync any faulty object until success, while logging each failure.

Once it's done with the fallback rows, and assuming no faults occurred,
SP2 is advanced to SP1. ::

                 SP2
                 SP1
                  |
                  v
   -1 0 1 2 3 4 5 6 7 8 9 10 11 12

Then, rows with row ID greater than SP1 are synchronized (provided
this container-sync process is responsible for them), and SP1 is moved
up to the greatest row ID seen. ::

                 SP2            SP1
                  |              |
                  v              v
   -1 0 1 2 3 4 5 6 7 8 9 10 11 12
