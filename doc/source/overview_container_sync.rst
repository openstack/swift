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

    Container sync will sync object POSTs only if the proxy server is set to
    use "object_post_as_copy = true" which is the default. So-called fast
    object posts, "object_post_as_copy = false" do not update the container
    listings and therefore can't be detected for synchronization.

.. note::

    If you are using the large objects feature you will need to ensure both
    your manifest file and your segment files are synced if they happen to be
    in different containers.

--------------------------------------------
Configuring a Cluster's Allowable Sync Hosts
--------------------------------------------

The Swift cluster operator must allow synchronization with a set of hosts
before the user can enable container synchronization. First, the backend
container server needs to be given this list of hosts in the
container-server.conf file::

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

Tracking sync progress, problems, and just general activity can only be
achieved with log processing for this first release of container
synchronization. In that light, you may wish to set the above `log_` options to
direct the container-sync logs to a different file for easier monitoring.
Additionally, it should be noted there is no way for an end user to detect sync
progress or problems other than HEADing both containers and comparing the
overall information.

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

The swift-container-sync does the job of sending updates to the remote
container.

This is done by scanning the local devices for container databases and
checking for x-container-sync-to and x-container-sync-key metadata values.
If they exist, newer rows since the last sync will trigger PUTs or DELETEs
to the other container.

.. note::

    The swift-container-sync process runs on each container server in
    the cluster and talks to the proxy servers in the remote cluster.
    Therefore, the container servers must be permitted to initiate
    outbound connections to the remote proxy servers.

.. note::

    Container sync will sync object POSTs only if the proxy server is set to
    use "object_post_as_copy = true" which is the default. So-called fast
    object posts, "object_post_as_copy = false" do not update the container
    listings and therefore can't be detected for synchronization.

The actual syncing is slightly more complicated to make use of the three
(or number-of-replicas) main nodes for a container without each trying to
do the exact same work but also without missing work if one node happens to
be down.

Two sync points are kept in each container database. When syncing a
container, the container-sync process figures out which replica of the
container it has. In a standard 3-replica scenario, the process will
have either replica number 0, 1, or 2. This is used to figure out
which rows are belong to this sync process and which ones don't.

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

Once it's finished syncing those rows, it updates SP1 to be the
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
sync process takes the ones it *does not* own and syncs them. Again,
this is based on the hashes, so this will be everything it didn't sync
before. In this example, that's rows 0, 1, 3, 4, and 6.

Under normal circumstances, the container-sync processes for the other
replicas will have already taken care of synchronizing those rows, so
this is a set of quick checks. However, if one of those other sync
processes failed for some reason, then this is a vital fallback to
make sure all the objects in the container get synchronized. Without
this seemingly-redundant work, any container-sync failure results in
unsynchronized objects.

Once it's done with the fallback rows, SP2 is advanced to SP1. ::

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
