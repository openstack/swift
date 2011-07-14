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

The authentication system also needs to be configured to allow synchronization
requests. Here is an example with TempAuth::

    [filter:tempauth]
    # This is a comma separated list of hosts allowed to send
    # X-Container-Sync-Key requests.
    # allowed_sync_hosts = 127.0.0.1
    allowed_sync_hosts = host1,host2,etc.

The default of 127.0.0.1 is just so no configuration is required for SAIO
setups -- for testing.

----------------------------------------------------------
Using the ``swift`` tool to set up synchronized containers
----------------------------------------------------------

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

    Container sync will sync object POSTs only if the proxy server is set to
    use "object_post_as_copy = true" which is the default. So-called fast
    object posts, "object_post_as_copy = false" do not update the container
    listings and therefore can't be detected for synchronization.

The actual syncing is slightly more complicated to make use of the three
(or number-of-replicas) main nodes for a container without each trying to
do the exact same work but also without missing work if one node happens to
be down.

Two sync points are kept per container database. All rows between the two
sync points trigger updates. Any rows newer than both sync points cause
updates depending on the node's position for the container (primary nodes
do one third, etc. depending on the replica count of course). After a sync
run, the first sync point is set to the newest ROWID known and the second
sync point is set to newest ROWID for which all updates have been sent.

An example may help. Assume replica count is 3 and perfectly matching
ROWIDs starting at 1.

    First sync run, database has 6 rows:

        * SyncPoint1 starts as -1.
        * SyncPoint2 starts as -1.
        * No rows between points, so no "all updates" rows.
        * Six rows newer than SyncPoint1, so a third of the rows are sent
          by node 1, another third by node 2, remaining third by node 3.
        * SyncPoint1 is set as 6 (the newest ROWID known).
        * SyncPoint2 is left as -1 since no "all updates" rows were synced.

    Next sync run, database has 12 rows:

        * SyncPoint1 starts as 6.
        * SyncPoint2 starts as -1.
        * The rows between -1 and 6 all trigger updates (most of which
          should short-circuit on the remote end as having already been
          done).
        * Six more rows newer than SyncPoint1, so a third of the rows are
          sent by node 1, another third by node 2, remaining third by node
          3.
        * SyncPoint1 is set as 12 (the newest ROWID known).
        * SyncPoint2 is set as 6 (the newest "all updates" ROWID).

In this way, under normal circumstances each node sends its share of
updates each run and just sends a batch of older updates to ensure nothing
was missed.
