=====================
Administrator's Guide
=====================

------------------
Managing the Rings
------------------

You need to build the storage rings on the proxy server node, and distribute them to all the servers in the cluster. Storage rings contain information about all the Swift storage partitions and how they are distributed between the different nodes and disks. For more information see :doc:`overview_ring`.

Removing a device from the ring::

    swift-ring-builder <builder-file> remove <ip_address>/<device_name>
    
Removing a server from the ring::

    swift-ring-builder <builder-file> remove <ip_address>
    
Adding devices to the ring:

See :ref:`ring-preparing`
    
See what devices for a server are in the ring::

    swift-ring-builder <builder-file> search <ip_address>

Once you are done with all changes to the ring, the changes need to be
"committed"::

    swift-ring-builder <builder-file> rebalance
    
Once the new rings are built, they should be pushed out to all the servers
in the cluster.

-----------------------
Scripting Ring Creation
-----------------------
You can create scripts to create the account and container rings and rebalance. Here's an example script for the Account ring. Use similar commands to create a make-container-ring.sh script on the proxy server node.

1. Create a script file called make-account-ring.sh on the proxy server node with the following content::

    #!/bin/bash
    cd /etc/swift
    rm -f account.builder account.ring.gz backups/account.builder backups/account.ring.gz
    swift-ring-builder account.builder create 18 3 1
    swift-ring-builder account.builder add z1-<account-server-1>:6002/sdb1 1
    swift-ring-builder account.builder add z2-<account-server-2>:6002/sdb1 1
    swift-ring-builder account.builder rebalance

You need to replace the values of <account-server-1>, <account-server-2>, etc. with the IP addresses of the account servers used in your setup. You can have as many account servers as you need. All account servers are assumed to be listening on port 6002, and have a storage device called "sdb1" (this is a directory name created under /drives when we setup the account server). The "z1", "z2", etc. designate zones, and you can choose whether you put devices in the same or different zones.

2. Make the script file executable and run it to create the account ring file::

    chmod +x make-account-ring.sh
    sudo ./make-account-ring.sh

3. Copy the resulting ring file /etc/swift/account.ring.gz to all the account server nodes in your Swift environment, and put them in the /etc/swift directory on these nodes. Make sure that every time you change the account ring configuration, you copy the resulting ring file to all the account nodes.

-----------------------
Handling System Updates
-----------------------

It is recommended that system updates and reboots are done a zone at a time.
This allows the update to happen, and for the Swift cluster to stay available
and responsive to requests.  It is also advisable when updating a zone, let
it run for a while before updating the other zones to make sure the update
doesn't have any adverse effects.

----------------------
Handling Drive Failure
----------------------

In the event that a drive has failed, the first step is to make sure the drive
is unmounted.  This will make it easier for swift to work around the failure
until it has been resolved.  If the drive is going to be replaced immediately,
then it is just best to replace the drive, format it, remount it, and let
replication fill it up.

If the drive can't be replaced immediately, then it is best to leave it
unmounted, and remove the drive from the ring. This will allow all the
replicas that were on that drive to be replicated elsewhere until the drive
is replaced.  Once the drive is replaced, it can be re-added to the ring.

-----------------------
Handling Server Failure
-----------------------

If a server is having hardware issues, it is a good idea to make sure the 
swift services are not running.  This will allow Swift to work around the
failure while you troubleshoot.

If the server just needs a reboot, or a small amount of work that should
only last a couple of hours, then it is probably best to let Swift work
around the failure and get the machine fixed and back online.  When the
machine comes back online, replication will make sure that anything that is
missing during the downtime will get updated.

If the server has more serious issues, then it is probably best to remove
all of the server's devices from the ring.  Once the server has been repaired
and is back online, the server's devices can be added back into the ring.
It is important that the devices are reformatted before putting them back
into the ring as it is likely to be responsible for a different set of
partitions than before.

-----------------------
Detecting Failed Drives
-----------------------

It has been our experience that when a drive is about to fail, error messages
will spew into `/var/log/kern.log`.  There is a script called
`swift-drive-audit` that can be run via cron to watch for bad drives.  If 
errors are detected, it will unmount the bad drive, so that Swift can
work around it.  The script takes a configuration file with the following
settings:

[drive-audit]

==================  ==========  ===========================================
Option              Default     Description
------------------  ----------  -------------------------------------------
log_facility        LOG_LOCAL0  Syslog log facility
log_level           INFO        Log level
device_dir          /srv/node   Directory devices are mounted under
minutes             60          Number of minutes to look back in
                                `/var/log/kern.log`
error_limit         1           Number of errors to find before a device
                                is unmounted
==================  ==========  ===========================================

This script has only been tested on Ubuntu 10.04, so if you are using a
different distro or OS, some care should be taken before using in production.

--------------
Cluster Health
--------------

There is a swift-stats-report tool for measuring overall cluster health. This
is accomplished by checking if a set of deliberately distributed containers and
objects are currently in their proper places within the cluster.

For instance, a common deployment has three replicas of each object. The health
of that object can be measured by checking if each replica is in its proper
place. If only 2 of the 3 is in place the object's heath can be said to be at
66.66%, where 100% would be perfect.

A single object's health, especially an older object, usually reflects the
health of that entire partition the object is in. If we make enough objects on
a distinct percentage of the partitions in the cluster, we can get a pretty
valid estimate of the overall cluster health. In practice, about 1% partition
coverage seems to balance well between accuracy and the amount of time it takes
to gather results.

The first thing that needs to be done to provide this health value is create a
new account solely for this usage. Next, we need to place the containers and
objects throughout the system so that they are on distinct partitions. The
swift-stats-populate tool does this by making up random container and object
names until they fall on distinct partitions. Last, and repeatedly for the life
of the cluster, we need to run the swift-stats-report tool to check the health
of each of these containers and objects.

These tools need direct access to the entire cluster and to the ring files
(installing them on an auth server or a proxy server will probably do). Both
swift-stats-populate and swift-stats-report use the same configuration file,
/etc/swift/stats.conf. Example conf file::

    [stats]
    # For DevAuth:
    auth_url = http://saio:11000/v1.0
    # For Swauth:
    # auth_url = http://saio:11000/auth/v1.0
    auth_user = test:tester
    auth_key = testing

There are also options for the conf file for specifying the dispersion coverage
(defaults to 1%), retries, concurrency, CSV output file, etc. though usually
the defaults are fine.

Once the configuration is in place, run `swift-stats-populate -d` to populate
the containers and objects throughout the cluster.

Now that those containers and objects are in place, you can run
`swift-stats-report -d` to get a dispersion report, or the overall health of
the cluster. Here is an example of a cluster in perfect health::

    $ swift-stats-report -d
    Queried 2621 containers for dispersion reporting, 19s, 0 retries
    100.00% of container copies found (7863 of 7863)
    Sample represents 1.00% of the container partition space
    
    Queried 2619 objects for dispersion reporting, 7s, 0 retries
    100.00% of object copies found (7857 of 7857)
    Sample represents 1.00% of the object partition space

Now I'll deliberately double the weight of a device in the object ring (with
replication turned off) and rerun the dispersion report to show what impact
that has::

    $ swift-ring-builder object.builder set_weight d0 200
    $ swift-ring-builder object.builder rebalance
    ...
    $ swift-stats-report -d
    Queried 2621 containers for dispersion reporting, 8s, 0 retries
    100.00% of container copies found (7863 of 7863)
    Sample represents 1.00% of the container partition space
    
    Queried 2619 objects for dispersion reporting, 7s, 0 retries
    There were 1763 partitions missing one copy.
    77.56% of object copies found (6094 of 7857)
    Sample represents 1.00% of the object partition space

You can see the health of the objects in the cluster has gone down
significantly. Of course, I only have four devices in this test environment, in
a production environment with many many devices the impact of one device change
is much less. Next, I'll run the replicators to get everything put back into
place and then rerun the dispersion report::

    ... start object replicators and monitor logs until they're caught up ...
    $ swift-stats-report -d
    Queried 2621 containers for dispersion reporting, 17s, 0 retries
    100.00% of container copies found (7863 of 7863)
    Sample represents 1.00% of the container partition space

    Queried 2619 objects for dispersion reporting, 7s, 0 retries
    100.00% of object copies found (7857 of 7857)
    Sample represents 1.00% of the object partition space

So that's a summation of how to use swift-stats-report to monitor the health of
a cluster. There are a few other things it can do, such as performance
monitoring, but those are currently in their infancy and little used. For
instance, you can run `swift-stats-populate -p` and `swift-stats-report -p` to
get performance timings (warning: the initial populate takes a while). These
timings are dumped into a CSV file (/etc/swift/stats.csv by default) and can
then be graphed to see how cluster performance is trending.

------------------------------------
Additional Cleanup Script for Swauth
------------------------------------

If you decide to use Swauth, you'll want to install a cronjob to clean up any
orphaned expired tokens. These orphaned tokens can occur when a "stampede"
occurs where a single user authenticates several times concurrently. Generally,
these orphaned tokens don't pose much of an issue, but it's good to clean them
up once a "token life" period (default: 1 day or 86400 seconds).

This should be as simple as adding `swauth-cleanup-tokens -K swauthkey >
/dev/null` to a crontab entry on one of the proxies that is running Swauth; but
run `swauth-cleanup-tokens` with no arguments for detailed help on the options
available.

------------------------
Debugging Tips and Tools
------------------------

When a request is made to Swift, it is given a unique transaction id.  This
id should be in every log line that has to do with that request.  This can
be useful when looking at all the services that are hit by a single request.

If you need to know where a specific account, container or object is in the
cluster, `swift-get-nodes` will show the location where each replica should be.

If you are looking at an object on the server and need more info,
`swift-object-info` will display the account, container, replica locations
and metadata of the object.

If you want to audit the data for an account, `swift-account-audit` can be
used to crawl the account, checking that all containers and objects can be
found.

-----------------
Managing Services
-----------------

Swift services are generally managed with `swift-init`. the general usage is
``swift-init <service> <command>``, where service is the swift service to 
manage (for example object, container, account, proxy) and command is one of:

==========  ===============================================
Command     Description
----------  -----------------------------------------------
start       Start the service
stop        Stop the service
restart     Restart the service
shutdown    Attempt to gracefully shutdown the service
reload      Attempt to gracefully restart the service
==========  ===============================================

A graceful shutdown or reload will finish any current requests before 
completely stopping the old service.  There is also a special case of 
`swift-init all <command>`, which will run the command for all swift services.

