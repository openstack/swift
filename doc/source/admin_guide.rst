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

There is a swift-dispersion-report tool for measuring overall cluster health.
This is accomplished by checking if a set of deliberately distributed
containers and objects are currently in their proper places within the cluster.

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
swift-dispersion-populate tool does this by making up random container and
object names until they fall on distinct partitions. Last, and repeatedly for
the life of the cluster, we need to run the swift-dispersion-report tool to
check the health of each of these containers and objects.

These tools need direct access to the entire cluster and to the ring files
(installing them on a proxy server will probably do). Both
swift-dispersion-populate and swift-dispersion-report use the same
configuration file, /etc/swift/dispersion.conf. Example conf file::

    [dispersion]
    auth_url = http://saio:11000/auth/v1.0
    auth_user = test:tester
    auth_key = testing

There are also options for the conf file for specifying the dispersion coverage
(defaults to 1%), retries, concurrency, etc. though usually the defaults are
fine.

Once the configuration is in place, run `swift-dispersion-populate` to populate
the containers and objects throughout the cluster.

Now that those containers and objects are in place, you can run
`swift-dispersion-report` to get a dispersion report, or the overall health of
the cluster. Here is an example of a cluster in perfect health::

    $ swift-dispersion-report
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
    $ swift-dispersion-report
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
    $ swift-dispersion-report
    Queried 2621 containers for dispersion reporting, 17s, 0 retries
    100.00% of container copies found (7863 of 7863)
    Sample represents 1.00% of the container partition space

    Queried 2619 objects for dispersion reporting, 7s, 0 retries
    100.00% of object copies found (7857 of 7857)
    Sample represents 1.00% of the object partition space

Alternatively, the dispersion report can also be output in json format. This 
allows it to be more easily consumed by third party utilities::

    $ swift-dispersion-report -j
    {"object": {"retries:": 0, "missing_two": 0, "copies_found": 7863, "missing_one": 0, "copies_expected": 7863, "pct_found": 100.0, "overlapping": 0, "missing_all": 0}, "container": {"retries:": 0, "missing_two": 0, "copies_found": 12534, "missing_one": 0, "copies_expected": 12534, "pct_found": 100.0, "overlapping": 15, "missing_all": 0}}


--------------------------------
Cluster Telemetry and Monitoring
--------------------------------

Various metrics and telemetry can be obtained from the object servers using
the recon server middleware and the swift-recon cli. To do so update your 
object-server.conf to enable the recon middleware by adding a pipeline entry
and setting its one option::

    [pipeline:main]
    pipeline = recon object-server
    
    [filter:recon]
    use = egg:swift#recon
    recon_cache_path = /var/cache/swift

The recon_cache_path simply sets the directory where stats for a few items will
be stored. Depending on the method of deployment you may need to create this
directory manually and ensure that swift has read/write.

If you wish to enable reporting of replication times you can enable recon 
support in the object-replicator section of the object-server.conf::

    [object-replicator]
    ...
    recon_enable = yes
    recon_cache_path = /var/cache/swift
    
Finally if you also wish to track asynchronous pending's you will need to setup
a cronjob to run the swift-recon-cron script periodically::

    */5 * * * * swift /usr/bin/swift-recon-cron /etc/swift/object-server.conf
   
Once enabled a GET request for "/recon/<metric>" to the object server will 
return a json formatted response::

    fhines@ubuntu:~$ curl -i http://localhost:6030/recon/async
    HTTP/1.1 200 OK
    Content-Type: application/json
    Content-Length: 20
    Date: Tue, 18 Oct 2011 21:03:01 GMT

    {"async_pending": 0}

The following metrics and telemetry are currently exposed:

==================  ====================================================
Request URI         Description
------------------  ----------------------------------------------------
/recon/load         returns 1,5, and 15 minute load average
/recon/async        returns count of async pending
/recon/mem          returns /proc/meminfo
/recon/replication  returns last logged object replication time
/recon/mounted      returns *ALL* currently mounted filesystems
/recon/unmounted    returns all unmounted drives if mount_check = True
/recon/diskusage    returns disk utilization for storage devices
/recon/ringmd5      returns object/container/account ring md5sums
/recon/quarantined  returns # of quarantined objects/accounts/containers
/recon/sockstat     returns consumable info from /proc/net/sockstat|6
==================  ====================================================

This information can also be queried via the swift-recon command line utility::

    fhines@ubuntu:~$ swift-recon -h
    ===============================================================================
    Usage: 
        usage: swift-recon [-v] [--suppress] [-a] [-r] [-u] [-d] [-l] [--objmd5]
    

    Options:
      -h, --help            show this help message and exit
      -v, --verbose         Print verbose info
      --suppress            Suppress most connection related errors
      -a, --async           Get async stats
      -r, --replication     Get replication stats
      -u, --unmounted       Check cluster for unmounted devices
      -d, --diskusage       Get disk usage stats
      -l, --loadstats       Get cluster load average stats
      -q, --quarantined     Get cluster quarantine stats
      --objmd5              Get md5sums of object.ring.gz and compare to local
                            copy
      --sockstat            Get cluster socket usage stats
      --all                 Perform all checks. Equivalent to -arudlq --objmd5
                            --socketstat
      -z ZONE, --zone=ZONE  Only query servers in specified zone
      --swiftdir=SWIFTDIR   Default = /etc/swift

For example, to obtain quarantine stats from all hosts in zone "3"::

    fhines@ubuntu:~$ swift-recon -q --zone 3
    ===============================================================================
    [2011-10-18 19:36:00] Checking quarantine dirs on 1 hosts...
    [Quarantined objects] low: 4, high: 4, avg: 4, total: 4
    [Quarantined accounts] low: 0, high: 0, avg: 0, total: 0
    [Quarantined containers] low: 0, high: 0, avg: 0, total: 0
    ===============================================================================


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

--------------
Object Auditor
--------------

On system failures, the XFS file system can sometimes truncate files it's
trying to write and produce zero byte files. The object-auditor will catch
these problems but in the case of a system crash it would be advisable to run
an extra, less rate limited sweep to check for these specific files. You can
run this command as follows:
`swift-object-auditor /path/to/object-server/config/file.conf once -z 1000`
"-z" means to only check for zero-byte files at 1000 files per second.

-------------
Swift Orphans
-------------

Swift Orphans are processes left over after a reload of a Swift server.

For example, when upgrading a proxy server you would probaby finish with a `swift-init proxy-server reload` or `/etc/init.d/swift-proxy reload`. This kills the parent proxy server process and leaves the child processes running to finish processing whatever requests they might be handling at the time. It then starts up a new parent proxy server process and its children to handle new incoming requests. This allows zero-downtime upgrades with no impact to existing requests.

The orphaned child processes may take a while to exit, depending on the length of the requests they were handling. However, sometimes an old process can be hung up due to some bug or hardware issue. In these cases, these orphaned processes will hang around forever. `swift-orphans` can be used to find and kill these orphans.

`swift-orphans` with no arguments will just list the orphans it finds that were started more than 24 hours ago. You shouldn't really check for orphans until 24 hours after you perform a reload, as some requests can take a long time to process. `swift-orphans -k TERM` will send the SIG_TERM signal to the orphans processes, or you can `kill -TERM` the pids yourself if you prefer.

You can run `swift-orphans --help` for more options.


------------
Swift Oldies
------------

Swift Oldies are processes that have just been around for a long time. There's nothing necessarily wrong with this, but it might indicate a hung process if you regularly upgrade and reload/restart services. You might have so many servers that you don't notice when a reload/restart fails, `swift-oldies` can help with this.

For example, if you upgraded and reloaded/restarted everything 2 days ago, and you've already cleaned up any orphans with `swift-orphans`, you can run `swift-oldies -a 48` to find any Swift processes still around that were started more than 2 days ago and then investigate them accordingly.
