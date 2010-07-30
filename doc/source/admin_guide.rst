=====================
Administrator's Guide
=====================

------------------
Managing the Rings
------------------

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

TODO: Greg, add docs here about how to use swift-stats-populate, and
swift-stats-report

------------------------
Debugging Tips and Tools
------------------------

When a request is made to Swift, it is given a unique transaction id.  This
id should be in every log line that has to do with that request.  This can
be usefult when looking at all the services that are hit by a single request.

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

