==================================
Identifying issues and resolutions
==================================

Is the system up?
-----------------

If you have a report that Swift is down, perform the following basic checks:

#. Run swift functional tests.

#. From a server in your data center, use ``curl`` to check ``/healthcheck``
   (see below).

#. If you have a monitoring system, check your monitoring system.

#. Check your hardware load balancers infrastructure.

#. Run swift-recon on a proxy node.

Functional tests usage
-----------------------

We would recommend that you set up the functional tests to run against your
production system. Run regularly this can be a useful tool to validate
that the system is configured correctly. In addition, it can provide
early warning about failures in your system (if the functional tests stop
working, user applications will also probably stop working).

A script for running the function tests is located in ``swift/.functests``.


External monitoring
-------------------

We use pingdom.com to monitor the external Swift API. We suggest the
following:

   -  Do a GET on ``/healthcheck``

   -  Create a container, make it public (x-container-read:
      .r*,.rlistings), create a small file in the container; do a GET
      on the object

Diagnose: General approach
--------------------------

-  Look at service status in your monitoring system.

-  In addition to system monitoring tools and issue logging by users,
   swift errors will often result in log entries (see :ref:`swift_logs`).

-  Look at any logs your deployment tool produces.

-  Log files should be reviewed for error signatures (see below) that
   may point to a known issue, or root cause issues reported by the
   diagnostics tools, prior to escalation.

Dependencies
^^^^^^^^^^^^

The Swift software is dependent on overall system health. Operating
system level issues with network connectivity, domain name resolution,
user management, hardware and system configuration and capacity in terms
of memory and free disk space, may result is secondary Swift issues.
System level issues should be resolved prior to diagnosis of swift
issues.


Diagnose: Swift-dispersion-report
---------------------------------

The swift-dispersion-report is a useful tool to gauge the general
health of the system. Configure the ``swift-dispersion`` report to cover at
a minimum every disk drive in your system (usually 1% coverage).
See :ref:`dispersion_report` for details of how to configure and
use the dispersion reporting tool.

The ``swift-dispersion-report`` tool can take a long time to run, especially
if any servers are down. We suggest you run it regularly
(e.g., in a cron job) and save the results. This makes it easy to refer
to the last report without having to wait for a long-running command
to complete.

Diagnose: Is system responding to /healthcheck?
-----------------------------------------------

When you want to establish if a swift endpoint is running, run ``curl -k``
against https://*[ENDPOINT]*/healthcheck.

.. _swift_logs:

Diagnose: Interpreting messages in ``/var/log/swift/`` files
------------------------------------------------------------

.. note::

   In the Hewlett Packard Enterprise Helion Public Cloud we send logs to
   ``proxy.log`` (proxy-server logs), ``server.log`` (object-server,
   account-server, container-server logs), ``background.log`` (all
   other servers [object-replicator, etc]).

The following table lists known issues:

.. list-table::
   :widths: 25 25 25 25
   :header-rows: 1

   * - **Logfile**
     - **Signature**
     - **Issue**
     - **Steps to take**
   * - /var/log/syslog
     - kernel: [] sd .... [csbu:sd...] Sense Key: Medium Error
     - Suggests disk surface issues
     - Run ``swift-drive-audit`` on the target node to check for disk errors,
       repair disk errors
   * - /var/log/syslog
     - kernel: [] sd .... [csbu:sd...] Sense Key: Hardware Error
     - Suggests storage hardware issues
     - Run diagnostics on the target node to check for disk failures,
       replace failed disks
   * - /var/log/syslog
     - kernel: [] .... I/O error, dev sd.... ,sector ....
     -
     - Run diagnostics on the target node to check for disk errors
   * - /var/log/syslog
     - pound: NULL get_thr_arg
     - Multiple threads woke up
     - Noise, safe to ignore
   * - /var/log/swift/proxy.log
     - .... ERROR .... ConnectionTimeout ....
     - A storage node is not responding in a timely fashion
     - Check if node is down, not running Swift,
       unconfigured, storage off-line or for network issues between the
       proxy and non responding node
   * - /var/log/swift/proxy.log
     - proxy-server .... HTTP/1.0 500 ....
     - A proxy server has reported an internal server error
     - Examine the logs for any errors at the time the error was reported to
       attempt to understand the cause of the error.
   * - /var/log/swift/server.log
     - .... ERROR .... ConnectionTimeout ....
     - A storage server is not responding in a timely fashion
     - Check if node is down, not running Swift,
       unconfigured, storage off-line or for network issues between the
       server and non responding node
   * - /var/log/swift/server.log
     - .... ERROR .... Remote I/O error: '/srv/node/disk....
     - A storage device is not responding as expected
     - Run ``swift-drive-audit`` and check the filesystem named in the error
       for corruption (unmount & xfs_repair). Check if the filesystem
       is mounted and working.
   * - /var/log/swift/background.log
     - object-server ERROR container update failed .... Connection refused
     - A container server node could not be contacted
     - Check if node is down, not running Swift,
       unconfigured, storage off-line or for network issues between the
       server and non responding node
   * - /var/log/swift/background.log
     - object-updater ERROR with remote .... ConnectionTimeout
     - The remote container server is busy
     - If the container is very large, some errors updating it can be
       expected. However, this error can also occur if there is a networking
       issue.
   * - /var/log/swift/background.log
     - account-reaper STDOUT: .... error: ECONNREFUSED
     - Network connectivity issue or the target server is down.
     - Resolve network issue or reboot the target server
   * - /var/log/swift/background.log
     - .... ERROR .... ConnectionTimeout
     - A storage server is not responding in a timely fashion
     - The target server may be busy. However, this error can also occur if
       there is a networking issue.
   * - /var/log/swift/background.log
     - .... ERROR syncing .... Timeout
     - A timeout occurred syncing data to another node.
     - The target server may be busy. However, this error can also occur if
       there is a networking issue.
   * - /var/log/swift/background.log
     - .... ERROR Remote drive not mounted ....
     - A storage server disk is unavailable
     - Repair and remount the file system (on the remote node)
   * - /var/log/swift/background.log
     - object-replicator .... responded as unmounted
     - A storage server disk is unavailable
     - Repair and remount the file system (on the remote node)
   * - /var/log/swift/\*.log
     - STDOUT: EXCEPTION IN
     - A unexpected error occurred
     - Read the Traceback details, if it matches known issues
       (e.g. active network/disk issues), check for re-ocurrences
       after the primary issues have been resolved
   * - /var/log/rsyncd.log
     - rsync: mkdir "/disk....failed: No such file or directory....
     - A local storage server disk is unavailable
     - Run diagnostics on the node to check for a failed or
       unmounted disk
   * - /var/log/swift*
     - Exception: Could not bind to 0.0.0.0:6xxx
     - Possible Swift process restart issue. This indicates an old swift
       process is still running.
     - Restart Swift services. If some swift services are reported down,
       check if they left residual process behind.

Diagnose: Parted reports the backup GPT table is corrupt
--------------------------------------------------------

-  If a GPT table is broken, a message like the following should be
   observed when the following command is run:

   .. code::

      $ sudo parted -l

   .. code::

      Error: The backup GPT table is corrupt, but the primary appears OK,
      so that will be used.

      OK/Cancel?

To fix, go to :ref:`fix_broken_gpt_table`


Diagnose: Drives diagnostic reports a FS label is not acceptable
----------------------------------------------------------------

If diagnostics reports something like  "FS label: obj001dsk011 is not
acceptable", it indicates that a partition has a valid disk label, but an
invalid filesystem label. In such cases proceed as follows:

#. Verify that the disk labels are correct:

   .. code::

      FS=/dev/sd#1

      sudo parted -l | grep object

#. If partition labels are inconsistent then, resolve the disk label issues
   before proceeding:

   .. code::

      sudo parted -s ${FS} name ${PART_NO} ${PART_NAME} #Partition Label
      #PART_NO is 1 for object disks and 3 for OS disks
      #PART_NAME follows the convention seen in "sudo parted -l | grep object"

#. If the Filesystem label is missing then create it with care:

   .. code::

      sudo xfs_admin -l ${FS} #Filesystem label (12 Char limit)

      #Check for the existence of a FS label

      OBJNO=<3 Length Object No.>

      #I.E OBJNO for sw-stbaz3-object0007 would be 007

      DISKNO=<3 Length Disk No.>

      #I.E DISKNO for /dev/sdb would be 001, /dev/sdc would be 002 etc.

      sudo xfs_admin -L "obj${OBJNO}dsk${DISKNO}" ${FS}

      #Create a FS Label

Diagnose: Failed LUNs
---------------------

.. note::

   The HPE Helion Public Cloud uses direct attach SmartArray
   controllers/drives. The information here is specific to that
   environment. The hpacucli utility mentioned here may be called
   hpssacli in your environment.

The ``swift_diagnostics`` mount checks may return a warning that a LUN has
failed, typically accompanied by DriveAudit check failures and device
errors.

Such cases are typically caused by a drive failure, and if drive check
also reports a failed status for the underlying drive, then follow
the procedure to replace the disk.

Otherwise the lun can be re-enabled as follows:

#. Generate a hpssacli diagnostic report. This report allows the DC
   team to troubleshoot potential cabling or hardware issues so it is
   imperative that you run it immediately when troubleshooting a failed
   LUN. You will come back later and grep this file for more details, but
   just generate it for now.

   .. code::

      sudo hpssacli controller all diag file=/tmp/hpacu.diag ris=on xml=off zip=off

Export the following variables using the below instructions before
proceeding further.

#. Print a list of logical drives and their numbers and take note of the
   failed drive's number and array value (example output: "array A
   logicaldrive 1..." would be exported as LDRIVE=1):

   .. code::

      sudo hpssacli controller slot=1 ld all show

#. Export the number of the logical drive that was retrieved from the
   previous command into the LDRIVE variable:

   .. code::

      export LDRIVE=<LogicalDriveNumber>

#. Print the array value and Port:Box:Bay for all drives and take note of
   the Port:Box:Bay for the failed drive (example output: " array A
   physicaldrive 2C:1:1..." would be exported as PBOX=2C:1:1). Match the
   array value of this output with the array value obtained from the
   previous command to be sure you are working on the same drive. Also,
   the array value usually matches the device name (For example, /dev/sdc
   in the case of "array c"), but we will run a different command to be sure
   we are operating on the correct device.

   .. code::

      sudo hpssacli controller slot=1 pd all show

.. note::

   Sometimes a LUN may appear to be failed as it is not and cannot
   be mounted but the hpssacli/parted commands may show no problems with
   the LUNS/drives. In this case, the filesystem may be corrupt and may be
   necessary to run ``sudo xfs_check /dev/sd[a-l][1-2]`` to see if there is
   an xfs issue. The results of running this command may require that
   ``xfs_repair`` is run.

#. Export the Port:Box:Bay for the failed drive into the PBOX variable:

   .. code::

      export PBOX=<Port:Box:Bay>

#. Print the physical device information and take note of the Disk Name
   (example output: "Disk Name: /dev/sdk" would be exported as
   DEV=/dev/sdk):

   .. code::

      sudo hpssacli controller slot=1 ld ${LDRIVE} show detail | grep -i "Disk Name"

#. Export the device name variable from the preceding command (example:
   /dev/sdk):

   .. code::

      export DEV=<Device>

#. Export the filesystem variable. Disks that are split between the
   operating system and data storage, typically sda and sdb, should  only
   have repairs done on their data filesystem, usually /dev/sda2 and
   /dev/sdb2, Other data only disks have just one partition on the device,
   so the filesystem will be 1. In any case you should verify the data
   filesystem by running ``df -h | grep /srv/node`` and using the listed
   data filesystem for the device in question as the export. For example:
   /dev/sdk1.

   .. code::

      export FS=<Filesystem>

#. Verify the LUN is failed, and the device is not:

   .. code::

      sudo hpssacli controller slot=1 ld all show
      sudo hpssacli controller slot=1 pd all show
      sudo hpssacli controller slot=1 ld ${LDRIVE} show detail
      sudo hpssacli controller slot=1 pd ${PBOX} show detail

#. Stop the swift and rsync service:

   .. code::

      sudo service rsync stop
      sudo swift-init shutdown all

#. Unmount the problem drive, fix the LUN and the filesystem:

   .. code::

      sudo umount ${FS}

#. If umount fails, you should run lsof search for the mountpoint and
   kill any lingering processes before repeating the unpount:

   .. code::

      sudo hpacucli controller slot=1 ld ${LDRIVE} modify reenable
      sudo xfs_repair ${FS}

#. If the ``xfs_repair`` complains about possible journal data, use the
   ``xfs_repair -L`` option to zeroise the journal log.

#. Once complete test-mount the filesystem, and tidy up its lost and
   found area.

   .. code::

      sudo mount ${FS} /mnt
      sudo rm -rf /mnt/lost+found/
      sudo umount /mnt

#. Mount the filesystem and restart swift and rsync.

#. Run the following to determine if a DC ticket is needed to check the
   cables on the node:

   .. code::

      grep -y media.exchanged /tmp/hpacu.diag
      grep -y hot.plug.count /tmp/hpacu.diag

#. If the output reports any non 0x00 values, it suggests that the cables
   should be checked. For example, log a DC ticket to check the sas cables
   between the drive and the expander.

.. _diagnose_slow_disk_drives:

Diagnose: Slow disk devices
---------------------------

.. note::

   collectl is an open-source performance gathering/analysis tool.

If the diagnostics report a message such as ``sda: drive is slow``, you
should log onto the node and run the following command (remove ``-c 1`` option to continuously monitor
the data):

.. code::

   $ /usr/bin/collectl -s D -c 1
   waiting for 1 second sample...
   # DISK STATISTICS (/sec)
   #          <---------reads---------><---------writes---------><--------averages--------> Pct
   #Name       KBytes Merged  IOs Size  KBytes Merged  IOs Size  RWSize  QLen  Wait SvcTim Util
   sdb            204      0   33    6      43      0    4   11       6     1     7      6   23
   sda             84      0   13    6     108     21    6   18      10     1     7      7   13
   sdc            100      0   16    6       0      0    0    0       6     1     7      6    9
   sdd            140      0   22    6      22      0    2   11       6     1     9      9   22
   sde             76      0   12    6     255      0   52    5       5     1     2      1   10
   sdf            276      0   44    6       0      0    0    0       6     1    11      8   38
   sdg            112      0   17    7      18      0    2    9       6     1     7      7   13
   sdh           3552      0   73   49       0      0    0    0      48     1     9      8   62
   sdi             72      0   12    6       0      0    0    0       6     1     8      8   10
   sdj            112      0   17    7      22      0    2   11       7     1    10      9   18
   sdk            120      0   19    6      21      0    2   11       6     1     8      8   16
   sdl            144      0   22    7      18      0    2    9       6     1     9      7   18
   dm-0             0      0    0    0       0      0    0    0       0     0     0      0    0
   dm-1             0      0    0    0      60      0   15    4       4     0     0      0    0
   dm-2             0      0    0    0      48      0   12    4       4     0     0      0    0
   dm-3             0      0    0    0       0      0    0    0       0     0     0      0    0
   dm-4             0      0    0    0       0      0    0    0       0     0     0      0    0
   dm-5             0      0    0    0       0      0    0    0       0     0     0      0    0


Look at the ``Wait`` and ``SvcTime`` values. It is not normal for
these values to exceed 50msec. This is known to impact customer
performance (upload/download). For a controller problem, many/all drives
will show long wait and service times. A reboot may correct the problem;
otherwise hardware replacement is needed.

Another way to look at the data is as follows:

.. code::

   $ /opt/hp/syseng/disk-anal.pl -d
   Disk: sda  Wait: 54580 371  65  25  12   6   6   0   1   2   0  46
   Disk: sdb  Wait: 54532 374  96  36  16   7   4   1   0   2   0  46
   Disk: sdc  Wait: 54345 554 105  29  15   4   7   1   4   4   0  46
   Disk: sdd  Wait: 54175 553 254  31  20  11   6   6   2   2   1  53
   Disk: sde  Wait: 54923  66  56  15   8   7   7   0   1   0   2  29
   Disk: sdf  Wait: 50952 941 565 403 426 366 442 447 338  99  38  97
   Disk: sdg  Wait: 50711 689 808 562 642 675 696 185  43  14   7  82
   Disk: sdh  Wait: 51018 668 688 483 575 542 692 275  55  22   9  87
   Disk: sdi  Wait: 51012 1011 849 672 568 240 344 280  38  13   6  81
   Disk: sdj  Wait: 50724 743 770 586 662 509 684 283  46  17  11  79
   Disk: sdk  Wait: 50886 700 585 517 633 511 729 352  89  23   8  81
   Disk: sdl  Wait: 50106 617 794 553 604 504 532 501 288 234 165 216
   Disk: sda  Time: 55040  22  16   6   1   1  13   0   0   0   3  12

   Disk: sdb  Time: 55014  41  19   8   3   1   8   0   0   0   3  17
   Disk: sdc  Time: 55032  23  14   8   9   2   6   1   0   0   0  19
   Disk: sdd  Time: 55022  29  17  12   6   2  11   0   0   0   1  14
   Disk: sde  Time: 55018  34  15  11  12   1   9   0   0   0   2  12
   Disk: sdf  Time: 54809 250  45   7   1   0   0   0   0   0   1   1
   Disk: sdg  Time: 55070  36   6   2   0   0   0   0   0   0   0   0
   Disk: sdh  Time: 55079  33   2   0   0   0   0   0   0   0   0   0
   Disk: sdi  Time: 55074  28   7   2   0   0   2   0   0   0   0   1
   Disk: sdj  Time: 55067  35  10   0   1   0   0   0   0   0   0   1
   Disk: sdk  Time: 55068  31  10   3   0   0   1   0   0   0   0   1
   Disk: sdl  Time: 54905 130  61   7   3   4   1   0   0   0   0   3

This shows the historical distribution of the wait and service times
over a day. This is how you read it:

-  sda did 54580 operations with a short wait time, 371 operations with
   a longer wait time and 65 with an even longer wait time.

-  sdl did 50106 operations with a short wait time, but as you can see
   many took longer.

There is a clear pattern that sdf to sdl have a problem. Actually, sda
to sde would more normally have lots of zeros in their data. But maybe
this is a busy system. In this example it is worth changing the
controller as the individual drives may be ok.

After the controller is changed, use collectl -s D as described above to
see if the problem has cleared. disk-anal.pl will continue to show
historical data. You can look at recent data as follows. It only looks
at data from 13:15 to 14:15. As you can see, this is a relatively clean
system (few if any long wait or service times):

.. code::

   $ /opt/hp/syseng/disk-anal.pl -d -t 13:15-14:15
   Disk: sda  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdb  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdc  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdd  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sde  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdf  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdg  Wait:  3594   6   0   0   0   0   0   0   0   0   0   0
   Disk: sdh  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdi  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdj  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdk  Wait:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdl  Wait:  3599   1   0   0   0   0   0   0   0   0   0   0
   Disk: sda  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdb  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdc  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdd  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sde  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdf  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdg  Time:  3594   6   0   0   0   0   0   0   0   0   0   0
   Disk: sdh  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdi  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdj  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdk  Time:  3600   0   0   0   0   0   0   0   0   0   0   0
   Disk: sdl  Time:  3599   1   0   0   0   0   0   0   0   0   0   0

For long wait times, where the service time appears normal is to check
the logical drive cache status. While the cache may be enabled, it can
be disabled on a per-drive basis.

Diagnose: Slow network link - Measuring network performance
-----------------------------------------------------------

Network faults can cause performance between Swift nodes to degrade. Testing
with ``netperf`` is recommended. Other methods (such as copying large
files) may also work, but can produce inconclusive results.

Install ``netperf`` on all systems if not
already installed. Check that the UFW rules for its control port are in place.
However, there are no pre-opened ports for netperf's data connection. Pick a
port number. In this example, 12866 is used because it is one higher
than netperf's default control port number, 12865. If you get very
strange results including zero values, you may not have gotten the data
port opened in UFW at the target or may have gotten the netperf
command-line wrong.

Pick a ``source`` and ``target`` node. The source is often a proxy node
and the target is often an object node. Using the same source proxy you
can test communication to different object nodes in different AZs to
identity possible bottlenecks.

Running tests
^^^^^^^^^^^^^

#. Prepare the ``target`` node as follows:

   .. code::

      sudo iptables -I INPUT -p tcp -j ACCEPT

   Or, do:

   .. code::

      sudo ufw allow 12866/tcp

#. On the ``source`` node, run the following command to check
   throughput. Note the double-dash before the -P option.
   The command takes 10 seconds to complete. The ``target`` node is 192.168.245.5.

   .. code::

      $ netperf -H 192.168.245.5 -- -P 12866
      MIGRATED TCP STREAM TEST from 0.0.0.0 (0.0.0.0) port 12866 AF_INET to
      <redacted>.72.4 (<redacted>.72.4) port 12866 AF_INET : demo
      Recv   Send    Send
      Socket Socket  Message  Elapsed
      Size   Size    Size     Time     Throughput
      bytes  bytes   bytes    secs.    10^6bits/sec
      87380  16384  16384    10.02     923.69

#. On the ``source`` node, run the following command to check latency:

   .. code::

      $ netperf -H 192.168.245.5 -t TCP_RR -- -P 12866
      MIGRATED TCP REQUEST/RESPONSE TEST from 0.0.0.0 (0.0.0.0) port 12866
      AF_INET to <redacted>.72.4 (<redacted>.72.4) port 12866 AF_INET : demo
      : first burst 0
      Local  Remote Socket   Size    Request  Resp.   Elapsed  Trans.
      Send   Recv   Size     Size    Time     Rate
      bytes  Bytes  bytes    bytes   secs.    per sec
      16384  87380  1        1       10.00    11753.37
      16384  87380

Expected results
^^^^^^^^^^^^^^^^

Faults will show up as differences between different pairs of nodes.
However, for reference, here are some expected numbers:

-  For throughput, proxy to proxy, expect ~9300 Mbit/sec  (proxies have
   a 10Ge link).

-  For throughout, proxy to object, expect ~920 Mbit/sec  (at time of
   writing this, object nodes have a 1Ge link).

-  For throughput, object to object, expect ~920 Mbit/sec.

-  For latency (all types), expect ~11000 transactions/sec.

Diagnose: Remapping sectors experiencing UREs
---------------------------------------------

#. Find the bad sector, device, and filesystem in ``kern.log``.

#. Set the environment variables SEC, DEV & FS, for example:

   .. code::

      SEC=2930954256
      DEV=/dev/sdi
      FS=/dev/sdi1

#. Verify that the sector is bad:

   .. code::

      sudo dd if=${DEV} of=/dev/null bs=512 count=1 skip=${SEC}

#. If the sector is bad this command will output an input/output error:

   .. code::

      dd: reading `/dev/sdi`: Input/output error
      0+0 records in
      0+0 records out

#. Prevent chef from attempting to re-mount the filesystem while the
   repair is in progress:

   .. code::

      sudo mv /etc/chef/client.pem /etc/chef/xx-client.xx-pem

#. Stop the swift and rsync service:

   .. code::

      sudo service rsync stop
      sudo swift-init shutdown all

#. Unmount the problem drive:

   .. code::

      sudo umount ${FS}

#. Overwrite/remap the bad sector:

   .. code::

      sudo dd_rescue -d -A -m8b -s ${SEC}b ${DEV} ${DEV}

#. This command should report an input/output error the first time
   it is run. Run the command a second time, if it successfully remapped
   the bad sector it should not report an input/output error.

#. Verify the sector is now readable:

   .. code::

      sudo dd if=${DEV} of=/dev/null bs=512 count=1 skip=${SEC}

#. If the sector is now readable this command should not report an
   input/output error.

#. If more than one problem sector is listed, set the SEC environment
   variable to the next sector in the list:

   .. code::

      SEC=123456789

#. Repeat from step 8.

#. Repair the filesystem:

   .. code::

      sudo xfs_repair ${FS}

#. If ``xfs_repair`` reports that the filesystem has valuable filesystem
   changes:

   .. code::

      sudo xfs_repair ${FS}
      Phase 1 - find and verify superblock...
      Phase 2 - using internal log
              - zero log...
      ERROR: The filesystem has valuable metadata changes in a log which
      needs to be replayed.
      Mount the filesystem to replay the log, and unmount it before
      re-running xfs_repair.
      If you are unable to mount the filesystem, then use the -L option to
      destroy the log and attempt a repair. Note that destroying the log may
      cause corruption -- please attempt a mount of the filesystem before
      doing this.

#. You should attempt to mount the filesystem, and clear the lost+found
   area:

   .. code::

      sudo mount $FS /mnt
      sudo rm -rf /mnt/lost+found/*
      sudo umount /mnt

#. If the filesystem fails to mount then you will need to use the
   ``xfs_repair -L`` option to force log zeroing.
   Repeat step 11.

#. If ``xfs_repair`` reports that an additional input/output error has been
   encountered, get the sector details as follows:

   .. code::

      sudo grep "I/O error" /var/log/kern.log | grep sector | tail -1

#. If new input/output error is reported then set the SEC environment
   variable to the problem sector number:

   .. code::

      SEC=234567890

#. Repeat from step 8


#. Remount the filesystem and restart swift and rsync.

   -  If all UREs in the kern.log have been fixed and you are still unable
      to have xfs_repair disk, it is possible that the URE's have
      corrupted the filesystem or possibly destroyed the drive altogether.
      In this case, the first step is to re-format the filesystem and if
      this fails, get the disk replaced.


Diagnose: High system latency
-----------------------------

.. note::

   The latency measurements described here are specific to the HPE
   Helion Public Cloud.

-  A bad NIC on a proxy server. However, as explained above, this
   usually causes the peak to rise, but average should remain near
   normal parameters. A quick fix is to shutdown the proxy.

-  A stuck memcache server. Accepts connections, but then will not respond.
   Expect to see timeout messages in ``/var/log/proxy.log`` (port 11211).
   Swift Diags will also report this as a failed node/port. A quick fix
   is to shutdown the proxy server.

-  A bad/broken object server can also cause problems if the accounts
   used by the monitor program happen to live on the bad object server.

-  A general network problem within the data canter. Compare the results
   with the Pingdom monitors to see if they also have a problem.

Diagnose: Interface reports errors
----------------------------------

Should a network interface on a Swift node begin reporting network
errors, it may well indicate a cable, switch, or network issue.

Get an overview of the interface with:

.. code::

   sudo ifconfig eth{n}
   sudo ethtool eth{n}

The ``Link Detected:`` indicator will read ``yes`` if the nic is
cabled.

Establish the adapter type with:

.. code::

   sudo ethtool  -i eth{n}

Gather the interface statistics with:

.. code::

   sudo ethtool  -S eth{n}

If the nick supports self test, this can be performed with:

.. code::

   sudo ethtool  -t eth{n}

Self tests should read ``PASS`` if the nic is operating correctly.

Nic module drivers can be re-initialised by carefully removing and
re-installing the modules (this avoids rebooting the server).
For example, mellanox drivers use a two part driver mlx4_en and
mlx4_core. To reload these you must carefully remove the mlx4_en
(ethernet) then the mlx4_core modules, and reinstall them in the
reverse order.

As the interface will be disabled while the modules are unloaded, you
must be very careful not to lock yourself out so it may be better
to script this.

Diagnose: Hung swift object replicator
--------------------------------------

A replicator reports in its log that remaining time exceeds
100 hours. This may indicate that the swift ``object-replicator`` is stuck and not
making progress. Another useful way to check this is with the
'swift-recon -r' command on a swift proxy server:

.. code::

   sudo swift-recon -r
   ===============================================================================

   --> Starting reconnaissance on 384 hosts
   ===============================================================================
   [2013-07-17 12:56:19] Checking on replication
   [replication_time] low: 2, high: 80, avg: 28.8, total: 11037, Failed: 0.0%, no_result: 0, reported: 383
   Oldest completion was 2013-06-12 22:46:50 (12 days ago) by 192.168.245.3:6200.
   Most recent completion was 2013-07-17 12:56:19 (5 seconds ago) by 192.168.245.5:6200.
   ===============================================================================

The ``Oldest completion`` line in this example indicates that the
object-replicator on swift object server 192.168.245.3 has not completed
the replication cycle in 12 days. This replicator is stuck. The object
replicator cycle is generally less than 1 hour. Though an replicator
cycle of 15-20 hours can occur if nodes are added to the system and a
new ring has been deployed.

You can further check if the object replicator is stuck by logging on
the object server and checking the object replicator progress with
the following command:

.. code::

   #  sudo grep object-rep /var/log/swift/background.log | grep -e "Starting object replication" -e "Object replication complete" -e "partitions rep"
   Jul 16 06:25:46 192.168.245.4 object-replicator 15344/16450 (93.28%) partitions replicated in 69018.48s (0.22/sec, 22h remaining)
   Jul 16 06:30:46 192.168.245.4object-replicator 15344/16450 (93.28%) partitions replicated in 69318.58s (0.22/sec, 22h remaining)
   Jul 16 06:35:46 192.168.245.4 object-replicator 15344/16450 (93.28%) partitions replicated in 69618.63s (0.22/sec, 23h remaining)
   Jul 16 06:40:46 192.168.245.4 object-replicator 15344/16450 (93.28%) partitions replicated in 69918.73s (0.22/sec, 23h remaining)
   Jul 16 06:45:46 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 70218.75s (0.22/sec, 24h remaining)
   Jul 16 06:50:47 192.168.245.4object-replicator 15348/16450 (93.30%) partitions replicated in 70518.85s (0.22/sec, 24h remaining)
   Jul 16 06:55:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 70818.95s (0.22/sec, 25h remaining)
   Jul 16 07:00:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 71119.05s (0.22/sec, 25h remaining)
   Jul 16 07:05:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 71419.15s (0.21/sec, 26h remaining)
   Jul 16 07:10:47 192.168.245.4object-replicator 15348/16450 (93.30%) partitions replicated in 71719.25s (0.21/sec, 26h remaining)
   Jul 16 07:15:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 72019.27s (0.21/sec, 27h remaining)
   Jul 16 07:20:47 192.168.245.4object-replicator 15348/16450 (93.30%) partitions replicated in 72319.37s (0.21/sec, 27h remaining)
   Jul 16 07:25:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 72619.47s (0.21/sec, 28h remaining)
   Jul 16 07:30:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 72919.56s (0.21/sec, 28h remaining)
   Jul 16 07:35:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 73219.67s (0.21/sec, 29h remaining)
   Jul 16 07:40:47 192.168.245.4 object-replicator 15348/16450 (93.30%) partitions replicated in 73519.76s (0.21/sec, 29h remaining)

The above status is output every 5 minutes to ``/var/log/swift/background.log``.

.. note::

   The 'remaining' time is increasing as time goes on, normally the
   time remaining should be decreasing. Also note the partition number. For example,
   15344 remains the same for several status lines. Eventually the object
   replicator detects the hang and attempts to make progress by killing the
   problem thread. The replicator then progresses to the next partition but
   quite often it again gets stuck on the same partition.

One of the reasons for the object replicator hanging like this is
filesystem corruption on the drive. The following is a typical log entry
of a corrupted filesystem detected by the object replicator:

.. code::

   # sudo bzgrep "Remote I/O error" /var/log/swift/background.log* |grep srv | - tail -1
   Jul 12 03:33:30 192.168.245.4 object-replicator STDOUT: ERROR:root:Error hashing suffix#012Traceback (most recent call last):#012 File
   "/usr/lib/python2.7/dist-packages/swift/obj/replicator.py", line 199, in get_hashes#012 hashes[suffix] = hash_suffix(suffix_dir,
   reclaim_age)#012 File "/usr/lib/python2.7/dist-packages/swift/obj/replicator.py", line 84, in hash_suffix#012 path_contents =
   sorted(os.listdir(path))#012OSError: [Errno 121] Remote I/O error: '/srv/node/disk4/objects/1643763/b51'

An ``ls`` of the problem file or directory usually shows something like the following:

.. code::

   # ls -l /srv/node/disk4/objects/1643763/b51
   ls: cannot access /srv/node/disk4/objects/1643763/b51: Remote I/O error

If no entry with ``Remote I/O error`` occurs in the ``background.log`` it is
not possible to determine why the object-replicator is hung. It may be
that the ``Remote I/O error`` entry is older than 7 days and so has been
rotated out of the logs. In this scenario it may be best to simply
restart the object-replicator.

#. Stop the object-replicator:

   .. code::

      # sudo swift-init object-replicator stop

#. Make sure the object replicator has stopped, if it has hung, the stop
   command will not stop the hung process:

   .. code::

      # ps auxww | - grep swift-object-replicator

#. If the previous ps shows the object-replicator is still running, kill
   the process:

   .. code::

      # kill -9 <pid-of-swift-object-replicator>

#. Start the object-replicator:

   .. code::

      # sudo swift-init object-replicator start

If the above grep did find an ``Remote I/O error`` then it may be possible
to repair the problem filesystem.

#. Stop swift and rsync:

   .. code::

      # sudo swift-init all shutdown
      # sudo service rsync stop

#. Make sure all swift process have stopped:

   .. code::

      # ps auxww | grep swift | grep python

#. Kill any swift processes still running.

#. Unmount the problem filesystem:

   .. code::

      # sudo umount /srv/node/disk4

#. Repair the filesystem:

   .. code::

      # sudo xfs_repair -P /dev/sde1

#. If the ``xfs_repair`` fails then it may be necessary to re-format the
   filesystem. See :ref:`fix_broken_xfs_filesystem`. If the
   ``xfs_repair`` is successful, re-enable chef using the following command
   and replication should commence again.


Diagnose: High CPU load
-----------------------

The CPU load average on an object server, as shown with the
'uptime' command, is typically under 10 when the server is
lightly-moderately loaded:

.. code::

   $ uptime
   07:59:26 up 99 days,  5:57,  1 user,  load average: 8.59, 8.39, 8.32

During times of increased activity, due to user transactions or object
replication, the CPU load average can increase to  to around 30.

However, sometimes the CPU load average can increase significantly. The
following is an example of an object server that has extremely high CPU
load:

.. code::

   $ uptime
   07:44:02 up 18:22,  1 user,  load average: 407.12, 406.36, 404.59

Further issues and resolutions
------------------------------

.. note::

   The urgency levels in each **Action** column indicates whether or
   not it is required to take immediate action, or if the problem can be worked
   on during business hours.

.. list-table::
   :widths: 33 33 33
   :header-rows: 1

   * - **Scenario**
     - **Description**
     - **Action**
   * - ``/healthcheck`` latency is high.
     - The ``/healthcheck`` test does not tax the proxy very much so any drop in value is probably related to
       network issues, rather than the proxies being very busy. A very slow proxy might impact the average
       number, but it would need to be very slow to shift the number that much.
     - Check networks. Do a ``curl https://<ip-address>:<port>/healthcheck`` where
       ``ip-address`` is individual proxy IP address.
       Repeat this for every proxy server to see if you can pin point the problem.

       Urgency: If there are other indications that your system is slow, you should treat
       this as an urgent problem.
   * - Swift process is not running.
     - You can use ``swift-init`` status to check if swift processes are running on any
       given server.
     - Run this command:

       .. code::

          sudo swift-init all start

       Examine messages in the swift log files to see if there are any
       error messages related to any of the swift processes since the time you
       ran the ``swift-init`` command.

       Take any corrective actions that seem necessary.

       Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - ntpd is not running.
     - NTP is not running.
     - Configure and start NTP.

       Urgency: For proxy servers, this is vital.

   * - Host clock is not syncd to an NTP server.
     - Node time settings does not match NTP server time.
       This may take some time to sync after a reboot.
     - Assuming NTP is configured and running, you have to wait until the times sync.
   * - A swift process has hundreds, to thousands of open file descriptors.
     - May happen to any of the swift processes.
       Known to have happened with a ``rsyslod`` restart and where ``/tmp`` was hanging.

     - Restart the swift processes on the affected node:

       .. code::

          % sudo swift-init all reload

       Urgency:
                If known performance problem: Immediate

                If system seems fine: Medium
   * - A swift process is not owned by the swift user.
     - If the UID of the swift user has changed, then the processes might not be
       owned by that UID.
     - Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - Object account or container files not owned by swift.
     - This typically happens if during a reinstall or a re-image of a server that the UID
       of the swift user was changed. The data files in the object account and container
       directories are owned by the original swift UID. As a result, the current swift
       user does not own these files.
     - Correct the UID of the swift user to reflect that of the original UID. An alternate
       action is to change the ownership of every file on all file systems. This alternate
       action is often impractical and will take considerable time.

       Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - A disk drive has a high IO wait or service time.
     - If high wait IO times are seen for a single disk, then the disk drive is the problem.
       If most/all devices are slow, the controller is probably the source of the problem.
       The controller cache may also be miss configured – which will cause similar long
       wait or service times.
     - As a first step, if your controllers have a cache, check that it is enabled and their battery/capacitor
       is working.

       Second, reboot the server.
       If problem persists, file a DC ticket to have the drive or controller replaced.
       See :ref:`diagnose_slow_disk_drives` on how to check the drive wait or service times.

       Urgency: Medium
   * - The network interface is not up.
     - Use the ``ifconfig`` and ``ethtool`` commands to determine the network state.
     - You can try restarting the interface. However, generally the interface
       (or cable) is probably broken, especially if the interface is flapping.

       Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - Network interface card (NIC) is not operating at the expected speed.
     - The NIC is running at a slower speed than its nominal rated speed.
       For example, it is running at 100 Mb/s and the NIC is a 1Ge NIC.
     - 1. Try resetting the interface with:

       .. code::

          sudo ethtool -s eth0 speed 1000

       ... and then run:

       .. code::

          sudo lshw -class

       See if size goes to the expected speed. Failing
       that, check hardware (NIC cable/switch port).

       2. If persistent, consider shutting down the server (especially if a proxy)
          until the problem is identified and resolved. If you leave this server
          running it can have a large impact on overall performance.

       Urgency: High
   * - The interface RX/TX error count is non-zero.
     - A value of 0 is typical, but counts of 1 or 2 do not indicate a problem.
     - 1. For low numbers (For example, 1 or 2), you can simply ignore. Numbers in the range
          3-30 probably indicate that the error count has crept up slowly over a long time.
          Consider rebooting the server to remove the report from the noise.

          Typically, when a cable or interface is bad, the error count goes to 400+. For example,
          it stands out. There may be other symptoms such as the interface going up and down or
          not running at correct speed. A server with a high error count should be watched.

       2. If the error count continues to climb, consider taking the server down until
          it can be properly investigated. In any case, a reboot should be done to clear
          the error count.

       Urgency: High, if the error count increasing.

   * - In a swift log you see a message that a process has not replicated in over 24 hours.
     - The replicator has not successfully completed a run in the last 24 hours.
       This indicates that the replicator has probably hung.
     - Use ``swift-init`` to stop and then restart the replicator process.

       Urgency: Low. However if you
       recently added or replaced disk drives then you should treat this urgently.
   * - Container Updater has not run in 4 hour(s).
     - The service may appear to be running however, it may be hung. Examine their swift
       logs to see if there are any error messages relating to the container updater. This
       may potentially explain why the container is not running.
     - Urgency: Medium
       This may have been triggered by a recent restart of the  rsyslog daemon.
       Restart the service with:
       .. code::

          sudo swift-init <service> reload
   * - Object replicator: Reports the remaining time and that time is more than 100 hours.
     - Each replication cycle the object replicator writes a log message to its log
       reporting statistics about the current cycle. This includes an estimate for the
       remaining time needed to replicate all objects. If this time is longer than
       100 hours, there is a problem with the replication process.
     - Urgency: Medium
       Restart the service with:
       .. code::

          sudo swift-init object-replicator reload

       Check that the remaining replication time is going down.

