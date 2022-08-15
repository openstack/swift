=================================
Software configuration procedures
=================================

.. _fix_broken_gpt_table:

Fix broken GPT table (broken disk partition)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  If a GPT table is broken, a message like the following should be
   observed when the command...

   .. code:: console

      $ sudo parted -l

-  ... is run.

   .. code:: console

      ...
      Error: The backup GPT table is corrupt, but the primary appears OK, so that will
      be used.
      OK/Cancel?

#. To fix this, firstly install the ``gdisk`` program to fix this:

   .. code:: console

      $ sudo aptitude install gdisk

#. Run ``gdisk`` for the particular drive with the damaged partition:

   .. code: console

      $ sudo gdisk /dev/sd*a-l*
      GPT fdisk (gdisk) version 0.6.14

      Caution: invalid backup GPT header, but valid main header; regenerating
      backup header from main header.

      Warning! One or more CRCs don't match. You should repair the disk!

      Partition table scan:
         MBR: protective
         BSD: not present
         APM: not present
         GPT: damaged
      /dev/sd
      *****************************************************************************
      Caution: Found protective or hybrid MBR and corrupt GPT. Using GPT, but disk
      verification and recovery are STRONGLY recommended.
      *****************************************************************************

#. On the command prompt, type ``r`` (recovery and transformation
   options), followed by ``d`` (use main GPT header) , ``v`` (verify disk)
   and finally ``w`` (write table to disk and exit). Will also need to
   enter ``Y`` when prompted in order to confirm actions.

   .. code:: console

      Command (? for help): r

      Recovery/transformation command (? for help): d

      Recovery/transformation command (? for help): v

      Caution: The CRC for the backup partition table is invalid. This table may
      be corrupt. This program will automatically create a new backup partition
      table when you save your partitions.

      Caution: Partition 1 doesn't begin on a 8-sector boundary. This may
      result in degraded performance on some modern (2009 and later) hard disks.

      Caution: Partition 2 doesn't begin on a 8-sector boundary. This may
      result in degraded performance on some modern (2009 and later) hard disks.

      Caution: Partition 3 doesn't begin on a 8-sector boundary. This may
      result in degraded performance on some modern (2009 and later) hard disks.

      Identified 1 problems!

      Recovery/transformation command (? for help): w

      Final checks complete. About to write GPT data. THIS WILL OVERWRITE EXISTING
      PARTITIONS!!

      Do you want to proceed, possibly destroying your data? (Y/N): Y

      OK; writing new GUID partition table (GPT).
      The operation has completed successfully.

#. Running the command:

   .. code:: console

      $ sudo parted /dev/sd#

#. Should now show that the partition is recovered and healthy again.

#. Finally, uninstall ``gdisk`` from the node:

   .. code:: console

      $ sudo aptitude remove gdisk

.. _fix_broken_xfs_filesystem:

Procedure: Fix broken XFS filesystem
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. A filesystem may be corrupt or broken if the following output is
   observed when checking its label:

   .. code:: console

      $ sudo xfs_admin -l /dev/sd#
      cache_node_purge: refcount was 1, not zero (node=0x25d5ee0)
      xfs_admin: cannot read root inode (117)
      cache_node_purge: refcount was 1, not zero (node=0x25d92b0)
      xfs_admin: cannot read realtime bitmap inode (117)
      bad sb magic # 0 in AG 1
      failed to read label in AG 1

#. Run the following commands to remove the broken/corrupt filesystem and replace.
   (This example uses the filesystem ``/dev/sdb2``) Firstly need to replace the partition:

   .. code:: console

      $ sudo parted
      GNU Parted 2.3
      Using /dev/sda
      Welcome to GNU Parted! Type 'help' to view a list of commands.
      (parted) select /dev/sdb
      Using /dev/sdb
      (parted) p
      Model: HP LOGICAL VOLUME (scsi)
      Disk /dev/sdb: 2000GB
      Sector size (logical/physical): 512B/512B
      Partition Table: gpt

      Number  Start   End     Size    File system  Name   Flags
      1      17.4kB  1024MB  1024MB  ext3                 boot
      2      1024MB  1751GB  1750GB  xfs          sw-aw2az1-object045-disk1
      3      1751GB  2000GB  249GB                        lvm

      (parted) rm 2
      (parted) mkpart primary 2 -1
      Warning: You requested a partition from 2000kB to 2000GB.
      The closest location we can manage is 1024MB to 1751GB.
      Is this still acceptable to you?
      Yes/No? Yes
      Warning: The resulting partition is not properly aligned for best performance.
      Ignore/Cancel? Ignore
      (parted) p
      Model: HP LOGICAL VOLUME (scsi)
      Disk /dev/sdb: 2000GB
      Sector size (logical/physical): 512B/512B
      Partition Table: gpt

      Number  Start   End     Size    File system  Name     Flags
      1      17.4kB  1024MB  1024MB  ext3                  boot
      2      1024MB  1751GB  1750GB  xfs          primary
      3      1751GB  2000GB  249GB                         lvm

      (parted) quit

#. Next step is to scrub the filesystem and format:

   .. code:: console

      $ sudo dd if=/dev/zero of=/dev/sdb2 bs=$((1024*1024)) count=1
      1+0 records in
      1+0 records out
      1048576 bytes (1.0 MB) copied, 0.00480617 s, 218 MB/s
      $ sudo /sbin/mkfs.xfs -f -i size=1024 /dev/sdb2
      meta-data=/dev/sdb2              isize=1024   agcount=4, agsize=106811524 blks
               =                       sectsz=512   attr=2, projid32bit=0
      data     =                       bsize=4096   blocks=427246093, imaxpct=5
               =                       sunit=0      swidth=0 blks
      naming   =version 2              bsize=4096   ascii-ci=0
      log      =internal log           bsize=4096   blocks=208616, version=2
               =                       sectsz=512   sunit=0 blks, lazy-count=1
      realtime =none                   extsz=4096   blocks=0, rtextents=0

#. You should now label and mount your filesystem.

#. Can now check to see if the filesystem is mounted using the command:

   .. code:: console

      $ mount

.. _checking_if_account_ok:

Procedure: Checking if an account is okay
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   ``swift-direct`` is only available in the HPE Helion Public Cloud.
   Use ``swiftly`` as an alternate (or use ``swift-get-nodes`` as explained
   here).

You must know the tenant/project ID. You can check if the account is okay as follows from a proxy.

.. code:: console

   $ sudo -u swift  /opt/hp/swift/bin/swift-direct show AUTH_<project-id>

The response will either be similar to a swift list of the account
containers, or an error indicating that the resource could not be found.

Alternatively, you can use ``swift-get-nodes`` to find the account database
files. Run the following on a proxy:

.. code:: console

   $ sudo swift-get-nodes /etc/swift/account.ring.gz  AUTH_<project-id>

The response will print curl/ssh commands that will list the replicated
account databases. Use the indicated ``curl`` or ``ssh`` commands to check
the status and existence of the account.

Procedure: Getting  swift account stats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   ``swift-direct`` is specific to the HPE Helion Public Cloud. Go look at
   ``swifty`` for an alternate or use ``swift-get-nodes`` as explained
   in :ref:`checking_if_account_ok`.

This procedure describes how you determine the swift usage for a given
swift account, that is the number of containers, number of objects and
total bytes used. To do this you will need the project ID.

Log onto one of the swift proxy servers.

Use swift-direct to show this accounts usage:

.. code:: console

   $ sudo -u swift /opt/hp/swift/bin/swift-direct show AUTH_<project-id>
   Status: 200
         Content-Length: 0
         Accept-Ranges: bytes
         X-Timestamp: 1379698586.88364
         X-Account-Bytes-Used: 67440225625994
         X-Account-Container-Count: 1
         Content-Type: text/plain; charset=utf-8
         X-Account-Object-Count: 8436776
         Status: 200
         name: my_container  count: 8436776  bytes: 67440225625994

This account has 1 container. That container has 8436776 objects. The
total bytes used is 67440225625994.

Procedure: Revive a deleted account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Swift accounts are normally not recreated. If a tenant/project is deleted,
the account can then be deleted. If the user wishes to use Swift again,
the normal process is to create a new tenant/project -- and hence a
new Swift account.

However, if the Swift account is deleted, but the tenant/project is not
deleted from Keystone, the user can no longer access the account. This
is because the account is marked deleted in Swift. You can revive
the account as described in this process.

.. note::

    The containers and objects in the "old" account cannot be listed
    anymore. In addition, if the Account Reaper process has not
    finished reaping the containers and objects in the "old" account, these
    are effectively orphaned and it is virtually impossible to find and delete
    them to free up disk space.

The solution is to delete the account database files and
re-create the account as follows:

#. You must know the tenant/project ID. The account name is AUTH_<project-id>.
   In this example, the tenant/project is ``4ebe3039674d4864a11fe0864ae4d905``
   so the Swift account name is ``AUTH_4ebe3039674d4864a11fe0864ae4d905``.

#. Use ``swift-get-nodes`` to locate the account's database files (on three
   servers). The output has been truncated so we can focus on the import pieces
   of data:

   .. code:: console

       $ sudo swift-get-nodes /etc/swift/account.ring.gz AUTH_4ebe3039674d4864a11fe0864ae4d905
       ...
       curl -I -XHEAD "http://192.168.245.5:6202/disk1/3934/AUTH_4ebe3039674d4864a11fe0864ae4d905"
       curl -I -XHEAD "http://192.168.245.3:6202/disk0/3934/AUTH_4ebe3039674d4864a11fe0864ae4d905"
       curl -I -XHEAD "http://192.168.245.4:6202/disk1/3934/AUTH_4ebe3039674d4864a11fe0864ae4d905"
       ...
       Use your own device location of servers:
       such as "export DEVICE=/srv/node"
       ssh 192.168.245.5 "ls -lah ${DEVICE:-/srv/node*}/disk1/accounts/3934/052/f5ecf8b40de3e1b0adb0dbe576874052"
       ssh 192.168.245.3 "ls -lah ${DEVICE:-/srv/node*}/disk0/accounts/3934/052/f5ecf8b40de3e1b0adb0dbe576874052"
       ssh 192.168.245.4 "ls -lah ${DEVICE:-/srv/node*}/disk1/accounts/3934/052/f5ecf8b40de3e1b0adb0dbe576874052"
       ...
       note: `/srv/node*` is used as default value of `devices`, the real value is set in the config file on each storage node.


#. Before proceeding check that the account is really deleted by using curl. Execute the
   commands printed by ``swift-get-nodes``. For example:

   .. code:: console

       $ curl -I -XHEAD "http://192.168.245.5:6202/disk1/3934/AUTH_4ebe3039674d4864a11fe0864ae4d905"
       HTTP/1.1 404 Not Found
       Content-Length: 0
       Content-Type: text/html; charset=utf-8

   Repeat for the other two servers (192.168.245.3 and 192.168.245.4).
   A ``404 Not Found`` indicates that the account is deleted (or never existed).

   If you get a ``204 No Content`` response, do **not** proceed.

#. Use the ssh commands printed by ``swift-get-nodes`` to check if database
   files exist. For example:

   .. code:: console

       $  ssh 192.168.245.5 "ls -lah ${DEVICE:-/srv/node*}/disk1/accounts/3934/052/f5ecf8b40de3e1b0adb0dbe576874052"
       total 20K
       drwxr-xr-x 2 swift swift 110 Mar  9 10:22 .
       drwxr-xr-x 3 swift swift  45 Mar  9 10:18 ..
       -rw------- 1 swift swift 17K Mar  9 10:22 f5ecf8b40de3e1b0adb0dbe576874052.db
       -rw-r--r-- 1 swift swift   0 Mar  9 10:22 f5ecf8b40de3e1b0adb0dbe576874052.db.pending
       -rwxr-xr-x 1 swift swift   0 Mar  9 10:18 .lock

   Repeat for the other two servers (192.168.245.3 and 192.168.245.4).

   If no files exist, no further action is needed.

#. Stop Swift processes on all nodes listed by ``swift-get-nodes``
   (In this example, that is 192.168.245.3, 192.168.245.4 and 192.168.245.5).

#. We recommend you make backup copies of the database files.

#. Delete the database files. For example:

   .. code:: console

       $ ssh 192.168.245.5
       $ cd /srv/node/disk1/accounts/3934/052/f5ecf8b40de3e1b0adb0dbe576874052
       $ sudo rm *

   Repeat for the other two servers (192.168.245.3 and 192.168.245.4).

#. Restart Swift on all three servers

At this stage, the account is fully deleted. If you enable the auto-create option, the
next time the user attempts to access the account, the account will be created.
You may also use swiftly to recreate the account.


Procedure: Temporarily stop load balancers from directing traffic to a proxy server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can stop the load balancers sending requests to a proxy server as
follows. This can be useful when a proxy is misbehaving but you need
Swift running to help diagnose the problem. By removing from the load
balancers, customer's are not impacted by the misbehaving proxy.

#. Ensure that in /etc/swift/proxy-server.conf the ``disable_path`` variable is set to
   ``/etc/swift/disabled-by-file``.

#. Log onto the proxy node.

#. Shut down Swift as follows:

   .. code:: console

      $ sudo swift-init proxy shutdown

   .. note::

      Shutdown, not stop.

#. Create the ``/etc/swift/disabled-by-file`` file. For example:

   .. code:: console

      $ sudo touch /etc/swift/disabled-by-file

#. Optional, restart Swift:

   .. code:: console

      $ sudo swift-init proxy start

It works because the healthcheck middleware looks for /etc/swift/disabled-by-file.
If it exists, the middleware will return 503/error instead of 200/OK. This means the load balancer
should stop sending traffic to the proxy.

Procedure: Ad-Hoc disk performance test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can get an idea whether a disk drive is performing as follows:

.. code:: console

   $ sudo dd bs=1M count=256 if=/dev/zero conv=fdatasync of=/srv/node/disk11/remember-to-delete-this-later

You can expect ~600MB/sec. If you get a low number, repeat many times as
Swift itself may also read or write to the disk, hence giving a lower
number.
