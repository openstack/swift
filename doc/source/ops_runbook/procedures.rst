=================================
Software configuration procedures
=================================

Fix broken GPT table (broken disk partition)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  If a GPT table is broken, a message like the following should be
   observed when the command...

   .. code::

      $ sudo parted -l

-  ... is run.

   .. code::

      ...
      Error: The backup GPT table is corrupt, but the primary appears OK, so that will
      be used.
      OK/Cancel?

#. To fix this, firstly install the ``gdisk`` program to fix this:

   .. code::

      $ sudo aptitude install gdisk

#. Run ``gdisk`` for the particular drive with the damaged partition:

   .. code:

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

   .. code::

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

   .. code::

      $ sudo parted /dev/sd#

#. Should now show that the partition is recovered and healthy again.

#. Finally, uninstall ``gdisk`` from the node:

   .. code::

      $ sudo aptitude remove gdisk

Procedure: Fix broken XFS filesystem
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. A filesystem may be corrupt or broken if the following output is
   observed when checking its label:

   .. code::

      $ sudo xfs_admin -l /dev/sd#
        cache_node_purge: refcount was 1, not zero (node=0x25d5ee0)
        xfs_admin: cannot read root inode (117)
        cache_node_purge: refcount was 1, not zero (node=0x25d92b0)
        xfs_admin: cannot read realtime bitmap inode (117)
        bad sb magic # 0 in AG 1
        failed to read label in AG 1

#. Run the following commands to remove the broken/corrupt filesystem and replace.
   (This example uses the filesystem ``/dev/sdb2``) Firstly need to replace the partition:

   .. code::

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

   .. code::

      $ sudo dd if=/dev/zero of=/dev/sdb2 bs=$((1024\*1024)) count=1
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

   .. code::

      $ mount

Procedure: Checking if an account is okay
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   ``swift-direct`` is only available in the HPE Helion Public Cloud.
   Use ``swiftly`` as an alternate.

If you have a tenant ID you can check the account is okay as follows from a proxy.

.. code::

   $ sudo -u swift  /opt/hp/swift/bin/swift-direct show <Api-Auth-Hash-or-TenantId>

The response will either be similar to a swift list of the account
containers, or an error indicating that the resource could not be found.

In the latter case you can establish if a backend database exists for
the tenantId by running the following on a proxy:

.. code::

   $ sudo -u swift  swift-get-nodes /etc/swift/account.ring.gz  <Api-Auth-Hash-or-TenantId>

The response will list ssh commands that will list the replicated
account databases, if they exist.

Procedure: Revive a deleted account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Swift accounts are normally not recreated. If a tenant unsubscribes from
Swift, the account is deleted. To re-subscribe to Swift, you can create
a new tenant (new tenant ID), and subscribe to Swift. This creates a
new Swift account with the new tenant ID.

However, until the unsubscribe/new tenant process is supported, you may
hit a situation where a Swift account is deleted and the user is locked
out of Swift.

Deleting the account database files
-----------------------------------

Here is one possible solution. The containers and objects may be lost
forever. The solution is to delete the account database files and
re-create the account. This may only be done once the containers and
objects are completely deleted. This process is untested, but could
work as follows:

#. Use swift-get-nodes to locate the account's database file (on three
   servers).

#. Rename the database files (on three servers).

#. Use ``swiftly`` to create the account (use original name).

Renaming account database so it can be revived
----------------------------------------------

Get the locations of the database files that hold the account data.

   .. code::

      sudo swift-get-nodes /etc/swift/account.ring.gz AUTH_redacted-1856-44ae-97db-31242f7ad7a1

      Account  AUTH_redacted-1856-44ae-97db-31242f7ad7a1
      Container None

      Object    None

      Partition 18914

      Hash        93c41ef56dd69173a9524193ab813e78

      Server:Port Device 15.184.9.126:6002 disk7
      Server:Port Device 15.184.9.94:6002 disk11
      Server:Port Device 15.184.9.103:6002 disk10
      Server:Port Device 15.184.9.80:6002 disk2  [Handoff]
      Server:Port Device 15.184.9.120:6002 disk2  [Handoff]
      Server:Port Device 15.184.9.98:6002 disk2  [Handoff]

      curl -I -XHEAD "`*http://15.184.9.126:6002/disk7/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.126:6002/disk7/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_
      curl -I -XHEAD "`*http://15.184.9.94:6002/disk11/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.94:6002/disk11/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_

      curl -I -XHEAD "`*http://15.184.9.103:6002/disk10/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.103:6002/disk10/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_

      curl -I -XHEAD "`*http://15.184.9.80:6002/disk2/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.80:6002/disk2/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ # [Handoff]
      curl -I -XHEAD "`*http://15.184.9.120:6002/disk2/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.120:6002/disk2/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ # [Handoff]
      curl -I -XHEAD "`*http://15.184.9.98:6002/disk2/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.98:6002/disk2/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ # [Handoff]

      ssh 15.184.9.126 "ls -lah /srv/node/disk7/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/"
      ssh 15.184.9.94 "ls -lah /srv/node/disk11/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/"
      ssh 15.184.9.103 "ls -lah /srv/node/disk10/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/"
      ssh 15.184.9.80 "ls -lah /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/" # [Handoff]
      ssh 15.184.9.120 "ls -lah /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/" # [Handoff]
      ssh 15.184.9.98 "ls -lah /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/" # [Handoff]

      $ sudo swift-get-nodes /etc/swift/account.ring.gz AUTH\_redacted-1856-44ae-97db-31242f7ad7a1Account  AUTH_redacted-1856-44ae-97db-
      31242f7ad7a1Container  NoneObject      NonePartition   18914Hash           93c41ef56dd69173a9524193ab813e78Server:Port Device  15.184.9.126:6002 disk7Server:Port Device   15.184.9.94:6002 disk11Server:Port Device   15.184.9.103:6002 disk10Server:Port Device  15.184.9.80:6002
      disk2   [Handoff]Server:Port Device    15.184.9.120:6002 disk2  [Handoff]Server:Port Device    15.184.9.98:6002 disk2   [Handoff]curl -I -XHEAD
      "`*http://15.184.9.126:6002/disk7/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"*<http://15.184.9.126:6002/disk7/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ curl -I -XHEAD

      "`*http://15.184.9.94:6002/disk11/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.94:6002/disk11/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ curl -I -XHEAD

      "`*http://15.184.9.103:6002/disk10/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.103:6002/disk10/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ curl -I -XHEAD

      "`*http://15.184.9.80:6002/disk2/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.80:6002/disk2/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ # [Handoff]curl -I -XHEAD

      "`*http://15.184.9.120:6002/disk2/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.120:6002/disk2/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ # [Handoff]curl -I -XHEAD

      "`*http://15.184.9.98:6002/disk2/18914/AUTH_redacted-1856-44ae-97db-31242f7ad7a1"* <http://15.184.9.98:6002/disk2/18914/AUTH_cc9ebdb8-1856-44ae-97db-31242f7ad7a1>`_ # [Handoff]ssh 15.184.9.126

      "ls -lah /srv/node/disk7/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/"ssh 15.184.9.94 "ls -lah /srv/node/disk11/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/"ssh 15.184.9.103
      "ls -lah /srv/node/disk10/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/"ssh 15.184.9.80 "ls -lah /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/" # [Handoff]ssh 15.184.9.120
      "ls -lah /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/" # [Handoff]ssh 15.184.9.98 "ls -lah /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/" # [Handoff]

Check that the handoff nodes do not have account databases:

.. code::

   $ ssh 15.184.9.80 "ls -lah /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/"
   ls: cannot access /srv/node/disk2/accounts/18914/e78/93c41ef56dd69173a9524193ab813e78/: No such file or directory

If the handoff node has a database, wait for rebalancing to occur.

Procedure: Temporarily stop load balancers from directing traffic to a proxy server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can stop the load balancers sending requests to a proxy server as
follows. This can be useful when a proxy is misbehaving but you need
Swift running to help diagnose the problem. By removing from the load
balancers, customer's are not impacted by the misbehaving proxy.

#. Ensure that in proxyserver.com the ``disable_path`` variable is set to
   ``/etc/swift/disabled-by-file``.

#. Log onto the proxy node.

#. Shut down Swift as follows:

   .. code::

      sudo swift-init proxy shutdown

      .. note::

         Shutdown, not stop.

#. Create the ``/etc/swift/disabled-by-file`` file. For example:

   .. code::

      sudo touch /etc/swift/disabled-by-file

#. Optional, restart Swift:

   .. code::

      sudo swift-init proxy start

It works because the healthcheck middleware looks for this file. If it
find it, it will return 503 error instead of 200/OK. This means the load balancer
should stop sending traffic to the proxy.

``/healthcheck`` will report
``FAIL: disabled by file`` if the ``disabled-by-file`` file exists.

Procedure: Ad-Hoc disk performance test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can get an idea whether a disk drive is performing as follows:

.. code::

   sudo dd bs=1M count=256 if=/dev/zero conv=fdatasync of=/srv/node/disk11/remember-to-delete-this-later

You can expect ~600MB/sec. If you get a low number, repeat many times as
Swift itself may also read or write to the disk, hence giving a lower
number.
