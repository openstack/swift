.. _saio:

=======================
SAIO (Swift All In One)
=======================

.. note::
    This guide assumes an existing Linux server. A physical machine or VM will
    work. We recommend configuring it with at least 2GB of memory and 40GB of
    storage space. We recommend using a VM in order to isolate Swift and its
    dependencies from other projects you may be working on.

---------------------------------------------
Instructions for setting up a development VM
---------------------------------------------

This section documents setting up a virtual machine for doing Swift
development.  The virtual machine will emulate running a four node Swift
cluster. To begin:

* Get a Linux system server image, this guide will cover:

  * Ubuntu 14.04, 16.04 LTS
  * CentOS 7
  * Fedora
  * OpenSuse

- Create guest virtual machine from the image.

----------------------------
What's in a <your-user-name>
----------------------------

Much of the configuration described in this guide requires escalated
administrator (``root``) privileges; however, we assume that administrator logs
in as an unprivileged user and can use ``sudo`` to run privileged commands.

Swift processes also run under a separate user and group, set by configuration
option, and referenced as ``<your-user-name>:<your-group-name>``.  The default user
is ``swift``, which may not exist on your system.  These instructions are
intended to allow a developer to use his/her username for
``<your-user-name>:<your-group-name>``.

.. note::
  For OpenSuse users, a user's primary group is ``users``, so you have 2 options:

  * Change ``${USER}:${USER}`` to ``${USER}:users`` in all references of this guide; or
  * Create a group for your username and add yourself to it::

     sudo groupadd ${USER} && sudo gpasswd -a ${USER} ${USER} && newgrp ${USER}

-----------------------
Installing dependencies
-----------------------

* On ``apt`` based systems::

        sudo apt-get update
        sudo apt-get install curl gcc memcached rsync sqlite3 xfsprogs \
                             git-core libffi-dev python-setuptools \
                             liberasurecode-dev libssl-dev
        sudo apt-get install python-coverage python-dev python-nose \
                             python-xattr python-eventlet \
                             python-greenlet python-pastedeploy \
                             python-netifaces python-pip python-dnspython \
                             python-mock

* On ``CentOS`` (requires additional repositories)::

        sudo yum update
        sudo yum install epel-release
        sudo yum-config-manager --enable epel extras
        sudo yum install centos-release-openstack-train
        sudo yum install curl gcc memcached rsync sqlite xfsprogs git-core \
                         libffi-devel xinetd liberasurecode-devel \
                         openssl-devel python-setuptools \
                         python-coverage python-devel python-nose \
                         pyxattr python-eventlet \
                         python-greenlet python-paste-deploy \
                         python-netifaces python-pip python-dns \
                         python-mock

* On ``Fedora``::

        sudo dnf update
        sudo dnf install curl gcc memcached rsync-daemon sqlite xfsprogs git-core \
                         libffi-devel liberasurecode-devel python-pyeclib \
                         openssl-devel python-setuptools \
                         python-coverage python-devel python-nose \
                         python-pyxattr python-eventlet \
                         python-greenlet python-paste-deploy \
                         python-netifaces python-pip python-dns \
                         python-mock

* On ``OpenSuse``::

        sudo zypper install curl gcc memcached rsync sqlite3 xfsprogs git-core \
                            libffi-devel liberasurecode-devel python2-setuptools \
                            libopenssl-devel
        sudo zypper install python2-coverage python-devel python2-nose \
                            python-xattr python-eventlet python2-greenlet \
                            python2-netifaces python2-pip python2-dnspython \
                            python2-mock

.. note::
   This installs necessary system dependencies and *most* of the python
   dependencies. Later in the process setuptools/distribute or pip will install
   and/or upgrade packages.

-------------------
Configuring storage
-------------------

Swift requires some space on XFS filesystems to store data and run tests.

Choose either :ref:`partition-section` or :ref:`loopback-section`.

.. _partition-section:

Using a partition for storage
=============================

If you are going to use a separate partition for Swift data, be sure to add
another device when creating the VM, and follow these instructions:

.. note::
   The disk does not have to be ``/dev/sdb1`` (for example, it could be
   ``/dev/vdb1``) however the mount point should still be ``/mnt/sdb1``.

#. Set up a single partition on the device (this will wipe the drive)::

      sudo parted /dev/sdb mklabel msdos mkpart p xfs 0% 100%

#. Create an XFS file system on the partition::

      sudo mkfs.xfs /dev/sdb1

#. Find the UUID of the new partition::

      sudo blkid

#. Edit ``/etc/fstab`` and add::

      UUID="<UUID-from-output-above>" /mnt/sdb1 xfs noatime 0 0

#. Create the Swift data mount point and test that mounting works::

      sudo mkdir /mnt/sdb1
      sudo mount -a

#. Next, skip to :ref:`common-dev-section`.

.. _loopback-section:

Using a loopback device for storage
===================================

If you want to use a loopback device instead of another partition, follow
these instructions:

#. Create the file for the loopback device::

      sudo mkdir -p /srv
      sudo truncate -s 1GB /srv/swift-disk
      sudo mkfs.xfs /srv/swift-disk

   Modify size specified in the ``truncate`` command to make a larger or
   smaller partition as needed.

#. Edit `/etc/fstab` and add::

      /srv/swift-disk /mnt/sdb1 xfs loop,noatime 0 0

#. Create the Swift data mount point and test that mounting works::

      sudo mkdir /mnt/sdb1
      sudo mount -a

.. _common-dev-section:

Common Post-Device Setup
========================

#. Create the individualized data links::

      sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
      sudo chown ${USER}:${USER} /mnt/sdb1/*
      for x in {1..4}; do sudo ln -s /mnt/sdb1/$x /srv/$x; done
      sudo mkdir -p /srv/1/node/sdb1 /srv/1/node/sdb5 \
                    /srv/2/node/sdb2 /srv/2/node/sdb6 \
                    /srv/3/node/sdb3 /srv/3/node/sdb7 \
                    /srv/4/node/sdb4 /srv/4/node/sdb8
      sudo mkdir -p /var/run/swift
      sudo mkdir -p /var/cache/swift /var/cache/swift2 \
                    /var/cache/swift3 /var/cache/swift4
      sudo chown -R ${USER}:${USER} /var/run/swift
      sudo chown -R ${USER}:${USER} /var/cache/swift*
      # **Make sure to include the trailing slash after /srv/$x/**
      for x in {1..4}; do sudo chown -R ${USER}:${USER} /srv/$x/; done

   .. note::
      We create the mount points and mount the loopback file under
      /mnt/sdb1. This file will contain one directory per simulated Swift node,
      each owned by the current Swift user.

      We then create symlinks to these directories under /srv.
      If the disk sdb or loopback file is unmounted, files will not be written under
      /srv/\*, because the symbolic link destination /mnt/sdb1/* will not
      exist. This prevents disk sync operations from writing to the root
      partition in the event a drive is unmounted.

#. Restore appropriate permissions on reboot.

   * On traditional Linux systems, add the following lines to ``/etc/rc.local`` (before the ``exit 0``)::

        mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
        chown <your-user-name>:<your-group-name> /var/cache/swift*
        mkdir -p /var/run/swift
        chown <your-user-name>:<your-group-name> /var/run/swift

   * On CentOS and Fedora we can use systemd (rc.local is deprecated)::

        cat << EOF |sudo tee /etc/tmpfiles.d/swift.conf
        d /var/cache/swift 0755 ${USER} ${USER} - -
        d /var/cache/swift2 0755 ${USER} ${USER} - -
        d /var/cache/swift3 0755 ${USER} ${USER} - -
        d /var/cache/swift4 0755 ${USER} ${USER} - -
        d /var/run/swift 0755 ${USER} ${USER} - -
        EOF

   * On OpenSuse place the lines in ``/etc/init.d/boot.local``.

   .. note::
      On some systems the rc file might need to be an executable shell script.

Creating an XFS tmp dir
-----------------------

Tests require having a directory available on an XFS filesystem. By default the
tests use ``/tmp``, however this can be pointed elsewhere with the ``TMPDIR``
environment variable.

.. note::
   If your root filesystem is XFS, you can skip this section if ``/tmp`` is
   just a directory and not a mounted tmpfs. Or you could simply point to any
   existing directory owned by your user by specifying it with the ``TMPDIR``
   environment variable.

   If your root filesystem is not XFS, you should create a loopback device,
   format it with XFS and mount it. You can mount it over ``/tmp`` or to
   another location and specify it with the ``TMPDIR`` environment variable.

* Create the file for the tmp loopback device::

      sudo mkdir -p /srv
      sudo truncate -s 1GB /srv/swift-tmp  # create 1GB file for XFS in /srv
      sudo mkfs.xfs /srv/swift-tmp

* To mount the tmp loopback device at ``/tmp``, do the following::

      sudo mount -o loop,noatime /srv/swift-tmp /tmp
      sudo chmod -R 1777 /tmp

  * To persist this, edit and add the following to ``/etc/fstab``::

        /srv/swift-tmp /tmp xfs rw,noatime,attr2,inode64,noquota 0 0

* To mount the tmp loopback at an alternate location (for example, ``/mnt/tmp``),
  do the following::

      sudo mkdir -p /mnt/tmp
      sudo mount -o loop,noatime /srv/swift-tmp /mnt/tmp
      sudo chown ${USER}:${USER} /mnt/tmp

  * To persist this, edit and add the following to ``/etc/fstab``::

        /srv/swift-tmp /mnt/tmp xfs rw,noatime,attr2,inode64,noquota 0 0

  * Set your ``TMPDIR`` environment dir so that Swift looks in the right location::

        export TMPDIR=/mnt/tmp
        echo "export TMPDIR=/mnt/tmp" >> $HOME/.bashrc

----------------
Getting the code
----------------

#. Check out the python-swiftclient repo::

      cd $HOME; git clone https://opendev.org/openstack/python-swiftclient.git

#. Build a development installation of python-swiftclient::

      cd $HOME/python-swiftclient; sudo python setup.py develop; cd -

   Ubuntu 12.04 users need to install python-swiftclient's dependencies before the installation of
   python-swiftclient. This is due to a bug in an older version of setup tools::

      cd $HOME/python-swiftclient; sudo pip install -r requirements.txt; sudo python setup.py develop; cd -

#. Check out the Swift repo::

      git clone https://github.com/openstack/swift.git

#. Build a development installation of Swift::

      cd $HOME/swift; sudo pip install --no-binary cryptography -r requirements.txt; sudo python setup.py develop; cd -

   .. note::
      Due to a difference in how ``libssl.so`` is named in OpenSuse vs. other Linux distros the
      wheel/binary won't work; thus we use ``--no-binary cryptography`` to build ``cryptography``
      locally.

   Fedora users might have to perform the following if development
   installation of Swift fails::

      sudo pip install -U xattr

#. Install Swift's test dependencies::

      cd $HOME/swift; sudo pip install -r test-requirements.txt

----------------
Setting up rsync
----------------

#. Create ``/etc/rsyncd.conf``::

      sudo cp $HOME/swift/doc/saio/rsyncd.conf /etc/
      sudo sed -i "s/<your-user-name>/${USER}/" /etc/rsyncd.conf

   Here is the default ``rsyncd.conf`` file contents maintained in the repo
   that is copied and fixed up above:

   .. literalinclude:: /../saio/rsyncd.conf
      :language: ini

#. Enable rsync daemon

   * On Ubuntu, edit the following line in ``/etc/default/rsync``::

      RSYNC_ENABLE=true

   .. note::
      You might have to create the file to perform the edits.

   * On CentOS and Fedora, enable the systemd service::

      sudo systemctl enable rsyncd

   * On OpenSuse, nothing needs to happen here.


#. On platforms with SELinux in ``Enforcing`` mode, either set to ``Permissive``::

      sudo setenforce Permissive
      sudo sed -i 's/^SELINUX=.*/SELINUX=permissive/g' /etc/selinux/config

   Or just allow rsync full access::

      sudo setsebool -P rsync_full_access 1

#. Start the rsync daemon

   * On Ubuntu 14.04, run::

      sudo service rsync restart

   * On Ubuntu 16.04, run::

      sudo systemctl enable rsync
      sudo systemctl start rsync

   * On CentOS, Fedora and OpenSuse, run::

      sudo systemctl start rsyncd

   * On other xinetd based systems simply run::

      sudo service xinetd restart

#. Verify rsync is accepting connections for all servers::

      rsync rsync://pub@localhost/

   You should see the following output from the above command::

      account6212
      account6222
      account6232
      account6242
      container6211
      container6221
      container6231
      container6241
      object6210
      object6220
      object6230
      object6240

------------------
Starting memcached
------------------

On non-Ubuntu distros you need to ensure memcached is running::

        sudo service memcached start
        sudo chkconfig memcached on

or::

        sudo systemctl enable memcached
        sudo systemctl start memcached

The tempauth middleware stores tokens in memcached. If memcached is not
running, tokens cannot be validated, and accessing Swift becomes impossible.

---------------------------------------------------
Optional: Setting up rsyslog for individual logging
---------------------------------------------------

Fedora and OpenSuse may not have rsyslog installed, in which case you will need
to install it if you want to use individual logging.

#. Install rsyslogd


   * On Fedora::

      sudo dnf install rsyslog

   * On OpenSuse::

      sudo zypper install rsyslog

#. Install the Swift rsyslogd configuration::

      sudo cp $HOME/swift/doc/saio/rsyslog.d/10-swift.conf /etc/rsyslog.d/

   Be sure to review that conf file to determine if you want all the logs
   in one file vs. all the logs separated out, and if you want hourly logs
   for stats processing. For convenience, we provide its default contents
   below:

   .. literalinclude:: /../saio/rsyslog.d/10-swift.conf
      :language: ini

#. Edit ``/etc/rsyslog.conf`` and make the following change (usually in the
   "GLOBAL DIRECTIVES" section)::

      $PrivDropToGroup adm

#. If using hourly logs (see above) perform::

      sudo mkdir -p /var/log/swift/hourly

   Otherwise perform::

      sudo mkdir -p /var/log/swift

#. Setup the logging directory and start syslog:

   * On Ubuntu::

      sudo chown -R syslog.adm /var/log/swift
      sudo chmod -R g+w /var/log/swift
      sudo service rsyslog restart

   * On CentOS, Fedora and OpenSuse::

      sudo chown -R root:adm /var/log/swift
      sudo chmod -R g+w /var/log/swift
      sudo systemctl restart rsyslog
      sudo systemctl enable rsyslog

---------------------
Configuring each node
---------------------

After performing the following steps, be sure to verify that Swift has access
to resulting configuration files (sample configuration files are provided with
all defaults in line-by-line comments).

#. Optionally remove an existing swift directory::

      sudo rm -rf /etc/swift

#. Populate the ``/etc/swift`` directory itself::

      cd $HOME/swift/doc; sudo cp -r saio/swift /etc/swift; cd -
      sudo chown -R ${USER}:${USER} /etc/swift

#. Update ``<your-user-name>`` references in the Swift config files::

      find /etc/swift/ -name \*.conf | xargs sudo sed -i "s/<your-user-name>/${USER}/"

The contents of the configuration files provided by executing the above
commands are as follows:

#. ``/etc/swift/swift.conf``

   .. literalinclude:: /../saio/swift/swift.conf
      :language: ini

#. ``/etc/swift/proxy-server.conf``

   .. literalinclude:: /../saio/swift/proxy-server.conf
      :language: ini

#. ``/etc/swift/object-expirer.conf``

   .. literalinclude:: /../saio/swift/object-expirer.conf
      :language: ini

#. ``/etc/swift/container-sync-realms.conf``

   .. literalinclude:: /../saio/swift/container-sync-realms.conf
      :language: ini

#. ``/etc/swift/account-server/1.conf``

   .. literalinclude:: /../saio/swift/account-server/1.conf
      :language: ini

#. ``/etc/swift/container-server/1.conf``

   .. literalinclude:: /../saio/swift/container-server/1.conf
      :language: ini

#. ``/etc/swift/container-reconciler/1.conf``

   .. literalinclude:: /../saio/swift/container-reconciler/1.conf
      :language: ini

#. ``/etc/swift/object-server/1.conf``

   .. literalinclude:: /../saio/swift/object-server/1.conf
      :language: ini

#. ``/etc/swift/account-server/2.conf``

   .. literalinclude:: /../saio/swift/account-server/2.conf
      :language: ini

#. ``/etc/swift/container-server/2.conf``

   .. literalinclude:: /../saio/swift/container-server/2.conf
      :language: ini

#. ``/etc/swift/container-reconciler/2.conf``

   .. literalinclude:: /../saio/swift/container-reconciler/2.conf
      :language: ini

#. ``/etc/swift/object-server/2.conf``

   .. literalinclude:: /../saio/swift/object-server/2.conf
      :language: ini

#. ``/etc/swift/account-server/3.conf``

   .. literalinclude:: /../saio/swift/account-server/3.conf
      :language: ini

#. ``/etc/swift/container-server/3.conf``

   .. literalinclude:: /../saio/swift/container-server/3.conf
      :language: ini

#. ``/etc/swift/container-reconciler/3.conf``

   .. literalinclude:: /../saio/swift/container-reconciler/3.conf
      :language: ini

#. ``/etc/swift/object-server/3.conf``

   .. literalinclude:: /../saio/swift/object-server/3.conf
      :language: ini

#. ``/etc/swift/account-server/4.conf``

   .. literalinclude:: /../saio/swift/account-server/4.conf
      :language: ini

#. ``/etc/swift/container-server/4.conf``

   .. literalinclude:: /../saio/swift/container-server/4.conf
      :language: ini

#. ``/etc/swift/container-reconciler/4.conf``

   .. literalinclude:: /../saio/swift/container-reconciler/4.conf
      :language: ini

#. ``/etc/swift/object-server/4.conf``

   .. literalinclude:: /../saio/swift/object-server/4.conf
      :language: ini

.. _setup_scripts:

------------------------------------
Setting up scripts for running Swift
------------------------------------

#. Copy the SAIO scripts for resetting the environment::

      mkdir -p $HOME/bin
      cd $HOME/swift/doc; cp saio/bin/* $HOME/bin; cd -
      chmod +x $HOME/bin/*

#. Edit the ``$HOME/bin/resetswift`` script

   The template ``resetswift`` script looks like the following:

   .. literalinclude:: /../saio/bin/resetswift
      :language: bash

   If you did not set up rsyslog for individual logging, remove the ``find
   /var/log/swift...`` line::

      sed -i "/find \/var\/log\/swift/d" $HOME/bin/resetswift


#. Install the sample configuration file for running tests::

      cp $HOME/swift/test/sample.conf /etc/swift/test.conf

   The template ``test.conf`` looks like the following:

   .. literalinclude:: /../../test/sample.conf
      :language: ini

-----------------------------------------
Configure environment variables for Swift
-----------------------------------------

#. Add an environment variable for running tests below::

      echo "export SWIFT_TEST_CONFIG_FILE=/etc/swift/test.conf" >> $HOME/.bashrc

#. Be sure that your ``PATH`` includes the ``bin`` directory::

      echo "export PATH=${PATH}:$HOME/bin" >> $HOME/.bashrc

#. If you are using a loopback device for Swift Storage, add an environment var
   to substitute ``/dev/sdb1`` with ``/srv/swift-disk``::

      echo "export SAIO_BLOCK_DEVICE=/srv/swift-disk" >> $HOME/.bashrc

#. If you are using a device other than ``/dev/sdb1`` for Swift storage (for
   example, ``/dev/vdb1``), add an environment var to substitute it::

      echo "export SAIO_BLOCK_DEVICE=/dev/vdb1" >> $HOME/.bashrc

#. If you are using a location other than ``/tmp`` for Swift tmp data (for
   example, ``/mnt/tmp``), add ``TMPDIR`` environment var to set it::

      export TMPDIR=/mnt/tmp
      echo "export TMPDIR=/mnt/tmp" >> $HOME/.bashrc

#. Source the above environment variables into your current environment::

      . $HOME/.bashrc

--------------------------
Constructing initial rings
--------------------------

#. Construct the initial rings using the provided script::

      remakerings

   The ``remakerings`` script looks like the following:

   .. literalinclude:: /../saio/bin/remakerings
      :language: bash

   You can expect the output from this command to produce the following.  Note
   that 3 object rings are created in order to test storage policies and EC in
   the SAIO environment.  The EC ring is the only one with all 8 devices.
   There are also two replication rings, one for 3x replication and another
   for 2x replication, but those rings only use 4 devices:


   .. code-block:: console

      Device d0r1z1-127.0.0.1:6210R127.0.0.1:6210/sdb1_"" with 1.0 weight got id 0
      Device d1r1z2-127.0.0.2:6220R127.0.0.2:6220/sdb2_"" with 1.0 weight got id 1
      Device d2r1z3-127.0.0.3:6230R127.0.0.3:6230/sdb3_"" with 1.0 weight got id 2
      Device d3r1z4-127.0.0.4:6240R127.0.0.4:6240/sdb4_"" with 1.0 weight got id 3
      Reassigned 3072 (300.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
      Device d0r1z1-127.0.0.1:6210R127.0.0.1:6210/sdb1_"" with 1.0 weight got id 0
      Device d1r1z2-127.0.0.2:6220R127.0.0.2:6220/sdb2_"" with 1.0 weight got id 1
      Device d2r1z3-127.0.0.3:6230R127.0.0.3:6230/sdb3_"" with 1.0 weight got id 2
      Device d3r1z4-127.0.0.4:6240R127.0.0.4:6240/sdb4_"" with 1.0 weight got id 3
      Reassigned 2048 (200.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
      Device d0r1z1-127.0.0.1:6210R127.0.0.1:6210/sdb1_"" with 1.0 weight got id 0
      Device d1r1z1-127.0.0.1:6210R127.0.0.1:6210/sdb5_"" with 1.0 weight got id 1
      Device d2r1z2-127.0.0.2:6220R127.0.0.2:6220/sdb2_"" with 1.0 weight got id 2
      Device d3r1z2-127.0.0.2:6220R127.0.0.2:6220/sdb6_"" with 1.0 weight got id 3
      Device d4r1z3-127.0.0.3:6230R127.0.0.3:6230/sdb3_"" with 1.0 weight got id 4
      Device d5r1z3-127.0.0.3:6230R127.0.0.3:6230/sdb7_"" with 1.0 weight got id 5
      Device d6r1z4-127.0.0.4:6240R127.0.0.4:6240/sdb4_"" with 1.0 weight got id 6
      Device d7r1z4-127.0.0.4:6240R127.0.0.4:6240/sdb8_"" with 1.0 weight got id 7
      Reassigned 6144 (600.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
      Device d0r1z1-127.0.0.1:6211R127.0.0.1:6211/sdb1_"" with 1.0 weight got id 0
      Device d1r1z2-127.0.0.2:6221R127.0.0.2:6221/sdb2_"" with 1.0 weight got id 1
      Device d2r1z3-127.0.0.3:6231R127.0.0.3:6231/sdb3_"" with 1.0 weight got id 2
      Device d3r1z4-127.0.0.4:6241R127.0.0.4:6241/sdb4_"" with 1.0 weight got id 3
      Reassigned 3072 (300.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
      Device d0r1z1-127.0.0.1:6212R127.0.0.1:6212/sdb1_"" with 1.0 weight got id 0
      Device d1r1z2-127.0.0.2:6222R127.0.0.2:6222/sdb2_"" with 1.0 weight got id 1
      Device d2r1z3-127.0.0.3:6232R127.0.0.3:6232/sdb3_"" with 1.0 weight got id 2
      Device d3r1z4-127.0.0.4:6242R127.0.0.4:6242/sdb4_"" with 1.0 weight got id 3
      Reassigned 3072 (300.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00


#. Read more about Storage Policies and your SAIO :doc:`policies_saio`

-------------
Testing Swift
-------------

#. Verify the unit tests run::

      $HOME/swift/.unittests

   Note that the unit tests do not require any Swift daemons running.

#. Start the "main" Swift daemon processes (proxy, account, container, and
   object)::

      startmain

   (The "``Unable to increase file descriptor limit.  Running as non-root?``"
   warnings are expected and ok.)

   The ``startmain`` script looks like the following:

   .. literalinclude:: /../saio/bin/startmain
      :language: bash

#. Get an ``X-Storage-Url`` and ``X-Auth-Token``::

      curl -v -H 'X-Storage-User: test:tester' -H 'X-Storage-Pass: testing' http://127.0.0.1:8080/auth/v1.0

#. Check that you can ``GET`` account::

      curl -v -H 'X-Auth-Token: <token-from-x-auth-token-above>' <url-from-x-storage-url-above>

#. Check that ``swift`` command provided by the python-swiftclient package works::

      swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing stat

#. Verify the functional tests run::

      $HOME/swift/.functests

   (Note: functional tests will first delete everything in the configured
   accounts.)

#. Verify the probe tests run::

      $HOME/swift/.probetests

   (Note: probe tests will reset your environment as they call ``resetswift``
   for each test.)

----------------
Debugging Issues
----------------

If all doesn't go as planned, and tests fail, or you can't auth, or something
doesn't work, here are some good starting places to look for issues:

#. Everything is logged using system facilities -- usually in ``/var/log/syslog``,
   but possibly in ``/var/log/messages`` on e.g. Fedora -- so that is a good first
   place to look for errors (most likely python tracebacks).
#. Make sure all of the server processes are running.  For the base
   functionality, the Proxy, Account, Container, and Object servers
   should be running.
#. If one of the servers are not running, and no errors are logged to syslog,
   it may be useful to try to start the server manually, for example:
   ``swift-object-server /etc/swift/object-server/1.conf`` will start the
   object server.  If there are problems not showing up in syslog,
   then you will likely see the traceback on startup.
#. If you need to, you can turn off syslog for unit tests. This can be
   useful for environments where ``/dev/log`` is unavailable, or which
   cannot rate limit (unit tests generate a lot of logs very quickly).
   Open the file ``SWIFT_TEST_CONFIG_FILE`` points to, and change the
   value of ``fake_syslog`` to ``True``.
#. If you encounter a ``401 Unauthorized`` when following Step 12 where
   you check that you can ``GET`` account, use ``sudo service memcached status``
   and check if memcache is running. If memcache is not running, start it using
   ``sudo service memcached start``. Once memcache is running, rerun ``GET`` account.

------------
Known Issues
------------

Listed here are some "gotcha's" that you may run into when using or testing your SAIO:

#. fallocate_reserve - in most cases a SAIO doesn't have a very large XFS partition
   so having fallocate enabled and fallocate_reserve set can cause issues, specifically
   when trying to run the functional tests. For this reason fallocate has been turned
   off on the object-servers in the SAIO. If you want to play with the fallocate_reserve
   settings then know that functional tests will fail unless you change the max_file_size
   constraint to something more reasonable then the default (5G). Ideally you'd make
   it 1/4 of your XFS file system size so the tests can pass.
