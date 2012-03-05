=======================
SAIO - Swift All In One
=======================

---------------------------------------------
Instructions for setting up a development VM
---------------------------------------------

This section documents setting up a virtual machine for doing Swift development.
The virtual machine will emulate running a four node Swift cluster.

* Get the *Ubuntu 10.04 LTS (Lucid Lynx)* server image:

  - Ubuntu Server ISO: http://releases.ubuntu.com/lucid/ubuntu-10.04.4-server-amd64.iso (717 MB)
  - Ubuntu Live/Install: http://cdimage.ubuntu.com/releases/lucid/release/ubuntu-10.04.4-dvd-amd64.iso (4.2 GB)
  - Ubuntu Mirrors: https://launchpad.net/ubuntu/+cdmirrors

* Create guest virtual machine from the Ubuntu image.

Additional information about setting up a Swift development snapshot on other distributions is
available on the wiki at http://wiki.openstack.org/SAIOInstructions.

-----------------------------------------
Installing dependencies and the core code
-----------------------------------------
* As root on guest (you'll have to log in as you, then `sudo su -`):

  #. `apt-get install python-software-properties`
  #. `add-apt-repository ppa:swift-core/release`
  #. `apt-get update`
  #. `apt-get install curl gcc git-core memcached python-configobj
     python-coverage python-dev python-nose python-setuptools python-simplejson
     python-xattr sqlite3 xfsprogs python-webob python-eventlet
     python-greenlet python-pastedeploy python-netifaces`
  #. Install anything else you want, like screen, ssh, vim, etc.

* On Fedora, log in as root and do:

  #. `yum install openstack-swift openstack-swift-proxy
     openstack-swift-account openstack-swift-container openstack-swift-object`
  #. `yum install xinetd rsync`
  #. `yum install memcached`
  #. `yum install python-netifaces python-nose`

  This installs all necessary dependencies, and also creates user `swift`
  and group `swift`. So, `swift:swift` ought to be used in every place where
  this manual calls for `<your-user-name>:<your-group-name>`.

  Ensure that you are installing the version of Swift that corresponds to
  this document. If not, enable the correct update repositories.

Next, choose either :ref:`partition-section` or :ref:`loopback-section`.

.. _partition-section:

Using a partition for storage
=============================

If you are going to use a separate partition for Swift data, be sure to add
another device when creating the VM, and follow these instructions.

  #. `fdisk /dev/sdb` (set up a single partition)
  #. `mkfs.xfs -i size=1024 /dev/sdb1`
  #. Edit `/etc/fstab` and add
       `/dev/sdb1 /mnt/sdb1 xfs noatime,nodiratime,nobarrier,logbufs=8 0 0`
  #. `mkdir /mnt/sdb1`
  #. `mount /mnt/sdb1`
  #. `mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4`
  #. `chown <your-user-name>:<your-group-name> /mnt/sdb1/*`
  #. `mkdir /srv`
  #. `for x in {1..4}; do ln -s /mnt/sdb1/$x /srv/$x; done`
  #. `mkdir -p /etc/swift/object-server /etc/swift/container-server /etc/swift/account-server /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4 /var/run/swift`
  #. `chown -R <your-user-name>:<your-group-name> /etc/swift /srv/[1-4]/ /var/run/swift` -- **Make sure to include the trailing slash after /srv/[1-4]/**
  #. Add to `/etc/rc.local` (before the `exit 0`)::

        mkdir /var/run/swift
        chown <your-user-name>:<your-group-name> /var/run/swift
  #. Next, skip to :ref:`rsync-section`.


.. _loopback-section:

Using a loopback device for storage
===================================

If you want to use a loopback device instead of another partition, follow these instructions.

  #. `mkdir /srv`
  #. `dd if=/dev/zero of=/srv/swift-disk bs=1024 count=0 seek=1000000`
       (modify seek to make a larger or smaller partition)
  #. `mkfs.xfs -i size=1024 /srv/swift-disk`
  #. Edit `/etc/fstab` and add
       `/srv/swift-disk /mnt/sdb1 xfs loop,noatime,nodiratime,nobarrier,logbufs=8 0 0`
  #. `mkdir /mnt/sdb1`
  #. `mount /mnt/sdb1`
  #. `mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4`
  #. `chown <your-user-name>:<your-group-name> /mnt/sdb1/*`
  #. `for x in {1..4}; do ln -s /mnt/sdb1/$x /srv/$x; done`
  #. `mkdir -p /etc/swift/object-server /etc/swift/container-server /etc/swift/account-server /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4 /var/run/swift`
  #. `chown -R <your-user-name>:<your-group-name> /etc/swift /srv/[1-4]/ /var/run/swift` -- **Make sure to include the trailing slash after /srv/[1-4]/**
  #. Add to `/etc/rc.local` (before the `exit 0`)::

        mkdir /var/run/swift
        chown <your-user-name>:<your-group-name> /var/run/swift

.. _rsync-section:

----------------
Setting up rsync
----------------

  #. Create /etc/rsyncd.conf::

        uid = <Your user name>
        gid = <Your group name>
        log file = /var/log/rsyncd.log
        pid file = /var/run/rsyncd.pid
        address = 127.0.0.1

        [account6012]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/account6012.lock

        [account6022]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/account6022.lock

        [account6032]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/account6032.lock

        [account6042]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/account6042.lock


        [container6011]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/container6011.lock

        [container6021]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/container6021.lock

        [container6031]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/container6031.lock

        [container6041]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/container6041.lock


        [object6010]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/object6010.lock

        [object6020]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/object6020.lock

        [object6030]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/object6030.lock

        [object6040]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/object6040.lock

  #. On Ubuntu, edit the following line in /etc/default/rsync::

        RSYNC_ENABLE=true

     On Fedora, edit the following line in /etc/xinetd.d/rsync::

        disable = no

  #. On Ubuntu `service rsync restart`

------------------
Starting memcached
------------------

On Fedora, make sure that memcached runs, running this if necessary:

  * `systemctl enable memcached.service`
  * `systemctl start memcached.service`

If this is not done, tokens of tempauth expire immediately and accessing
Swift with curl becomes impossible.

---------------------------------------------------
Optional: Setting up rsyslog for individual logging
---------------------------------------------------

  #. Create /etc/rsyslog.d/10-swift.conf::

      # Uncomment the following to have a log containing all logs together
      #local1,local2,local3,local4,local5.*   /var/log/swift/all.log

      # Uncomment the following to have hourly proxy logs for stats processing
      #$template HourlyProxyLog,"/var/log/swift/hourly/%$YEAR%%$MONTH%%$DAY%%$HOUR%"
      #local1.*;local1.!notice ?HourlyProxyLog

      local1.*;local1.!notice /var/log/swift/proxy.log
      local1.notice           /var/log/swift/proxy.error
      local1.*                ~

      local2.*;local2.!notice /var/log/swift/storage1.log
      local2.notice           /var/log/swift/storage1.error
      local2.*                ~

      local3.*;local3.!notice /var/log/swift/storage2.log
      local3.notice           /var/log/swift/storage2.error
      local3.*                ~

      local4.*;local4.!notice /var/log/swift/storage3.log
      local4.notice           /var/log/swift/storage3.error
      local4.*                ~

      local5.*;local5.!notice /var/log/swift/storage4.log
      local5.notice           /var/log/swift/storage4.error
      local5.*                ~

  #. Edit /etc/rsyslog.conf and make the following change::

      $PrivDropToGroup adm

  #. `mkdir -p /var/log/swift/hourly`
  #. `chown -R syslog.adm /var/log/swift`
  #. `service rsyslog restart`

------------------------------------------------
Getting the code and setting up test environment
------------------------------------------------

Sample configuration files are provided with all defaults in line-by-line comments.

Do these commands as you on guest.

  #. `mkdir ~/bin`
  #. Check out the swift repo with `git clone https://github.com/openstack/swift.git`
  #. Build a development installation of swift, for example:
     `cd ~/swift; sudo python setup.py develop`
  #. Edit `~/.bashrc` and add to the end::

        export SWIFT_TEST_CONFIG_FILE=/etc/swift/func_test.conf
        export PATH=${PATH}:~/bin

  #. `. ~/.bashrc`

---------------------
Configuring each node
---------------------

Sample configuration files are provided with all defaults in line-by-line comments.

  #. Create `/etc/swift/proxy-server.conf`::

        [DEFAULT]
        bind_port = 8080
        user = <your-user-name>
        log_facility = LOG_LOCAL1

        [pipeline:main]
        pipeline = healthcheck cache tempauth proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        allow_account_management = true
        account_autocreate = true

        [filter:tempauth]
        use = egg:swift#tempauth
        user_admin_admin = admin .admin .reseller_admin
        user_test_tester = testing .admin
        user_test2_tester2 = testing2 .admin
        user_test_tester3 = testing3

        [filter:healthcheck]
        use = egg:swift#healthcheck

        [filter:cache]
        use = egg:swift#memcache

  #. Create `/etc/swift/swift.conf`::

        [swift-hash]
        # random unique string that can never change (DO NOT LOSE)
        swift_hash_path_suffix = changeme

  #. Create `/etc/swift/account-server/1.conf`::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        bind_port = 6012
        user = <your-user-name>
        log_facility = LOG_LOCAL2

        [pipeline:main]
        pipeline = account-server

        [app:account-server]
        use = egg:swift#account

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/2.conf`::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        bind_port = 6022
        user = <your-user-name>
        log_facility = LOG_LOCAL3

        [pipeline:main]
        pipeline = account-server

        [app:account-server]
        use = egg:swift#account

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/3.conf`::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        bind_port = 6032
        user = <your-user-name>
        log_facility = LOG_LOCAL4

        [pipeline:main]
        pipeline = account-server

        [app:account-server]
        use = egg:swift#account

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/4.conf`::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        bind_port = 6042
        user = <your-user-name>
        log_facility = LOG_LOCAL5

        [pipeline:main]
        pipeline = account-server

        [app:account-server]
        use = egg:swift#account

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/container-server/1.conf`::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        bind_port = 6011
        user = <your-user-name>
        log_facility = LOG_LOCAL2

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]

  #. Create `/etc/swift/container-server/2.conf`::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        bind_port = 6021
        user = <your-user-name>
        log_facility = LOG_LOCAL3

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]

  #. Create `/etc/swift/container-server/3.conf`::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        bind_port = 6031
        user = <your-user-name>
        log_facility = LOG_LOCAL4

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]

  #. Create `/etc/swift/container-server/4.conf`::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        bind_port = 6041
        user = <your-user-name>
        log_facility = LOG_LOCAL5

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]


  #. Create `/etc/swift/object-server/1.conf`::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        bind_port = 6010
        user = <your-user-name>
        log_facility = LOG_LOCAL2

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/2.conf`::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        bind_port = 6020
        user = <your-user-name>
        log_facility = LOG_LOCAL3

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/3.conf`::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        bind_port = 6030
        user = <your-user-name>
        log_facility = LOG_LOCAL4

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/4.conf`::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        bind_port = 6040
        user = <your-user-name>
        log_facility = LOG_LOCAL5

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

------------------------------------
Setting up scripts for running Swift
------------------------------------

  #. Create `~/bin/resetswift.`

     If you are using a loopback device substitute `/dev/sdb1` with `/srv/swift-disk`.

     If you did not set up rsyslog for individual logging, remove the `find /var/log/swift...` line::

        #!/bin/bash

        swift-init all stop
        find /var/log/swift -type f -exec rm -f {} \;
        sudo umount /mnt/sdb1
        sudo mkfs.xfs -f -i size=1024 /dev/sdb1
        sudo mount /mnt/sdb1
        sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
        sudo chown <your-user-name>:<your-group-name> /mnt/sdb1/*
        mkdir -p /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4
        sudo rm -f /var/log/debug /var/log/messages /var/log/rsyncd.log /var/log/syslog
        sudo service rsyslog restart
        sudo service memcached restart

  #. Create `~/bin/remakerings`::

        #!/bin/bash

        cd /etc/swift

        rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz

        swift-ring-builder object.builder create 18 3 1
        swift-ring-builder object.builder add z1-127.0.0.1:6010/sdb1 1
        swift-ring-builder object.builder add z2-127.0.0.1:6020/sdb2 1
        swift-ring-builder object.builder add z3-127.0.0.1:6030/sdb3 1
        swift-ring-builder object.builder add z4-127.0.0.1:6040/sdb4 1
        swift-ring-builder object.builder rebalance
        swift-ring-builder container.builder create 18 3 1
        swift-ring-builder container.builder add z1-127.0.0.1:6011/sdb1 1
        swift-ring-builder container.builder add z2-127.0.0.1:6021/sdb2 1
        swift-ring-builder container.builder add z3-127.0.0.1:6031/sdb3 1
        swift-ring-builder container.builder add z4-127.0.0.1:6041/sdb4 1
        swift-ring-builder container.builder rebalance
        swift-ring-builder account.builder create 18 3 1
        swift-ring-builder account.builder add z1-127.0.0.1:6012/sdb1 1
        swift-ring-builder account.builder add z2-127.0.0.1:6022/sdb2 1
        swift-ring-builder account.builder add z3-127.0.0.1:6032/sdb3 1
        swift-ring-builder account.builder add z4-127.0.0.1:6042/sdb4 1
        swift-ring-builder account.builder rebalance

  #. Create `~/bin/startmain`::

        #!/bin/bash

        swift-init main start

  #. Create `~/bin/startrest`::

        #!/bin/bash

        swift-init rest start

  #. `chmod +x ~/bin/*`
  #. `remakerings`
  #. `cd ~/swift; ./.unittests`
  #. `startmain` (The ``Unable to increase file descriptor limit.  Running as non-root?`` warnings are expected and ok.)
  #. Get an `X-Storage-Url` and `X-Auth-Token`: ``curl -v -H 'X-Storage-User: test:tester' -H 'X-Storage-Pass: testing' http://127.0.0.1:8080/auth/v1.0``
  #. Check that you can GET account: ``curl -v -H 'X-Auth-Token: <token-from-x-auth-token-above>' <url-from-x-storage-url-above>``
  #. Check that `swift` works: `swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing stat`
  #. `cp ~/swift/test/functional/sample.conf /etc/swift/func_test.conf`
  #. `cd ~/swift; ./.functests` (Note: functional tests will first delete
     everything in the configured accounts.)
  #. `cd ~/swift; ./.probetests` (Note: probe tests will reset your
     environment as they call `resetswift` for each test.)

If you plan to work on documentation (and who doesn't?!) you must
install Sphinx and then you can build the documentation:

On Ubuntu:
  #. `sudo apt-get install python-sphinx`
  #. `python setup.py build_sphinx`

On MacOS:
  #. `sudo easy_install -U sphinx`
  #. `python setup.py build_sphinx`

----------------
Debugging Issues
----------------

If all doesn't go as planned, and tests fail, or you can't auth, or something doesn't work, here are some good starting places to look for issues:

#. Everything is logged in /var/log/syslog, so that is a good first place to
   look for errors (most likely python tracebacks).
#. Make sure all of the server processes are running.  For the base
   functionality, the Proxy, Account, Container, and Object servers
   should be running.
#. If one of the servers are not running, and no errors are logged to syslog,
   it may be useful to try to start the server manually, for example:
   `swift-object-server /etc/swift/object-server/1.conf` will start the
   object server.  If there are problems not showing up in syslog,
   then you will likely see the traceback on startup.
