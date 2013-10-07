=======================
SAIO - Swift All In One
=======================

---------------------------------------------
Instructions for setting up a development VM
---------------------------------------------

This section documents setting up a virtual machine for doing Swift development.
The virtual machine will emulate running a four node Swift cluster.

* Get an Ubuntu 12.04 LTS (Precise Pangolin) server image or try something Fedora/CentOS.

* Create guest virtual machine from the image.

Additional information about setting up a Swift development snapshot on other distributions is
available on the wiki at http://wiki.openstack.org/SAIOInstructions.

----------------------------
What's in a <your-user-name>
----------------------------

Much of the configuration described in this guide requires escalated root
privileges; however, we assume that administrator logs in an unprivileged
user. Swift processes also run under a separate user and group, set by
configuration option, and refered as <your-user-name>:<your-group-name>.
The default user is `swift`, which may not exist on your system.

.. note::

    The instructions in the first first half of this guide are expected to be
    performed as the root user.

-----------------------
Installing dependencies
-----------------------

* On apt based systems,

  #. `apt-get update`
  #. `apt-get install curl gcc memcached rsync sqlite3 xfsprogs git-core libffi-dev python-setuptools`
  #. `apt-get install python-coverage python-dev python-nose python-simplejson
     python-xattr python-eventlet python-greenlet python-pastedeploy
     python-netifaces python-pip python-dnspython python-mock`

* On yum based systems,

  #. `yum install curl gcc memcached rsync sqlite xfsprogs git-core libffi-devel xinetd python-setuptools`
  #. `yum install python-coverage python-devel python-nose python-simplejson
     python-xattr python-eventlet python-greenlet python-pastedeploy
     python-netifaces python-pip python-dnspython python-mock`

  This installs necessary system dependencies; and *most* of the python
  dependencies.  Later in the process setuptools/distribute or pip will
  install and/or upgrade some other stuff - it's getting harder to avoid.
  You can also install anything else you want, like screen, ssh, vim, etc.

Next, choose either :ref:`partition-section` or :ref:`loopback-section`.

.. _partition-section:

Using a partition for storage
=============================

If you are going to use a separate partition for Swift data, be sure to add
another device when creating the VM, and follow these instructions.

  #. `fdisk /dev/sdb` (set up a single partition)
  #. `mkfs.xfs /dev/sdb1`
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

        mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
        chown <your-user-name>:<your-group-name> /var/cache/swift*
        mkdir -p /var/run/swift
        chown <your-user-name>:<your-group-name> /var/run/swift
  #. Next, skip to :ref:`rsync-section`.


.. _loopback-section:

Using a loopback device for storage
===================================

If you want to use a loopback device instead of another partition, follow these instructions.

  #. `mkdir /srv`
  #. `truncate -s 1GB /srv/swift-disk`
       (modify size to make a larger or smaller partition)
  #. `mkfs.xfs /srv/swift-disk`
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

        mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
        chown <your-user-name>:<your-group-name> /var/cache/swift*
        mkdir -p /var/run/swift
        chown <your-user-name>:<your-group-name> /var/run/swift

     Note that on some systems you might have to create `/etc/rc.local`.

     On Fedora 19 or later, you need to place these in `/etc/rc.d/rc.local`.

.. _rsync-section:

----------------
Setting up rsync
----------------

  #. Create /etc/rsyncd.conf::

        uid = <your-user-name>
        gid = <your-group-name>
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

  #. On Ubuntu, edit the following line in `/etc/default/rsync`::

        RSYNC_ENABLE=true

     On Fedora, edit the following line in `/etc/xinetd.d/rsync`::

        disable = no

  #. On platforms with SELinux in `Enforcing` mode, either set to `Permissive`::

        setenforce Permissive

     Or just allow rsync full access::

        setsebool -P rsync_full_access 1

  #. On Ubuntu, run `service rsync restart`

     On Fedora, run::

        systemctl enable rsyncd.service
        systemctl start rsyncd.service

     On other xinetd based systems run `service xinetd restart`.

  #. Verify rsync is accepting connections for all servers::

        rsync rsync://pub@localhost/

------------------
Starting memcached
------------------

On non-Ubuntu distros you need to ensure memcached is running:

  * `service memcached start`
  * `chkconfig memcached on`

or:

  * `systemctl enable memcached.service`
  * `systemctl start memcached.service`

The tempauth middleware stores tokens in memcached. If memcached is not
running, tokens cannot be validated, and accessing Swift becomes impossible.

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

  #. On Ubuntu:

  #. `chown -R syslog.adm /var/log/swift`
  #. `chmod -R g+w /var/log/swift`
  #. `service rsyslog restart`

  #. On Fedora:
  #. `chown -R root:adm /var/log/swift`
  #. `chmod -R g+w /var/log/swift`
  #. `systemctl restart rsyslog.service`


.. note::

    Starting here, from this point on, all instructions are expected to be
    performed as the unprivledged user you selected as <your-user-name>.


----------------
Getting the code
----------------

  #. Check out the python-swiftclient repo
       `git clone https://github.com/openstack/python-swiftclient.git`
  #. Build a development installation of python-swiftclient
       `cd ~/python-swiftclient; sudo python setup.py develop; cd -`
  #. Check out the swift repo
       `git clone https://github.com/openstack/swift.git`
  #. Build a development installation of swift
       `cd ~/swift; sudo python setup.py develop; cd -`
  #. Install swift's test dependencies
       `sudo pip install -r swift/test-requirements.txt`

Fedora 19 or later users might have to perform the following if development
installation of swift fails::

        sudo pip install -U xattr dnspython

---------------------
Configuring each node
---------------------

Sample configuration files are provided with all defaults in line-by-line comments.

  #. Create `/etc/swift/proxy-server.conf`::

        [DEFAULT]
        bind_port = 8080
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL1
        eventlet_debug = true

        [pipeline:main]
        # Yes, proxy-logging appears twice. This is not a mistake.
        pipeline = healthcheck proxy-logging cache tempauth proxy-logging proxy-server

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

        [filter:proxy-logging]
        use = egg:swift#proxy_logging

  #. Create `/etc/swift/swift.conf`::

        [swift-hash]
        # random unique strings that can never change (DO NOT LOSE)
        swift_hash_path_prefix = changeme
        swift_hash_path_suffix = changeme

  #. Create `/etc/swift/account-server/1.conf`::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6012
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL2
        recon_cache_path = /var/cache/swift
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/2.conf`::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6022
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL3
        recon_cache_path = /var/cache/swift2
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/3.conf`::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6032
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL4
        recon_cache_path = /var/cache/swift3
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/4.conf`::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6042
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL5
        recon_cache_path = /var/cache/swift4
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/container-server/1.conf`::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6011
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL2
        recon_cache_path = /var/cache/swift
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]

  #. Create `/etc/swift/container-server/2.conf`::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6021
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL3
        recon_cache_path = /var/cache/swift2
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]

  #. Create `/etc/swift/container-server/3.conf`::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6031
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL4
        recon_cache_path = /var/cache/swift3
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]

  #. Create `/etc/swift/container-server/4.conf`::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6041
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL5
        recon_cache_path = /var/cache/swift4
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

        [container-sync]


  #. Create `/etc/swift/object-server/1.conf`::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6010
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL2
        recon_cache_path = /var/cache/swift
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/2.conf`::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6020
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL3
        recon_cache_path = /var/cache/swift2
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/3.conf`::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6030
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL4
        recon_cache_path = /var/cache/swift3
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/4.conf`::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6040
        workers = 1
        user = <your-user-name>
        log_facility = LOG_LOCAL5
        recon_cache_path = /var/cache/swift4
        eventlet_debug = true

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Update <your-user-name>::

        find /etc/swift/ -name \*.conf | xargs sed -i "s/<your-user-name>/${USER}/"

------------------------------------
Setting up scripts for running Swift
------------------------------------

  #. `mkdir ~/bin`

  #. Create `~/bin/resetswift`.

     If you are using a loopback device substitute `/dev/sdb1` with
     `/srv/swift-disk` in the `mkfs` step.

     If you did not set up rsyslog for individual logging, remove the `find
     /var/log/swift...` line.

     On Fedora, replace `service `<name>` restart` with `systemctl restart `<name>`.service`::

        #!/bin/bash

        swift-init all stop
        find /var/log/swift -type f -exec rm -f {} \;
        sudo umount /mnt/sdb1
        sudo mkfs.xfs -f /dev/sdb1
        sudo mount /mnt/sdb1
        sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
        sudo chown <your-user-name>:<your-group-name> /mnt/sdb1/*
        mkdir -p /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4
        sudo rm -f /var/log/debug /var/log/messages /var/log/rsyncd.log /var/log/syslog
        find /var/cache/swift* -type f -name *.recon -exec rm -f {} \;
        sudo service rsyslog restart
        sudo service memcached restart

  #. Create `~/bin/remakerings`::

        #!/bin/bash

        cd /etc/swift

        rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz

        swift-ring-builder object.builder create 10 3 1
        swift-ring-builder object.builder add r1z1-127.0.0.1:6010/sdb1 1
        swift-ring-builder object.builder add r1z2-127.0.0.1:6020/sdb2 1
        swift-ring-builder object.builder add r1z3-127.0.0.1:6030/sdb3 1
        swift-ring-builder object.builder add r1z4-127.0.0.1:6040/sdb4 1
        swift-ring-builder object.builder rebalance
        swift-ring-builder container.builder create 10 3 1
        swift-ring-builder container.builder add r1z1-127.0.0.1:6011/sdb1 1
        swift-ring-builder container.builder add r1z2-127.0.0.1:6021/sdb2 1
        swift-ring-builder container.builder add r1z3-127.0.0.1:6031/sdb3 1
        swift-ring-builder container.builder add r1z4-127.0.0.1:6041/sdb4 1
        swift-ring-builder container.builder rebalance
        swift-ring-builder account.builder create 10 3 1
        swift-ring-builder account.builder add r1z1-127.0.0.1:6012/sdb1 1
        swift-ring-builder account.builder add r1z2-127.0.0.1:6022/sdb2 1
        swift-ring-builder account.builder add r1z3-127.0.0.1:6032/sdb3 1
        swift-ring-builder account.builder add r1z4-127.0.0.1:6042/sdb4 1
        swift-ring-builder account.builder rebalance

  #. Create `~/bin/startmain`::

        #!/bin/bash

        swift-init main start

  #. Create `~/bin/startrest`::

        #!/bin/bash

        swift-init rest start

  #. `chmod +x ~/bin/*`
  #. Edit `~/.bashrc` and add to the end::

        export SWIFT_TEST_CONFIG_FILE=/etc/swift/test.conf
        export PATH=${PATH}:~/bin

  #. `. ~/.bashrc`

  #. `remakerings`
  #. `cp ~/swift/test/sample.conf /etc/swift/test.conf`
  #. `~/swift/.unittests`
  #. `startmain` (The ``Unable to increase file descriptor limit.  Running as non-root?`` warnings are expected and ok.)
  #. Get an `X-Storage-Url` and `X-Auth-Token`: ``curl -v -H 'X-Storage-User: test:tester' -H 'X-Storage-Pass: testing' http://127.0.0.1:8080/auth/v1.0``
  #. Check that you can GET account: ``curl -v -H 'X-Auth-Token: <token-from-x-auth-token-above>' <url-from-x-storage-url-above>``
  #. Check that `swift` works: `swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing stat`
  #. `~/swift/.functests` (Note: functional tests will first delete
     everything in the configured accounts.)
  #. `~/swift/.probetests` (Note: probe tests will reset your
     environment as they call `resetswift` for each test.)

----------------
Debugging Issues
----------------

If all doesn't go as planned, and tests fail, or you can't auth, or something doesn't work, here are some good starting places to look for issues:

#. Everything is logged using system facilities -- usually in /var/log/syslog,
   but possibly in /var/log/messages on e.g. Fedora -- so that is a good first
   place to look for errors (most likely python tracebacks).
#. Make sure all of the server processes are running.  For the base
   functionality, the Proxy, Account, Container, and Object servers
   should be running.
#. If one of the servers are not running, and no errors are logged to syslog,
   it may be useful to try to start the server manually, for example:
   `swift-object-server /etc/swift/object-server/1.conf` will start the
   object server.  If there are problems not showing up in syslog,
   then you will likely see the traceback on startup.
#. If you need to, you can turn off syslog for unit tests. This can be
   useful for environments where /dev/log is unavailable, or which
   cannot rate limit (unit tests generate a lot of logs very quickly).
   Open the file SWIFT_TEST_CONFIG_FILE points to, and change the
   value of fake_syslog to True.
