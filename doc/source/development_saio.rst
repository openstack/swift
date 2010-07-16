=======================
SAIO - Swift All In One
=======================

-----------------------------------
Instructions for seting up a dev VM
-----------------------------------

This documents setting up a virtual machine for doing Swift development. The
virtual machine will emulate running a four node Swift cluster. It assumes
you're using *VMware Fusion 3* on *Mac OS X Snow Leopard*, but should give a
good idea what to do on other environments.

* Get the *Ubuntu 10.04 LTS (Lucid Lynx)* server image from: 
  http://cdimage.ubuntu.com/releases/10.04/release/ubuntu-10.04-dvd-amd64.iso
* Create guest virtual machine:

  #. `Continue without disc`
  #. `Use operating system installation disc image file`, pick the .iso
     from above.
  #. Select `Linux` and `Ubuntu 64-bit`.
  #. Fill in the *Linux Easy Install* details.
  #. `Customize Settings`, name the image whatever you want 
     (`SAIO` for instance.)
  #. When the `Settings` window comes up, select `Hard Disk`, create an
     extra disk (the defaults are fine).
  #. Start the virtual machine up and wait for the easy install to
     finish.

* As root on guest (you'll have to log in as you, then `sudo su -`):

  #. `apt-get install python-software-properties`
  #. `add-apt-repository ppa:swift-core/ppa`
  #. `apt-get update`
  #. `apt-get install curl gcc bzr memcached python-configobj
     python-coverage python-dev python-nose python-setuptools python-simplejson
     python-xattr sqlite3 xfsprogs python-webob python-eventlet
     python-greenlet`
  #. Install anything else you want, like screen, ssh, vim, etc.
  #. `fdisk /dev/sdb` (set up a single partition)
  #. `mkfs.xfs -i size=1024 /dev/sdb1`
  #. `mkdir /mnt/sdb1`
  #. Edit `/etc/fstab` and add
     `/dev/sdb1 /mnt/sdb1 xfs noatime,nodiratime,nobarrier,logbufs=8 0 0`
  #. `mount /mnt/sdb1`
  #. `mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4 /mnt/sdb1/test`
  #. `chown <your-user-name>:<your-group-name> /mnt/sdb1/*`
  #. `mkdir /srv`
  #. `for x in {1..4}; do ln -s /mnt/sdb1/$x /srv/$x; done`
  #. `mkdir -p /etc/swift/object-server /etc/swift/container-server /etc/swift/account-server /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4 /var/run/swift`
  #. `chown -R <your-user-name>:<your-group-name> /etc/swift /srv/[1-4]/ /var/run/swift` -- **Make sure to include the trailing slash after /srv/[1-4]/**
  #. Add to `/etc/rc.local` (before the `exit 0`)::

        mkdir /var/run/swift
        chown <your-user-name>:<your-user-name> /var/run/swift

  #. Create /etc/rsyncd.conf::

        uid = <Your user name>
        gid = <Your group name>
        log file = /var/log/rsyncd.log
        pid file = /var/run/rsyncd.pid


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

  #. Edit the following line in /etc/default/rsync::

        RSYNC_ENABLE=true

  #. `service rsync restart`

* As you on guest:

  #. `mkdir ~/bin`
  #. Create `~/.bazaar/.bazaar.conf`::

        [DEFAULT]
                email = Your Name <your-email-address>
  #. If you are using launchpad to get the code or make changes, run
     `bzr launchpad-login <launchpad_id>`
  #. Create the swift repo with `bzr init-repo swift`
  #. Check out your bzr branch of swift, for example:
     `cd ~/swift; bzr branch lp:~swift-core/swift/trunk swift`
  #. `cd ~/swift/swift; sudo python setup.py develop`
  #. Edit `~/.bashrc` and add to the end::

        export PATH_TO_TEST_XFS=/mnt/sdb1/test
        export SWIFT_TEST_CONFIG_FILE=/etc/swift/func_test.conf
        export PATH=${PATH}:~/bin

  #. `. ~/.bashrc`
  #. Create `/etc/swift/auth-server.conf`::

        [auth-server]
        default_cluster_url = http://127.0.0.1:8080/v1
        user = <your-user-name>

  #. Create `/etc/swift/proxy-server.conf`::

        [proxy-server]
        bind_port = 8080
        user = <your-user-name>

  #. Create `/etc/swift/account-server/1.conf`::

        [account-server]
        devices = /srv/1/node
        mount_check = false
        bind_port = 6012
        user = <your-user-name>

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/2.conf`::

        [account-server]
        devices = /srv/2/node
        mount_check = false
        bind_port = 6022
        user = <your-user-name>

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/3.conf`::

        [account-server]
        devices = /srv/3/node
        mount_check = false
        bind_port = 6032
        user = <your-user-name>

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/account-server/4.conf`::

        [account-server]
        devices = /srv/4/node
        mount_check = false
        bind_port = 6042
        user = <your-user-name>

        [account-replicator]
        vm_test_mode = yes

        [account-auditor]

        [account-reaper]

  #. Create `/etc/swift/container-server/1.conf`::

        [container-server]
        devices = /srv/1/node
        mount_check = false
        bind_port = 6011
        user = <your-user-name>

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

  #. Create `/etc/swift/container-server/2.conf`::

        [container-server]
        devices = /srv/2/node
        mount_check = false
        bind_port = 6021
        user = <your-user-name>

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

  #. Create `/etc/swift/container-server/3.conf`::

        [container-server]
        devices = /srv/3/node
        mount_check = false
        bind_port = 6031
        user = <your-user-name>

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

  #. Create `/etc/swift/container-server/4.conf`::

        [container-server]
        devices = /srv/4/node
        mount_check = false
        bind_port = 6041
        user = <your-user-name>

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

  #. Create `/etc/swift/object-server/1.conf`::

        [object-server]
        devices = /srv/1/node
        mount_check = false
        bind_port = 6010
        user = <your-user-name>

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/2.conf`::

        [object-server]
        devices = /srv/2/node
        mount_check = false
        bind_port = 6020
        user = <your-user-name>

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/3.conf`::

        [object-server]
        devices = /srv/3/node
        mount_check = false
        bind_port = 6030
        user = <your-user-name>

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `/etc/swift/object-server/4.conf`::

        [object-server]
        devices = /srv/4/node
        mount_check = false
        bind_port = 6040
        user = <your-user-name>

        [object-replicator]
        vm_test_mode = yes

        [object-updater]

        [object-auditor]

  #. Create `~/bin/resetswift`::

        #!/bin/bash

        swift-init all stop
        sleep 5
        sudo umount /mnt/sdb1
        sudo mkfs.xfs -f -i size=1024 /dev/sdb1
        sudo mount /mnt/sdb1
        sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4 /mnt/sdb1/test
        sudo chown <your-user-name>:<your-group-name> /mnt/sdb1/*
        mkdir -p /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4
        sudo rm -f /var/log/debug /var/log/messages /var/log/rsyncd.log /var/log/syslog
        sudo service rsyslog restart
        sudo service memcached restart

  #. Create `~/bin/remakerings`::

        #!/bin/bash

        cd /etc/swift

        rm *.builder *.ring.gz backups/*.builder backups/*.ring.gz

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

        swift-init auth-server start
        swift-init proxy-server start
        swift-init account-server start
        swift-init container-server start
        swift-init object-server start

  #. Create `~/bin/startrest`::

        #!/bin/bash

        swift-auth-recreate-accounts
        swift-init object-updater start
        swift-init container-updater start
        swift-init object-replicator start
        swift-init container-replicator start
        swift-init account-replicator start
        swift-init object-auditor start
        swift-init container-auditor start
        swift-init account-auditor start
        swift-init account-reaper start

  #. `chmod +x ~/bin/*`
  #. `remakerings`
  #. `cd ~/swift; ./.unittests`
  #. `startmain`
  #. `swift-auth-create-account test tester testing`
  #. Get an `X-Storage-Url` and `X-Auth-Token`: ``curl -v -H 'X-Storage-User: test:tester' -H 'X-Storage-Pass: testing' http://127.0.0.1:11000/v1.0``
  #. Check that you can GET account: ``curl -v -H 'X-Auth-Token: <token-from-x-auth-token-above>' <url-from-x-storage-url-above>``
  #. Check that `st` works: `st -A http://127.0.0.1:11000/v1.0 -U test:tester -K testing stat`
  #. Create `/etc/swift/func_test.conf`::

        auth_host = 127.0.0.1
        auth_port = 11000
        auth_ssl = no

        account = test
        username = tester
        password = testing

        collate = C

  #. `cd ~/swift; ./.functests`
  #. `cd ~/swift; ./.probetests`

If you plan to work on documentation (and who doesn't?!):

  #. `sudo apt-get install python-sphinx`
  #. `cd ~/swift/doc`
  #. `make html`

----------------
Debugging Issues
----------------

If all doesn't go as planned, and tests fail, or you can't auth, or something doesn't work, here are some good starting places to look for issues:

#. Everything is logged in /var/log/syslog, so that is a good first place to
   look for errors (most likely python tracebacks).
#. Make sure all of the server processes are running.  For the base
   functionality, the Proxy, Account, Container, Object and Auth servers
   should be running
#. If one of the servers are not running, and no errors are logged to syslog,
   it may be useful to try to start the server manually, for example: 
   `swift-object-server /etc/swift/object-server/1.conf` will start the 
   object server.  If there are problems not showing up in syslog, 
   then you will likely see the traceback on startup.
