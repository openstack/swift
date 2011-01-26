=======================
SAIO - Swift All In One
=======================

---------------------------------------------
Instructions for setting up a development VM
---------------------------------------------

This documents setting up a virtual machine for doing Swift development. The
virtual machine will emulate running a four node Swift cluster.

* Get the *Ubuntu 10.04 LTS (Lucid Lynx)* server image:

  - Ubuntu Server ISO: http://releases.ubuntu.com/10.04/ubuntu-10.04.1-server-amd64.iso (682 MB)
  - Ubuntu Live/Install: http://cdimage.ubuntu.com/releases/10.04/release/ubuntu-10.04-dvd-amd64.iso (4.1 GB)
  - Ubuntu Mirrors: https://launchpad.net/ubuntu/+cdmirrors

* Create guest virtual machine from the Ubuntu image. 

Additional information about setting up a Swift development snapshot on other distributions is 
available on the wiki at http://wiki.openstack.org/SAIOInstructions.

-----------------------------------------
Installing dependencies and the core code
-----------------------------------------
* As root on guest (you'll have to log in as you, then `sudo su -`):

  #. `apt-get install python-software-properties`
  #. `add-apt-repository ppa:swift-core/ppa`
  #. `apt-get update`
  #. `apt-get install curl gcc bzr memcached python-configobj
     python-coverage python-dev python-nose python-setuptools python-simplejson
     python-xattr sqlite3 xfsprogs python-webob python-eventlet
     python-greenlet python-pastedeploy python-netifaces`
  #. Install anything else you want, like screen, ssh, vim, etc.
  #. Next, choose either :ref:`partition-section` or :ref:`loopback-section`. 


.. _partition-section:

Using a partition for storage
=============================

If you are going to use a separate partition for Swift data, be sure to add another device when
  creating the VM, and follow these instructions. 
  
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

  #. `dd if=/dev/zero of=/srv/swift-disk bs=1024 count=0 seek=1000000` 
       (modify seek to make a larger or smaller partition)
  #. `mkfs.xfs -i size=1024 /srv/swift-disk`
  #. Edit `/etc/fstab` and add
       `/srv/swift-disk /mnt/sdb1 xfs loop,noatime,nodiratime,nobarrier,logbufs=8 0 0`
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

  #. Edit the following line in /etc/default/rsync::

        RSYNC_ENABLE=true

  #. `service rsync restart`


------------------------------------------------
Getting the code and setting up test environment
------------------------------------------------

Sample configuration files are provided with all defaults in line-by-line comments. 

Do these commands as you on guest:

  #. `mkdir ~/bin`
  #. Create `~/.bazaar/bazaar.conf`::

        [DEFAULT]
                email = Your Name <your-email-address>
  #. If you are using launchpad to get the code or make changes, run
     `bzr launchpad-login <launchpad_id>`
  #. Create the swift repo with `bzr init-repo swift`
  #. Check out your bzr branch of swift, for example:
     `cd ~/swift; bzr branch lp:swift trunk`
  #. `cd ~/swift/trunk; sudo python setup.py develop`
  #. Edit `~/.bashrc` and add to the end::

        export SWIFT_TEST_CONFIG_FILE=/etc/swift/func_test.conf
        export PATH=${PATH}:~/bin

  #. `. ~/.bashrc`
  
---------------------
Configuring each node
---------------------

Sample configuration files are provided with all defaults in line-by-line comments.
  
  #. If your going to use the DevAuth (the default swift-auth-server), create
     `/etc/swift/auth-server.conf` (you can skip this if you're going to use
     Swauth)::

        [DEFAULT]
        user = <your-user-name>

        [pipeline:main]
        pipeline = auth-server

        [app:auth-server]
        use = egg:swift#auth
        default_cluster_url = http://127.0.0.1:8080/v1
        # Highly recommended to change this.
        super_admin_key = devauth

  #. Create `/etc/swift/proxy-server.conf`::

        [DEFAULT]
        bind_port = 8080
        user = <your-user-name>

        [pipeline:main]
        # For DevAuth:
        pipeline = healthcheck cache auth proxy-server
        # For Swauth:
        # pipeline = healthcheck cache swauth proxy-server
        
        [app:proxy-server]
        use = egg:swift#proxy
        allow_account_management = true

        # Only needed for DevAuth
        [filter:auth]
        use = egg:swift#auth

        # Only needed for Swauth
        [filter:swauth]
        use = egg:swift#swauth
        # Highly recommended to change this.
        super_admin_key = swauthkey

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

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

  #. Create `/etc/swift/container-server/2.conf`::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        bind_port = 6021
        user = <your-user-name>

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

  #. Create `/etc/swift/container-server/3.conf`::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        bind_port = 6031
        user = <your-user-name>

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]

  #. Create `/etc/swift/container-server/4.conf`::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        bind_port = 6041
        user = <your-user-name>

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container

        [container-replicator]
        vm_test_mode = yes

        [container-updater]

        [container-auditor]


  #. Create `/etc/swift/object-server/1.conf`::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        bind_port = 6010
        user = <your-user-name>

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

  #. Create `~/bin/resetswift.` If you are using a loopback device substitute `/dev/sdb1` with `/srv/swift-disk`::
  
        #!/bin/bash

        swift-init all stop
        sleep 5
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

        # The auth-server line is only needed for DevAuth:
        swift-init auth-server start
        swift-init proxy-server start
        swift-init account-server start
        swift-init container-server start
        swift-init object-server start

  #. For Swauth (not needed for DevAuth), create `~/bin/recreateaccounts`::
  
        #!/bin/bash

        # Replace devauth with whatever your super_admin key is (recorded in
        # /etc/swift/proxy-server.conf).
        swauth-prep -K swauthkey
        swauth-add-user -K swauthkey -a test tester testing
        swauth-add-user -K swauthkey -a test2 tester2 testing2
        swauth-add-user -K swauthkey test tester3 testing3
        swauth-add-user -K swauthkey -a -r reseller reseller reseller

  #. Create `~/bin/startrest`::

        #!/bin/bash

        # Replace devauth with whatever your super_admin key is (recorded in
        # /etc/swift/auth-server.conf). This swift-auth-recreate-accounts line
        # is only needed for DevAuth:
        swift-auth-recreate-accounts -K devauth
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
  #. `cd ~/swift/trunk; ./.unittests`
  #. `startmain` (The ``Unable to increase file descriptor limit.  Running as non-root?`` warnings are expected and ok.)
  #. For Swauth: `recreateaccounts`
  #. For DevAuth: `swift-auth-add-user -K devauth -a test tester testing` # Replace ``devauth`` with whatever your super_admin key is (recorded in /etc/swift/auth-server.conf).
  #. Get an `X-Storage-Url` and `X-Auth-Token`: ``curl -v -H 'X-Storage-User: test:tester' -H 'X-Storage-Pass: testing' http://127.0.0.1:11000/v1.0`` # For Swauth, make the last URL `http://127.0.0.1:8080/auth/v1.0`
  #. Check that you can GET account: ``curl -v -H 'X-Auth-Token: <token-from-x-auth-token-above>' <url-from-x-storage-url-above>``
  #. Check that `st` works: `st -A http://127.0.0.1:11000/v1.0 -U test:tester -K testing stat` # For Swauth, make the URL `http://127.0.0.1:8080/auth/v1.0`
  #. For DevAuth: `swift-auth-add-user -K devauth -a test2 tester2 testing2` # Replace ``devauth`` with whatever your super_admin key is (recorded in /etc/swift/auth-server.conf).
  #. For DevAuth: `swift-auth-add-user -K devauth test tester3 testing3` # Replace ``devauth`` with whatever your super_admin key is (recorded in /etc/swift/auth-server.conf).
  #. `cp ~/swift/trunk/test/functional/sample.conf /etc/swift/func_test.conf` # For Swauth, add auth_prefix = /auth/ and change auth_port = 8080.
  #. `cd ~/swift/trunk; ./.functests` (Note: functional tests will first delete
     everything in the configured accounts.)
  #. `cd ~/swift/trunk; ./.probetests` (Note: probe tests will reset your
     environment as they call `resetswift` for each test.)

If you plan to work on documentation (and who doesn't?!):

On Ubuntu:
  #. `sudo apt-get install python-sphinx` installs Sphinx.
  #. `python setup.py build_sphinx` builds the documentation.

On MacOS: 
  #. `sudo easy_install -U sphinx` installs Sphinx.
  #. `python setup.py build_sphinx` builds the documentation.
  
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
