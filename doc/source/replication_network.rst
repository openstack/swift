.. _Dedicated-replication-network:

=============================
Dedicated replication network
=============================

-------
Summary
-------

Swift's replication process is essential for consistency and availability of
data. By default, replication activity will use the same network interface as
other cluster operations. However, if a replication interface is set in the
ring for a node, that node will send replication traffic on its designated
separate replication network interface. Replication traffic includes REPLICATE
requests and rsync traffic.

To separate the cluster-internal replication traffic from client traffic,
separate replication servers can be used. These replication servers are based
on the standard storage servers, but they listen on the replication IP and
only respond to REPLICATE requests. Storage servers can serve REPLICATE
requests, so an operator can transition to using a separate replication
network with no cluster downtime.

Replication IP and port information is stored in the ring on a per-node basis.
These parameters will be used if they are present, but they are not required.
If this information does not exist or is empty for a particular node, the
node's standard IP and port will be used for replication.

--------------------
For SAIO replication
--------------------

#. Create new script in ``~/bin/`` (for example: ``remakerings_new``)::

        #!/bin/bash
        set -e
        cd /etc/swift
        rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz
        swift-ring-builder object.builder create 10 3 1
        swift-ring-builder object.builder add z1-127.0.0.1:6210R127.0.0.1:6250/sdb1 1
        swift-ring-builder object.builder add z2-127.0.0.1:6220R127.0.0.1:6260/sdb2 1
        swift-ring-builder object.builder add z3-127.0.0.1:6230R127.0.0.1:6270/sdb3 1
        swift-ring-builder object.builder add z4-127.0.0.1:6240R127.0.0.1:6280/sdb4 1
        swift-ring-builder object.builder rebalance
        swift-ring-builder object-1.builder create 10 2 1
        swift-ring-builder object-1.builder add z1-127.0.0.1:6210R127.0.0.1:6250/sdb1 1
        swift-ring-builder object-1.builder add z2-127.0.0.1:6220R127.0.0.1:6260/sdb2 1
        swift-ring-builder object-1.builder add z3-127.0.0.1:6230R127.0.0.1:6270/sdb3 1
        swift-ring-builder object-1.builder add z4-127.0.0.1:6240R127.0.0.1:6280/sdb4 1
        swift-ring-builder object-1.builder rebalance
        swift-ring-builder object-2.builder create 10 6 1
        swift-ring-builder object-2.builder add z1-127.0.0.1:6210R127.0.0.1:6250/sdb1 1
        swift-ring-builder object-2.builder add z1-127.0.0.1:6210R127.0.0.1:6250/sdb5 1
        swift-ring-builder object-2.builder add z2-127.0.0.1:6220R127.0.0.1:6260/sdb2 1
        swift-ring-builder object-2.builder add z2-127.0.0.1:6220R127.0.0.1:6260/sdb6 1
        swift-ring-builder object-2.builder add z3-127.0.0.1:6230R127.0.0.1:6270/sdb3 1
        swift-ring-builder object-2.builder add z3-127.0.0.1:6230R127.0.0.1:6270/sdb7 1
        swift-ring-builder object-2.builder add z4-127.0.0.1:6240R127.0.0.1:6280/sdb4 1
        swift-ring-builder object-2.builder add z4-127.0.0.1:6240R127.0.0.1:6280/sdb8 1
        swift-ring-builder object-2.builder rebalance
        swift-ring-builder container.builder create 10 3 1
        swift-ring-builder container.builder add z1-127.0.0.1:6211R127.0.0.1:6251/sdb1 1
        swift-ring-builder container.builder add z2-127.0.0.1:6221R127.0.0.1:6261/sdb2 1
        swift-ring-builder container.builder add z3-127.0.0.1:6231R127.0.0.1:6271/sdb3 1
        swift-ring-builder container.builder add z4-127.0.0.1:6241R127.0.0.1:6281/sdb4 1
        swift-ring-builder container.builder rebalance
        swift-ring-builder account.builder create 10 3 1
        swift-ring-builder account.builder add z1-127.0.0.1:6212R127.0.0.1:6252/sdb1 1
        swift-ring-builder account.builder add z2-127.0.0.1:6222R127.0.0.1:6262/sdb2 1
        swift-ring-builder account.builder add z3-127.0.0.1:6232R127.0.0.1:6272/sdb3 1
        swift-ring-builder account.builder add z4-127.0.0.1:6242R127.0.0.1:6282/sdb4 1
        swift-ring-builder account.builder rebalance

   .. note::
      Syntax of adding device has been changed: ``R<ip_replication>:<port_replication>``
      was added between ``z<zone>-<ip>:<port>`` and ``/<device_name>_<meta> <weight>``.
      Added devices will use <ip_replication> and <port_replication> for replication activities.

#. Add next rows in ``/etc/rsyncd.conf``::

        [account6252]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/account6252.lock

        [account6262]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/account6262.lock

        [account6272]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/account6272.lock

        [account6282]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/account6282.lock


        [container6251]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/container6251.lock

        [container6261]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/container6261.lock

        [container6271]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/container6271.lock

        [container6281]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/container6281.lock


        [object6250]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/object6250.lock

        [object6260]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/object6260.lock

        [object6270]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/object6270.lock

        [object6280]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/object6280.lock

#. Restart rsync daemon::

        service rsync restart

#. Update configuration files in directories:

   * /etc/swift/object-server(files: 1.conf, 2.conf, 3.conf, 4.conf)
   * /etc/swift/container-server(files: 1.conf, 2.conf, 3.conf, 4.conf)
   * /etc/swift/account-server(files: 1.conf, 2.conf, 3.conf, 4.conf)

   delete all configuration options in section ``[<*>-replicator]``

#. Add configuration files for object-server, in ``/etc/swift/object-server/``

   * 5.conf::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6250
        user = swift
        log_facility = LOG_LOCAL2
        recon_cache_path = /var/cache/swift

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        rsync_module = {replication_ip}::object{replication_port}

   * 6.conf::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6260
        user = swift
        log_facility = LOG_LOCAL3
        recon_cache_path = /var/cache/swift2

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        rsync_module = {replication_ip}::object{replication_port}

   * 7.conf::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6270
        user = swift
        log_facility = LOG_LOCAL4
        recon_cache_path = /var/cache/swift3

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        rsync_module = {replication_ip}::object{replication_port}

   * 8.conf::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6280
        user = swift
        log_facility = LOG_LOCAL5
        recon_cache_path = /var/cache/swift4

        [pipeline:main]
        pipeline = recon object-server

        [app:object-server]
        use = egg:swift#object
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [object-replicator]
        rsync_module = {replication_ip}::object{replication_port}

#. Add configuration files for container-server, in ``/etc/swift/container-server/``

   * 5.conf::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6251
        user = swift
        log_facility = LOG_LOCAL2
        recon_cache_path = /var/cache/swift

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        rsync_module = {replication_ip}::container{replication_port}

   * 6.conf::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6261
        user = swift
        log_facility = LOG_LOCAL3
        recon_cache_path = /var/cache/swift2

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        rsync_module = {replication_ip}::container{replication_port}

   * 7.conf::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6271
        user = swift
        log_facility = LOG_LOCAL4
        recon_cache_path = /var/cache/swift3

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        rsync_module = {replication_ip}::container{replication_port}

   * 8.conf::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6281
        user = swift
        log_facility = LOG_LOCAL5
        recon_cache_path = /var/cache/swift4

        [pipeline:main]
        pipeline = recon container-server

        [app:container-server]
        use = egg:swift#container
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [container-replicator]
        rsync_module = {replication_ip}::container{replication_port}

#. Add configuration files for account-server, in ``/etc/swift/account-server/``

   * 5.conf::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6252
        user = swift
        log_facility = LOG_LOCAL2
        recon_cache_path = /var/cache/swift

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        rsync_module = {replication_ip}::account{replication_port}

   * 6.conf::

        [DEFAULT]
        devices = /srv/2/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6262
        user = swift
        log_facility = LOG_LOCAL3
        recon_cache_path = /var/cache/swift2

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        rsync_module = {replication_ip}::account{replication_port}

   * 7.conf::

        [DEFAULT]
        devices = /srv/3/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6272
        user = swift
        log_facility = LOG_LOCAL4
        recon_cache_path = /var/cache/swift3

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        rsync_module = {replication_ip}::account{replication_port}

   * 8.conf::

        [DEFAULT]
        devices = /srv/4/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6282
        user = swift
        log_facility = LOG_LOCAL5
        recon_cache_path = /var/cache/swift4

        [pipeline:main]
        pipeline = recon account-server

        [app:account-server]
        use = egg:swift#account
        replication_server = True

        [filter:recon]
        use = egg:swift#recon

        [account-replicator]
        rsync_module = {replication_ip}::account{replication_port}


---------------------------------
For a Multiple Server replication
---------------------------------

#. Move configuration file.

   * Configuration file for object-server from /etc/swift/object-server.conf to /etc/swift/object-server/1.conf

   * Configuration file for container-server from /etc/swift/container-server.conf to /etc/swift/container-server/1.conf

   * Configuration file for account-server from /etc/swift/account-server.conf to /etc/swift/account-server/1.conf

#. Add changes in configuration files in directories:

   * /etc/swift/object-server(files: 1.conf)
   * /etc/swift/container-server(files: 1.conf)
   * /etc/swift/account-server(files: 1.conf)

   delete all configuration options in section [<*>-replicator]

#. Add configuration files for object-server, in /etc/swift/object-server/2.conf::

        [DEFAULT]
        bind_ip = $STORAGE_LOCAL_NET_IP
        workers = 2

        [pipeline:main]
        pipeline = object-server

        [app:object-server]
        use = egg:swift#object
        replication_server = True

        [object-replicator]

#. Add configuration files for container-server, in /etc/swift/container-server/2.conf::

        [DEFAULT]
        bind_ip = $STORAGE_LOCAL_NET_IP
        workers = 2

        [pipeline:main]
        pipeline = container-server

        [app:container-server]
        use = egg:swift#container
        replication_server = True

        [container-replicator]

#. Add configuration files for account-server, in /etc/swift/account-server/2.conf::

        [DEFAULT]
        bind_ip = $STORAGE_LOCAL_NET_IP
        workers = 2

        [pipeline:main]
        pipeline = account-server

        [app:account-server]
        use = egg:swift#account
        replication_server = True

        [account-replicator]

