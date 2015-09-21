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

#. Create new script in ~/bin/ (for example: remakerings_new)::

        #!/bin/bash
        cd /etc/swift
        rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz
        swift-ring-builder object.builder create 18 3 1
        swift-ring-builder object.builder add z1-127.0.0.1:6010R127.0.0.1:6050/sdb1 1
        swift-ring-builder object.builder add z2-127.0.0.1:6020R127.0.0.1:6060/sdb2 1
        swift-ring-builder object.builder add z3-127.0.0.1:6030R127.0.0.1:6070/sdb3 1
        swift-ring-builder object.builder add z4-127.0.0.1:6040R127.0.0.1:6080/sdb4 1
        swift-ring-builder object.builder rebalance
        swift-ring-builder container.builder create 18 3 1
        swift-ring-builder container.builder add z1-127.0.0.1:6011R127.0.0.1:6051/sdb1 1
        swift-ring-builder container.builder add z2-127.0.0.1:6021R127.0.0.1:6061/sdb2 1
        swift-ring-builder container.builder add z3-127.0.0.1:6031R127.0.0.1:6071/sdb3 1
        swift-ring-builder container.builder add z4-127.0.0.1:6041R127.0.0.1:6081/sdb4 1
        swift-ring-builder container.builder rebalance
        swift-ring-builder account.builder create 18 3 1
        swift-ring-builder account.builder add z1-127.0.0.1:6012R127.0.0.1:6052/sdb1 1
        swift-ring-builder account.builder add z2-127.0.0.1:6022R127.0.0.1:6062/sdb2 1
        swift-ring-builder account.builder add z3-127.0.0.1:6032R127.0.0.1:6072/sdb3 1
        swift-ring-builder account.builder add z4-127.0.0.1:6042R127.0.0.1:6082/sdb4 1
        swift-ring-builder account.builder rebalance

   .. note::
      Syntax of adding device has been changed: R<ip_replication>:<port_replication> was added between z<zone>-<ip>:<port> and /<device_name>_<meta> <weight>. Added devices will use <ip_replication> and <port_replication> for replication activities.

#. Add next rows in /etc/rsyncd.conf::

        [account6052]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/account6052.lock

        [account6062]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/account6062.lock

        [account6072]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/account6072.lock

        [account6082]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/account6082.lock


        [container6051]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/container6051.lock

        [container6061]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/container6061.lock

        [container6071]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/container6071.lock

        [container6081]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/container6081.lock


        [object6050]
        max connections = 25
        path = /srv/1/node/
        read only = false
        lock file = /var/lock/object6050.lock

        [object6060]
        max connections = 25
        path = /srv/2/node/
        read only = false
        lock file = /var/lock/object6060.lock

        [object6070]
        max connections = 25
        path = /srv/3/node/
        read only = false
        lock file = /var/lock/object6070.lock

        [object6080]
        max connections = 25
        path = /srv/4/node/
        read only = false
        lock file = /var/lock/object6080.lock

#. Restart rsync daemon::

        service rsync restart

#. Add changes in configuration files in directories:

   * /etc/swift/object-server(files: 1.conf, 2.conf, 3.conf, 4.conf)
   * /etc/swift/container-server(files: 1.conf, 2.conf, 3.conf, 4.conf)
   * /etc/swift/account-server(files: 1.conf, 2.conf, 3.conf, 4.conf)

   delete all configuration options in section [<*>-replicator]

#. Add configuration files for object-server, in /etc/swift/object-server/

   * 5.conf::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6050
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
        bind_port = 6060
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
        bind_port = 6070
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
        bind_port = 6080
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

#. Add configuration files for container-server, in /etc/swift/container-server/

   * 5.conf::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6051
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
        bind_port = 6061
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
        bind_port = 6071
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
        bind_port = 6081
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

#. Add configuration files for account-server, in /etc/swift/account-server/

   * 5.conf::

        [DEFAULT]
        devices = /srv/1/node
        mount_check = false
        disable_fallocate = true
        bind_port = 6052
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
        bind_port = 6062
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
        bind_port = 6072
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
        bind_port = 6082
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

