==============================================================
Instructions for a Multiple Server Swift Installation (Ubuntu)
==============================================================

Prerequisites
-------------
* Ubuntu Server 10.04 LTS installation media

.. note:
    Swift can run with other distros, but for this document we will focus
    on installing on Ubuntu Server, ypmv (your packaging may vary).

Basic architecture and terms
----------------------------
- *node* - a host machine running one or more Swift services
- *Proxy node* - node that runs Proxy services
- *Auth node* - node that runs the Auth service
- *Storage node* - node that runs Account, Container, and Object services
- *ring* - a set of mappings of Swift data to physical devices

This document shows a cluster using the following types of nodes:

- one Proxy node

  - Runs the swift-proxy-server processes which proxy requests to the
    appropriate Storage nodes.

- one Auth node

  - Runs the swift-auth-server which controls authentication and
    authorization for all requests.  This can be on the same node as a
    Proxy node.

- five Storage nodes

  - Runs the swift-account-server, swift-container-server, and 
    swift-object-server processes which control storage of the account
    databases, the container databases, as well as the actual stored
    objects.
    
.. note::
    Fewer Storage nodes can be used initially, but a minimum of 5 is
    recommended for a production cluster.

This document describes each Storage node as a separate zone in the ring.
It is recommended to have a minimum of 5 zones. A zone is a group of nodes
that is as isolated as possible from other nodes (separate servers, network,
power, even geography). The ring guarantees that every replica is stored
in a separate zone.  For more information about the ring and zones, see: :doc:`The Rings <overview_ring>`.

Network Setup Notes
-------------------

This document refers to two networks.  An external network for connecting to the Proxy server, and a storage network that is not accessibile from outside the cluster, to which all of the nodes are connected.  All of the Swift services, as well as the rsync daemon on the Storage nodes are configured to listen on their STORAGE_LOCAL_NET IP addresses.

General OS configuration and partitioning for each node
-------------------------------------------------------

#. Install the baseline Ubuntu Server 10.04 LTS on all nodes.

#. Install common Swift software prereqs::

        apt-get install python-software-properties
        add-apt-repository ppa:swift-core/ppa
        apt-get update
        apt-get install swift openssh-server

#. Create and populate configuration directories::

        mkdir -p /etc/swift
        chown -R swift:swift /etc/swift/

#. Create /etc/swift/swift.conf::

        [swift-hash]
        # random unique string that can never change (DO NOT LOSE)
        swift_hash_path_suffix = changeme

.. note::
    /etc/swift/swift.conf should be set to some random string of text to be
    used as a salt when hashing to determine mappings in the ring.  This
    file should be the same on every node in the cluster!


Configure the Proxy node
------------------------

.. note::
    It is assumed that all commands are run as the root user

#. Install swift-proxy service::

        apt-get install swift-proxy memcached

#. Create self-signed cert for SSL::

        cd /etc/swift
        openssl req -new -x509 -nodes -out cert.crt -keyout cert.key

#. Modify memcached to listen on the default interfaces.  Preferably this should be on a local, non-public network.  Edit the following line in /etc/memcached.conf, changing::

        -l 127.0.0.1
        to
        -l <PROXY_LOCAL_NET_IP>

#. Restart the memcached server::

        service memcached restart

#. Create /etc/swift/proxy-server.conf::

        [DEFAULT]
        cert_file = /etc/swift/cert.crt
        key_file = /etc/swift/cert.key
        bind_port = 8080
        workers = 8
        user = swift
        
        [pipeline:main]
        pipeline = healthcheck cache auth proxy-server
        
        [app:proxy-server]
        use = egg:swift#proxy
        
        [filter:auth]
        use = egg:swift#auth
        ssl = true
        
        [filter:healthcheck]
        use = egg:swift#healthcheck
        
        [filter:cache]
        use = egg:swift#memcache
        memcache_servers = <PROXY_LOCAL_NET_IP>:11211

   .. note::

    If you run multiple memcache servers, put the multiple IP:port listings    
    in the [filter:cache] section of the proxy-server.conf file like:
    `10.1.2.3:11211,10.1.2.4:11211`. Only the proxy server uses memcache.

#. Create the account, container and object rings::

    cd /etc/swift
    swift-ring-builder account.builder create 18 3 1
    swift-ring-builder container.builder create 18 3 1
    swift-ring-builder object.builder create 18 3 1
    
   .. note::

    For more information on building rings, see :doc:`overview_ring`.
        
#. For every storage device on each node add entries to each ring::

    swift-ring-builder account.builder add z<ZONE>-<STORAGE_LOCAL_NET_IP>:6002/<DEVICE> 100
    swift-ring-builder container.builder add z<ZONE>-<STORAGE_LOCAL_NET_IP_1>:6001/<DEVICE> 100
    swift-ring-builder object.builder add z<ZONE>-<STORAGE_LOCAL_NET_IP_1>:6000/<DEVICE> 100

   .. note::
    Assuming there are 5 zones with 1 node per zone, ZONE should start at
    1 and increment by one for each additional node.

#. Verify the ring contents for each ring::

    swift-ring-builder account.builder
    swift-ring-builder container.builder
    swift-ring-builder object.builder
    
#. Rebalance the rings::

    swift-ring-builder account.builder rebalance
    swift-ring-builder container.builder rebalance
    swift-ring-builder object.builder rebalance

   .. note::
    Rebalancing rings can take some time.

#. Copy the account.ring.gz, container.ring.gz, and object.ring.gz files
   to each of the Proxy and Storage nodes in /etc/swift.

#. Make sure all the config files are owned by the swift user::

        chown -R swift:swift /etc/swift

#. Start Proxy services::

        swift-init proxy start


Configure the Auth node
-----------------------

#. If this node is not running on the same node as a proxy, create a
   self-signed cert as you did for the Proxy node

#. Install swift-auth service::

        apt-get install swift-auth

#. Create /etc/swift/auth-server.conf::

        [DEFAULT]
        cert_file = /etc/swift/cert.crt
        key_file = /etc/swift/cert.key
        user = swift
        
        [pipeline:main]
        pipeline = auth-server
        
        [app:auth-server]
        use = egg:swift#auth
        default_cluster_url = https://<PROXY_HOSTNAME>:8080/v1
        # Highly recommended to change this key to something else!
        super_admin_key = devauth

#. Start Auth services::

        swift-init auth start
        chown swift:swift /etc/swift/auth.db
        swift-init auth restart            # 1.1.0 workaround because swift creates auth.db owned as root

Configure the Storage nodes
---------------------------

..  note::
    Swift *should* work on any modern filesystem that supports
    Extended Attributes (XATTRS).  We currently recommend XFS as it
    demonstrated the best overall performance for the swift use case after
    considerable testing and benchmarking at Rackspace.  It is also the
    only filesystem that has been thoroughly tested.

#. Install Storage node packages::

        apt-get install swift-account swift-container swift-object xfsprogs

#. For every device on the node, setup the XFS volume (/dev/sdb is used
   as an example)::

        fdisk /dev/sdb  (set up a single partition)
        mkfs.xfs -i size=1024 /dev/sdb1
        echo "/dev/sdb1 /srv/node/sdb1 xfs noatime,nodiratime,nobarrier,logbufs=8 0 0" >> /etc/fstab
        mkdir -p /srv/node/sdb1
        mount /srv/node/sdb1
        chown -R swift:swift /srv/node

#. Create /etc/rsyncd.conf::

        uid = swift
        gid = swift
        log file = /var/log/rsyncd.log
        pid file = /var/run/rsyncd.pid
        address = <STORAGE_LOCAL_NET_IP>
        
        [account]
        max connections = 2
        path = /srv/node/
        read only = false
        lock file = /var/lock/account.lock
        
        [container]
        max connections = 2
        path = /srv/node/
        read only = false
        lock file = /var/lock/container.lock
        
        [object]
        max connections = 2
        path = /srv/node/
        read only = false
        lock file = /var/lock/object.lock

#. Edit the following line in /etc/default/rsync::

        RSYNC_ENABLE=true

#. Start rsync daemon::

        service rsync start

   ..  note::
    The rsync daemon requires no authentication, so it should be run on
    a local, private network.

#. Create /etc/swift/account-server.conf::

        [DEFAULT]
        bind_ip = <STORAGE_LOCAL_NET_IP>
        workers = 2
        
        [pipeline:main]
        pipeline = account-server
        
        [app:account-server]
        use = egg:swift#account
        
        [account-replicator]

        [account-auditor]
        
        [account-reaper]

#. Create /etc/swift/container-server.conf::

        [DEFAULT]
        bind_ip = <STORAGE_LOCAL_NET_IP>
        workers = 2
        
        [pipeline:main]
        pipeline = container-server
        
        [app:container-server]
        use = egg:swift#container
        
        [container-replicator]
        
        [container-updater]
        
        [container-auditor]

#. Create /etc/swift/object-server.conf::

        [DEFAULT]
        bind_ip = <STORAGE_LOCAL_NET_IP>
        workers = 2
        
        [pipeline:main]
        pipeline = object-server
        
        [app:object-server]
        use = egg:swift#object
        
        [object-replicator]
        
        [object-updater]
        
        [object-auditor]

#. Start the storage services::

    swift-init object-server start
    swift-init object-replicator start
    swift-init object-updater start
    swift-init object-auditor start
    swift-init container-server start
    swift-init container-replicator start
    swift-init container-updater start
    swift-init container-auditor start
    swift-init account-server start
    swift-init account-replicator start
    swift-init account-auditor start

Create Swift admin account and test (run commands from Auth node)
-----------------------------------------------------------------

#. Create a user with administrative priviledges (account = system,
   username = root, password = testpass).  Make sure to replace 
   ``devauth`` with whatever super_admin key you assigned in the 
   auth-server.conf file above.  *Note: None of the values of 
   account, username, or password are special - they can be anything.*::

        swift-auth-add-user -K devauth -a system root testpass

#. Get an X-Storage-Url and X-Auth-Token::

        curl -k -v -H 'X-Storage-User: system:root' -H 'X-Storage-Pass: testpass' https://<AUTH_HOSTNAME>:11000/v1.0

#. Check that you can HEAD the account::

        curl -k -v -H 'X-Auth-Token: <token-from-x-auth-token-above>' <url-from-x-storage-url-above>

#. Check that ``st`` works::

        st -A https://<AUTH_HOSTNAME>:11000/v1.0 -U system:root -K testpass stat

#. Use ``st`` to upload a few files named 'bigfile[1-2].tgz' to a container named 'myfiles'::

        st -A https://<AUTH_HOSTNAME>:11000/v1.0 -U system:root -K testpass upload myfiles bigfile1.tgz
        st -A https://<AUTH_HOSTNAME>:11000/v1.0 -U system:root -K testpass upload myfiles bigfile2.tgz

#. Use ``st`` to download all files from the 'myfiles' container::

        st -A https://<AUTH_HOSTNAME>:11000/v1.0 -U system:root -K testpass download myfiles

Troubleshooting Notes
---------------------
If you see problems, look in var/log/syslog (or messages on some distros). 

Also, at Rackspace we have seen hints at drive failures by looking at error messages in /var/log/kern.log. 

There are more debugging hints and tips in the :doc:`admin_guide`.

