=======================
Expiring Object Support
=======================

The ``swift-object-expirer`` offers scheduled deletion of objects. The Swift client would use the ``X-Delete-At`` or ``X-Delete-After`` headers during an object ``PUT`` or ``POST`` and the cluster would automatically quit serving that object at the specified time and would shortly thereafter remove the object from the system.

The ``X-Delete-At`` header takes a Unix Epoch timestamp, in integer form; for example: ``1317070737`` represents ``Mon Sep 26 20:58:57 2011 UTC``.

The ``X-Delete-After`` header takes a integer number of seconds. The proxy server that receives the request will convert this header into an ``X-Delete-At`` header using its current time plus the value given.

As expiring objects are added to the system, the object servers will record the expirations in a hidden ``.expiring_objects`` account for the ``swift-object-expirer`` to handle later.

Just one instance of the ``swift-object-expirer`` daemon needs to run for a cluster. This isn't exactly automatic failover high availability, but if this daemon doesn't run for a few hours it should not be any real issue. The expired-but-not-yet-deleted objects will still ``404 Not Found`` if someone tries to ``GET`` or ``HEAD`` them and they'll just be deleted a bit later when the daemon is restarted.

The daemon uses the ``/etc/swift/object-expirer.conf`` by default, and here is a quick sample conf file::

    [DEFAULT]
    # swift_dir = /etc/swift
    # user = swift
    # You can specify default log routing here if you want:
    # log_name = swift
    # log_facility = LOG_LOCAL0
    # log_level = INFO
    
    [object-expirer]
    interval = 300
    
    [pipeline:main]
    pipeline = catch_errors cache proxy-server
    
    [app:proxy-server]
    use = egg:swift#proxy
    # See proxy-server.conf-sample for options
    
    [filter:cache]
    use = egg:swift#memcache
    # See proxy-server.conf-sample for options
    
    [filter:catch_errors]
    use = egg:swift#catch_errors
    # See proxy-server.conf-sample for options

The daemon needs to run on a machine with access to all the backend servers in the cluster, but does not need proxy server or public access. The daemon will use its own internal proxy code instance to access the backend servers.
