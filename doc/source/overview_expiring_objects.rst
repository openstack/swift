=======================
Expiring Object Support
=======================

The ``swift-object-expirer`` offers scheduled deletion of objects. The Swift
client would use the ``X-Delete-At`` or ``X-Delete-After`` headers during an
object ``PUT`` or ``POST`` and the cluster would automatically quit serving
that object at the specified time and would shortly thereafter remove the
object from the system.

The ``X-Delete-At`` header takes a Unix Epoch timestamp, in integer form; for
example: ``1317070737`` represents ``Mon Sep 26 20:58:57 2011 UTC``.

The ``X-Delete-After`` header takes a positive integer number of seconds. The
proxy server that receives the request will convert this header into an
``X-Delete-At`` header using the request timestamp plus the value given.

If both the ``X-Delete-At`` and ``X-Delete-After`` headers are sent with a
request then the ``X-Delete-After`` header will take precedence.

As expiring objects are added to the system, the object servers will record the
expirations in a hidden ``.expiring_objects`` account for the
``swift-object-expirer`` to handle later.

Usually, just one instance of the ``swift-object-expirer`` daemon needs to run
for a cluster. This isn't exactly automatic failover high availability, but if
this daemon doesn't run for a few hours it should not be any real issue. The
expired-but-not-yet-deleted objects will still ``404 Not Found`` if someone
tries to ``GET`` or ``HEAD`` them and they'll just be deleted a bit later when
the daemon is restarted.

By default, the ``swift-object-expirer`` daemon will run with a concurrency of
1.  Increase this value to get more concurrency.  A concurrency of 1 may not be
enough to delete expiring objects in a timely fashion for a particular Swift
cluster.

It is possible to run multiple daemons to do different parts of the work if a
single process with a concurrency of more than 1 is not enough (see the sample
config file for details).

To run the ``swift-object-expirer`` as multiple processes, set ``processes`` to
the number of processes (either in the config file or on the command line).
Then run one process for each part.  Use ``process`` to specify the part of the
work to be done by a process using the command line or the config.  So, for
example, if you'd like to run three processes, set ``processes`` to 3 and run
three processes with ``process`` set to 0, 1, and 2 for the three processes.
If multiple processes are used, it's necessary to run one for each part of the
work or that part of the work will not be done.

By default the daemon looks for two different config files. When launching,
the process searches for the ``[object-expirer]`` section in the

``/etc/swift/object-server.conf`` config. If the section or the config is missing
it will then look for and use the ``/etc/swift/object-expirer.conf`` config.
The latter config file is considered deprecated and is searched for to aid
in cluster upgrades.

Upgrading impact: General Task Queue vs Legacy Queue
----------------------------------------------------

The expirer daemon will be moving to a new general task-queue based design that
will divide the work across all object servers, as such only expirers defined
in the object-server config will be able to use the new system.
The parameters in both files are identical except for a new option in the
object-server ``[object-expirer]`` section, ``dequeue_from_legacy``
which when set to ``True`` will tell the expirer that in addition to using
the new task queueing system to also check the legacy (soon to be deprecated)
queue.

.. note::
    The new task-queue system has not been completed yet. So an expirer's with
    ``dequeue_from_legacy`` set to ``False`` will currently do nothing.

By default ``dequeue_from_legacy`` will be ``False``, it is necessary to
be set to ``True`` explicitly while migrating from the old expiring queue.

Any expirer using the old config ``/etc/swift/object-expirer.conf`` will not
use the new general task queue. It'll ignore the ``dequeue_from_legacy``
and will only check the legacy queue. Meaning it'll run as a legacy expirer.

Why is this important? If you are currently running object-expirers on nodes
that are not object storage nodes, then for the time being they will still
work but only by dequeuing from the old queue.
When the new general task queue is introduced, expirers will be required to
run on the object servers so that any new objects added can be removed.
If you're in this situation, you can safely setup the new expirer
section in the ``object-server.conf`` to deal with the new queue and leave the
legacy expirers running elsewhere.

However, if your old expirers are running on the object-servers, the most
common topology, then you would add the new section to all object servers, to
deal the new queue. In order to maintain the same number of expirers checking
the legacy queue, pick the same number of nodes as you previously had and turn
on ``dequeue_from_legacy`` on those nodes only. Also note on these nodes
you'd need to keep the legacy ``process`` and ``processes`` options to maintain
the concurrency level for the legacy queue.

.. note::
    Be careful not to enable ``dequeue_from_legacy`` on too many expirers as
    all legacy tasks are stored in a single hidden account and the same hidden
    containers. On a large cluster one may inadvertently overload the
    acccount/container servers handling the legacy expirer queue.

Here is a quick sample of the ``object-expirer`` section required in the
``object-server.conf``::

    [object-expirer]
    # log_name = object-expirer
    # log_facility = LOG_LOCAL0
    # log_level = INFO
    # log_address = /dev/log
    #
    interval = 300

    # If this true, expirer execute tasks in legacy expirer task queue
    dequeue_from_legacy = false

    # processes can only be used in conjunction with `dequeue_from_legacy`.
    # So this option is ignored if dequeue_from_legacy=false.
    # processes is how many parts to divide the legacy work into, one part per
    # process that will be doing the work
    # processes set 0 means that a single legacy process will be doing all the work
    # processes can also be specified on the command line and will override the
    # config value
    # processes = 0

    # process can only be used in conjunction with `dequeue_from_legacy`.
    # So this option is ignored if dequeue_from_legacy=false.
    # process is which of the parts a particular legacy process will work on
    # process can also be specified on the command line and will override the config
    # value
    # process is "zero based", if you want to use 3 processes, you should run
    # processes with process set to 0, 1, and 2
    # process = 0

    report_interval = 300

    # request_tries is the number of times the expirer's internal client will
    # attempt any given request in the event of failure. The default is 3.
    # request_tries = 3

    # concurrency is the level of concurrency to use to do the work, this value
    # must be set to at least 1
    # concurrency = 1

    # The expirer will re-attempt expiring if the source object is not available
    # up to reclaim_age seconds before it gives up and deletes the entry in the
    # queue.
    # reclaim_age = 604800

And for completeness, here is a quick sample of the legacy
``object-expirer.conf`` file::

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


.. note::
    When running legacy expirers, the daemon needs to run on a machine with
    access to all the backend servers in the cluster, but does not need proxy
    server or public access. The daemon will use its own internal proxy code
    instance to access the backend servers.
