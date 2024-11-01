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

Delay Reaping of Objects from Disk
----------------------------------

Swift's expiring object ``x-delete-at`` feature can be used to have the cluster
reap user's objects automatically from disk on their behalf when they no longer
want them stored in their account. In some cases it may be necessary to
"intervene" in the expected expiration process to prevent accidental or
premature data loss if an object marked for expiration should NOT be deleted
immediately when it expires for whatever reason. In these cases
``swift-object-expirer`` offers configuration of a ``delay_reaping`` value
on accounts and containers, which provides a delay between when an object
is marked for deletion, or expired, and when it is actually reaped from disk.
When this is set in the object expirer config the object expirer leaves expired
objects on disk (and in container listings) for the ``delay_reaping`` time.
After this delay has passed objects will be reaped as normal.

The ``delay_reaping`` value can be set either at an account level or a
container level. When set at an account level, the object expirer will
only reap objects within the account after the delay. A container level
``delay_reaping`` works similarly for containers and overrides an account
level ``delay_reaping`` value.

The ``delay_reaping`` values are set in the ``[object-expirer]`` section in
either the object-server or object-expirer config files. They are configured
with dynamic config option names prefixed with ``delay_reaping_<ACCT>``
at the account level and ``delay_reaping_<ACCT>/<CNTR>`` at the container
level, with the ``delay_reaping`` value in seconds.

Here is an example of ``delay_reaping`` configs in the``object-expirer``
section in the ``object-server.conf``::

    [object-expirer]
    delay_reaping_AUTH_test = 300.0
    delay_reaping_AUTH_test2 = 86400.0
    delay_reaping_AUTH_test/test = 0.0
    delay_reaping_AUTH_test/test2 = 600.0

.. note::
    A container level ``delay_reaping`` value does not require an account level
    ``delay_reaping`` value but overrides the account level value for the same
    account if it exists. By default, no ``delay_reaping`` value is configured
    for any accounts or containers.

Accessing Objects After Expiration
----------------------------------

By default, objects that expire become inaccessible, even to the account owner.
The object may not have been deleted, but any GET/HEAD/POST client request for
the object will respond 404 Not Found after the ``x-delete-at`` timestamp
has passed.

The ``swift-proxy-server`` offers the ability to globally configure a flag to
allow requests to access expired objects that have not yet been deleted.
When this flag is enabled, a user can make a GET, HEAD, or POST request with
the header ``x-open-expired`` set to true to access the expired object.

The global configuration is an opt-in flag that can be set in the
``[proxy-server]`` section of the ``proxy-server.conf`` file. It is configured
with a single flag ``allow_open_expired`` set to true or false. By default,
this flag is set to false.

Here is an example in the ``proxy-server`` section in ``proxy-server.conf``::

    [proxy-server]
    allow_open_expired = false

To discover whether this flag is set, you can send a **GET** request to the
``/info`` :ref:`discoverability <discoverability>` path. This will return
configuration data in JSON format where the value of ``allow_open_expired`` is
exposed.

When using a temporary URL to access the object, this feature is not enabled.
This means that adding the header will not allow requests to temporary URLs
to access expired objects.

Upgrading impact: General Task Queue vs Legacy Queue
----------------------------------------------------

The expirer daemon will be moving to a new general task-queue based design that
will divide the work across all object servers, as such only expirers defined
in the object-server config will be able to use the new system.

The legacy object expirer config is documented in
``etc/object-expirer.conf-sample``. The alternative object-server config
section is documented in ``etc/object-server.conf-sample``.

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

.. note::
    When running legacy expirers, the daemon needs to run on a machine with
    access to all the backend servers in the cluster, but does not need proxy
    server or public access. The daemon will use its own internal proxy code
    instance to access the backend servers.
