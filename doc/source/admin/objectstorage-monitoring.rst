=========================
Object Storage monitoring
=========================

.. note::

   This section was excerpted from a `blog post by Darrell
   Bishop <https://swiftstack.com/blog/2012/04/11/swift-monitoring-with-statsd>`_ and
   has since been edited.

An OpenStack Object Storage cluster is a collection of many daemons that
work together across many nodes. With so many different components, you
must be able to tell what is going on inside the cluster. Tracking
server-level meters like CPU utilization, load, memory consumption, disk
usage and utilization, and so on is necessary, but not sufficient.

Swift Recon
~~~~~~~~~~~

The Swift Recon middleware (see :ref:`cluster_telemetry_and_monitoring`)
provides general machine statistics, such as load average, socket
statistics, ``/proc/meminfo`` contents, as well as Swift-specific meters:

-  The ``MD5`` sum of each ring file.

-  The most recent object replication time.

-  Count of each type of quarantined file: Account, container, or
   object.

-  Count of "async_pendings" (deferred container updates) on disk.

Swift Recon is middleware that is installed in the object servers
pipeline and takes one required option: A local cache directory. To
track ``async_pendings``, you must set up an additional cron job for
each object server. You access data by either sending HTTP requests
directly to the object server or using the ``swift-recon`` command-line
client.

There are Object Storage cluster statistics but the typical
server meters overlap with existing server monitoring systems. To get
the Swift-specific meters into a monitoring system, they must be polled.
Swift Recon acts as a middleware meters collector. The
process that feeds meters to your statistics system, such as
``collectd`` and ``gmond``, should already run on the storage node.
You can choose to either talk to Swift Recon or collect the meters
directly.

Swift-Informant
~~~~~~~~~~~~~~~

Swift-Informant middleware (see
`swift-informant <https://github.com/pandemicsyn/swift-informant>`_) has
real-time visibility into Object Storage client requests. It sits in the
pipeline for the proxy server, and after each request to the proxy server it
sends three meters to a ``StatsD`` server:

-  A counter increment for a meter like ``obj.GET.200`` or
   ``cont.PUT.404``.

-  Timing data for a meter like ``acct.GET.200`` or ``obj.GET.200``.
   [The README says the meters look like ``duration.acct.GET.200``, but
   I do not see the ``duration`` in the code. I am not sure what the
   Etsy server does but our StatsD server turns timing meters into five
   derivative meters with new segments appended, so it probably works as
   coded. The first meter turns into ``acct.GET.200.lower``,
   ``acct.GET.200.upper``, ``acct.GET.200.mean``,
   ``acct.GET.200.upper_90``, and ``acct.GET.200.count``].

-  A counter increase by the bytes transferred for a meter like
   ``tfer.obj.PUT.201``.

This is used for receiving information on the quality of service clients
experience with the timing meters, as well as sensing the volume of the
various modifications of a request server type, command, and response
code. Swift-Informant requires no change to core Object
Storage code because it is implemented as middleware. However, it gives
no insight into the workings of the cluster past the proxy server.
If the responsiveness of one storage node degrades, you can only see
that some of the requests are bad, either as high latency or error
status codes.

Statsdlog
~~~~~~~~~

The `Statsdlog <https://github.com/pandemicsyn/statsdlog>`_
project increments StatsD counters based on logged events. Like
Swift-Informant, it is also non-intrusive, however statsdlog can track
events from all Object Storage daemons, not just proxy-server. The
daemon listens to a UDP stream of syslog messages, and StatsD counters
are incremented when a log line matches a regular expression. Meter
names are mapped to regex match patterns in a JSON file, allowing
flexible configuration of what meters are extracted from the log stream.

Currently, only the first matching regex triggers a StatsD counter
increment, and the counter is always incremented by one. There is no way
to increment a counter by more than one or send timing data to StatsD
based on the log line content. The tool could be extended to handle more
meters for each line and data extraction, including timing data. But a
coupling would still exist between the log textual format and the log
parsing regexes, which would themselves be more complex to support
multiple matches for each line and data extraction. Also, log processing
introduces a delay between the triggering event and sending the data to
StatsD. It would be preferable to increment error counters where they
occur and send timing data as soon as it is known to avoid coupling
between a log string and a parsing regex and prevent a time delay
between events and sending data to StatsD.

The next section describes another method for gathering Object Storage
operational meters.

Swift StatsD logging
~~~~~~~~~~~~~~~~~~~~

StatsD (see `Measure Anything, Measure Everything
<http://codeascraft.etsy.com/2011/02/15/measure-anything-measure-everything/>`_)
was designed for application code to be deeply instrumented. Meters are
sent in real-time by the code that just noticed or did something. The
overhead of sending a meter is extremely low: a ``sendto`` of one UDP
packet. If that overhead is still too high, the StatsD client library
can send only a random portion of samples and StatsD approximates the
actual number when flushing meters upstream.

To avoid the problems inherent with middleware-based monitoring and
after-the-fact log processing, the sending of StatsD meters is
integrated into Object Storage itself. Details of the meters tracked
are in the :doc:`/admin_guide`.

The sending of meters is integrated with the logging framework. To
enable, configure ``log_statsd_host`` in the relevant config file. You
can also specify the port and a default sample rate. The specified
default sample rate is used unless a specific call to a statsd logging
method (see the list below) overrides it. Currently, no logging calls
override the sample rate, but it is conceivable that some meters may
require accuracy (``sample_rate=1``) while others may not.

.. code-block:: ini

   [DEFAULT]
   # ...
   log_statsd_host = 127.0.0.1
   log_statsd_port = 8125
   log_statsd_default_sample_rate = 1

Then the LogAdapter object returned by ``get_logger()``, usually stored
in ``self.logger``, has these new methods:

-  ``set_statsd_prefix(self, prefix)`` Sets the client library stat
   prefix value which gets prefixed to every meter. The default prefix
   is the ``name`` of the logger such as ``object-server``,
   ``container-auditor``, and so on. This is currently used to turn
   ``proxy-server`` into one of ``proxy-server.Account``,
   ``proxy-server.Container``, or ``proxy-server.Object`` as soon as the
   Controller object is determined and instantiated for the request.

-  ``update_stats(self, metric, amount, sample_rate=1)`` Increments
   the supplied meter by the given amount. This is used when you need
   to add or subtract more that one from a counter, like incrementing
   ``suffix.hashes`` by the number of computed hashes in the object
   replicator.

-  ``increment(self, metric, sample_rate=1)`` Increments the given counter
   meter by one.

-  ``decrement(self, metric, sample_rate=1)`` Lowers the given counter
   meter by one.

-  ``timing(self, metric, timing_ms, sample_rate=1)`` Record that the
   given meter took the supplied number of milliseconds.

-  ``timing_since(self, metric, orig_time, sample_rate=1)``
   Convenience method to record a timing meter whose value is "now"
   minus an existing timestamp.

.. note::

   These logging methods may safely be called anywhere you have a
   logger object. If StatsD logging has not been configured, the methods
   are no-ops. This avoids messy conditional logic each place a meter is
   recorded. These example usages show the new logging methods:

   .. code-block:: python

      # swift/obj/replicator.py
      def update(self, job):
           # ...
          begin = time.time()
          try:
              hashed, local_hash = tpool.execute(tpooled_get_hashes, job['path'],
                      do_listdir=(self.replication_count % 10) == 0,
                      reclaim_age=self.reclaim_age)
              # See tpooled_get_hashes "Hack".
              if isinstance(hashed, BaseException):
                  raise hashed
              self.suffix_hash += hashed
              self.logger.update_stats('suffix.hashes', hashed)
              # ...
          finally:
              self.partition_times.append(time.time() - begin)
              self.logger.timing_since('partition.update.timing', begin)

   .. code-block:: python

      # swift/container/updater.py
      def process_container(self, dbfile):
          # ...
          start_time = time.time()
          # ...
              for event in events:
                  if 200 <= event.wait() < 300:
                      successes += 1
                  else:
                      failures += 1
              if successes > failures:
                self.logger.increment('successes')
                  # ...
              else:
                  self.logger.increment('failures')
                  # ...
              # Only track timing data for attempted updates:
              self.logger.timing_since('timing', start_time)
          else:
              self.logger.increment('no_changes')
              self.no_changes += 1
