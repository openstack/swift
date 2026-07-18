=========================
Object Storage monitoring
=========================

.. note::

   This section was excerpted from a `blog post by Darrell Bishop
   <https://web.archive.org/web/20190721104553/https://www.swiftstack.com/blog/2012/04/11/swift-monitoring-with-statsd/>`_
   and has since been edited.

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

Swift StatsD logging
~~~~~~~~~~~~~~~~~~~~

StatsD (see `Measure Anything, Measure Everything
<https://codeascraft.com/2011/02/15/measure-anything-measure-everything/>`_)
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
