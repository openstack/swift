:orphan:

Labeled Metrics
===============

.. note::
   Labeled metrics are still an experimental feature. This document contains
   forward looking statements that anticipate future development of labeled
   metrics support. In particular, metric names and labels may be subject to
   change as we explore the space.

.. warning::
   Enabling labeled metrics will likely cause a dramatic increase in the number
   of distinct metrics time series. Ensure your metrics pipeline is prepared.

Recent versions of Swift emit StatsD metrics with explicit application-defined
labels, rather than relying on consumers knowing how to unpack the legacy label
names. A variety of StatsD extension formats are available, many of which are
parsed by `statsd_exporter <https://github.com/prometheus/statsd_exporter/>`__:

- ``librato``
- ``influxdb``
- ``dogstatsd``
- ``graphite``

See the ``proxy-server.conf-sample`` file for more information on configuring
labeled metrics.

Labeled metrics are emitted in addition to legacy StatsD metrics.  However,
legacy StatsD metrics can be disabled by setting the ``statsd_emit_legacy``
option to ``False``.  This is not recommended until more legacy metrics have
been supplemented with equivalent labeled metrics.

As various Swift middlewares, services and daemons are upgraded to emit labeled
metrics, they will be documented in the relevant section of the :doc:`all`
page.

Common Labels
-------------

Each labeled metric may have its own unique labels, but many labeled metrics
will use some or all of a common set of labels.  The common labels are
documented here for information purposes, but the authoritative set of labels
for each metric can be found in the sections of the :doc:`all` page.

.. table::
   :align: left

   ================ ==========================================================
   Label Name       Value
   ---------------- ----------------------------------------------------------
   ``type``         The type of resource associated with the metric
                    i.e. ``account``, ``container`` or ``object``.
   ``account``      The quoted account name associated with the metric.
   ``container``    The quoted container name associated with the metric.
   ``policy``       The storage policy index associated with the metric.
   ``status``       The status int of an HTTP response associated with the
                    metric.
   ``method``       The method of an HTTP request associated with the metric.
   ================ ==========================================================


.. note::
   Note that metrics will *not* have labels that would likely have a very high
   cardinality of values, such as object names, as this is expected to be
   problematic for metrics collectors. Nevertheless, some operators may still
   need to drop labels such as ``container`` in order to keep metric
   cardinalities reasonable.
