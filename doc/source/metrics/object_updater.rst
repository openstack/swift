``object-updater`` Metrics
==========================

==============================  ====================================================
Metric Name                     Description
------------------------------  ----------------------------------------------------
``object-updater.errors``       Count of drives not mounted or async_pending files
                                with an unexpected name.
``object-updater.timing``       Timing data for object sweeps to flush async_pending
                                container updates.  Does not include object sweeps
                                which did not find an existing async_pending storage
                                directory.
``object-updater.quarantines``  Count of async_pending container updates which were
                                corrupted and moved to quarantine.
``object-updater.successes``    Count of successful container updates.
``object-updater.failures``     Count of failed container updates.
``object-updater.unlinks``      Count of async_pending files unlinked. An
                                async_pending file is unlinked either when it is
                                successfully processed or when the replicator sees
                                that there is a newer async_pending file for the
                                same object.
==============================  ====================================================
