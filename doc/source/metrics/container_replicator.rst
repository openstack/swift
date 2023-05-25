``container-replicator`` Metrics
================================

=========================================  ====================================================
Metric Name                                Description
-----------------------------------------  ----------------------------------------------------
``container-replicator.diffs``             Count of syncs handled by sending differing rows.
``container-replicator.diff_caps``         Count of "diffs" operations which failed because
                                           "max_diffs" was hit.
``container-replicator.no_changes``        Count of containers found to be in sync.
``container-replicator.hashmatches``       Count of containers found to be in sync via hash
                                           comparison (``broker.merge_syncs`` was called).
``container-replicator.rsyncs``            Count of completely missing containers where were sent
                                           via rsync.
``container-replicator.remote_merges``     Count of syncs handled by sending entire database
                                           via rsync.
``container-replicator.attempts``          Count of database replication attempts.
``container-replicator.failures``          Count of database replication attempts which failed
                                           due to corruption (quarantined) or inability to read
                                           as well as attempts to individual nodes which
                                           failed.
``container-replicator.removes.<device>``  Count of databases deleted on <device> because the
                                           delete_timestamp was greater than the put_timestamp
                                           and the database had no rows or because it was
                                           successfully sync'ed to other locations and doesn't
                                           belong here anymore.
``container-replicator.successes``         Count of replication attempts to an individual node
                                           which were successful.
``container-replicator.timing``            Timing data for each database replication attempt
                                           not resulting in a failure.
=========================================  ====================================================
