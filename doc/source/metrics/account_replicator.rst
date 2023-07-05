``account-replicator`` Metrics
==============================

=======================================  ====================================================
Metric Name                              Description
---------------------------------------  ----------------------------------------------------
``account-replicator.diffs``             Count of syncs handled by sending differing rows.
``account-replicator.diff_caps``         Count of "diffs" operations which failed because
                                         "max_diffs" was hit.
``account-replicator.no_changes``        Count of accounts found to be in sync.
``account-replicator.hashmatches``       Count of accounts found to be in sync via hash
                                         comparison (``broker.merge_syncs`` was called).
``account-replicator.rsyncs``            Count of completely missing accounts which were sent
                                         via rsync.
``account-replicator.remote_merges``     Count of syncs handled by sending entire database
                                         via rsync.
``account-replicator.attempts``          Count of database replication attempts.
``account-replicator.failures``          Count of database replication attempts which failed
                                         due to corruption (quarantined) or inability to read
                                         as well as attempts to individual nodes which
                                         failed.
``account-replicator.removes.<device>``  Count of databases on <device> deleted because the
                                         delete_timestamp was greater than the put_timestamp
                                         and the database had no rows or because it was
                                         successfully sync'ed to other locations and doesn't
                                         belong here anymore.
``account-replicator.successes``         Count of replication attempts to an individual node
                                         which were successful.
``account-replicator.timing``            Timing data for each database replication attempt
                                         not resulting in a failure.
=======================================  ====================================================
