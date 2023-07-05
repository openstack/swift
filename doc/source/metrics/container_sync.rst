``container-sync`` Metrics
==========================

=================================  ====================================================
Metric Name                        Description
---------------------------------  ----------------------------------------------------
``container-sync.skips``           Count of containers skipped because they don't have
                                   sync'ing enabled.
``container-sync.failures``        Count of failures sync'ing of individual containers.
``container-sync.syncs``           Count of individual containers sync'ed successfully.
``container-sync.deletes``         Count of container database rows sync'ed by
                                   deletion.
``container-sync.deletes.timing``  Timing data for each container database row
                                   synchronization via deletion.
``container-sync.puts``            Count of container database rows sync'ed by Putting.
``container-sync.puts.timing``     Timing data for each container database row
                                   synchronization via Putting.
=================================  ====================================================
