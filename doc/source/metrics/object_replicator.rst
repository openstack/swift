``object-replicator`` Metrics
=============================

=====================================================  ====================================================
Metric Name                                            Description
-----------------------------------------------------  ----------------------------------------------------
``object-replicator.partition.delete.count.<device>``  A count of partitions on <device> which were
                                                       replicated to another node because they didn't
                                                       belong on this node.  This metric is tracked
                                                       per-device to allow for "quiescence detection" for
                                                       object replication activity on each device.
``object-replicator.partition.delete.timing``          Timing data for partitions replicated to another
                                                       node because they didn't belong on this node.  This
                                                       metric is not tracked per device.
``object-replicator.partition.update.count.<device>``  A count of partitions on <device> which were
                                                       replicated to another node, but also belong on this
                                                       node.  As with delete.count, this metric is tracked
                                                       per-device.
``object-replicator.partition.update.timing``          Timing data for partitions replicated which also
                                                       belong on this node.  This metric is not tracked
                                                       per-device.
``object-replicator.suffix.hashes``                    Count of suffix directories whose hash (of filenames)
                                                       was recalculated.
``object-replicator.suffix.syncs``                     Count of suffix directories replicated with rsync.
=====================================================  ====================================================
