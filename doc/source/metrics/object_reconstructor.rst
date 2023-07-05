``object-reconstructor`` Metrics
================================

========================================================  ======================================================
Metric Name                                               Description
--------------------------------------------------------  ------------------------------------------------------
``object-reconstructor.partition.delete.count.<device>``  A count of partitions on <device> which were
                                                          reconstructed and synced to another node because they
                                                          didn't belong on this node. This metric is tracked
                                                          per-device to allow for "quiescence detection" for
                                                          object reconstruction activity on each device.
``object-reconstructor.partition.delete.timing``          Timing data for partitions reconstructed and synced to
                                                          another node because they didn't belong on this node.
                                                          This metric is not tracked per device.
``object-reconstructor.partition.update.count.<device>``  A count of partitions on <device> which were
                                                          reconstructed and synced to another node, but also
                                                          belong on this node. As with delete.count, this metric
                                                          is tracked per-device.
``object-reconstructor.partition.update.timing``          Timing data for partitions reconstructed which also
                                                          belong on this node. This metric is not tracked
                                                          per-device.
``object-reconstructor.suffix.hashes``                    Count of suffix directories whose hash (of filenames)
                                                          was recalculated.
``object-reconstructor.suffix.syncs``                     Count of suffix directories reconstructed with ssync.
========================================================  ======================================================
