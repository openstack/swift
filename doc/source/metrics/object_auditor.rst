``object-auditor`` Metrics
==========================

==============================  ====================================================
Metric Name                     Description
------------------------------  ----------------------------------------------------
``object-auditor.quarantines``  Count of objects failing audit and quarantined.
``object-auditor.errors``       Count of errors encountered while auditing objects.
``object-auditor.timing``       Timing data for each object audit (does not include
                                any rate-limiting sleep time for
                                max_files_per_second, but does include rate-limiting
                                sleep time for max_bytes_per_second).
==============================  ====================================================
