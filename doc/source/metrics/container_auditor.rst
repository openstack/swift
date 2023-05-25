``container-auditor`` Metrics
=============================

==============================  ====================================================
Metric Name                     Description
------------------------------  ----------------------------------------------------
``container-auditor.errors``    Incremented when an Exception is caught in an audit
                                pass (only once per pass, max).
``container-auditor.passes``    Count of individual containers passing an audit.
``container-auditor.failures``  Count of individual containers failing an audit.
``container-auditor.timing``    Timing data for each container audit.
==============================  ====================================================
