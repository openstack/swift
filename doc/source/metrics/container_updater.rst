``container-updater`` Metrics
=============================

================================  ====================================================
Metric Name                       Description
--------------------------------  ----------------------------------------------------
``container-updater.successes``   Count of containers which successfully updated their
                                  account.
``container-updater.failures``    Count of containers which failed to update their
                                  account.
``container-updater.no_changes``  Count of containers which didn't need to update
                                  their account.
``container-updater.timing``      Timing data for processing a container; only
                                  includes timing for containers which needed to
                                  update their accounts (i.e. "successes" and
                                  "failures" but not "no_changes").
================================  ====================================================
