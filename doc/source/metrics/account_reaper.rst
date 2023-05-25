``account-reaper`` Metrics
==========================

================================================  ====================================================
Metric Name                                       Description
------------------------------------------------  ----------------------------------------------------
``account-reaper.errors``                         Count of devices failing the mount check.
``account-reaper.timing``                         Timing data for each reap_account() call.
``account-reaper.return_codes.X``                 Count of HTTP return codes from various operations
                                                  (e.g. object listing, container deletion, etc.). The
                                                  value for X is the first digit of the return code
                                                  (2 for 201, 4 for 404, etc.).
``account-reaper.containers_failures``            Count of failures to delete a container.
``account-reaper.containers_deleted``             Count of containers successfully deleted.
``account-reaper.containers_remaining``           Count of containers which failed to delete with
                                                  zero successes.
``account-reaper.containers_possibly_remaining``  Count of containers which failed to delete with
                                                  at least one success.
``account-reaper.objects_failures``               Count of failures to delete an object.
``account-reaper.objects_deleted``                Count of objects successfully deleted.
``account-reaper.objects_remaining``              Count of objects which failed to delete with zero
                                                  successes.
``account-reaper.objects_possibly_remaining``     Count of objects which failed to delete with at
                                                  least one success.
================================================  ====================================================
