``account-server`` Metrics
==========================

..note::
   "Not Found" is not considered an error and requests
   which increment ``errors`` are not included in the timing data.

==========================================  =======================================================
Metric Name                                 Description
------------------------------------------  -------------------------------------------------------
``account-server.DELETE.errors.timing``     Timing data for each DELETE request resulting in an
                                            error: bad request, not mounted, missing timestamp.
``account-server.DELETE.timing``            Timing data for each DELETE request not resulting in
                                            an error.
``account-server.PUT.errors.timing``        Timing data for each PUT request resulting in an error:
                                            bad request, not mounted, conflict, recently-deleted.
``account-server.PUT.timing``               Timing data for each PUT request not resulting in an
                                            error.
``account-server.HEAD.errors.timing``       Timing data for each HEAD request resulting in an
                                            error: bad request, not mounted.
``account-server.HEAD.timing``              Timing data for each HEAD request not resulting in
                                            an error.
``account-server.GET.errors.timing``        Timing data for each GET request resulting in an
                                            error: bad request, not mounted, bad delimiter,
                                            account listing limit too high, bad accept header.
``account-server.GET.timing``               Timing data for each GET request not resulting in
                                            an error.
``account-server.REPLICATE.errors.timing``  Timing data for each REPLICATE request resulting in an
                                            error: bad request, not mounted.
``account-server.REPLICATE.timing``         Timing data for each REPLICATE request not resulting
                                            in an error.
``account-server.POST.errors.timing``       Timing data for each POST request resulting in an
                                            error: bad request, bad or missing timestamp, not
                                            mounted.
``account-server.POST.timing``              Timing data for each POST request not resulting in
                                            an error.
==========================================  =======================================================
